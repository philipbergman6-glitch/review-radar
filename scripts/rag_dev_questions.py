"""The ten development questions P7's prompt is written against (ADR-0006).

The thirty evaluation questions run **once**, after the prompt freezes, and never tune
anything. So the prompt needs somewhere else to be developed, and ADR-0006 says where: a
separate ten-question set on **pre-2020 windows of non-candidate products**. Pre-2020 keeps the
temporal holdout intact; non-candidate keeps the decline ranking's products -- the ones the
evaluation set asks about -- out of prompt development entirely.

The draw is mechanical and seeded, never chosen by hand:

* every product in the evaluation manifest, the decline ranking and the slot draw is excluded,
  so nothing the evaluation set touches can be developed against;
* the remaining products are those with at least `--min-reviews` reviews inside a pre-2020
  window, taken in a seeded order rather than by review count -- picking the busiest products
  would develop the prompt against unusually rich contexts;
* seven questions are instantiated from the frozen answerable templates and three from the
  frozen out-of-domain probes, so refusal behaviour is developed rather than hoped for.

These questions carry **no answer keys and no answerability verdict**. They are not scored and
never enter the evaluation table: no evidence scan ran over them, so calling one answerable
would be exactly the retriever-defined answerability ADR-0006 forbids. They exist to shake the
citation and scope contract out of the prompt before the one run that counts.

Run:  ./run.sh python scripts/rag_dev_questions.py   (or `make rag-dev-questions`)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import random
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ai import rag_questions as Q
from src.common.config import PROJECT_ROOT
from src.serving import projection as P

OUT_PATH = PROJECT_ROOT / "eval" / "rag" / "dev" / "questions.json"
MANIFEST = PROJECT_ROOT / "conf" / "rag-questions.json"
RANKING = PROJECT_ROOT / "eval" / "rag" / "decline-ranking.json"
SLOTS = PROJECT_ROOT / "eval" / "rag" / "slots.json"

#: The last month a development window may reach into. The 2020-01-01 temporal holdout
#: (ADR-0001) is the evaluation set's ground, and a prompt developed against it would be a
#: prompt developed against the holdout.
DEV_WINDOW_END = "2019-12"
PRODUCT_SCOPED = 4
TEMPORAL = 3
OUT_OF_DOMAIN = 3


def excluded_asins() -> set[str]:
    out: set[str] = set()
    manifest = json.loads(MANIFEST.read_text())
    out |= {q["parent_asin"] for q in manifest["questions"]}
    if RANKING.exists():
        out |= {e["candidate_asin"] for e in json.loads(RANKING.read_text())["ranking"]}
    if SLOTS.exists():
        out |= {s["parent_asin"] for s in json.loads(SLOTS.read_text())["slots"]}
    return out


def months_before(end: str, n: int) -> str:
    y, m = (int(x) for x in end.split("-"))
    total = y * 12 + (m - 1) - (n - 1)
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def candidate_products(es, alias: str, *, start: str, end: str, min_reviews: int,
                       exclude: set[str], size: int) -> list[dict[str, Any]]:
    """Products with at least `min_reviews` reviews inside one pre-2020 window.

    One `terms` aggregation with a `min_doc_count`, ordered by key so the bucket list itself is
    deterministic; the seeded shuffle downstream is what picks from it.
    """
    body = {"size": 0,
            "query": {"bool": {"filter": [
                {"range": {"review_month": {"gte": f"{start}-01", "lte": f"{end}-01",
                                            "format": "yyyy-MM-dd"}}}]}},
            "aggs": {"products": {"terms": {"field": "parent_asin", "size": size,
                                            "min_doc_count": min_reviews,
                                            "order": {"_key": "asc"}},
                                  "aggs": {"title": {"top_hits": {
                                      "size": 1, "_source": ["product_title"]}}}}}}
    resp = es.search(index=alias, **body)
    out = []
    for b in resp["aggregations"]["products"]["buckets"]:
        if b["key"] in exclude:
            continue
        hit = b["title"]["hits"]["hits"]
        title = (hit[0]["_source"].get("product_title") if hit else None) or b["key"]
        out.append({"parent_asin": b["key"], "reviews": b["doc_count"], "product_title": title})
    return out


def build(products: list[dict[str, Any]], spec: Q.QuestionSpec, *, scope_start: str,
          scope_end: str, baseline: tuple[str, str], recent: tuple[str, str]) -> list[dict[str, Any]]:
    qs: list[dict[str, Any]] = []
    for i, p in enumerate(products[:PRODUCT_SCOPED], start=1):
        qs.append({
            "question_id": f"dev-product_scoped-{i:02d}", "family": "product_scoped",
            "parent_asin": p["parent_asin"], "product_title": p["product_title"],
            "question": spec.templates["product_scoped"].format(
                product_title=p["product_title"], parent_asin=p["parent_asin"],
                scope_start=scope_start, scope_end=scope_end),
            "scope": {"windows": {"scope": {"start": scope_start, "end": scope_end}}},
            "retrieval": {"mode": "hybrid_filtered", "top_k": 10, "per_window_k": None},
            "slot_role": "filler", "answerability": "development_unscored"})
    for i, p in enumerate(products[PRODUCT_SCOPED:PRODUCT_SCOPED + TEMPORAL], start=1):
        qs.append({
            "question_id": f"dev-temporal-{i:02d}", "family": "temporal",
            "parent_asin": p["parent_asin"], "product_title": p["product_title"],
            "question": spec.templates["temporal"].format(
                product_title=p["product_title"], parent_asin=p["parent_asin"],
                baseline_start=baseline[0], baseline_end=baseline[1],
                recent_start=recent[0], recent_end=recent[1]),
            "scope": {"windows": {"baseline": {"start": baseline[0], "end": baseline[1]},
                                  "recent": {"start": recent[0], "end": recent[1]}}},
            "retrieval": {"mode": "hybrid_per_window", "top_k": 10, "per_window_k": 5},
            "slot_role": "filler", "answerability": "development_unscored"})
    for i, (p, probe) in enumerate(zip(products[:OUT_OF_DOMAIN], spec.out_of_domain,
                                       strict=False), start=1):
        qs.append({
            "question_id": f"dev-out_of_domain-{i:02d}", "family": "product_scoped",
            "parent_asin": p["parent_asin"], "product_title": p["product_title"],
            "probe": {"id": probe["id"], "label": probe["label"]},
            "question": spec.templates["out_of_domain"].format(
                product_title=p["product_title"], parent_asin=p["parent_asin"],
                probe_label=probe["label"], scope_start=scope_start, scope_end=scope_end),
            "scope": {"windows": {"scope": {"start": scope_start, "end": scope_end}}},
            "retrieval": {"mode": "hybrid_filtered", "top_k": 10, "per_window_k": None},
            "slot_role": "filler", "answerability": "development_unscored",
            "stratum": "out_of_domain"})
    return qs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--min-reviews", type=int, default=12)
    ap.add_argument("--window-months", type=int, default=12)
    ap.add_argument("--pool", type=int, default=400, help="how many product buckets to draw from")
    ap.add_argument("--out", default=str(OUT_PATH))
    args = ap.parse_args()

    spec = Q.load_spec()
    scope_end = DEV_WINDOW_END
    scope_start = months_before(scope_end, args.window_months)
    recent = (months_before(scope_end, args.window_months // 2), scope_end)
    baseline_end = months_before(recent[0], 2)[:7]
    baseline = (months_before(baseline_end, args.window_months // 2), baseline_end)

    es = P.client()
    alias = "reviews" if args.scope == "full" else f"reviews_{args.scope}"
    pool = candidate_products(es, alias, start=scope_start, end=scope_end,
                              min_reviews=args.min_reviews, exclude=excluded_asins(),
                              size=args.pool)
    need = PRODUCT_SCOPED + TEMPORAL
    if len(pool) < need:
        raise SystemExit(f"only {len(pool)} eligible products in {scope_start}..{scope_end} with "
                         f"{args.min_reviews}+ reviews; need {need}")
    rng = random.Random(spec.seed)
    rng.shuffle(pool)
    questions = build(pool, spec, scope_start=scope_start, scope_end=scope_end,
                      baseline=baseline, recent=recent)

    doc = {"question_set": "development", "status": "development",
           "questions_version": "1", "scope": args.scope,
           "spec_hash": spec.spec_hash, "seed": spec.seed,
           "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
           "window": {"scope": [scope_start, scope_end], "baseline": list(baseline),
                      "recent": list(recent)},
           "note": "Prompt development only (ADR-0006): pre-2020 windows of non-candidate "
                   "products, no answer keys, no answerability verdict, never scored and never "
                   "in the evaluation table.",
           "excluded_products": sorted(excluded_asins()),
           "questions": questions}
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    digest = hashlib.sha256(out.read_bytes()).hexdigest()
    print(f"RAG_DEV_QUESTIONS path={out.relative_to(PROJECT_ROOT)} questions={len(questions)} "
          f"pool={len(pool)} scope={scope_start}..{scope_end} "
          f"baseline={baseline[0]}..{baseline[1]} recent={recent[0]}..{recent[1]} "
          f"sha256={digest[:12]}")


if __name__ == "__main__":
    main()

"""Which ranked episodes could possibly carry a question, decided lexically and cheaply.

The problem this solves is a real property of the corpus, not a convenience. ADR-0006 needs
twenty answerable questions, each resting on **three manually validated complaint-bearing
reviews in every window it declares**. The top-ranked decline candidates are small products --
eleven to thirty reviews in a six-month window is typical -- on a corpus whose ratings are
J-shaped. Several of them simply do not carry three complaints in both windows, and the first
scan found exactly that: eight of the first ten products' twenty windows matched fewer than
three reviews *before* a human read a single one.

ADR-0006 already anticipates walking further down the ranking: it says *top five **eligible**
candidates*, and a candidate the evidence scan cannot confirm is not eligible. What it does not
do is say how to avoid hand-reading four hundred products to find out. So the walk is staged:

  **lexical pre-filter (here)** -- the frozen term lists over every window of every product in
  the ranking head. Cheap, mechanical, and *necessary but not sufficient*: it can only ever
  narrow the field, because manual validation removes reviews and never adds them. A window
  needs `MIN_LEXICAL_MATCHES` matches to stay in, twice the support the protocol requires, so a
  window that survives has headroom for validation to reject half of what it matched.

  **manual semantic validation (`scripts/rag_evidence_scan.py`, then a human)** -- runs on the
  ten products the pre-filter leaves, and is what actually decides answerability. If validation
  drops a product below three in either window, the draw descends the same ranking again.

The pre-filter never picks a product. It rules products out, in ranking order, on a stated
threshold fixed before the counts were looked at. Its output is `eval/rag/prefilter.json`, which
`scripts/rag_slots.py --eligible-from` consumes.

Run:  ./run.sh python scripts/rag_prefilter.py    (or `make rag-prefilter`)
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from pyspark.sql import functions as F

from src.ai.rag_questions import load_spec, scan_review, scan_terms
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import CATALOG, build

sys.stdout.reconfigure(line_buffering=True)

OUT_DIR = PROJECT_ROOT / "eval" / "rag"
RANKING_PATH = OUT_DIR / "decline-ranking.json"
PREFILTER_PATH = OUT_DIR / "prefilter.json"

#: Twice ADR-0006's three-review bar. Fixed before the counts were read, and stated here rather
#: than tuned: a threshold chosen after seeing which products it admits is a threshold chosen
#: for its answer.
MIN_LEXICAL_MATCHES = 6


def _table(name: str, scope: str) -> str:
    return f"{CATALOG}.{name}" + ("" if scope == "full" else f"_{scope}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=("full", "sample"))
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    spec = load_spec()
    ranking_doc = json.loads(RANKING_PATH.read_text())
    if ranking_doc["spec_hash"] != spec.spec_hash:
        raise RuntimeError("decline-ranking.json was produced under a different spec hash")
    ranked = ranking_doc["ranking"]
    t0 = time.time()

    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    if silver_run is None:
        raise RuntimeError(f"no successful silver run at scope={args.scope}")

    spark = build("rag-prefilter")
    try:
        ctl = [r.asDict() for r in spark.table(_table("gold.matched_controls", args.scope))
               .filter(F.col("episode_id").isin([e["episode_id"] for e in ranked])).collect()]
        controls: dict[str, list[dict[str, Any]]] = {}
        for r in ctl:
            controls.setdefault(r["episode_id"], []).append(r)

        # Every product in the ranking head -- candidates and all their matched controls --
        # observed over the candidate episode's own windows. A control is matched *on those
        # windows*, so scanning it on any other window would answer a different question.
        windows: set[tuple[str, str, str, str, str]] = set()
        for e in ranked:
            products = [e["candidate_asin"]] + [c["control_asin"] for c in controls.get(e["episode_id"], [])]
            for asin in products:
                windows.add((e["episode_id"], asin, "baseline", e["baseline_start"], e["baseline_end"]))
                windows.add((e["episode_id"], asin, "recent", e["recent_start"], e["recent_end"]))
        wanted = spark.createDataFrame(
            sorted(windows),
            "episode_id string, parent_asin string, window string, w_start string, w_end string")
        silver = (spark.table(_table("silver.reviews", args.scope))
                  .select("review_id", "parent_asin", "title", "text",
                          F.date_format("review_month", "yyyy-MM").alias("month")))
        rows = [r.asDict() for r in
                silver.join(F.broadcast(wanted), "parent_asin")
                .filter((F.col("month") >= F.col("w_start")) & (F.col("month") <= F.col("w_end")))
                .select("episode_id", "parent_asin", "window", "review_id", "title", "text")
                .collect()]
    finally:
        spark.stop()

    patterns = scan_terms(spec)
    counts: dict[str, dict[str, int]] = {}
    in_scope: dict[str, dict[str, int]] = {}
    for r in rows:
        key = f"{r['episode_id']}|{r['parent_asin']}"
        in_scope.setdefault(key, {}).setdefault(r["window"], 0)
        in_scope[key][r["window"]] += 1
        if scan_review(f"{r['title'] or ''} {r['text'] or ''}", patterns):
            counts.setdefault(key, {}).setdefault(r["window"], 0)
            counts[key][r["window"]] += 1

    per_product: dict[str, dict[str, Any]] = {}
    for key in sorted(set(in_scope) | set(counts)):
        c = counts.get(key, {})
        baseline, recent = c.get("baseline", 0), c.get("recent", 0)
        per_product[key] = {
            "episode_id": key.split("|")[0], "parent_asin": key.split("|")[1],
            "reviews_in_scope": in_scope.get(key, {}),
            "lexical_matches": {"baseline": baseline, "recent": recent},
            "passes_prefilter": min(baseline, recent) >= MIN_LEXICAL_MATCHES}
    eligible = sorted({v["parent_asin"] for v in per_product.values() if v["passes_prefilter"]})

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PREFILTER_PATH.write_text(json.dumps(
        {"spec_hash": spec.spec_hash, "scope": args.scope, "silver_run_id": silver_run["run_id"],
         "min_lexical_matches": MIN_LEXICAL_MATCHES,
         "rule": "necessary, never sufficient: a window needs this many lexically matched "
                 "reviews to be worth reading, and manual validation still decides",
         "episodes_considered": len(ranked), "products_considered": len(per_product),
         "eligible_products": eligible, "per_product": per_product},
        indent=2, sort_keys=True) + "\n")
    print(f"RAG_PREFILTER episodes={len(ranked)} products={len(per_product)} "
          f"eligible={len(eligible)} min_matches={MIN_LEXICAL_MATCHES} "
          f"elapsed_s={round(time.time() - t0, 1)} path={PREFILTER_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

"""Freeze the thirty P7 questions and their answer keys (ticket 11, ADR-0006, RR-17).

This is the last step of the question pipeline and the only one that produces something
immutable. It assembles `conf/rag-questions.json` from artefacts that already exist -- the slot
draw, the evidence scan, the validation record -- and it invents nothing: every supporting review
id here was surfaced by the frozen scan and judged complaint-bearing by a reader, and every
product was reached by walking the decline ranking in order.

**Why the freeze is a separate step with its own refusals.** The thirty questions run once, after
the prompt freezes, and they never tune anything. That only holds if the set cannot be edited
after an answer exists, so this script refuses to write when:

  * the ledger already holds a `rag_answers` run, or an answers file exists on disk -- questions
    written after answers are questions written to fit them;
  * `conf/rag-questions.json` is already frozen -- a second freeze is a rewrite;
  * any supporting review id is absent from the evaluated Elasticsearch index generation -- a key
    that cites evidence the retriever provably cannot reach would score `retrieval_miss` against
    the generator for the index's mistake;
  * `check_manifest` finds any violation -- 20/10, 10/10 per answerable family, the three
    unanswerable strata at 4/3/3, the same ten products in both answerable families, three
    supporting reviews per declared window, and the frozen forbidden-claims list on every key.

**The three unanswerable strata, and how each is filled deterministically.** Slot order is fixed
by the draw; probe order is fixed by the spec; nothing here is chosen by hand.

  `absent_attribute` (4) -- a subject a beauty shopper might plausibly ask about that this
      product's reviews in scope never mention. The first (product, probe) pair whose probe scan
      returned zero in *both* windows, each product and probe used once.
  `zero_review_scope` (3) -- a six-month window in which the product has no reviews at all.
      One per product first, so the stratum is not three questions about one product unless the
      corpus leaves no alternative.
  `out_of_domain` (3) -- a subject the corpus does not cover, probes taken in ascending
      corpus-wide hit count. The corpus count is recorded on each question: these terms are not
      literally absent from 694k reviews, so the refusal reason claims only what was checked.

**What the answer keys may and may not contain.** `required_propositions` is the minimum content
of a fully adequate answer, and for a temporal question it is one supported complaint from each
window plus an explicit qualitative contrast. `forbidden_claims` is ADR-0006's list, identical on
every key. Theme-shift *direction* is never written down here -- not in the question, not in the
key, not in the notes -- because it is diagnostic metadata hidden from the generator and the
judge, and the quantitative claim belongs to the gold table.

Run:  ./run.sh python scripts/rag_freeze_questions.py --dry-run   (build, check, draft only)
      ./run.sh python scripts/rag_freeze_questions.py --freeze    (or `make rag-freeze`)
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from typing import Any

from src.ai.rag_questions import (
    FORBIDDEN_CLAIMS,
    MANIFEST_PATH,
    UNANSWERABLE_STRATA,
    check_manifest,
    load_spec,
    question_id,
)
from src.common import config as C
from src.common import pg
from src.common.config import PROJECT_ROOT

sys.stdout.reconfigure(line_buffering=True)

OUT_DIR = PROJECT_ROOT / "eval" / "rag"
SLOTS_PATH = OUT_DIR / "slots.json"
VALIDATION_PATH = OUT_DIR / "validation.json"
VALIDATION_JSONL = OUT_DIR / "validation.jsonl"
PROBE_PATH = OUT_DIR / "probe-scan.json"
EMPTY_PATH = OUT_DIR / "empty-windows.json"
DRAFT_PATH = OUT_DIR / "questions-draft.json"
ANSWERS_GLOB = "answers*.json*"

#: ADR-0006's retrieval spec, carried on each question so the runner never has to infer it.
RETRIEVAL = {
    "product_scoped": {"mode": "hybrid_filtered", "top_k": 10, "per_window_k": None},
    "temporal": {"mode": "hybrid_per_window", "top_k": 10, "per_window_k": 5},
}

#: How many required propositions a product-scoped key may carry. Two, because the key states the
#: *minimum* content of an adequate answer: requiring every theme the scan validated would make
#: adequacy a recall test against a list the generator never sees.
MAX_PRODUCT_SCOPED_PROPOSITIONS = 2


def theme_name(theme_id: str) -> str:
    return theme_id.replace("_", " ")


def _supporting(rows: list[dict[str, Any]], window_label: str | None = None) -> list[dict[str, Any]]:
    return [{"review_id": r["review_id"], "window": window_label or r["window"],
             "month": r["month"], "themes": r["validated_themes"]}
            for r in rows if r["complaint_bearing"]]


def _top_themes(rows: list[dict[str, Any]], *, minimum: int) -> list[str]:
    counts = Counter(t for r in rows if r["complaint_bearing"] for t in r["validated_themes"])
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [t for t, n in ranked if n >= minimum]


def build_answerable(slots, validation_rows, spec) -> list[dict[str, Any]]:
    questions = []
    for i, slot in enumerate(slots, start=1):
        asin = slot["parent_asin"]
        mine = [r for r in validation_rows if r["parent_asin"] == asin]
        base = [r for r in mine if r["window"] == "baseline"]
        rec = [r for r in mine if r["window"] == "recent"]
        common = {"parent_asin": asin, "product_title": slot["product_title"],
                  "episode_id": slot["episode_id"], "decline_rank": slot["decline_rank"],
                  "slot_role": slot["slot_role"], "stratum": None,
                  "answerability": "answerable"}
        fmt = {"product_title": slot["product_title"], "parent_asin": asin,
               "scope_start": slot["baseline_start"], "scope_end": slot["recent_end"],
               "baseline_start": slot["baseline_start"], "baseline_end": slot["baseline_end"],
               "recent_start": slot["recent_start"], "recent_end": slot["recent_end"]}

        # --- product-scoped: one declared window spanning both halves of the episode ----
        scope_themes = _top_themes(mine, minimum=3) or _top_themes(mine, minimum=1)[:1]
        required = [(f"At least one complaint about {theme_name(t)}, cited to a review inside "
                     f"the declared window.")
                    for t in scope_themes[:MAX_PRODUCT_SCOPED_PROPOSITIONS]]
        questions.append({
            **common, "question_id": question_id("product_scoped", i),
            "family": "product_scoped",
            "question": spec.templates["product_scoped"].format(**fmt),
            "scope": {"windows": {"scope": {"start": slot["baseline_start"],
                                            "end": slot["recent_end"]}}},
            "retrieval": RETRIEVAL["product_scoped"],
            "evidence": {"examined": len(mine),
                         "validated": sum(1 for r in mine if r["complaint_bearing"]),
                         "validated_per_window": {"scope": len(_supporting(mine))}},
            "answer_key": {"required_propositions": required,
                           "acceptable_themes": _top_themes(mine, minimum=1),
                           "supporting_review_ids": _supporting(mine, window_label="scope"),
                           "forbidden_claims": list(FORBIDDEN_CLAIMS),
                           "expected_refusal_reason": None}})

        # --- temporal: two declared windows, contrasted by cited example only ----
        rec_theme = (_top_themes(rec, minimum=1) or [""])[0]
        base_theme = (_top_themes(base, minimum=1) or [""])[0]
        questions.append({
            **common, "question_id": question_id("temporal", i),
            "family": "temporal",
            "question": spec.templates["temporal"].format(**fmt),
            "scope": {"windows": {"baseline": {"start": slot["baseline_start"],
                                               "end": slot["baseline_end"]},
                                  "recent": {"start": slot["recent_start"],
                                             "end": slot["recent_end"]}}},
            "retrieval": RETRIEVAL["temporal"],
            "evidence": {"examined": len(mine),
                         "validated": sum(1 for r in mine if r["complaint_bearing"]),
                         "validated_per_window": {"baseline": len(_supporting(base)),
                                                  "recent": len(_supporting(rec))}},
            "answer_key": {
                "required_propositions": [
                    (f"At least one complaint drawn from the recent window, cited to a review "
                     f"dated inside it -- {theme_name(rec_theme)} is one such complaint."),
                    (f"At least one complaint drawn from the baseline window, cited to a review "
                     f"dated inside it -- {theme_name(base_theme)} is one such complaint."),
                    ("An explicit qualitative contrast between the cited examples of the two "
                     "windows, stated without any claim about how often either complaint occurs "
                     "or about which direction anything moved.")],
                "acceptable_themes": _top_themes(mine, minimum=1),
                "supporting_review_ids": _supporting(base) + _supporting(rec),
                "forbidden_claims": list(FORBIDDEN_CLAIMS),
                "expected_refusal_reason": None}})
    return questions


def build_unanswerable(slots, probes, empties, spec) -> list[dict[str, Any]]:
    questions: list[dict[str, Any]] = []
    hits = probes["in_scope_hits"]

    def probe_hits(asin: str, probe_id: str) -> int:
        return sum(hits.get(f"{asin}|{w}", {}).get(probe_id, 0) for w in ("baseline", "recent"))

    # -- absent attribute: first zero-hit (product, probe) pair, each used once --------
    used_products: set[str] = set()
    used_probes: set[str] = set()
    for slot in slots:
        if len(used_products) >= UNANSWERABLE_STRATA["absent_attribute"]:
            break
        for probe in spec.absent_attributes:
            if probe["id"] in used_probes or probe_hits(slot["parent_asin"], probe["id"]) > 0:
                continue
            fmt = {"product_title": slot["product_title"], "parent_asin": slot["parent_asin"],
                   "scope_start": slot["baseline_start"], "scope_end": slot["recent_end"],
                   "probe_label": probe["label"]}
            questions.append({
                "question_id": question_id("absent_attribute", len(used_products) + 1),
                "family": "product_scoped", "stratum": "absent_attribute",
                "answerability": "unanswerable", "slot_role": slot["slot_role"],
                "parent_asin": slot["parent_asin"], "product_title": slot["product_title"],
                "episode_id": slot["episode_id"], "decline_rank": slot["decline_rank"],
                "question": spec.templates["absent_attribute"].format(**fmt),
                "scope": {"windows": {"scope": {"start": slot["baseline_start"],
                                                "end": slot["recent_end"]}}},
                "retrieval": RETRIEVAL["product_scoped"],
                "probe": {"id": probe["id"], "label": probe["label"], "in_scope_hits": 0},
                "evidence": {"scan_hits_in_scope": 0, "validated": 0},
                "answer_key": {"required_propositions": [], "acceptable_themes": [],
                               "supporting_review_ids": [],
                               "forbidden_claims": list(FORBIDDEN_CLAIMS),
                               "expected_refusal_reason":
                                   spec.refusal_reasons["absent_attribute"].format(**fmt)}})
            used_products.add(slot["parent_asin"])
            used_probes.add(probe["id"])
            break

    # -- zero-review scope: one window per product first, then a second from the same ----
    by_product: dict[str, list[dict[str, Any]]] = {}
    for w in empties["windows"]:
        by_product.setdefault(w["parent_asin"], []).append(w)
    order = [s for s in slots if s["parent_asin"] in by_product]
    picked: list[tuple[dict[str, Any], dict[str, Any]]] = []
    depth = 0
    while len(picked) < UNANSWERABLE_STRATA["zero_review_scope"]:
        progressed = False
        for slot in order:
            windows = by_product[slot["parent_asin"]]
            if depth < len(windows) and len(picked) < UNANSWERABLE_STRATA["zero_review_scope"]:
                picked.append((slot, windows[depth]))
                progressed = True
        if not progressed:
            raise SystemExit("not enough zero-review windows among the slot products to fill the "
                             "stratum; the fallback in ADR-0006 applies and must be recorded")
        depth += 1
    for n, (slot, window) in enumerate(picked, start=1):
        fmt = {"product_title": slot["product_title"], "parent_asin": slot["parent_asin"],
               "scope_start": window["start"], "scope_end": window["end"]}
        questions.append({
            "question_id": question_id("zero_review_scope", n),
            "family": "product_scoped", "stratum": "zero_review_scope",
            "answerability": "unanswerable", "slot_role": slot["slot_role"],
            "parent_asin": slot["parent_asin"], "product_title": slot["product_title"],
            "episode_id": slot["episode_id"], "decline_rank": slot["decline_rank"],
            "question": spec.templates["zero_review_scope"].format(**fmt),
            "scope": {"windows": {"scope": {"start": window["start"], "end": window["end"]}}},
            "retrieval": RETRIEVAL["product_scoped"],
            "evidence": {"reviews_in_scope": 0,
                         "product_lifetime": [window["lifetime_start"], window["lifetime_end"]],
                         "validated": 0},
            "answer_key": {"required_propositions": [], "acceptable_themes": [],
                           "supporting_review_ids": [],
                           "forbidden_claims": list(FORBIDDEN_CLAIMS),
                           "expected_refusal_reason":
                               spec.refusal_reasons["zero_review_scope"].format(**fmt)}})

    # -- out of domain: rarest probes corpus-wide, on products the other strata did not use --
    corpus = probes["out_of_domain_corpus_hits"]
    ranked = sorted(spec.out_of_domain, key=lambda p: (corpus.get(p["id"], 0), p["id"]))
    spare = [s for s in slots if s["parent_asin"] not in used_products] or list(slots)
    for n, probe in enumerate(ranked[:UNANSWERABLE_STRATA["out_of_domain"]], start=1):
        slot = spare[(n - 1) % len(spare)]
        if probe_hits(slot["parent_asin"], probe["id"]) > 0:
            raise SystemExit(f"{probe['id']} is not absent from {slot['parent_asin']}'s scope")
        fmt = {"product_title": slot["product_title"], "parent_asin": slot["parent_asin"],
               "scope_start": slot["baseline_start"], "scope_end": slot["recent_end"],
               "probe_label": probe["label"]}
        questions.append({
            "question_id": question_id("out_of_domain", n),
            "family": "product_scoped", "stratum": "out_of_domain",
            "answerability": "unanswerable", "slot_role": slot["slot_role"],
            "parent_asin": slot["parent_asin"], "product_title": slot["product_title"],
            "episode_id": slot["episode_id"], "decline_rank": slot["decline_rank"],
            "question": spec.templates["out_of_domain"].format(**fmt),
            "scope": {"windows": {"scope": {"start": slot["baseline_start"],
                                            "end": slot["recent_end"]}}},
            "retrieval": RETRIEVAL["product_scoped"],
            "probe": {"id": probe["id"], "label": probe["label"], "in_scope_hits": 0,
                      "corpus_hits": corpus.get(probe["id"], 0),
                      "corpus_reviews": probes["corpus_reviews"]},
            "evidence": {"scan_hits_in_scope": 0, "validated": 0},
            "answer_key": {"required_propositions": [], "acceptable_themes": [],
                           "supporting_review_ids": [],
                           "forbidden_claims": list(FORBIDDEN_CLAIMS),
                           "expected_refusal_reason":
                               spec.refusal_reasons["out_of_domain"].format(**fmt)}})
    return questions


def assert_no_answers_yet() -> None:
    with pg.connect() as conn:
        n = conn.execute("SELECT count(*) FROM pipeline_runs WHERE job_name='rag_answers'"
                         ).fetchone()[0]
    if n:
        raise SystemExit(f"the ledger already holds {n} rag_answers run(s): the question set "
                         f"cannot be frozen after answers exist")
    existing = sorted(OUT_DIR.glob(ANSWERS_GLOB))
    if existing:
        raise SystemExit(f"answers already on disk ({[p.name for p in existing]}): "
                         f"freezing now would fit the questions to them")


def assert_supporting_ids_indexed(questions: list[dict[str, Any]]) -> dict[str, Any]:
    from src.serving.projection import client
    es = client()
    ids = sorted({s["review_id"] for q in questions
                  for s in q["answer_key"]["supporting_review_ids"]})
    alias = "reviews"
    found = set()
    for i in range(0, len(ids), 500):
        chunk = ids[i:i + 500]
        res = es.mget(index=alias, body={"ids": chunk}, _source=False)
        found.update(d["_id"] for d in res["docs"] if d.get("found"))
    missing = sorted(set(ids) - found)
    if missing:
        raise SystemExit(f"{len(missing)} supporting review ids are absent from the `{alias}` "
                         f"index; a key cannot cite evidence the retriever cannot reach: "
                         f"{missing[:5]}")
    generation = sorted(es.indices.get_alias(name=alias).keys())
    return {"alias": alias, "generations": generation, "supporting_ids_checked": len(ids)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true",
                       help="build and check; writes the draft to eval/rag/questions-draft.json "
                            "for inspection and never touches conf/rag-questions.json")
    group.add_argument("--freeze", action="store_true", help="write conf/rag-questions.json")
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    spec = load_spec()
    slots_doc = json.loads(SLOTS_PATH.read_text())
    validation = json.loads(VALIDATION_PATH.read_text())
    probes = json.loads(PROBE_PATH.read_text())
    empties = json.loads(EMPTY_PATH.read_text())
    for name, doc in (("slots", slots_doc), ("validation", validation), ("probe scan", probes),
                      ("empty windows", empties)):
        if doc["spec_hash"] != spec.spec_hash:
            raise SystemExit(f"the {name} artefact was produced under a different spec hash; "
                             f"re-run the question pipeline from scripts/rag_slots.py")

    slots = slots_doc["slots"]
    confirmed = set(validation["confirmed_products"])
    unconfirmed = [s["parent_asin"] for s in slots if s["parent_asin"] not in confirmed]
    if unconfirmed:
        raise SystemExit(f"manual validation has not confirmed {unconfirmed}; re-draw with "
                         f"--reject-from before freezing")

    rows = [json.loads(line) for line in VALIDATION_JSONL.read_text().splitlines() if line.strip()]
    questions = build_answerable(slots, rows, spec) + build_unanswerable(slots, probes, empties, spec)

    violations = check_manifest({"questions": questions})
    if violations:
        for v in violations:
            print(f"RAG_QUESTIONS_VIOLATION {v}")
        raise SystemExit(f"{len(violations)} manifest violation(s); nothing written")

    if args.freeze and MANIFEST_PATH.exists():
        current = json.loads(MANIFEST_PATH.read_text())
        if current.get("status") == "frozen":
            raise SystemExit("conf/rag-questions.json is already frozen; a second freeze is a "
                             "rewrite, and the thirty questions run once")

    index_note = None
    if args.freeze:
        assert_no_answers_yet()
        index_note = assert_supporting_ids_indexed(questions)

    manifest = {
        "questions_version": "1",
        "status": "frozen" if args.freeze else "draft",
        "decided_in": "ticket 11 (ADR-0006, RR-17)",
        "spec_version": spec.version, "spec_hash": spec.spec_hash,
        "category": args.category, "scope": slots_doc["scope"],
        "provenance": {
            "gold_run_id": slots_doc["gold_run_id"], "silver_run_id": slots_doc["silver_run_id"],
            "theme_samples_run_id": slots_doc["theme_samples_run_id"],
            "slots": "eval/rag/slots.json", "ranking": "eval/rag/decline-ranking.json",
            "prefilter": "eval/rag/prefilter.json", "scan": "eval/rag/evidence-scan.json",
            "worklist": "eval/rag/scan-worklist.jsonl",
            "validation": "eval/rag/validation.json",
            "walked_past": "eval/rag/walked-past.json",
            "elasticsearch": index_note},
        "annotator": validation["annotator"],
        "annotator_note": validation["annotator_note"],
        "note": "Theme-shift direction is deliberately absent from every question and every key: "
                "it is diagnostic metadata hidden from the generator and the judge, and the "
                "quantitative claim about it lives in the gold table, not in a RAG answer.",
        "questions": questions}

    counts = Counter((q["answerability"], q.get("stratum") or q["family"]) for q in questions)
    if args.freeze:
        MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    else:
        DRAFT_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"RAG_QUESTIONS n={len(questions)} "
          f"answerable={sum(1 for q in questions if q['answerability'] == 'answerable')} "
          f"unanswerable={sum(1 for q in questions if q['answerability'] == 'unanswerable')} "
          f"products={len({q['parent_asin'] for q in questions})} "
          f"status={manifest['status']} spec_hash={spec.spec_hash[:12]} "
          f"path={MANIFEST_PATH.relative_to(PROJECT_ROOT)}")
    for (answerability, group), n in sorted(counts.items()):
        print(f"RAG_QUESTIONS_GROUP {answerability}/{group}={n}")
    if index_note:
        print(f"RAG_QUESTIONS_INDEX alias={index_note['alias']} "
              f"generations={','.join(index_note['generations'])} "
              f"supporting_ids_checked={index_note['supporting_ids_checked']} all_present=true")


if __name__ == "__main__":
    main()

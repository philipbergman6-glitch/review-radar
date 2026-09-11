"""The evidence scan: what the corpus can and cannot answer, decided before P7 exists.

ADR-0006 fixes how a P7 question's answerability is decided, and the rule is a negative one:
**never by what the retriever returned.** A question called answerable because the retriever
found something would grade the retriever against a test the retriever wrote. So answerability
is decided here, against silver, by a two-step *evidence scan*:

  1. **Lexical candidate discovery** -- a frozen per-question term list (`conf/rag-question-spec.json`)
     run over *every* silver review in scope. Generous by design: it is looking for reviews
     worth reading, not for evidence.
  2. **Manual semantic validation** -- a human reading of what step 1 matched. Regex alone
     proves nothing: "doesn't leak, unlike the last one I bought" matches the leak terms and is
     praise, and ADR-0003's taxonomy counts praise as no complaint at all.

This script does step 1 and lays out step 2's worklist. It writes:

  eval/rag/evidence-scan.json        the census: per product, per window, how many reviews are
                                     in scope, how many the terms matched, and per theme
  eval/rag/scan-worklist.jsonl       the reviews to read, in a seeded order, with their text
  eval/rag/probe-scan.json           the unanswerable probes: per (product, probe) match counts,
                                     plus every out-of-domain probe's corpus-wide count
  eval/rag/empty-windows.json        six-month windows in which a slot product has zero reviews

The worklist order is seeded, not rating-sorted. Reading the one-star reviews first would find
supporting evidence faster and would make every validated set a sample of the angriest reviews
in the window -- which is exactly the shape of thing the answer keys forbid claiming. The
questions need *existence* of complaint-bearing evidence, so an unbiased order costs nothing
and keeps the record honest.

Run:  ./run.sh python scripts/rag_evidence_scan.py     (or `make rag-scan`)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from typing import Any

from pyspark.sql import functions as F

from src.ai.rag_questions import (
    compile_terms,
    load_spec,
    probe_regex,
    scan_review,
    scan_terms,
)
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import CATALOG, build

sys.stdout.reconfigure(line_buffering=True)

OUT_DIR = PROJECT_ROOT / "eval" / "rag"
SLOTS_PATH = OUT_DIR / "slots.json"
CENSUS_PATH = OUT_DIR / "evidence-scan.json"
WORKLIST_PATH = OUT_DIR / "scan-worklist.jsonl"
PROBE_PATH = OUT_DIR / "probe-scan.json"
EMPTY_PATH = OUT_DIR / "empty-windows.json"
WALKED_PAST_PATH = OUT_DIR / "walked-past.json"

#: How many matched reviews go on the worklist per window. Twelve is four times the support a
#: window needs, so a window that cannot clear the bar from twelve reads is a window whose
#: matches are mostly praise -- which is a finding about the window, not a reason to read on.
WORKLIST_PER_WINDOW = 12

#: Length of the zero-review windows searched for the `zero_review_scope` stratum. Six months
#: is the decline rule's own window length, so an empty window is the same unit of time every
#: other question is scoped by.
EMPTY_WINDOW_MONTHS = 6


def _table(name: str, scope: str) -> str:
    return f"{CATALOG}.{name}" + ("" if scope == "full" else f"_{scope}")


def month_range(start: str, months: int) -> tuple[str, str]:
    y, m = int(start[:4]), int(start[5:7])
    end_index = (y * 12 + (m - 1)) + months - 1
    return start, f"{end_index // 12:04d}-{end_index % 12 + 1:02d}"


def add_months(month: str, n: int) -> str:
    y, m = int(month[:4]), int(month[5:7])
    i = y * 12 + (m - 1) + n
    return f"{i // 12:04d}-{i % 12 + 1:02d}"


def worklist_key(seed: int, review_id: str) -> str:
    return hashlib.sha256(f"{seed}:worklist:{review_id}".encode()).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=("full", "sample"))
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    spec = load_spec()
    slots_doc = json.loads(SLOTS_PATH.read_text())
    if slots_doc["spec_hash"] != spec.spec_hash:
        raise RuntimeError("slots.json was drawn under a different spec hash; redraw the slots")
    slots = slots_doc["slots"]
    t0 = time.time()

    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    if silver_run is None:
        raise RuntimeError(f"no successful silver run at scope={args.scope}")

    # (product, window_name, start, end) -- the windows every scan reads. Product-scoped
    # questions span both, so scanning the two halves and taking the union is the same evidence
    # with the window recorded, which citation checking needs anyway.
    # The current slots, plus every product an earlier draw reached and walked past. Reading
    # those again costs one join and keeps the validation census complete: the record of why the
    # walk descended is the reading that rejected them.
    scanned = list(slots)
    if WALKED_PAST_PATH.exists():
        scanned += json.loads(WALKED_PAST_PATH.read_text())["products"]
    # De-duplicated: the walked-past record and the current slots overlap by construction, and a
    # window listed twice joins every review in it twice, which would double the worklist and
    # every count drawn from it.
    windows = sorted({w for s in scanned for w in (
        (s["parent_asin"], "baseline", s["baseline_start"], s["baseline_end"]),
        (s["parent_asin"], "recent", s["recent_start"], s["recent_end"]))})

    spark = build("rag-evidence-scan")
    try:
        wanted = spark.createDataFrame(windows,
                                       "parent_asin string, window string, w_start string, w_end string")
        silver = (spark.table(_table("silver.reviews", args.scope))
                  .select("review_id", "parent_asin", "rating", "title", "text",
                          "helpful_vote", "verified_purchase",
                          F.date_format("review_month", "yyyy-MM").alias("month")))
        in_scope = (silver.join(F.broadcast(wanted), "parent_asin")
                    .filter((F.col("month") >= F.col("w_start")) & (F.col("month") <= F.col("w_end")))
                    .drop("w_start", "w_end"))
        rows = [r.asDict() for r in in_scope.collect()]

        # Every month a slot product has any review at all, for the empty-window search.
        months = (silver.filter(F.col("parent_asin").isin([s["parent_asin"] for s in slots]))
                  .groupBy("parent_asin", "month").count().collect())

        # Out-of-domain probes are checked against the whole corpus, not just the ten products:
        # "this corpus does not discuss mortgages" is a stronger and more honest claim than
        # "this window does not", and it is the claim the refusal reason actually makes. Run in
        # Spark, because collecting 694k reviews to the driver to prove they say nothing is a
        # great deal of network for a zero.
        blob = silver.select(F.lower(F.concat_ws(" ", F.coalesce(F.col("title"), F.lit("")),
                                                 F.coalesce(F.col("text"), F.lit("")))).alias("t"))
        corpus_reviews = blob.count()
        ood_corpus = {p["id"]: blob.filter(F.col("t").rlike(probe_regex(p["terms"]))).count()
                      for p in spec.out_of_domain}
    finally:
        spark.stop()

    theme_patterns = scan_terms(spec)
    compiled_probes = {p["id"]: compile_terms({p["id"]: p["terms"]})
                       for p in [*spec.absent_attributes, *spec.out_of_domain]}

    census: dict[str, Any] = {}
    worklist: list[dict[str, Any]] = []
    probe_hits: dict[str, dict[str, int]] = {}

    by_window: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        by_window.setdefault((r["parent_asin"], r["window"]), []).append(r)

    for (asin, window), rs in sorted(by_window.items()):
        matched = []
        theme_counts: dict[str, int] = {}
        for r in rs:
            text = f"{r['title'] or ''} {r['text'] or ''}"
            hits = scan_review(text, theme_patterns)
            if hits:
                matched.append((r, hits))
                for theme in hits:
                    theme_counts[theme] = theme_counts.get(theme, 0) + 1
            for pid, pats in compiled_probes.items():
                if scan_review(text, pats):
                    probe_hits.setdefault(f"{asin}|{window}", {})
                    probe_hits[f"{asin}|{window}"][pid] = \
                        probe_hits[f"{asin}|{window}"].get(pid, 0) + 1
        census[f"{asin}|{window}"] = {"parent_asin": asin, "window": window,
                                      "reviews_in_scope": len(rs), "matched": len(matched),
                                      "matched_per_theme": dict(sorted(theme_counts.items()))}
        matched.sort(key=lambda pair: worklist_key(spec.seed, pair[0]["review_id"]))
        for rank, (r, hits) in enumerate(matched[:WORKLIST_PER_WINDOW], start=1):
            worklist.append({"parent_asin": asin, "window": window, "worklist_rank": rank,
                             "review_id": r["review_id"], "month": r["month"],
                             "rating": r["rating"], "verified_purchase": r["verified_purchase"],
                             "helpful_vote": r["helpful_vote"],
                             "matched_themes": sorted(hits), "matched_terms": hits,
                             "title": r["title"], "text": r["text"]})

    # Empty windows: six-month spans inside the product's own observed lifetime with no reviews.
    present: dict[str, set[str]] = {}
    for r in months:
        present.setdefault(r["parent_asin"], set()).add(r["month"])
    empty: list[dict[str, Any]] = []
    for asin, ms in sorted(present.items()):
        lo, hi = min(ms), max(ms)
        cur = lo
        while cur <= hi:
            start, end = month_range(cur, EMPTY_WINDOW_MONTHS)
            if end > hi:
                break
            span = {add_months(start, i) for i in range(EMPTY_WINDOW_MONTHS)}
            if not (span & ms):
                empty.append({"parent_asin": asin, "start": start, "end": end,
                              "lifetime_start": lo, "lifetime_end": hi})
                cur = add_months(end, 1)
                continue
            cur = add_months(cur, 1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CENSUS_PATH.write_text(json.dumps(
        {"spec_hash": spec.spec_hash, "scope": args.scope, "silver_run_id": silver_run["run_id"],
         "worklist_per_window": WORKLIST_PER_WINDOW,
         "worklist_order": "sha256(seed:worklist:review_id) ascending -- unbiased by rating",
         "windows": dict(sorted(census.items()))}, indent=2, sort_keys=True) + "\n")
    with WORKLIST_PATH.open("w") as f:
        for row in worklist:
            f.write(json.dumps(row, sort_keys=True) + "\n")
    PROBE_PATH.write_text(json.dumps(
        {"spec_hash": spec.spec_hash, "scope": args.scope, "silver_run_id": silver_run["run_id"],
         "in_scope_hits": dict(sorted(probe_hits.items())),
         "out_of_domain_corpus_hits": dict(sorted(ood_corpus.items())),
         "corpus_reviews": corpus_reviews}, indent=2, sort_keys=True) + "\n")
    EMPTY_PATH.write_text(json.dumps(
        {"spec_hash": spec.spec_hash, "scope": args.scope, "silver_run_id": silver_run["run_id"],
         "window_months": EMPTY_WINDOW_MONTHS, "windows": empty}, indent=2, sort_keys=True) + "\n")

    total_scope = sum(c["reviews_in_scope"] for c in census.values())
    total_matched = sum(c["matched"] for c in census.values())
    thin = [k for k, c in census.items() if c["matched"] < 3]
    print(f"RAG_SCAN windows={len(census)} reviews_in_scope={total_scope} matched={total_matched} "
          f"worklist={len(worklist)} windows_below_three_matches={len(thin)} "
          f"empty_windows={len(empty)} elapsed_s={round(time.time() - t0, 1)}")
    for pid, n in sorted(ood_corpus.items()):
        print(f"RAG_SCAN_OOD probe={pid} corpus_hits={n} corpus_reviews={corpus_reviews}")
    if thin:
        print(f"RAG_SCAN_THIN {' '.join(sorted(thin))}")



if __name__ == "__main__":
    main()

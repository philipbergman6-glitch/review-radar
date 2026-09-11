"""Step two of the evidence scan: the manual reading, expanded into a checkable record.

`scripts/rag_evidence_scan.py` puts reviews on a worklist. A human reads them and records, in
`eval/rag/validation-decisions.json`, which worklist ranks are genuinely complaint-bearing and
which themes each one evidences. This script joins that judgement back to the worklist and
writes the record the freeze reads:

  eval/rag/validation.jsonl   one row per examined review -- id, window, the decision, the
                              themes, and the quote's rating and month
  eval/rag/validation.json    the verdict per product per window, and which products are
                              confirmed answerable

Two things it refuses to do, both of which would quietly break the protocol:

* **It will not accept a decision for a review that is not on the worklist.** A supporting id
  that no scan surfaced is an id chosen by hand, and hand-chosen evidence is how a question set
  drifts toward what the system happens to do well.
* **It records negatives explicitly.** A worklist row the reader judged *not* complaint-bearing
  is written out as `complaint_bearing: false`, so the record shows what was read and rejected,
  not only what survived. Of the 212 reviews the scan put on the worklist, 89 turned out to be
  praise or a neutral mention; a census that hid them would make the lexical scan look far more
  precise than it is, and precision is exactly what it does not have.

Answerability follows ADR-0006 exactly: three validated complaint-bearing reviews in every
window a question declares, and the 1-2 band excluded from the set entirely.

Run:  ./run.sh python scripts/rag_validate.py     (or `make rag-validate`)
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from src.ai.rag_questions import ANSWERABLE_MIN_SUPPORT, answerability, load_spec
from src.common.config import PROJECT_ROOT

sys.stdout.reconfigure(line_buffering=True)

OUT_DIR = PROJECT_ROOT / "eval" / "rag"
WORKLIST_PATH = OUT_DIR / "scan-worklist.jsonl"
DECISIONS_PATH = OUT_DIR / "validation-decisions.json"
VALIDATION_JSONL = OUT_DIR / "validation.jsonl"
VALIDATION_PATH = OUT_DIR / "validation.json"

WINDOWS = ("baseline", "recent")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.parse_args()

    spec = load_spec()
    worklist = [json.loads(line) for line in WORKLIST_PATH.read_text().splitlines() if line.strip()]
    decisions_doc = json.loads(DECISIONS_PATH.read_text())
    decisions = decisions_doc["validated"]
    taxonomy_themes = set(spec.scan_terms)

    by_key: dict[str, dict[int, dict[str, Any]]] = {}
    for row in worklist:
        by_key.setdefault(f"{row['parent_asin']}|{row['window']}", {})[row["worklist_rank"]] = row

    # A decision for a product the current scan has not reached is *pending*, not an error: the
    # walk down the ranking reaches products one draw at a time, and the reading that rejected an
    # earlier product is recorded before the draw that replaces it has run. A decision for a rank
    # a scanned product never offered is still a hard failure -- that is hand-picked evidence.
    pending = sorted(k for k in decisions if k not in by_key)
    for key, ranks in decisions.items():
        if key not in by_key:
            continue
        for rank, themes in ranks.items():
            if int(rank) not in by_key[key]:
                raise SystemExit(f"{key}: decision for worklist rank {rank}, which was never "
                                 f"offered -- evidence chosen by hand is not evidence")
            bad = set(themes) - taxonomy_themes
            if bad:
                raise SystemExit(f"{key} rank {rank}: {sorted(bad)} are not taxonomy themes")

    rows: list[dict[str, Any]] = []
    for key, ranks in sorted(by_key.items()):
        chosen = decisions.get(key, {})
        for rank, row in sorted(ranks.items()):
            themes = chosen.get(str(rank), [])
            rows.append({"parent_asin": row["parent_asin"], "window": row["window"],
                         "worklist_rank": rank, "review_id": row["review_id"],
                         "month": row["month"], "rating": row["rating"],
                         "matched_themes": row["matched_themes"],
                         "complaint_bearing": bool(themes),
                         "validated_themes": sorted(themes)})

    per_product: dict[str, Any] = {}
    for asin in sorted({r["parent_asin"] for r in rows}):
        mine = [r for r in rows if r["parent_asin"] == asin]
        verdict = answerability(mine, windows=list(WINDOWS))
        per_product[asin] = {
            **verdict,
            "examined_per_window": {w: sum(1 for r in mine if r["window"] == w) for w in WINDOWS},
            # A product is usable for the temporal family only if *both* windows clear the bar;
            # the product-scoped family spans both windows, so it clears whenever either does,
            # but the two families must cover the same ten products, so both-windows is the rule.
            "confirmed": verdict["verdict"] == "answerable"}

    confirmed = sorted(a for a, v in per_product.items() if v["confirmed"])
    rejected = sorted(a for a, v in per_product.items() if not v["confirmed"])

    with VALIDATION_JSONL.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, sort_keys=True) + "\n")
    VALIDATION_PATH.write_text(json.dumps(
        {"spec_hash": spec.spec_hash, "annotator": decisions_doc["annotator"],
         "annotator_note": decisions_doc["annotator_note"],
         "min_support_per_window": ANSWERABLE_MIN_SUPPORT,
         "examined": len(rows),
         "complaint_bearing": sum(1 for r in rows if r["complaint_bearing"]),
         "confirmed_products": confirmed, "rejected_products": rejected,
         "pending_decisions": pending,
         "per_product": per_product}, indent=2, sort_keys=True) + "\n")

    kept = sum(1 for r in rows if r["complaint_bearing"])
    print(f"RAG_VALIDATION examined={len(rows)} complaint_bearing={kept} "
          f"rejected_as_praise_or_neutral={len(rows) - kept} products={len(per_product)} "
          f"confirmed={len(confirmed)} rejected={len(rejected)} pending={len(pending)}")
    for asin in rejected:
        v = per_product[asin]
        print(f"RAG_VALIDATION_REJECT product={asin} verdict={v['verdict']} "
              f"support={v['validated_per_window']} needs={ANSWERABLE_MIN_SUPPORT}")


if __name__ == "__main__":
    main()

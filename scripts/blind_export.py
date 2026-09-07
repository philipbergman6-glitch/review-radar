"""Export one frame's reviews for blind labelling (RR-21, ADR-0003 amendment (c)).

RR-21 makes the in-session agent the author of P6's ground truth and requires the labelling
to be *blind*: only review title and text, never `qwen3:8b`'s output, the star rating, the
product, the window, or an earlier label for the same review. A run with access to the
system under test is void and gets discarded, not patched.

Blindness is enforced by the file layout rather than by intent. This script writes two files:

  eval/themes/blind-<sample>.jsonl      blind_id, title, text -- the only file the labeller opens
  eval/themes/blind-<sample>.map.json   blind_id -> review_id -- opened only by the importer

`blind_id` is a sequence number in seeded `draw_key` order, so it carries no rating, product
or window signal, and the export reproduces exactly. The map is a separate file so that
reading the labelling input cannot incidentally reveal which review it is.

Run:  ./run.sh python scripts/blind_export.py --sample development [--scope full]
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build

OUT_DIR = PROJECT_ROOT / "eval" / "themes"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit", "training_pool"])
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    sample_run = runs.latest_success("theme_samples", category=args.category, data_scope=args.scope,
                                     params_match={"sample": args.sample})
    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    if sample_run is None or silver_run is None:
        raise SystemExit(f"no successful theme_samples run for sample {args.sample!r}, or no silver run")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    slv = silver_run["outputs"]["silver.reviews"]

    spark = build("blind_export", cores="local[2]", driver_memory="2g")
    try:
        asg = (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
               .filter(F.col("sample_name") == args.sample).select("review_id", "draw_key"))
        silver = (spark.read.option("snapshot-id", slv["snapshot_id"]).table(slv["table"])
                  .select("review_id", "title", "text"))
        rows = [r.asDict() for r in asg.join(silver, on="review_id", how="inner").collect()]
        missing = asg.count() - len(rows)
        if missing:
            raise SystemExit(f"{missing} assigned review(s) are absent from the pinned silver snapshot")
    finally:
        spark.stop()

    rows.sort(key=lambda r: (r["draw_key"], r["review_id"]))
    prefix = {"development": "dev", "audit": "aud", "training_pool": "trn"}[args.sample]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    blind = OUT_DIR / f"blind-{args.sample}.jsonl"
    mapping = OUT_DIR / f"blind-{args.sample}.map.json"
    with blind.open("w") as f:
        for i, r in enumerate(rows, start=1):
            f.write(json.dumps({"blind_id": f"{prefix}-{i:04d}", "title": r["title"] or "",
                                "text": r["text"] or ""}, ensure_ascii=False) + "\n")
    mapping.write_text(json.dumps(
        {"sample": args.sample, "scope": args.scope,
         "theme_samples_run_id": sample_run["run_id"], "silver_run_id": silver_run["run_id"],
         "assignments_snapshot_id": asg_out["snapshot_id"],
         "blind_ids": {f"{prefix}-{i:04d}": r["review_id"] for i, r in enumerate(rows, start=1)}},
        indent=1) + "\n")
    print(f"BLIND_EXPORT sample={args.sample} scope={args.scope} rows={len(rows)} "
          f"samples_run={sample_run['run_id']} out={blind.relative_to(PROJECT_ROOT)} "
          f"map={mapping.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

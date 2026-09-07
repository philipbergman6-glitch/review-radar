"""Characterise the discovery run's terminal failures by named validation cause.

`src.ai.labels.validate_discovery` never repairs a response: it names every failure and the
row lands as `parse_failed`. This script reads back those names from
`gold.discovery_phrases.attempts` and writes

  docs/theme-taxonomy/discovery-failures.csv   one row per cause, with reviews and an example

so the discovery failure rate is reported by cause rather than as one opaque percentage.
A review is counted once per distinct cause, using the *last* attempt (the terminal one).

Run:  ./run.sh python scripts/discovery_failures.py [--scope full]
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict

from pyspark.sql import functions as F

from src.ai.discover_phrases import table_name
from src.ai.labels import DISCOVERY_SCHEMA, load_spec
from src.common.config import PROJECT_ROOT
from src.common.spark import build

OUT = PROJECT_ROOT / "docs" / "theme-taxonomy" / "discovery-failures.csv"

# The failure names carry the offending value; strip the variable part to get the cause.
CAUSES = (
    (re.compile(r"^complaints\[\d+\] quote is not present in the review"), "quote_not_in_review"),
    (re.compile(r"^complaints\[\d+\] quote has (\d+) words"), "quote_too_long"),
    (re.compile(r"^complaints\[\d+\] aspect has (\d+) words"), "aspect_too_long"),
    (re.compile(r"^complaints\[\d+\] repeats an earlier quote"), "duplicate_quote"),
    (re.compile(r"^complaints\[\d+\] must have exactly"), "item_shape"),
    (re.compile(r"^complaints\[\d+\] quote and aspect must be strings"), "item_types"),
    (re.compile(r"^complaints has (\d+) items"), "too_many_items"),
    (re.compile(r"^complaints must be a list"), "complaints_not_a_list"),
    (re.compile(r"^top level must be an object"), "top_level_shape"),
)


def cause_of(fail: str) -> str:
    for pat, name in CAUSES:
        if pat.match(fail):
            return name
    return "unclassified"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--config-hash", default=None,
                   help="inference config hash to report on; defaults to the current spec, so rows\nfrom a superseded prompt or limit never mix into the counts")
    args = ap.parse_args()

    config_hash = args.config_hash or load_spec().config_hash("discovery", DISCOVERY_SCHEMA)

    spark = build("discovery_failures", cores="local[2]", driver_memory="2g")
    try:
        df = spark.table(table_name(args.scope)).filter(
            (F.col("budget_line") == "discovery")
            & (F.col("inference_config_hash") == config_hash))
        total = df.count()
        rows = df.filter(F.col("label_status") != "succeeded").select(
            "source_review_id", "label_status", "attempt_count", "attempts").collect()

        per_cause: dict[str, dict] = defaultdict(lambda: {"reviews": set(), "example": ""})
        identical_retries = 0
        for r in rows:
            attempts = list(r["attempts"] or [])
            if len(attempts) >= 2 and attempts[0]["validation_error"] == attempts[-1]["validation_error"]:
                identical_retries += 1
            err = (attempts[-1]["validation_error"] if attempts else "") or ""
            # validate_discovery joins its named failures with "; "
            for fail in [f.strip() for f in err.split(";") if f.strip()]:
                c = per_cause[cause_of(fail)]
                c["reviews"].add(r["source_review_id"])
                if not c["example"]:
                    c["example"] = fail[:160]

        OUT.parent.mkdir(parents=True, exist_ok=True)
        with OUT.open("w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["cause", "reviews", "share_of_all_reviews", "share_of_failures", "example"])
            for name, c in sorted(per_cause.items(), key=lambda kv: -len(kv[1]["reviews"])):
                n = len(c["reviews"])
                w.writerow([name, n, f"{n / total:.4f}" if total else "",
                            f"{n / len(rows):.4f}" if rows else "", c["example"]])

        top = sorted(per_cause.items(), key=lambda kv: -len(kv[1]["reviews"]))[:3]
        print(f"DISCOVERY_FAILURES scope={args.scope} config={config_hash[:12]} reviews={total} failed={len(rows)} "
              f"rate={len(rows) / total:.4f} identical_retries={identical_retries} "
              f"causes={len(per_cause)} top=" + ",".join(f"{k}:{len(v['reviews'])}" for k, v in top)
              + f" out={OUT.relative_to(PROJECT_ROOT)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Aggregate the discovery phrases into a merge proposal (ADR-0003 "Taxonomy and human labels").

The taxonomy is merged **by hand**: this script only counts, so the merge itself stays a
recorded human act. It reads `gold.discovery_phrases`, joins each phrase back to its review's
product and stratum through `gold.theme_sample_assignments`, and writes

  docs/theme-taxonomy/aspect-counts.csv    one row per normalised aspect phrase, with
                                           reviews, low-rated reviews, distinct products,
                                           low-rated share, and three example quotes

Support rule, printed per aspect so the hand merge can apply it: a theme survives only if it
occurs in at least 3% of the low-rated discovery reviews and on at least two products. That
test is applied to the *merged* theme, not to a raw aspect phrase, so this file is input to
the decision and never the decision itself.

Run:  ./run.sh python scripts/propose_taxonomy.py [--scope full] [--min-share 0.03]
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
from src.spark.theme_samples import table_names as sample_tables

OUT = PROJECT_ROOT / "docs" / "theme-taxonomy" / "aspect-counts.csv"
WS = re.compile(r"\s+")


def normalise_aspect(a: str) -> str:
    return WS.sub(" ", a.strip().casefold()).strip(" .,;:!?-")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--min-share", type=float, default=0.03, help="support rule: share of low-rated reviews")
    ap.add_argument("--min-products", type=int, default=2)
    ap.add_argument("--config-hash", default=None,
                   help="inference config hash to report on; defaults to the current spec, so rows\nfrom a superseded prompt or limit never mix into the counts")
    args = ap.parse_args()
    config_hash = args.config_hash or load_spec().config_hash("discovery", DISCOVERY_SCHEMA)

    spark = build("propose_taxonomy", cores="local[2]", driver_memory="2g")
    try:
        labels = (spark.table(table_name(args.scope))
                  .filter((F.col("budget_line") == "discovery")
                          & (F.col("label_status") == "succeeded")
                          & (F.col("inference_config_hash") == config_hash)))
        asg = (spark.table(sample_tables(args.scope)["assignments"])
               .filter(F.col("sample_name") == "discovery")
               .select(F.col("review_id").alias("source_review_id"), "parent_asin", "rating", "stratum"))
        rows = (labels.join(asg, on="source_review_id", how="inner")
                .select("source_review_id", "parent_asin", "rating", "stratum",
                        F.explode_outer("complaints").alias("c"))
                .collect())
        low_reviews = {r["source_review_id"] for r in rows if r["stratum"] == "low"}
        per_aspect: dict[str, dict] = defaultdict(
            lambda: {"reviews": set(), "low_reviews": set(), "products": set(), "quotes": []})
        for r in rows:
            if r["c"] is None:
                continue
            a = normalise_aspect(r["c"]["aspect"])
            if not a:
                continue
            e = per_aspect[a]
            e["reviews"].add(r["source_review_id"])
            e["products"].add(r["parent_asin"])
            if r["stratum"] == "low":
                e["low_reviews"].add(r["source_review_id"])
            if len(e["quotes"]) < 3:
                e["quotes"].append(r["c"]["quote"].replace("\n", " ").strip())
    finally:
        spark.stop()

    denom = max(len(low_reviews), 1)
    ranked = sorted(per_aspect.items(), key=lambda kv: (-len(kv[1]["low_reviews"]), kv[0]))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["aspect", "reviews", "low_rated_reviews", "low_rated_share", "distinct_products",
                    "meets_support_rule_alone", "example_1", "example_2", "example_3"])
        for a, e in ranked:
            share = len(e["low_reviews"]) / denom
            meets = share >= args.min_share and len(e["products"]) >= args.min_products
            w.writerow([a, len(e["reviews"]), len(e["low_reviews"]), f"{share:.4f}", len(e["products"]),
                        "yes" if meets else "no", *(e["quotes"] + ["", "", ""])[:3]])
    alone = sum(1 for a, e in ranked
                if len(e["low_reviews"]) / denom >= args.min_share and len(e["products"]) >= args.min_products)
    print(f"TAXONOMY_INPUT scope={args.scope} config={config_hash[:12]} aspects={len(ranked)} low_rated_reviews={len(low_reviews)} "
          f"min_share={args.min_share} min_products={args.min_products} "
          f"aspects_meeting_rule_unmerged={alone} out={OUT.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

"""Score a hand-written aspect->theme merge against the ADR-0003 support rule.

The merge itself is a human act: this script does not propose or alter it. It reads
`docs/theme-taxonomy/merge-proposal.csv` (aspect,proposed_theme), recomputes each merged
theme's support over *distinct* discovery reviews and products -- summing per-aspect counts
would double-count a review that yielded two aspects in the same theme -- and writes

  docs/theme-taxonomy/merge-table.csv     one row per merged aspect: theme, aspect, phrases,
                                          reviews, low-rated reviews, distinct products
  docs/theme-taxonomy/theme-support.csv   one row per theme, with the support rule applied

Support rule (ADR-0003): keep a theme only when it occurs in at least 3% of the low-rated
discovery reviews and on at least two products. Target eight themes; ten is a hard ceiling.
The ceiling is checked against the themes that *survive* the rule, and a breach is reported,
never silently trimmed -- which of the surviving themes to drop is Philip's call.

Run:  ./run.sh python scripts/score_taxonomy.py [--scope full]
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict

from propose_taxonomy import normalise_aspect  # same scripts/ directory
from pyspark.sql import functions as F

from src.ai.discover_phrases import table_name
from src.ai.labels import DISCOVERY_SCHEMA, load_spec
from src.common.config import PROJECT_ROOT
from src.common.spark import build
from src.spark.theme_samples import table_names as sample_tables

DOCS = PROJECT_ROOT / "docs" / "theme-taxonomy"
PROPOSAL = DOCS / "merge-proposal.csv"
MERGE_TABLE = DOCS / "merge-table.csv"
SUPPORT = DOCS / "theme-support.csv"
CEILING = 10


def load_proposal() -> dict[str, str]:
    if not PROPOSAL.exists():
        raise SystemExit(f"no merge proposal at {PROPOSAL}")
    mapping: dict[str, str] = {}
    with PROPOSAL.open(newline="") as fh:
        for row in csv.DictReader(fh):
            aspect, theme = normalise_aspect(row["aspect"]), row["proposed_theme"].strip()
            if not aspect or not theme:
                raise SystemExit(f"blank aspect or theme in {PROPOSAL}: {row!r}")
            if aspect in mapping and mapping[aspect] != theme:
                raise SystemExit(f"aspect {aspect!r} mapped to two themes in {PROPOSAL}")
            mapping[aspect] = theme
    return mapping


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--min-share", type=float, default=0.03)
    ap.add_argument("--min-products", type=int, default=2)
    ap.add_argument("--config-hash", default=None,
                   help="inference config hash to report on; defaults to the current spec, so rows\nfrom a superseded prompt or limit never mix into the counts")
    args = ap.parse_args()
    config_hash = args.config_hash or load_spec().config_hash("discovery", DISCOVERY_SCHEMA)

    mapping = load_proposal()
    spark = build("score_taxonomy", cores="local[2]", driver_memory="2g")
    try:
        labels = (spark.table(table_name(args.scope))
                  .filter((F.col("budget_line") == "discovery")
                          & (F.col("label_status") == "succeeded")
                          & (F.col("inference_config_hash") == config_hash)))
        asg = (spark.table(sample_tables(args.scope)["assignments"])
               .filter(F.col("sample_name") == "discovery")
               .select(F.col("review_id").alias("source_review_id"), "parent_asin", "stratum"))
        rows = (labels.join(asg, on="source_review_id", how="inner")
                .select("source_review_id", "parent_asin", "stratum",
                        F.explode_outer("complaints").alias("c"))
                .collect())
    finally:
        spark.stop()

    low_reviews = {r["source_review_id"] for r in rows if r["stratum"] == "low"}
    seen_aspects: set[str] = set()
    per_aspect: dict[str, dict] = defaultdict(lambda: {"phrases": 0, "reviews": set(), "low": set(), "products": set()})
    per_theme: dict[str, dict] = defaultdict(lambda: {"phrases": 0, "reviews": set(), "low": set(), "products": set()})
    for r in rows:
        c = r["c"]
        if c is None:
            continue
        aspect = normalise_aspect(c["aspect"])
        seen_aspects.add(aspect)
        theme = mapping.get(aspect)
        for bucket, key in ((per_aspect, aspect), (per_theme, theme)):
            if key is None:
                continue
            b = bucket[key]
            b["phrases"] += 1
            b["reviews"].add(r["source_review_id"])
            b["products"].add(r["parent_asin"])
            if r["stratum"] == "low":
                b["low"].add(r["source_review_id"])

    unknown = sorted(set(mapping) - seen_aspects)
    if unknown:  # a typo in the proposal must not silently vanish from the merge
        raise SystemExit(f"{len(unknown)} proposed aspects are absent from the discovery run: {unknown[:10]}")

    n_low = len(low_reviews)
    DOCS.mkdir(parents=True, exist_ok=True)
    with MERGE_TABLE.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["theme", "aspect", "phrases", "reviews", "low_rated_reviews", "distinct_products"])
        for aspect, theme in sorted(mapping.items(), key=lambda kv: (kv[1], kv[0])):
            a = per_aspect[aspect]
            w.writerow([theme, aspect, a["phrases"], len(a["reviews"]), len(a["low"]), len(a["products"])])

    kept: list[str] = []
    with SUPPORT.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["theme", "aspects_merged", "phrases", "reviews", "low_rated_reviews",
                    "low_rated_share", "distinct_products", "meets_support_rule"])
        order = sorted(per_theme.items(), key=lambda kv: -len(kv[1]["low"]))
        for theme, t in order:
            share = len(t["low"]) / n_low if n_low else 0.0
            ok = share >= args.min_share and len(t["products"]) >= args.min_products
            if ok:
                kept.append(theme)
            w.writerow([theme, sum(1 for a in mapping.values() if a == theme), t["phrases"],
                        len(t["reviews"]), len(t["low"]), f"{share:.4f}", len(t["products"]),
                        "yes" if ok else "no"])

    # Coverage is per review, not per phrase: a review is covered when at least one of its
    # aspects merged into a theme. Reviews with an unmapped aspect *as well* are still covered.
    covered = {r["source_review_id"] for r in rows
               if r["c"] is not None and normalise_aspect(r["c"]["aspect"]) in mapping}
    uncovered_low = sorted(low_reviews - covered)
    print(f"TAXONOMY_MERGE scope={args.scope} config={config_hash[:12]} low_rated_reviews={n_low} "
          f"themes_proposed={len(per_theme)} themes_meeting_rule={len(kept)} ceiling={CEILING} "
          f"over_ceiling={'yes' if len(kept) > CEILING else 'no'} "
          f"low_rated_covered={n_low - len(uncovered_low)} "
          f"low_rated_coverage={(n_low - len(uncovered_low)) / n_low:.4f} "
          f"kept={','.join(kept)} out={MERGE_TABLE.relative_to(PROJECT_ROOT)},{SUPPORT.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

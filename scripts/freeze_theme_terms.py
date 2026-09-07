"""Freeze `conf/theme-terms.json` from the discovery evidence quotes (RR-22).

Enrichment needs strings that occur in review text. The taxonomy holds prose and the merge
table holds the model's aspect abstractions, so neither can drive a `rlike`. This script
mines the terms mechanically instead: `merge-proposal.csv` maps aspect -> theme,
`gold.discovery_phrases` supplies each complaint's exact quote, and one document per
(discovery review, theme) is the concatenation of that review's quotes for that theme.
`src.ai.theme_terms.mine_terms` keeps a term for a theme when it appears in at least
`--min-reviews` of the theme's documents and at least `--min-odds` times more often there
than outside it; a term claimed by two themes goes to the higher-odds theme only.

The artifact is frozen at the same moment as the frames: regenerating it after a frame is
drawn would silently change what "enriched" meant, so an existing file is never overwritten
without `--force`.

Run:  ./run.sh python scripts/freeze_theme_terms.py [--scope full]
"""
from __future__ import annotations

import argparse
import json

from propose_taxonomy import normalise_aspect  # same scripts/ directory
from pyspark.sql import functions as F
from score_taxonomy import load_proposal

from src.ai.discover_phrases import table_name
from src.ai.labels import DISCOVERY_SCHEMA, load_spec
from src.ai.theme_terms import TERMS_PATH, TERMS_VERSION, corpus_frequency, mine_terms
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build
from src.spark.theme_samples import table_names as sample_tables

TAXONOMY = PROJECT_ROOT / "conf" / "theme-taxonomy.json"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--min-reviews", type=int, default=2)
    ap.add_argument("--min-odds", type=float, default=3.0)
    ap.add_argument("--max-per-theme", type=int, default=120)
    ap.add_argument("--max-corpus-share", type=float, default=0.10,
                    help="drop a term occurring in more than this share of whole review texts")
    ap.add_argument("--force", action="store_true", help="overwrite an already frozen artifact")
    args = ap.parse_args()
    if TERMS_PATH.exists() and not args.force:
        raise SystemExit(f"{TERMS_PATH.relative_to(PROJECT_ROOT)} is already frozen; "
                         "pass --force only if no frame has been drawn from it")

    taxonomy = json.loads(TAXONOMY.read_text())
    frozen_themes = [t["id"] for t in taxonomy["themes"]]
    mapping = load_proposal()
    merged = {"breaks_or_wears_out": "poor_build_quality"}   # RR-20, amendment (b)
    mapping = {a: merged.get(t, t) for a, t in mapping.items()}
    unknown = sorted(set(mapping.values()) - set(frozen_themes))
    if unknown:
        raise SystemExit(f"merge proposal maps aspects to themes outside the frozen taxonomy: {unknown}")

    config_hash = load_spec().config_hash("discovery", DISCOVERY_SCHEMA)
    spark = build("freeze_theme_terms", cores="local[2]", driver_memory="2g")
    try:
        rows = (spark.table(table_name(args.scope))
                .filter((F.col("budget_line") == "discovery")
                        & (F.col("label_status") == "succeeded")
                        & (F.col("inference_config_hash") == config_hash))
                .select("source_review_id", F.explode("complaints").alias("c"))
                .collect())
        # The rarity guard is measured on the whole discovery review texts, which is the only
        # review population this stage is allowed to read (the other frames are undrawn).
        silver_run = runs.latest_success("silver", category=C.CATEGORY, data_scope=args.scope)
        if silver_run is None:
            raise SystemExit("no successful silver run to measure term rarity against")
        slv = silver_run["outputs"]["silver.reviews"]
        asg = (spark.table(sample_tables(args.scope)["assignments"])
               .filter(F.col("sample_name") == "discovery").select("review_id"))
        texts = [f"{r['title'] or ''} {r['text'] or ''}" for r in
                 (spark.read.option("snapshot-id", slv["snapshot_id"]).table(slv["table"])
                  .select("review_id", "title", "text").join(asg, on="review_id", how="inner")
                  .collect())]
    finally:
        spark.stop()
    corpus_df, corpus_docs = corpus_frequency(texts)

    per_doc: dict[tuple[str, str], list[str]] = {}
    unmapped = 0
    for r in rows:
        theme = mapping.get(normalise_aspect(r["c"]["aspect"]))
        if theme is None:
            unmapped += 1
            continue
        per_doc.setdefault((r["source_review_id"], theme), []).append(r["c"]["quote"])
    docs = [(theme, " ".join(quotes)) for (_, theme), quotes in sorted(per_doc.items())]
    terms = mine_terms(docs, min_reviews=args.min_reviews, min_odds=args.min_odds,
                       max_per_theme=args.max_per_theme, corpus_df=corpus_df,
                       corpus_docs=corpus_docs, max_corpus_share=args.max_corpus_share)
    missing = [t for t in frozen_themes if not terms.get(t)]
    if missing:
        raise SystemExit(f"no term cleared the bar for {missing}; an enriched quota could never be "
                         "filled. Loosen --min-odds deliberately or record the theme as unenrichable.")

    doc = {
        "terms_version": TERMS_VERSION,
        "frozen_at": "2026-09-07",
        "decided_in": "RR-22",
        "source": {"discovery_config_hash": config_hash,
                   "taxonomy_version": taxonomy["taxonomy_version"],
                   "merge_proposal": "docs/theme-taxonomy/merge-proposal.csv",
                   "documents": len(docs), "unmapped_complaints": unmapped,
                   "rarity_corpus_reviews": corpus_docs},
        "params": {"min_reviews": args.min_reviews, "min_odds": args.min_odds,
                   "max_per_theme": args.max_per_theme,
                   "max_corpus_share": args.max_corpus_share},
        "note": ("Enrichment terms only. They decide which reviews are offered for labelling; "
                 "they never enter a prompt and never assign a label."),
        "terms": {t: terms[t] for t in frozen_themes},
    }
    TERMS_PATH.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")
    per_theme = " ".join(f"{t}={len(terms[t])}" for t in frozen_themes)
    print(f"THEME_TERMS version={TERMS_VERSION} config={config_hash[:12]} documents={len(docs)} "
          f"unmapped_complaints={unmapped} rarity_corpus={corpus_docs} "
          f"min_reviews={args.min_reviews} min_odds={args.min_odds} "
          f"max_corpus_share={args.max_corpus_share} "
          f"terms_total={sum(len(v) for v in terms.values())} {per_theme} "
          f"out={TERMS_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

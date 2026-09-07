"""The sentiment weak-label check, exactly as ADR-0003 specifies.

This is a sanity check of `overall_sentiment` against the star rating. It is **not** theme
validation and **not** the star-only theme baseline, and no text-derived label is ever revised
to agree with a star: a disagreement here is evidence about the labeller, not a correction.

Rules, from ADR-0003 verbatim: exclude 3-star reviews; for 1-2 stars agreement means
`negative`, for 4-5 stars it means `positive`; `mixed` and `none` are disagreements. One row
per star bucket plus an overall row with n, counts for all four predicted sentiments, the
agreement numerator and rate, and a Wilson 95% interval. The fixed adjudication-cause counts
are printed beside it when the adjudication CSV exists.

Run:  ./run.sh python scripts/sentiment_check.py --sample audit --prompt label_v4
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from typing import Any

from pyspark.sql import functions as F

from src.ai.label_themes import PROMPT_MAX_THEMES
from src.ai.labels import decoding_schema, load_spec, load_taxonomy
from src.ai.theme_labels import table_name
from src.ai.wilson import wilson
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build

SENTIMENTS = ("positive", "negative", "mixed", "none")
OUT_DIR = PROJECT_ROOT / "eval" / "themes"
DOCS = PROJECT_ROOT / "docs" / "theme-taxonomy"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit"])
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--prompt", default="label_v4")
    ap.add_argument("--model", default=None)
    ap.add_argument("--source", default="local_llm", choices=["local_llm", "agent_reference"])
    args = ap.parse_args()

    spec, tax = load_spec(), load_taxonomy()
    model = args.model or spec.model_id
    prompt = spec.prompts[args.prompt]
    schema = decoding_schema(tax.ids, max_themes=PROMPT_MAX_THEMES.get(prompt.version))
    config_hash = spec.config_hash(args.prompt, schema, extra={"taxonomy": tax.file_hash},
                                   model_id=model)
    sample_run = runs.latest_success("theme_samples", category=args.category, data_scope=args.scope,
                                     params_match={"sample": args.sample})
    if sample_run is None:
        raise SystemExit(f"no theme_samples run for sample {args.sample!r}")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]

    spark = build("sentiment_check", cores="local[2]", driver_memory="2g")
    try:
        labels = spark.table(table_name(args.scope)).filter(
            (F.col("budget_line") == args.sample) & (F.col("label_source") == args.source))
        if args.source == "local_llm":
            labels = labels.filter(F.col("inference_config_hash") == config_hash)
        rows = [r.asDict() for r in labels.select("source_review_id", "label_status",
                                                  "overall_sentiment").collect()]
        ratings = {r["review_id"]: int(r["rating"]) for r in
                   (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
                    .filter(F.col("sample_name") == args.sample)
                    .select("review_id", "rating").collect())}
    finally:
        spark.stop()

    buckets: dict[str, list[dict[str, Any]]] = {"1-2": [], "4-5": []}
    excluded_three = excluded_no_label = 0
    for r in rows:
        star = ratings.get(r["source_review_id"])
        if star is None:
            continue
        if star == 3:
            excluded_three += 1
            continue
        if r["overall_sentiment"] is None:
            excluded_no_label += 1
            continue
        buckets["1-2" if star <= 2 else "4-5"].append(r)

    def line(name: str, items: list[dict[str, Any]], want: str | None) -> dict[str, Any]:
        counts = Counter(i["overall_sentiment"] for i in items)
        hits = sum(1 for i in items
                   if i["overall_sentiment"] == (want or ("negative" if ratings[i["source_review_id"]] <= 2
                                                          else "positive")))
        lo, hi = wilson(hits, len(items))
        return {"bucket": name, "n": len(items),
                **{f"predicted_{s}": counts.get(s, 0) for s in SENTIMENTS},
                "agreements": hits, "rate": round(hits / len(items), 4) if items else None,
                "wilson_95": [round(lo, 4), round(hi, 4)]}

    report = [line("1-2", buckets["1-2"], "negative"),
              line("4-5", buckets["4-5"], "positive"),
              line("overall", buckets["1-2"] + buckets["4-5"], None)]

    causes: dict[str, int] = {}
    adj = DOCS / f"adjudication-{args.sample}.csv"
    if adj.exists():
        with adj.open(newline="") as f:
            causes = dict(Counter(row["cause"] for row in csv.DictReader(f)))

    out = {"sample": args.sample, "label_source": args.source, "model_id": model,
           "prompt_version": prompt.version, "inference_config_hash": config_hash,
           "excluded_three_star": excluded_three, "excluded_without_label": excluded_no_label,
           "rows": report, "adjudication_causes": causes,
           "note": ("A sanity check of overall_sentiment, not theme validation and not the "
                    "star-only theme baseline. No text-derived label was revised to agree "
                    "with a star (ADR-0003).")}
    path = OUT_DIR / f"sentiment-{args.sample}-{prompt.version}-{model.replace(':', '_')}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1) + "\n")

    print(f"\n{'bucket':<8} {'n':>4} {'pos':>5} {'neg':>5} {'mix':>5} {'none':>5} "
          f"{'agree':>6} {'rate':>7}  wilson95")
    for r in report:
        rate = "   -   " if r["rate"] is None else f"{r['rate']:.4f}"
        print(f"{r['bucket']:<8} {r['n']:>4} {r['predicted_positive']:>5} {r['predicted_negative']:>5} "
              f"{r['predicted_mixed']:>5} {r['predicted_none']:>5} {r['agreements']:>6} "
              f"{rate:>7}  {r['wilson_95']}")
    o = report[-1]
    print(f"\nSENTIMENT_CHECK sample={args.sample} source={args.source} model={model} "
          f"prompt={prompt.version} n={o['n']} agreement={o['rate']} wilson95={o['wilson_95']} "
          f"excluded_three_star={excluded_three} excluded_without_label={excluded_no_label} "
          + (" ".join(f"{k}={v}" for k, v in sorted(causes.items())) if causes else "adjudication=not_run")
          + f" out={path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

"""Score one labelling configuration against the blind reference labels (ADR-0003, RR-21).

Reads `gold.review_theme_labels`, pairs each `agent_reference` row for a budget line with the
`local_llm` row for the same review under one `inference_config_hash`, and prints the per-theme
table ADR-0003 requires: precision, recall, F1 and support per theme, macro-F1 over supported
themes, `other`, abstention, parse/API failure coverage, and a seeded product-clustered
bootstrap interval as context.

Nothing here decides anything. The pass rule (`audit macro-F1 >= 0.70`, `no supported-theme
recall < 0.50`) lives in scripts/gate_themes.py, and it was frozen before any of these numbers
existed.

Run:  ./run.sh python scripts/score_themes.py --sample development --prompt label_v4
"""
from __future__ import annotations

import argparse
import json
from typing import Any

from pyspark.sql import functions as F

from src.ai.label_themes import PROMPT_MAX_THEMES
from src.ai.label_usage import for_evaluation, theme_ids
from src.ai.labels import decoding_schema, load_spec, load_taxonomy
from src.ai.theme_labels import table_name
from src.ai.theme_scoring import (
    bootstrap_macro_f1,
    failure_coverage,
    macro_f1,
    other_agreement,
    score_themes,
)
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build

OUT_DIR = PROJECT_ROOT / "eval" / "themes"
REFERENCE_SOURCE = "agent_reference"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit"])
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    _frozen = load_spec().frozen
    ap.add_argument("--prompt", default=_frozen.name if _frozen else "label_v4",
                    help="prompt name in conf/theme-label-spec.json (default: the frozen one)")
    ap.add_argument("--model", default=None, help="the comparison model, e.g. llama3.2:3b")
    ap.add_argument("--source", default="local_llm",
                    help="label_source of the system under test (local_llm | classifier)")
    ap.add_argument("--min-support", type=int, default=10,
                    help="ADR-0003: a supported theme has >= 10 positives in the evaluation set")
    ap.add_argument("--bootstrap-draws", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=20260907)
    ap.add_argument("--out", default=None, help="artefact path; defaults to eval/themes/score-<sample>-<prompt>.json")
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

    spark = build("score_themes", cores="local[2]", driver_memory="2g")
    try:
        table = table_name(args.scope)
        labels = spark.table(table).filter(F.col("budget_line") == args.sample)
        ref_rows = [r.asDict(recursive=True) for r in
                    labels.filter(F.col("label_source") == REFERENCE_SOURCE).collect()]
        sys_rows = [r.asDict(recursive=True) for r in
                    labels.filter((F.col("label_source") == args.source)
                                  & (F.col("inference_config_hash") == config_hash)).collect()]
        asg = {r["review_id"]: r.asDict() for r in
               (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
                .filter(F.col("sample_name") == args.sample)
                .select("review_id", "parent_asin", "stratum", "rating").collect())}
    finally:
        spark.stop()

    if not ref_rows:
        raise SystemExit(f"no {REFERENCE_SOURCE} labels for budget line {args.sample!r}")
    reference = {r["source_review_id"]: theme_ids(r["themes"]) for r in ref_rows}
    reference_other = {r["source_review_id"]: bool(r["other_present"]) for r in ref_rows}
    # `for_evaluation`, not `r["themes"] or []`: a parse failure scoring as an empty prediction
    # is a decision, and it is asserted against the training drop in tests/test_label_usage.py.
    system = {r["source_review_id"]: for_evaluation(r) for r in sys_rows}
    system_other = {r["source_review_id"]: r["other_present"] for r in sys_rows}
    statuses = {r["source_review_id"]: r["label_status"] for r in sys_rows}
    selected = sorted(reference)
    product_of = {rid: asg[rid]["parent_asin"] for rid in selected if rid in asg}

    def report(ids: list[str], name: str) -> dict[str, Any]:
        ref = {k: reference[k] for k in ids}
        scores = score_themes(ref, system, tax.ids, min_support=args.min_support)
        m = macro_f1(scores)
        lo, hi = bootstrap_macro_f1(ref, system, tax.ids, product_of, min_support=args.min_support,
                                    seed=args.seed, draws=args.bootstrap_draws)
        return {
            "subset": name, "reviews": len(ids), "macro_f1": m,
            "bootstrap_95": [lo, hi],
            "supported_themes": [s.theme_id for s in scores if s.supported],
            "min_supported_recall": min([s.recall for s in scores if s.supported and s.recall is not None],
                                        default=None),
            "per_theme": [{"theme_id": s.theme_id, "support": s.support, "predicted": s.predicted,
                           "tp": s.tp, "fp": s.fp, "fn": s.fn, "precision": s.precision,
                           "recall": s.recall, "f1": s.f1, "supported": s.supported}
                          for s in scores],
            "other": other_agreement(reference_other, system_other, ids),
            "coverage": failure_coverage(statuses, ids),
        }

    enriched = [r for r in selected if (asg.get(r, {}).get("stratum") or "").startswith("enriched")]
    representative = [r for r in selected if asg.get(r, {}).get("stratum") == "representative"]
    out: dict[str, Any] = {
        "sample": args.sample, "scope": args.scope, "label_source": args.source,
        "model_id": model, "prompt_version": prompt.version,
        "inference_config_hash": config_hash, "taxonomy_version": tax.version,
        "taxonomy_hash": tax.file_hash, "min_support": args.min_support,
        "reference_source": REFERENCE_SOURCE, "reference_rows": len(ref_rows),
        "system_rows": len(sys_rows), "seed": args.seed, "bootstrap_draws": args.bootstrap_draws,
        "overall": report(selected, "all"),
    }
    # ADR-0003 requires the prevalence-representative audit rows reported separately from the
    # term-enriched ones: they are drawn differently, so mixing them hides both numbers.
    if representative:
        out["enriched"] = report(enriched, "enriched")
        out["representative"] = report(representative, "representative")

    path = OUT_DIR / (args.out or f"score-{args.sample}-{prompt.version}-{model.replace(':', '_')}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1) + "\n")

    o = out["overall"]
    print(f"\n{'theme':<22} {'sup':>4} {'pred':>5} {'TP':>4} {'FP':>4} {'FN':>4} "
          f"{'P':>6} {'R':>6} {'F1':>6}")
    for t in o["per_theme"]:
        def fmt(v):
            return f"{v:.3f}" if isinstance(v, float) else "  -  "
        mark = " " if t["supported"] else "*"
        print(f"{t['theme_id']:<21}{mark} {t['support']:>4} {t['predicted']:>5} {t['tp']:>4} "
              f"{t['fp']:>4} {t['fn']:>4} {fmt(t['precision']):>6} {fmt(t['recall']):>6} "
              f"{fmt(t['f1']):>6}")
    print("* below the support threshold; excluded from macro-F1")
    cov = o["coverage"]
    mf = o["macro_f1"]
    lo, hi = o["bootstrap_95"]
    print(f"\nTHEME_SCORE sample={args.sample} source={args.source} model={model} "
          f"prompt={prompt.version} config={config_hash[:12]} reviews={o['reviews']} "
          f"macro_f1={mf if mf is None else round(mf, 4)} "
          f"bootstrap95=[{'' if lo is None else round(lo, 4)},{'' if hi is None else round(hi, 4)}] "
          f"supported={len(o['supported_themes'])} "
          f"min_supported_recall={o['min_supported_recall'] if o['min_supported_recall'] is None else round(o['min_supported_recall'], 4)} "
          f"ok={cov['succeeded']} abstained={cov['model_abstained']} "
          f"parse_failed={cov['parse_failed']} api_failed={cov['api_failed']} absent={cov['absent']} "
          f"failure_rate={cov['failure_rate']} other_agreement={o['other']['agreement']} "
          f"out={path.relative_to(PROJECT_ROOT)}")
    for key in ("enriched", "representative"):
        if key in out:
            s = out[key]
            m2 = s["macro_f1"]
            print(f"THEME_SCORE_SUBSET subset={key} reviews={s['reviews']} "
                  f"macro_f1={m2 if m2 is None else round(m2, 4)} "
                  f"supported={len(s['supported_themes'])} "
                  f"failure_rate={s['coverage']['failure_rate']}")


if __name__ == "__main__":
    main()

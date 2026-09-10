"""The star-only theme baseline (ADR-0002, defined in RR-23).

The floor the labeller and the classifier are compared against: for each theme `t`,
`predict t iff rating <= k_t`. `k_t` is chosen from {1,2,3,4,5} to maximise that theme's F1 on
the **development** set against the `agent_reference` labels; a theme whose best F1 is zero at
every threshold gets `k_t = 0` and predicts nothing, reported rather than smoothed.

  --fit    fits every k_t on development and writes conf/theme-star-baseline.json
  --score  applies the frozen thresholds to a sample and writes a score artefact in exactly
           the shape scripts/score_themes.py writes, so the three systems sit in one table.
           The **audit** set is refused here: it is opened once, for all three systems
           together, by `make audit-once` (ticket 09).

The thresholds freeze with the prompt and the classifier's cuts (ADR-0002) and are applied
unchanged to the audit set. Refitting after seeing an audit number is the thing this file
exists to make impossible to do quietly: --fit refuses to overwrite without --force.

Run:  ./run.sh python scripts/baseline_star_only.py --fit
      ./run.sh python scripts/baseline_star_only.py --score --sample audit
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import functions as F

from src.ai.audit_seal import (
    STAR_SPEC_PATH,
    require_measurement_goes_through_the_pass,
)
from src.ai.labels import load_taxonomy
from src.ai.theme_labels import table_name
from src.ai.theme_scoring import score_themes, system_report
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build

SPEC_PATH = STAR_SPEC_PATH
OUT_DIR = PROJECT_ROOT / "eval" / "themes"
STARS = (1, 2, 3, 4, 5)


def load_frame(spark, *, sample: str, scope: str, category: str):
    """Reference labels for one frame, joined to the star rating the baseline is allowed to see."""
    sample_run = runs.latest_success("theme_samples", category=category, data_scope=scope,
                                     params_match={"sample": sample})
    if sample_run is None:
        raise SystemExit(f"no theme_samples run for sample {sample!r}")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    ref = [r.asDict(recursive=True) for r in
           spark.table(table_name(scope)).filter((F.col("budget_line") == sample)
                                                 & (F.col("label_source") == "agent_reference"))
           .select("source_review_id", "themes").collect()]
    asg = {r["review_id"]: r.asDict() for r in
           (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
            .filter(F.col("sample_name") == sample)
            .select("review_id", "parent_asin", "rating", "stratum").collect())}
    if not ref:
        raise SystemExit(f"no agent_reference labels for budget line {sample!r}")
    reference = {r["source_review_id"]: {t["theme_id"] for t in (r["themes"] or [])} for r in ref}
    return reference, asg


def predict(reference: dict[str, set[str]], asg: dict, thresholds: dict[str, int]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for review_id in reference:
        star = asg.get(review_id, {}).get("rating")
        out[review_id] = {t for t, k in thresholds.items() if k and star is not None and star <= k}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--sample", default="audit", choices=["development", "audit"])
    ap.add_argument("--min-support", type=int, default=10)
    ap.add_argument("--seed", type=int, default=20260907)
    ap.add_argument("--bootstrap-draws", type=int, default=1000)
    ap.add_argument("--force", action="store_true", help="refit over a frozen threshold file")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--fit", action="store_true")
    g.add_argument("--score", action="store_true")
    args = ap.parse_args()
    tax = load_taxonomy()
    if args.score:
        try:
            # The floor is scored on audit only as part of the one three-way pass (ticket 09).
            require_measurement_goes_through_the_pass(args.sample,
                                                      command="baseline_star_only.py --score")
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if args.fit and SPEC_PATH.exists() and not args.force:
        raise SystemExit(f"{SPEC_PATH.relative_to(PROJECT_ROOT)} is already frozen; refitting "
                         "after an audit number exists is exactly what ADR-0002 forbids")

    spark = build("baseline_star_only", cores="local[2]", driver_memory="2g")
    try:
        sample = "development" if args.fit else args.sample
        reference, asg = load_frame(spark, sample=sample, scope=args.scope, category=args.category)
    finally:
        spark.stop()

    if args.fit:
        thresholds, fit_rows = {}, []
        for theme in tax.ids:
            best = (0.0, 0)
            grid = []
            for k in STARS:
                pred = predict(reference, asg, {theme: k})
                s = next(x for x in score_themes(reference, pred, [theme], min_support=1))
                f1 = s.f1 or 0.0
                grid.append({"k": k, "f1": round(f1, 4), "tp": s.tp, "fp": s.fp, "fn": s.fn})
                if f1 > best[0]:
                    best = (f1, k)
            thresholds[theme] = best[1]
            fit_rows.append({"theme_id": theme, "k": best[1], "development_f1": round(best[0], 4),
                             "grid": grid})
        doc = {"baseline": "star_only", "version": "1", "decided_in": "RR-23",
               "fitted_on": "development", "fitted_at": "2026-09-07",
               "taxonomy_version": tax.version, "taxonomy_hash": tax.file_hash,
               "rule": "predict theme t iff rating <= k_t; k_t = 0 predicts nothing",
               "thresholds": thresholds, "fit": fit_rows}
        SPEC_PATH.write_text(json.dumps(doc, indent=1) + "\n")
        print("STAR_BASELINE_FIT " + " ".join(f"{t}=k{thresholds[t]}" for t in tax.ids)
              + f" never_predicts={sum(1 for v in thresholds.values() if v == 0)} "
                f"out={SPEC_PATH.relative_to(PROJECT_ROOT)}")
        return

    if not SPEC_PATH.exists():
        raise SystemExit(f"{SPEC_PATH.relative_to(PROJECT_ROOT)} missing; run --fit first")
    spec = json.loads(SPEC_PATH.read_text())
    if spec["taxonomy_hash"] != tax.file_hash:
        raise SystemExit("the star baseline was fitted against a different taxonomy")
    thresholds = {t: int(k) for t, k in spec["thresholds"].items()}
    system = predict(reference, asg, thresholds)
    product_of = {r: asg[r]["parent_asin"] for r in reference if r in asg}
    overall = system_report(subset="all", reference=reference, system=system,
                            theme_ids=tax.ids, product_of=product_of,
                            min_support=args.min_support, seed=args.seed,
                            draws=args.bootstrap_draws)
    m = overall["macro_f1"]
    lo, hi = overall["bootstrap_95"]
    out = {"sample": args.sample, "scope": args.scope, "label_source": "star_only",
           "model_id": "star_only", "prompt_version": f"star-baseline-v{spec['version']}",
           "inference_config_hash": "", "taxonomy_version": tax.version,
           "taxonomy_hash": tax.file_hash, "min_support": args.min_support,
           "thresholds": thresholds, "seed": args.seed,
           # Declared so the floor can be shown to have been measured the same way as the
           # prompts it is a floor for: src/ai/prompt_selection.require_comparable reads these.
           "bootstrap_draws": args.bootstrap_draws, "reference_source": "agent_reference",
           "reference_rows": len(reference),
           # The floor predicts from a rating it always has, so it cannot fail to answer and
           # passes no statuses: `system_report` then counts every review as succeeded. That
           # is a claim about this system, not a default -- see its docstring.
           "overall": overall}
    path = OUT_DIR / f"score-{args.sample}-star_only.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1) + "\n")
    print(f"STAR_BASELINE sample={args.sample} reviews={len(reference)} "
          f"macro_f1={None if m is None else round(m, 4)} "
          f"bootstrap95=[{'' if lo is None else round(lo, 4)},{'' if hi is None else round(hi, 4)}] "
          f"supported={len(overall['supported_themes'])} "
          f"out={path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

"""What the training pool actually teaches, after the drop (ticket 07, ADR-0002, RR-23).

The classifier's teacher is the `training_pool` frame labelled by the **frozen** prompt. This
script is the record of what that teacher is: how many of the frame's assigned reviews were
labelled, how many the model never answered for and are therefore dropped from training, and
what the surviving rows say -- per theme, so an unlearnable theme is visible before a training
run rather than after a disappointing audit number.

The drop is the point. `src.ai.label_usage` states the asymmetry once: a `parse_failed` row is
dropped here, and the *same* row scores as an empty prediction on an evaluation set. This
script reports the training side of it; `scripts/score_themes.py` reports the other.

It refuses two things out loud:

* **a pool labelled by anything but the frozen prompt.** Rows from a superseded prompt land in
  the same table and differ only by a hash, so nothing downstream would notice.
* **a frame that is not fully labelled.** A review assigned to the pool with no row at all is
  not a drop with a reason -- it is a run that has not finished, and calling the remainder a
  teacher would overstate it silently.

It is a census, not a gate: it publishes no verdict and never enters `make eval-table`.

Run:  ./run.sh python scripts/pool_census.py
"""
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from pyspark.sql import functions as F

from src.ai.label_failures import census as failure_census
from src.ai.label_themes import LABEL_SOURCE
from src.ai.label_usage import for_training, training_census
from src.ai.labels import load_spec, load_taxonomy, prompt_schema
from src.ai.theme_labels import table_name
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build

OUT_DIR = PROJECT_ROOT / "eval" / "themes"
SAMPLE = "training_pool"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--model", default=None, help="defaults to the spec's primary model")
    ap.add_argument("--out", default=None,
                    help="artefact path; defaults to eval/themes/training-pool-<version>-<model>.json")
    args = ap.parse_args()

    spec, tax = load_spec(), load_taxonomy()
    frozen = spec.require_frozen(purpose="censusing the training pool")
    model = args.model or spec.model_id
    schema = prompt_schema(frozen.version, tax.ids)
    config_hash = spec.config_hash(frozen.name, schema, extra={"taxonomy": tax.file_hash},
                                   model_id=model)
    sample_run = runs.latest_success("theme_samples", category=args.category,
                                     data_scope=args.scope, params_match={"sample": SAMPLE})
    if sample_run is None:
        raise SystemExit(f"no theme_samples run for {SAMPLE!r}; the frame has not been drawn")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]

    spark = build("pool_census", cores="local[2]", driver_memory="2g")
    try:
        table = table_name(args.scope)
        rows = [r.asDict(recursive=True) for r in
                spark.table(table)
                .filter((F.col("budget_line") == SAMPLE) & (F.col("label_source") == LABEL_SOURCE)
                        & (F.col("inference_config_hash") == config_hash)
                        & (F.col("model_id") == model))
                .select("source_review_id", "label_status", "themes", "other_present", "abstain",
                        "attempts", "run_id").collect()]
        assigned = [r["review_id"] for r in
                    (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
                     .filter(F.col("sample_name") == SAMPLE).select("review_id").collect())]
    finally:
        spark.stop()

    if not rows:
        raise SystemExit(f"no {LABEL_SOURCE} rows for {SAMPLE} under {frozen.version} "
                         f"config {config_hash[:12]}; label the pool first")
    labelled = {r["source_review_id"] for r in rows}
    unlabelled = sorted(set(assigned) - labelled)
    if unlabelled:
        raise SystemExit(f"{len(unlabelled)} of {len(assigned)} assigned {SAMPLE} reviews have no "
                         f"label row (e.g. {unlabelled[:3]}); the labelling run has not finished, "
                         "and a partial pool is not a teacher")

    stats = training_census(r["label_status"] for r in rows)
    # Per-theme positives over the rows training actually keeps -- the same rows `for_training`
    # would hand the trainer, counted here so an unlearnable theme is visible before the fit.
    positives = {t: 0 for t in tax.ids}
    kept = empty = abstained = other = 0
    for r in rows:
        themes = for_training(r)
        if themes is None:
            continue
        kept += 1
        empty += 1 if not themes else 0
        abstained += 1 if r["abstain"] else 0
        other += 1 if r["other_present"] else 0
        for t in themes:
            positives[t] += 1
    failed = [r for r in rows if r["label_status"] == "parse_failed"]
    causes = failure_census(r["attempts"][0]["validation_error"] if r["attempts"] else None
                            for r in failed)
    run_ids = sorted({r["run_id"] for r in rows if r["run_id"]})
    rows_without_run_id = sum(1 for r in rows if not r["run_id"])

    out = {
        "kind": "training_pool_census", "sample": SAMPLE, "scope": args.scope,
        "label_source": LABEL_SOURCE, "model_id": model, "prompt_version": frozen.version,
        "prompt_frozen_in": frozen.decided_in, "freeze_commit": frozen.freeze_commit,
        "inference_config_hash": config_hash, "taxonomy_version": tax.version,
        "taxonomy_hash": tax.file_hash,
        "assigned_reviews": len(assigned), "labelled_reviews": len(labelled),
        **stats,
        "kept_rows_with_no_themes": empty, "abstentions": abstained, "other_present": other,
        "theme_positives": dict(sorted(positives.items(), key=lambda kv: -kv[1])),
        "themes_below_min_support": [t for t, n in positives.items() if n < 10],
        "parse_failure_causes": {k: causes[k] for k in ("by_cause", "by_combination",
                                                        "by_theme_slot")},
        "run_ids": run_ids, "rows_without_run_id": rows_without_run_id,
        "note": "dropped_rows are dropped from classifier training only. The same rows score as "
                "empty predictions on an evaluation set (src/ai/label_usage.py); the two "
                "readings are asserted against each other in tests/test_label_usage.py.",
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    path = OUT_DIR / (args.out or
                      f"training-pool-{frozen.version}-{model.replace(':', '_')}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1) + "\n")

    print(f"\n{'theme':<22} {'positives':>9} {'prevalence':>11}")
    for theme, n in out["theme_positives"].items():
        print(f"{theme:<22} {n:>9} {n / max(kept, 1):>11.4f}")
    print(f"\nby status: {stats['by_status']}")
    print(f"POOL_CENSUS sample={SAMPLE} prompt={frozen.version} model={model} "
          f"config={config_hash[:12]} assigned={len(assigned)} labelled={len(labelled)} "
          f"trains_on={stats['training_rows']} dropped={stats['dropped_rows']} "
          f"drop_rate={stats['drop_rate']} dropped_by={stats['dropped_by_status'] or '{}'} "
          f"no_themes={empty} below_support={len(out['themes_below_min_support'])} "
          f"runs={len(run_ids)} rows_without_run_id={rows_without_run_id} "
          f"out={path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

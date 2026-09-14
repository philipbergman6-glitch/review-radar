"""Import the agent's blind ground-truth labels into gold.review_theme_labels (RR-21).

Job `theme_labels_reference`. Reads `eval/themes/reference-<sample>.jsonl` -- one line per
blind id, carrying exactly the ADR-0003 output object -- resolves each blind id through the
export's map file, and writes rows with `label_source="agent_reference"`.

Three things this refuses to do:

* **Repair.** Every submitted label goes through the same `validate_label` the model's output
  goes through, against the same taxonomy and the same word limits, including the check that
  each evidence quote occurs verbatim in the review. A quote the agent typed from memory
  fails exactly as a quote the model invented fails.
* **Import partially.** A single rejected label fails the whole run. Ground truth with holes
  in it silently shrinks the denominator of every metric that rests on it, so the agent
  re-labels and resubmits instead.
* **Call itself human.** `label_source` is `agent_reference`, a value distinct from `human`,
  which is reserved for Philip's 50-row adjudication subset.

Run:  ./run.sh python scripts/import_reference_labels.py --sample development [--scope full]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from typing import Any

from pyspark.sql import functions as F

from src.ai.labels import (
    decoding_schema,
    idempotency_key,
    load_spec,
    load_taxonomy,
    validate_label,
)
from src.ai.theme_labels import ensure_table, label_row, merge_chunk, table_name, terminal_status
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.console import line_buffered_stdout
from src.common.spark import build

line_buffered_stdout()

LABEL_SOURCE = "agent_reference"
MODEL_ID = "claude-opus-5"          # the annotator's identity, recorded like any other
REFERENCE_PROMPT_VERSION = "reference-blind-v1"
OUT_DIR = PROJECT_ROOT / "eval" / "themes"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit"])
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--chunk", type=int, default=50)
    args = ap.parse_args()
    t0 = time.time()

    blind_path = OUT_DIR / f"blind-{args.sample}.jsonl"
    map_path = OUT_DIR / f"blind-{args.sample}.map.json"
    labels_path = OUT_DIR / f"reference-{args.sample}.jsonl"
    for p in (blind_path, map_path, labels_path):
        if not p.exists():
            raise SystemExit(f"missing {p.relative_to(PROJECT_ROOT)}")
    blind = {json.loads(line)["blind_id"]: json.loads(line) for line in blind_path.read_text().splitlines() if line.strip()}
    mapping = json.loads(map_path.read_text())
    submitted = [json.loads(line) for line in labels_path.read_text().splitlines() if line.strip()]

    spec, tax = load_spec(), load_taxonomy()
    config_hash = spec.config_hash("label_v1", decoding_schema(tax.ids),
                                   extra={"taxonomy": tax.file_hash, "annotator": MODEL_ID,
                                          "protocol": REFERENCE_PROMPT_VERSION},
                                   model_id=MODEL_ID)
    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    sample_run = runs.latest_success("theme_samples", category=args.category, data_scope=args.scope,
                                     params_match={"sample": args.sample})
    if silver_run is None or sample_run is None:
        raise SystemExit("the reference import needs a successful silver run and the frame's own run")
    if sample_run["run_id"] != mapping["theme_samples_run_id"]:
        raise SystemExit(f"the blind export was drawn from theme_samples run "
                         f"{mapping['theme_samples_run_id']}, but the latest run for sample "
                         f"{args.sample!r} is {sample_run['run_id']}; re-export before importing")

    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    table = table_name(args.scope)
    run = runs.start("theme_labels_reference", runs.THEME_REFERENCE_SPEC_VERSION,
                     category=args.category, data_scope=args.scope,
                     inputs={"samples": {"run_id": sample_run["run_id"], "table": asg_out["table"],
                                         "snapshot_id": asg_out["snapshot_id"],
                                         "sample_name": args.sample},
                             "silver": {"run_id": silver_run["run_id"],
                                        "table": silver_run["outputs"]["silver.reviews"]["table"],
                                        "snapshot_id": silver_run["outputs"]["silver.reviews"]["snapshot_id"]},
                             "blind_export": {"path": str(blind_path.relative_to(PROJECT_ROOT)),
                                              "map_path": str(map_path.relative_to(PROJECT_ROOT)),
                                              "rows": len(blind),
                                              "sha256": hashlib.sha256(blind_path.read_bytes()).hexdigest()},
                             "taxonomy": {"path": "conf/theme-taxonomy.json",
                                          "version": tax.version, "file_hash": tax.file_hash}},
                     params={"budget_line": args.sample, "label_source": LABEL_SOURCE,
                             "annotator": MODEL_ID, "protocol": REFERENCE_PROMPT_VERSION,
                             "api_mode": "manual", "config_hash": config_hash})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"inference_config_hash": config_hash[:12],
                              "taxonomy_hash": tax.file_hash[:12],
                              "reviews_selected": len(blind), "labels_submitted": len(submitted)}
    try:
        spark = build("theme_labels_reference", cores="local[2]", driver_memory="2g")
        try:
            ensure_table(spark, table)
            seen: set[str] = set()
            rejected: list[str] = []
            rows: list[dict[str, Any]] = []
            theme_hits = {t: 0 for t in tax.ids}
            abstained = other_present = no_theme = 0
            for entry in submitted:
                bid = entry.get("blind_id")
                if bid not in blind:
                    rejected.append(f"{bid}: not in the blind export")
                    continue
                if bid in seen:
                    rejected.append(f"{bid}: submitted twice")
                    continue
                seen.add(bid)
                label = {k: v for k, v in entry.items() if k != "blind_id"}
                fails = validate_label(label, title=blind[bid]["title"], text=blind[bid]["text"],
                                       theme_ids=tax.ids, limits=spec.limits)
                if fails:
                    rejected.append(f"{bid}: {'; '.join(fails)}")
                    continue
                review_id = mapping["blind_ids"][bid]
                for t in label["themes"]:
                    theme_hits[t["theme_id"]] += 1
                abstained += 1 if label["abstain"] else 0
                other_present += 1 if label["other"]["present"] else 0
                no_theme += 1 if not label["themes"] and not label["abstain"] else 0
                rows.append(label_row(
                    idempotency_key=idempotency_key(
                        source_review_id=review_id, label_source=LABEL_SOURCE, model_id=MODEL_ID,
                        label_spec_version=spec.label_spec_version,
                        prompt_version=REFERENCE_PROMPT_VERSION, inference_config_hash=config_hash),
                    source_review_id=review_id, budget_line=args.sample, label_source=LABEL_SOURCE,
                    model_id=MODEL_ID, label_spec_version=spec.label_spec_version,
                    prompt_version=REFERENCE_PROMPT_VERSION, inference_config_hash=config_hash,
                    api_mode="manual", status=terminal_status("succeeded", label), parsed=label,
                    attempts=[{"attempt_no": 1, "provider_request_id": None,
                               "raw_response": json.dumps(label, ensure_ascii=False),
                               "validation_error": None, "input_tokens": None, "output_tokens": None,
                               "estimated_cost_usd": None, "duration_s": None,
                               "completed_at": None}],
                    source_silver_run_id=silver_run["run_id"], run_id=run.run_id))
            missing = sorted(set(blind) - seen)
            counts.update({"accepted": len(rows), "rejected": len(rejected), "abstained": abstained,
                           "theme_hits": theme_hits, "other_present": other_present,
                           "no_theme_labels": no_theme})
            if rejected or missing:
                for r in rejected[:20]:
                    print(f"[reference] REJECTED {r}")
                if missing:
                    print(f"[reference] MISSING {len(missing)} blind id(s): {missing[:10]}")
                raise RuntimeError(f"{len(rejected)} rejected, {len(missing)} missing; ground truth "
                                   "is never partially imported (RR-21)")
            for i in range(0, len(rows), args.chunk):
                merge_chunk(spark, table, rows[i:i + args.chunk])
            snap = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
            outputs["gold.review_theme_labels"] = {"table": table, "budget_line": args.sample,
                                                   "label_source": LABEL_SOURCE,
                                                   "snapshot_id": int(snap["snapshot_id"])}
            landed = spark.table(table).filter((F.col("inference_config_hash") == config_hash)
                                               & (F.col("label_source") == LABEL_SOURCE)
                                               & (F.col("budget_line") == args.sample))
            counts.update({"table_rows_for_config": landed.count(),
                           "distinct_keys_for_config": landed.select("idempotency_key").distinct().count(),
                           "elapsed_s": round(time.time() - t0, 1)})
        finally:
            spark.stop()
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["reviews_selected"], records_out=counts["accepted"],
                 records_rejected=counts["rejected"], outputs=outputs, counts=counts)
    print(f"REFERENCE_LABELS run_id={run.run_id} sample={args.sample} scope={args.scope} "
          f"annotator={MODEL_ID} source={LABEL_SOURCE} config={config_hash[:12]} "
          f"selected={counts['reviews_selected']} accepted={counts['accepted']} "
          f"rejected={counts['rejected']} abstained={counts['abstained']} "
          f"no_theme={counts['no_theme_labels']} other={counts['other_present']} "
          f"rows={counts['table_rows_for_config']} elapsed_s={counts['elapsed_s']}")


if __name__ == "__main__":
    main()

"""Stream gate: re-derive P8's control claim from the topic and the pinned snapshots.

The projection job records what it did; this decides whether that was right, and it decides it
from sources the job does not control:

* the **topic** is read again, in a plain batch job over the same offsets, through silver's
  validation -- so `records_read`, `rows_valid` and the duplicate count come from a second
  pass rather than from the run being judged;
* the **sort job's ledger row** supplies the expected duplicate count, recorded before any
  streaming job existed;
* **`src/spark/silver.py`** supplies the validation digest, so a streaming path that forked
  silver's parser is caught here and not by a code review someone remembered to do;
* **`gold.product_month`**, pinned to the snapshot the published gold run recorded, is what
  the projection is reconciled against, product-month by product-month.

The one number that cannot be re-derived is `numRowsDroppedByWatermark`: it is an event inside
a query that has ended, so it is read from the run's counts and named as such. That is the
same shape `SILVER_GATE` uses for the facts only the job could have observed.

For the **demo run** (`--run-kind demo`, ticket 16) two more sources join: the producer's
held-back sidecar names every row it released late, and the gate finds those rows in its own
re-read of the topic, aggregates the far slice through the stream's own expressions, and asks
whether gold minus those contributions is the projection, product-month by product-month. The
control run's artefact and ledger row are read too, because the demo's drop count means
nothing without a control run that dropped nothing under the same frozen protocol.

The decision is pure and lives in `src/gates/stream.py`. On the way out this writes
`eval/stream_control/gate.json` or `eval/stream_demo/gate.json`.

Exit 0 on STREAM_GATE=PASS, 1 otherwise.

Run:  ./run.sh python scripts/gate_stream.py [--scope full|sample] [--run-kind control|demo]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.spark import build
from src.gates import lineage as L
from src.gates import stream as gate
from src.ingest.lateness import InjectionSpec
from src.ingest.replay_config import load_replay_config
from src.ingest.sort_replay import file_sha256
from src.spark import stream_product_month as S
from src.spark.gold import gold_names

CAPABILITIES = {"control": "stream_control", "demo": "stream_demo"}


def reread_topic(spark: SparkSession, topic: str, *,
                 ingested_at: datetime) -> tuple[dict[str, int], DataFrame]:
    """The topic counted again, from the beginning, through silver's validation.

    A batch read of the same offsets the stream consumed. `rows_valid` and `duplicates` come
    from here rather than from the run's own tally, so a micro-batch the stream never saw is a
    disagreement between two passes instead of a smaller number nobody compares.

    `ingested_at` is the run's own recorded value, not this gate's clock. Silver's validation
    rejects a review timestamped after it was ingested, so a re-derivation that supplied
    today's date could decide a row's validity differently from the run it is judging -- and
    report the disagreement as a lost micro-batch.
    """
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    records = S.shaped(raw, ingested_at=ingested_at).cache()
    read = records.count()
    validated = S.VALIDATE(records)
    rejected = validated.filter(F.col("reject_reason").isNotNull()).count()
    typed = S.TYPE_ROWS(validated).cache()
    rows_valid = typed.count()
    distinct = typed.select("review_id").distinct().count()
    return ({"records_read": read, "rows_rejected": rejected, "rows_valid": rows_valid,
             "distinct_review_ids": distinct, "duplicates": rows_valid - distinct}, typed)


def held_back(replay_run: dict) -> tuple[dict[str, list[str]], str]:
    """The rows the producer released late, by slice, from the sidecar its ledger row names.

    Read from disk and checked against the recorded digest, so the list the gate explains the
    differences with is the list the producer actually sent from -- not a later plan over the
    same file.
    """
    entry = replay_run["outputs"]["held_back"]
    path = C.PROJECT_ROOT / entry["path"]
    if not path.exists():
        sys.exit(f"the replay's held-back sidecar {entry['path']} is missing: STREAM_GATE=FAIL")
    sha, _ = file_sha256(path)
    if sha != entry["sha256"]:
        sys.exit(f"{entry['path']} has sha256 {sha[:12]}… but the replay recorded "
                 f"{str(entry['sha256'])[:12]}…: the sidecar is not the one the run sent from: "
                 "STREAM_GATE=FAIL")
    by_slice: dict[str, list[str]] = {"near": [], "far": []}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            by_slice[row["slice"]].append(row["review_id"])
    if sum(len(v) for v in by_slice.values()) != int(entry["rows"]):
        sys.exit(f"{entry['path']} lists {sum(len(v) for v in by_slice.values())} rows, the "
                 f"replay recorded {entry['rows']}: STREAM_GATE=FAIL")
    return by_slice, sha


def dropped_contributions(typed: DataFrame, far_ids: list[str]) -> DataFrame:
    """What the far slice contributed to each product-month, through the stream's own
    expressions -- so the explanation is arithmetic the projection would agree with."""
    rows = S.typed_reviews(typed.filter(F.col("review_id").isin(far_ids)))
    return S.batch_aggregates(rows)


def reconcile(projection: DataFrame, gold: DataFrame,
              dropped: DataFrame | None = None) -> dict[str, int]:
    """Compare the projection with gold's spine, product-month by product-month.

    Only gold's *active* months take part: the spine carries every calendar month between a
    product's first and last review, empty ones included (ADR-0001), and the stream has no row
    to emit for a month in which nothing happened. An empty gold month with a stream row would
    still be caught -- it lands in `only_in_stream`.

    Gold also materialises only products with enough reviews to ever fill a decline window, so
    a product the stream carries and gold never wrote is not a disagreement about arithmetic --
    it is the two tables answering different questions, and gold says which products it left
    out and why (`products_below_min_reviews`). Those rows are counted separately as
    `only_in_stream_unmaterialised` and named in the gate line rather than folded into the
    verdict.

    What is left over after that split *is* a disagreement, and the gate blocks on it: a
    product gold did materialise, in a month gold recorded no reviews for, or in no month at
    all. That is the stream claiming reviews batch never saw.

    `dropped` (the demo run) is what the far slice contributed to each product-month. With it,
    a differing product-month is *explained* when the projection equals gold minus that
    contribution in every reconciled column, and a gold month the projection has no row for
    is explained when the contribution *is* gold's row -- the whole month was held back. What
    is neither is unexplained, and the demo gate blocks on it exactly as the control gate
    blocks on any difference at all.
    """
    cols = list(gate.RECONCILED_COLUMNS)
    g = gold.filter(F.col("review_count") > 0).select(
        "parent_asin", "month", *[F.col(c).alias(f"g_{c}") for c in cols])
    s = projection.select("parent_asin", "month", *[F.col(c).alias(f"s_{c}") for c in cols])
    joined = s.join(g, on=["parent_asin", "month"], how="full_outer")
    if dropped is not None:
        x = dropped.select("parent_asin", "month", *[F.col(c).alias(f"x_{c}") for c in cols])
        joined = joined.join(x, on=["parent_asin", "month"], how="full_outer")
    else:
        for c in cols:
            joined = joined.withColumn(f"x_{c}", F.lit(None).cast("double"))
    joined = joined.cache()

    only_stream = joined.filter(F.col("g_review_count").isNull()
                                & F.col("s_review_count").isNotNull())
    only_gold = joined.filter(F.col("s_review_count").isNull()
                              & F.col("g_review_count").isNotNull())
    both = joined.filter(F.col("g_review_count").isNotNull()
                         & F.col("s_review_count").isNotNull())
    # Rating sums are doubles built from integer ratings, so equality is exact here; using a
    # tolerance would hide exactly the kind of drift this gate exists to find.
    differs = F.lit(False)
    explained = F.col("x_review_count").isNotNull()
    whole_month = F.col("x_review_count").isNotNull()
    for c in cols:
        differs = differs | ~F.col(f"s_{c}").eqNullSafe(F.col(f"g_{c}"))
        explained = explained & (F.col(f"s_{c}") == F.col(f"g_{c}") - F.col(f"x_{c}"))
        whole_month = whole_month & (F.col(f"g_{c}") == F.col(f"x_{c}"))
    # Every product gold materialised, from the whole spine rather than from the active
    # months: a product gold wrote is materialised even in the months it wrote as empty.
    materialised = gold.select("parent_asin").distinct()
    unmaterialised = only_stream.join(materialised, on="parent_asin", how="left_anti").count()
    differing = both.filter(differs)
    out = {"compared": both.count(),
           "differing": differing.count(),
           "only_in_stream": only_stream.count() - unmaterialised,
           "only_in_stream_unmaterialised": unmaterialised,
           "only_in_gold": only_gold.count()}
    if dropped is not None:
        x_rows = joined.filter(F.col("x_review_count").isNotNull())
        out.update({
            "differing_explained": differing.filter(explained).count(),
            "differing_unexplained": differing.filter(~explained).count(),
            "only_in_gold_explained": only_gold.filter(whole_month).count(),
            "only_in_gold_unexplained": only_gold.filter(~whole_month).count(),
            "dropped_product_months": x_rows.count(),
            # Far rows in products gold never materialised: they explain nothing because
            # there is nothing in gold to differ from. Reported, not failed.
            "dropped_outside_gold": x_rows.filter(F.col("g_review_count").isNull()).count(),
        })
    return out


def control_facts(category: str, scope: str) -> dict:
    """The control run the demo leans on: its artefact, and the ledger row it judged."""
    path = E.EVAL_ROOT / CAPABILITIES["control"] / "gate.json"
    doc = json.loads(path.read_text()) if path.exists() else {}
    current = runs.latest_success("stream_aggregate", category=category, data_scope=scope,
                                  params_match={"run_kind": "control"})
    judged = runs.by_id(doc["pipeline_run_id"]) if doc.get("pipeline_run_id") else None
    terminal = (doc.get("constituents") or [""])[-1]
    fields = dict(kv.split("=", 1) for kv in terminal.split() if "=" in kv)
    return {"run_id": current["run_id"] if current else "",
            "ledger_run_found": current is not None,
            "artifact_run_id": doc.get("pipeline_run_id", ""),
            "status": doc.get("status", "MISSING"),
            "scope": doc.get("scope", ""),
            "ledger_status": judged["status"] if judged else "missing",
            "natural_drops": int(fields.get("natural_drops", -1)),
            "differing": int(fields.get("differing_product_months", -1)),
            "protocol_hash": doc.get("protocol_hash", "")}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--run-kind", default="control", choices=list(CAPABILITIES))
    args = ap.parse_args()
    capability = CAPABILITIES[args.run_kind]

    row = runs.latest_success("stream_aggregate", category=args.category, data_scope=args.scope,
                              params_match={"run_kind": args.run_kind})
    if row is None:
        sys.exit(f"no successful {args.run_kind} stream_aggregate run for "
                 f"{args.category}/{args.scope}: STREAM_GATE=FAIL")
    # The runs this gate judges against are the ones the projection *recorded*, not whichever
    # ran last. A later `make sort-replay` over a changed raw file records a different
    # `key_collision_rows`, and comparing the topic's duplicates against that number would be
    # comparing a measurement to an unrelated run.
    sort_run = runs.by_id(row["inputs"]["source"]["run_id"])
    replay_run = runs.by_id(row["inputs"]["replay"]["run_id"])
    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    gold_run = runs.latest_success("gold", category=args.category, data_scope=args.scope)
    if sort_run is None or replay_run is None:
        sys.exit("the stream run names a sort_replay or stream_produce run that is not in the "
                 "ledger: STREAM_GATE=FAIL")
    if silver_run is None or gold_run is None:
        sys.exit("the stream gate needs a successful silver run (for the collision classes the "
                 "reconciliation rests on) and a successful gold run (to reconcile against): "
                 "STREAM_GATE=FAIL")

    counts, inputs, outputs = row["counts"], row["inputs"], row["outputs"]
    topic = inputs["replay"]["topic"]
    gold_out = gold_run["outputs"]["gold.product_month"]

    slices, held_sha = held_back(replay_run) if args.run_kind == "demo" else ({}, "")
    injection: dict = {}
    spark = build("gate-stream")
    try:
        topic_facts, typed = reread_topic(
            spark, topic, ingested_at=datetime.fromisoformat(counts["ingested_at"]))
        projection = spark.read.option("snapshot-id",
                                       outputs["stream.product_month"]["snapshot_id"]) \
            .table(outputs["stream.product_month"]["table"])
        gold = spark.read.option("snapshot-id", gold_out["snapshot_id"]).table(gold_out["table"])
        dropped = None
        if args.run_kind == "demo":
            rc = replay_run["counts"]
            on_topic = {name: typed.filter(F.col("review_id").isin(ids)).count()
                        for name, ids in slices.items()}
            dropped = dropped_contributions(typed, slices["far"]).cache()
            # `frozen` is read from conf/stream_replay.toml here, not from the run: the
            # counts the replay recorded are the run's own claim about what it injected, and
            # a gate that checked a run against itself would pass a run that injected 1,999.
            frozen = InjectionSpec.from_config(load_replay_config())
            injection = {"near_rows": int(rc["near_rows"]), "far_rows": int(rc["far_rows"]),
                         "near_lag_days": int(rc["near_lag_days"]),
                         "far_lag_days": int(rc["far_lag_days"]),
                         "held_back_sha256": held_sha,
                         "near_on_topic": on_topic["near"], "far_on_topic": on_topic["far"],
                         "expected_drops": int(rc["expected_drops"]),
                         "frozen": {"near_rows": frozen.near_rows, "far_rows": frozen.far_rows,
                                    "near_lag_days": frozen.near_lag_days,
                                    "far_lag_days": frozen.far_lag_days,
                                    "watermark_days": frozen.watermark_days}}
        recon = reconcile(projection, gold, dropped)
    finally:
        spark.stop()

    sc = silver_run["counts"]
    facts = {
        "replay_run_id": replay_run["run_id"],
        "topic": topic,
        # The *replay's* acked count, on a line whose every other field describes the replay.
        # The stream's own read count is `ledger_records_read` below, and the two coincide only
        # when the stream read the whole topic -- which is what `topic_fully_read` asserts.
        "records_acked": int(replay_run["counts"]["records_acked"]),
        "ingested_at": counts.get("ingested_at", ""),
        "source_sha256": inputs["source"]["sha256"],
        "replay_config_hash": counts.get("replay_config_hash", ""),
        "validation": {"function": "src.spark.silver.parse_and_validate",
                       "recorded": counts.get("validation_source_sha256", ""),
                       "expected": S.validation_digest()},
        "dedupe": {"records_read": topic_facts["records_read"],
                   "ledger_read": int(counts["records_read"]),
                   "rows_valid": topic_facts["rows_valid"],
                   "rows_rejected": topic_facts["rows_rejected"],
                   "unique": int(counts["unique_review_ids"]),
                   "dropped": topic_facts["duplicates"],
                   "expected": int(sort_run["counts"]["key_collision_rows"]),
                   "sort_run_id": sort_run["run_id"]},
        "watermark": {"delay": counts.get("watermark", ""),
                      "natural_drops": int(counts["natural_drops"])},
        "survivor": {"silver_run_id": silver_run["run_id"],
                     "groups": int(sc["collision_groups"]),
                     "exact": int(sc["exact_groups"]),
                     "conflicting": int(sc["conflicting_groups"]),
                     "unresolvable": int(sc["unresolvable_groups"])},
        "reconcile": {"gold_run_id": gold_run["run_id"], **recon},
    }
    notes = [(f"reconciled against {gold_names(args.scope)['product_month']}@"
              f"{gold_out['snapshot_id']} (gold run {gold_run['run_id'][:8]})"),
             (f"watermark {counts.get('watermark')}, "
              f"{counts['micro_batches']} micro-batch(es)"),
             ("distinct_users is not projected: a distinct count cannot be summed "
              "across micro-batch contributions")]

    if args.run_kind == "demo":
        facts["injection"] = injection
        facts["control"] = control_facts(args.category, args.scope)
        v = gate.demo(facts, run_id=row["run_id"], scope=args.scope)
        population = gate.demo_population(facts, category=args.category)
        notes.append(f"lateness injected from {replay_run['outputs']['held_back']['path']}: "
                     f"near slice {injection['near_rows']} rows {injection['near_lag_days']} "
                     f"days late (accepted), far slice {injection['far_rows']} rows "
                     f"{injection['far_lag_days']} days late (dropped); the near slice's "
                     "acceptance is derived from the drop count and the explained differences, "
                     "not observed per row")
        notes.append(f"control run {facts['control']['run_id'][:8]} from "
                     f"eval/{CAPABILITIES['control']}/gate.json")
    else:
        v = gate.control(facts, run_id=row["run_id"], scope=args.scope)
        population = gate.population(facts, category=args.category)

    v = L.attest(v, capability)
    v.emit()
    E.record(v, capability=capability, phase="P8 Stream", kind="reproducibility",
             protocol_hash=counts.get("replay_config_hash", ""),
             population=population, pipeline_run_id=row["run_id"], scope=args.scope,
             notes=notes)
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

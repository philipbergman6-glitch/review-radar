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

The decision is pure and lives in `src/gates/stream.py`. On the way out this writes
`eval/stream_control/gate.json`.

Exit 0 on STREAM_GATE=PASS, 1 otherwise.

Run:  ./run.sh python scripts/gate_stream.py [--scope full|sample]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.spark import build
from src.gates import lineage as L
from src.gates import stream as gate
from src.spark import stream_product_month as S
from src.spark.gold import gold_names

CAPABILITY = "stream_control"


def reread_topic(spark: SparkSession, topic: str, *, ingested_at: datetime) -> dict[str, int]:
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
    valid = S.TYPE_ROWS(validated).select("review_id").cache()
    rows_valid = valid.count()
    distinct = valid.select("review_id").distinct().count()
    return {"records_read": read, "rows_rejected": rejected, "rows_valid": rows_valid,
            "distinct_review_ids": distinct, "duplicates": rows_valid - distinct}


def reconcile(projection: DataFrame, gold: DataFrame) -> dict[str, int]:
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
    """
    cols = list(gate.RECONCILED_COLUMNS)
    g = gold.filter(F.col("review_count") > 0).select(
        "parent_asin", "month", *[F.col(c).alias(f"g_{c}") for c in cols])
    s = projection.select("parent_asin", "month", *[F.col(c).alias(f"s_{c}") for c in cols])
    joined = s.join(g, on=["parent_asin", "month"], how="full_outer").cache()

    only_stream = joined.filter(F.col("g_review_count").isNull())
    only_gold = joined.filter(F.col("s_review_count").isNull())
    both = joined.filter(F.col("g_review_count").isNotNull()
                         & F.col("s_review_count").isNotNull())
    # Rating sums are doubles built from integer ratings, so equality is exact here; using a
    # tolerance would hide exactly the kind of drift this gate exists to find.
    differs = F.lit(False)
    for c in cols:
        differs = differs | ~F.col(f"s_{c}").eqNullSafe(F.col(f"g_{c}"))
    # Every product gold materialised, from the whole spine rather than from the active
    # months: a product gold wrote is materialised even in the months it wrote as empty.
    materialised = gold.select("parent_asin").distinct()
    unmaterialised = only_stream.join(materialised, on="parent_asin", how="left_anti").count()
    return {"compared": both.count(),
            "differing": both.filter(differs).count(),
            "only_in_stream": only_stream.count() - unmaterialised,
            "only_in_stream_unmaterialised": unmaterialised,
            "only_in_gold": only_gold.count()}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    row = runs.latest_success("stream_aggregate", category=args.category, data_scope=args.scope)
    if row is None:
        sys.exit(f"no successful stream_aggregate run for {args.category}/{args.scope}: "
                 "STREAM_GATE=FAIL")
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

    spark = build("gate-stream")
    try:
        topic_facts = reread_topic(
            spark, topic, ingested_at=datetime.fromisoformat(counts["ingested_at"]))
        projection = spark.read.option("snapshot-id",
                                       outputs["stream.product_month"]["snapshot_id"]) \
            .table(outputs["stream.product_month"]["table"])
        gold = spark.read.option("snapshot-id", gold_out["snapshot_id"]).table(gold_out["table"])
        recon = reconcile(projection, gold)
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

    v = L.attest(gate.control(facts, run_id=row["run_id"], scope=args.scope), CAPABILITY)
    v.emit()
    E.record(v, capability=CAPABILITY, phase="P8 Stream", kind="reproducibility",
             protocol_hash=counts.get("replay_config_hash", ""),
             population=gate.population(facts, category=args.category),
             pipeline_run_id=row["run_id"], scope=args.scope,
             notes=[(f"reconciled against {gold_names(args.scope)['product_month']}@"
                     f"{gold_out['snapshot_id']} (gold run {gold_run['run_id'][:8]})"),
                    (f"watermark {counts.get('watermark')}, "
                     f"{counts['micro_batches']} micro-batch(es)"),
                    ("distinct_users is not projected: a distinct count cannot be summed "
                     "across micro-batch contributions")])
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

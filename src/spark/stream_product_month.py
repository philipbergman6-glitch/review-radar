"""Job `stream_aggregate`: the P8 projection that sits beside batch and must agree with it.

One Structured Streaming query reads the event-time-sorted replay off `reviews.stream`,
validates it **with silver's own function**, deduplicates review ids inside a 30-day event-time
watermark, and writes product-and-calendar-month aggregates. Batch silver and gold stay the
only source of truth (ADR-0010); this table is a demonstration projection whose whole claim is
that it reconciles.

Three things about the shape are worth being able to say out loud.

**Validation is imported, not reimplemented.** `parse_and_validate` and `typed_valid_rows` are
silver's, bound here as `VALIDATE` and `TYPE_ROWS`. A streaming copy that drifted would make
the two paths disagree about what a review *is*, and every aggregate downstream would then
agree with the wrong thing while still reconciling against itself. The run records the digest
of the source it actually called, and `STREAM_GATE` recomputes it from `src/spark/silver.py`,
so a fork is caught as a fact about the run rather than as a code review someone remembered to
do.

**The aggregate is append-only contributions plus a summed projection.** ADR-0010 asks for
append-mode aggregates per product and calendar month, and Spark cannot express that directly:
`window()` refuses any interval carrying months, so there is no calendar-month time window to
group by and no event-time attribute in a `date_trunc` grouping for append mode to key its
state on. What is expressible, and is what runs here, is a `foreachBatch` that appends each
micro-batch's partial aggregate to `stream.product_month_batches` and a final, atomic sum of
those contributions into `stream.product_month`. Every projected column is additive, which is
what makes the sum equal to the aggregate. `distinct_users` is the column that is not, so the
projection does not carry it and `STREAM_GATE` names it as unprojected rather than letting it
go quietly missing.

**Duplicate removal is exact here, not approximate.** `dropDuplicatesWithinWatermark` is only
correct when duplicates cannot straddle the watermark. In this source they cannot: `review_id`
is a hash of (user, product, timestamp), so two rows sharing an id share an event time to the
millisecond. The count it removes is therefore predictable before the run -- `sort_replay`
recorded it as `key_collision_rows` -- and the gate asserts the two agree.

The control run (ticket 15) reads a topic that holds the sorted file and nothing else, so its
`numRowsDroppedByWatermark` must be zero. The demo run (`--run-kind demo`, ticket 16) reads the
topic into which the producer released the held-back slices late, and that number is then
predicted -- the far slice, exactly -- rather than zero. The job is the same job either way:
it names the replay it consumed, and `STREAM_GATE` decides what the number must be.

Run:  ./run.sh python -m src.spark.stream_product_month [--scope full|sample] [--run-kind control|demo]
"""
from __future__ import annotations

import argparse
import hashlib
import inspect
import time
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import runs
from src.common.console import line_buffered_stdout
from src.common.spark import CATALOG, build
from src.ingest.replay_config import ReplayConfig, load_replay_config
from src.spark import silver
from src.spark.gold import monthly_aggregates
from src.spark.silver import digest, write_replace

line_buffered_stdout()

#: Silver's validation, bound rather than reimplemented. The names exist so the sharing is a
#: fact a test can assert (`stream.VALIDATE is silver.parse_and_validate`) and so the digest
#: below has something to name.
VALIDATE = silver.parse_and_validate
TYPE_ROWS = silver.typed_valid_rows

#: Every function whose behaviour decides what the two paths consider a valid review, in the
#: order they are applied. The digest over their source is what `STREAM_GATE` checks.
SHARED_WITH_SILVER = (silver.parse_and_validate, silver.typed_valid_rows,
                      silver.review_id_col, silver.word_count_col, silver.images_json_col)

#: The projected columns. Additive, so the projection is the sum of its micro-batch
#: contributions; see the module docstring for why `distinct_users` is not among them.
SUMMED = ("review_count", "rating_sum", "neg_count", "verified_count", "verified_rating_sum",
          "nonempty_text_count", "long_text_count")

#: How many per-batch progress reports Spark keeps. The run's `records_read` is the sum over
#: them, so a query with more batches than this would report a number missing its earliest
#: batches -- and the contract would then fail on arithmetic rather than on the truth, which is
#: that the evidence was thrown away. Raised well past any batch count this job produces, and
#: checked rather than trusted (`progress_counts`).
PROGRESS_RETAINED_CONF = "spark.sql.streaming.numRecentProgressUpdates"
PROGRESS_RETAINED = 1000


def validation_digest() -> str:
    """SHA-256 over the source of the functions the two paths share.

    A digest rather than an import check, because an import proves only that the symbol
    resolved: a stream that imported silver's parser and then wrapped it in a locally patched
    predicate would still pass an identity test. The digest moves whenever the behaviour
    the two paths share moves, which is the property the gate actually needs.
    """
    h = hashlib.sha256()
    for fn in SHARED_WITH_SILVER:
        h.update(f"{fn.__module__}.{fn.__qualname__}\x1f".encode())
        h.update(inspect.getsource(fn).encode())
    return h.hexdigest()


# ------------------------------------------------------------------ the stream ----
def shaped(raw: DataFrame, *, ingested_at: datetime) -> DataFrame:
    """Kafka records in the shape silver's parser expects (the same shape bronze writes).

    `ingested_at` is not the wall clock. Silver's validation rejects a review timestamped
    after the moment it was ingested, and batch silver answers "when" from the drain time
    bronze stored per row. A stream that answered it with `current_timestamp()` would make the
    same function decide differently on a rerun -- a review dated next week is invalid today
    and valid a month from now -- so the projection would stop being reproducible from the same
    topic. The replay's own start time is the honest fixed answer: these rows reached the
    topic during that run, and the ledger records when it began.
    """
    return raw.select(
        F.col("value").cast("string").alias("payload"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.lit(ingested_at).cast("timestamp").alias("ingested_at"))


def review_rows(records: DataFrame) -> DataFrame:
    """Validated, typed reviews with the two derived columns gold's aggregation reads.

    `event_ts`, `review_month` and `text_word_count` are computed exactly as silver computes
    them (`src/spark/silver.py`), from the shared column expressions -- this is projection, not
    a second opinion about what a review month is.
    """
    return typed_reviews(TYPE_ROWS(VALIDATE(records)))


def typed_reviews(typed: DataFrame) -> DataFrame:
    """The projection's columns from silver-typed rows; `review_rows` after validation.

    Split out so `STREAM_GATE` can aggregate the held-back rows it finds in its own re-read of
    the topic through exactly the expressions the stream used (ticket 16).
    """
    event_ts = F.timestamp_millis(F.col("timestamp_ms"))
    return typed.select(
        "review_id", "parent_asin", "user_id", "rating", "verified_purchase",
        event_ts.alias("event_ts"),
        F.to_date(F.date_trunc("month", event_ts)).alias("review_month"),
        silver.word_count_col(F.col("text")).alias("text_word_count"))


def deduplicated(reviews: DataFrame, *, watermark: str) -> DataFrame:
    """One row per review id, within the watermark. Exact for this source -- see the docstring."""
    return (reviews.withWatermark("event_ts", watermark)
            .dropDuplicatesWithinWatermark(["review_id"]))


def batch_aggregates(reviews: DataFrame) -> DataFrame:
    """One micro-batch's contribution to each (product, month) it touched.

    Gold's own `monthly_aggregates`, so the two paths cannot disagree about what a month's
    statistics are either; `distinct_users` is dropped because it is the one column that
    cannot be summed across contributions.
    """
    return monthly_aggregates(reviews).select("parent_asin", "month", *SUMMED)


# ---------------------------------------------------------------------- tables ----
def stream_names(scope: str) -> dict[str, str]:
    suffix = "" if scope == "full" else f"_{scope}"
    return {"product_month": f"{CATALOG}.stream.product_month{suffix}",
            "batches": f"{CATALOG}.stream.product_month_batches{suffix}"}


def ensure_tables(spark: SparkSession, names: dict[str, str]) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.stream")
    props = ("TBLPROPERTIES ('write.format.default'='parquet', "
             "'write.parquet.compression-codec'='zstd', 'format-version'='2')")
    summed = ", ".join(f"{c} {'DOUBLE' if c.endswith('_sum') else 'INT'}" for c in SUMMED)
    # Partitioned by run_id: several control runs live in this table at once and the
    # projection must sum one run's contributions, never a mixture of two.
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['batches']} (
        parent_asin STRING, month STRING, batch_id BIGINT, {summed}, run_id STRING
    ) USING iceberg PARTITIONED BY (run_id) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['product_month']} (
        parent_asin STRING, month STRING, month_date DATE, {summed},
        mean_rating DOUBLE, neg_share DOUBLE, verified_mean DOUBLE,
        source_replay_run_id STRING, run_id STRING
    ) USING iceberg PARTITIONED BY (years(month_date)) {props}""")


def project(batches: DataFrame, *, replay_run_id: str, run_id: str) -> DataFrame:
    """The projection: the run's micro-batch contributions summed, with the derived rates."""
    summed = batches.groupBy("parent_asin", "month").agg(
        *[F.sum(c).alias(c) for c in SUMMED])
    return (summed
            .withColumn("month_date", F.to_date(F.concat(F.col("month"), F.lit("-01"))))
            .withColumn("mean_rating", F.when(F.col("review_count") > 0,
                                              F.col("rating_sum") / F.col("review_count")))
            .withColumn("neg_share", F.when(F.col("review_count") > 0,
                                            F.col("neg_count") / F.col("review_count")))
            .withColumn("verified_mean", F.when(F.col("verified_count") > 0,
                                                F.col("verified_rating_sum")
                                                / F.col("verified_count")))
            .withColumn("source_replay_run_id", F.lit(replay_run_id))
            .withColumn("run_id", F.lit(run_id))
            .select("parent_asin", "month", "month_date", *SUMMED,
                    "mean_rating", "neg_share", "verified_mean",
                    "source_replay_run_id", "run_id"))


# ------------------------------------------------------------------------ run ----
def progress_counts(query: Any, *, spark: SparkSession) -> dict[str, int]:
    """What the query itself says it did: rows read, rows the watermark dropped, batches.

    Read off `recentProgress` rather than tallied by this job, because the two numbers that
    matter here are Spark's own. `numRowsDroppedByWatermark` in particular is the only
    trustworthy source for the control run's central claim: a count this job derived would be
    a count of rows it already knows it kept.
    """
    retained = int(spark.conf.get(PROGRESS_RETAINED_CONF))
    if len(query.recentProgress) >= retained:
        raise RuntimeError(
            f"the query reported {len(query.recentProgress)} progress updates and Spark "
            f"retains {retained} ({PROGRESS_RETAINED_CONF}); the earliest batches have been "
            "evicted and `records_read` would silently undercount. Raise the setting or the "
            "batch size.")
    read = late = batches = 0
    for p in query.recentProgress:
        rows = int(p.get("numInputRows", 0) or 0)
        batches += 1
        read += rows
        for op in p.get("stateOperators", []) or []:
            late += int(op.get("numRowsDroppedByWatermark", 0) or 0)
    return {"records_read": read, "natural_drops": late, "micro_batches": batches}


def run_stream(spark: SparkSession, *, scope: str, category: str, cfg: ReplayConfig,
               replay_run: dict[str, Any], sort_run: dict[str, Any],
               run_kind: str = "control") -> dict[str, Any]:
    t0 = time.time()
    # The topic the replay *recorded*, checked against the one the frozen config names for
    # this run kind: a demo projection draining the control topic would print zero drops and
    # call the injection a success.
    topic = replay_run["outputs"]["kafka"]["topic"]
    if topic != cfg.topic_for(scope, run_kind):
        raise SystemExit(f"replay run {replay_run['run_id'][:8]} wrote to '{topic}' but the "
                         f"frozen config names '{cfg.topic_for(scope, run_kind)}' for a "
                         f"{run_kind} run at scope={scope}")
    names = stream_names(scope)
    checkpoint = C.CHECKPOINTS / f"stream_product_month_{scope}_{run_kind}"
    source = sort_run["outputs"]["sorted_file"]

    run = runs.start(
        "stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
        category=category, data_scope=scope,
        inputs={"replay": {"run_id": replay_run["run_id"], "topic": topic},
                "source": {"run_id": sort_run["run_id"], "path": source["path"],
                           "sha256": source["sha256"]},
                "protocol": {"path": str(cfg.path.relative_to(C.PROJECT_ROOT)),
                             "status": cfg.status, "config_hash": cfg.config_hash}},
        params={"run_kind": run_kind, "watermark": cfg.watermark,
                "max_offsets_per_trigger": cfg.max_offsets_per_trigger,
                "replay_config_hash": cfg.config_hash,
                "validation_source_sha256": validation_digest()})

    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        ensure_tables(spark, names)
        # A control run is a measurement, not a resumption: a checkpoint left over from an
        # earlier run would make the topic partly unread and the projection partly this run's.
        _clear(checkpoint)

        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
               .option("failOnDataLoss", "true")
               .load())
        stream = deduplicated(review_rows(shaped(raw, ingested_at=replay_run["started_at"])),
                              watermark=cfg.watermark)

        projected = {"n": 0}

        def write_batch(batch: DataFrame, batch_id: int) -> None:
            batch = batch.persist()
            try:
                rows = batch.count()
                projected["n"] += rows
                print(f"[stream] batch {batch_id}: {rows:,} deduplicated rows "
                      f"({projected['n']:,} so far)")
                (batch_aggregates(batch)
                 .withColumn("batch_id", F.lit(batch_id).cast("long"))
                 .withColumn("run_id", F.lit(run.run_id))
                 .select("parent_asin", "month", "batch_id", *SUMMED, "run_id")
                 .writeTo(names["batches"])
                 .option("snapshot-property.run_id", run.run_id)
                 .append())
            finally:
                batch.unpersist()

        query = (stream.writeStream
                 .outputMode("append")
                 .option("checkpointLocation", str(checkpoint))
                 .foreachBatch(write_batch)
                 .trigger(availableNow=True)
                 .start())
        print(f"[stream] {run_kind} run: draining '{topic}' under a {cfg.watermark} watermark "
              f"({cfg.max_offsets_per_trigger:,} offsets per batch)")
        query.awaitTermination()
        counts.update(progress_counts(query, spark=spark))
        print(f"[stream] query finished: {counts['records_read']:,} read over "
              f"{counts['micro_batches']} batch(es), {counts['natural_drops']:,} dropped by "
              "the watermark")

        # Same staleness as `_latest_snapshot` guards: this table was created empty at the
        # start of the run and the catalog caches it for thirty seconds.
        spark.sql(f"REFRESH TABLE {names['batches']}")
        batches = (spark.read.table(names["batches"])
                   .filter(F.col("run_id") == run.run_id))
        outputs["stream.product_month_batches"] = {
            "table": names["batches"],
            "snapshot_id": _latest_snapshot(spark, names["batches"], run.run_id)}

        projection = project(batches, replay_run_id=replay_run["run_id"],
                             run_id=run.run_id).cache()
        product_months = projection.count()
        on_projection = int(projection.agg(F.sum("review_count")).first()[0] or 0)
        outputs["stream.product_month"] = {
            "table": names["product_month"],
            "snapshot_id": write_replace(projection, names["product_month"], run.run_id)}

        counts.update({
            "unique_review_ids": projected["n"],
            "run_kind": run_kind,
            "product_months": product_months,
            "reviews_on_projection": on_projection,
            "ingested_at": replay_run["started_at"].isoformat(),
            "watermark": cfg.watermark,
            "validation_source_sha256": validation_digest(),
            "replay_config_hash": cfg.config_hash,
            "digests": {"product_month": digest(projection, "parent_asin", "month",
                                                "review_count", "rating_sum", "verified_count",
                                                "long_text_count")},
            "elapsed_s": round(time.time() - t0, 1),
        })
        projection.unpersist()
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["records_read"], records_out=counts["product_months"],
                 records_rejected=counts["records_read"] - counts["unique_review_ids"],
                 outputs=outputs, counts=counts)
    print(f"[stream] read {counts['records_read']:,}, projected {counts['unique_review_ids']:,} "
          f"reviews over {counts['product_months']:,} product-months in "
          f"{counts['micro_batches']} micro-batch(es); "
          f"watermark dropped {counts['natural_drops']:,}")
    print("[stream] STREAM_GATE is printed by `make gate-stream`, which re-derives the "
          "reconciliation against gold from the pinned snapshots")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def _clear(path) -> None:
    import shutil
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _latest_snapshot(spark: SparkSession, table: str, run_id: str) -> int:
    # The Iceberg catalog caches table instances for 30 seconds, and this table was loaded
    # empty when the run created it. A control run that drains the topic in less than that
    # would otherwise read its own metadata from before its own writes and conclude that it
    # never wrote anything.
    spark.sql(f"REFRESH TABLE {table}")
    row = spark.sql(f"SELECT snapshot_id, summary['run_id'] AS rid FROM {table}.snapshots "
                    "ORDER BY committed_at DESC LIMIT 1").first()
    if row is None or row["rid"] != run_id:
        raise RuntimeError(f"{table}: newest snapshot is not stamped with run {run_id}")
    return int(row["snapshot_id"])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--run-kind", default="control", choices=["control", "demo"],
                    help="which replay to project: the control run's or the demo run's, "
                         "each on its own topic (ticket 16)")
    args = ap.parse_args()

    cfg = load_replay_config()
    # Pinned by run kind, not by recency: after the demo replay, "the latest stream_produce
    # run" is the demo's, and a control projection built on it would drain injected lateness.
    replay_run = runs.latest_success("stream_produce", category=args.category,
                                     data_scope=args.scope,
                                     params_match={"run_kind": args.run_kind})
    if replay_run is None:
        raise SystemExit(f"no successful {args.run_kind} stream_produce run for "
                         f"{args.category}/{args.scope}; run `make stream-produce"
                         f"{'-demo' if args.run_kind == 'demo' else ''}` first -- the "
                         "projection names the replay it consumed")
    sort_run = runs.latest_success("sort_replay", category=args.category, data_scope=args.scope)
    if sort_run is None:
        raise SystemExit(f"no successful sort_replay run for {args.category}/{args.scope}; "
                         "run `make sort-replay` first")
    # A replay that stopped early (`--limit`, a Ctrl-C) leaves a successful ledger row and a
    # partial topic, and the projection would then reconcile against gold over whatever
    # fraction of history happened to arrive -- and disagree everywhere else for a reason no
    # constituent would name. The topic has to hold the whole sorted file or there is nothing
    # here worth reconciling.
    acked = int(replay_run["counts"].get("records_acked") or 0)
    sorted_rows = int(sort_run["counts"].get("rows_sorted") or 0)
    if acked != sorted_rows:
        raise SystemExit(
            f"the latest stream_produce run {replay_run['run_id'][:8]} acknowledged {acked:,} "
            f"records but sort_replay {sort_run['run_id'][:8]} wrote {sorted_rows:,}: the "
            "topic holds a partial replay. Re-run `make stream-produce` without --limit.")

    spark = build("stream-product-month")
    spark.conf.set(PROGRESS_RETAINED_CONF, str(PROGRESS_RETAINED))
    try:
        run_stream(spark, scope=args.scope, category=args.category, cfg=cfg,
                   replay_run=replay_run, sort_run=sort_run, run_kind=args.run_kind)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

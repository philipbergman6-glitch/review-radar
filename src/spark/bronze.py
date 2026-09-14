"""Bronze layer: Kafka -> Iceberg, raw and unparsed.

The bronze table stores the review payload exactly as it arrived, as a string,
alongside the Kafka coordinates it arrived at. Nothing is parsed, validated or
dropped here. That is deliberate: if the parsing logic in silver turns out to be
wrong, bronze still holds the original bytes and the pipeline can be replayed
without going back to the source. A transformation bug should never be able to
destroy data.

Exactly-once, and why it actually holds:

* Spark checkpoints the Kafka offsets it has committed for each micro-batch.
* Iceberg's streaming sink records, in the table metadata, the last batch id it
  committed. On restart, a batch that was already committed is recognised and
  skipped rather than appended twice.
* Together those give effectively-exactly-once appends across a restart, which
  is what `scripts/prove_exactly_once.py` demonstrates by killing the job
  mid-stream.

Run:
    ./run.sh python -m src.spark.bronze --trigger once      # drain what is on the topic
    ./run.sh python -m src.spark.bronze --trigger 5s        # keep running
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

from pyspark.sql import functions as F

from src.common import config as C
from src.common.console import line_buffered_stdout
from src.common.spark import CATALOG, build

# The exactly-once gate kills this process with SIGKILL. stdout redirected to a
# file is block-buffered by default, so everything printed since the last 4 KB
# boundary dies with the process and the log ends mid-startup (F7). Line
# buffering costs nothing here and makes the log a usable post-mortem.
line_buffered_stdout()


def names_for(topic: str) -> tuple[str, Path]:
    """Derive the bronze table and checkpoint directory from the topic name.

    These used to be module constants, which meant `--topic reviews.eos` still
    wrote to -- and `--reset-only` still dropped -- the production table. The
    exactly-once gate is destructive by design, so the only safe arrangement is
    that a different topic can only ever reach a different table and a
    different checkpoint.

    'reviews.raw' -> ('...bronze.reviews_raw', checkpoints/bronze_reviews_raw),
    which is the table that already holds the production data.
    """
    slug = re.sub(r"[^a-z0-9]+", "_", topic.lower()).strip("_")
    if not slug or slug[0].isdigit():
        raise ValueError(
            f"Topic '{topic}' does not yield a usable table name. "
            "Topics must contain a letter and start with one.")
    return f"{CATALOG}.bronze.{slug}", C.CHECKPOINTS / f"bronze_{slug}"


_UNITS = {"ms": "milliseconds", "s": "seconds", "m": "minutes", "h": "hours"}


def normalise_trigger(value: str) -> str:
    """Accept '5s' / '500ms' / '1m' and return what Spark's parser expects.

    Spark's ProcessingTime parser wants a full interval string ('5 seconds').
    Shorthand is nicer on the command line, so we translate rather than making
    the user remember the exact spelling.
    """
    v = value.strip()
    if " " in v:                      # already a full interval, pass through
        return v
    import re
    m = re.fullmatch(r"(\d+)(ms|s|m|h)", v)
    if not m:
        raise ValueError(
            f"Cannot parse trigger '{value}'. Use 'once', shorthand like '5s'/'500ms'/'1m', "
            "or a full interval like '5 seconds'.")
    return f"{m.group(1)} {_UNITS[m.group(2)]}"


def ensure_table(spark, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            payload         STRING   COMMENT 'the raw JSON line, untouched',
            kafka_key       STRING   COMMENT 'parent_asin -- the partition key',
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT   COMMENT 'with partition, uniquely identifies the record',
            kafka_timestamp TIMESTAMP,
            ingested_at     TIMESTAMP,
            ingest_date     DATE
        )
        USING iceberg
        PARTITIONED BY (ingest_date)
        TBLPROPERTIES (
            'write.format.default'          = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'format-version'                = '2'
        )
    """)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default=C.TOPIC_REVIEWS)
    ap.add_argument("--trigger", default="once",
                    help="'once' to drain and stop, or a duration like '5s' to keep running")
    ap.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    ap.add_argument("--max-per-trigger", type=int, default=100_000,
                    help="records per micro-batch -- the throttle that keeps memory bounded")
    ap.add_argument("--reset", action="store_true",
                    help="drop the table and checkpoint before running (destructive)")
    ap.add_argument("--reset-only", action="store_true",
                    help="drop the table and checkpoint, then exit without streaming")
    args = ap.parse_args()

    table, checkpoint = names_for(args.topic)

    spark = build("bronze-reviews")

    if args.reset or args.reset_only:
        print(f"[bronze] RESET: dropping {table} and {checkpoint}")
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        if checkpoint.exists():
            import shutil
            shutil.rmtree(checkpoint)

    ensure_table(spark, table)
    checkpoint.mkdir(parents=True, exist_ok=True)

    if args.reset_only:
        print("[bronze] reset complete, exiting without streaming")
        spark.stop()
        return

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
           .option("subscribe", args.topic)
           .option("startingOffsets", args.starting_offsets)
           .option("maxOffsetsPerTrigger", args.max_per_trigger)
           # Fail loudly if data we still need has already aged out of the topic,
           # rather than quietly skipping the gap.
           .option("failOnDataLoss", "true")
           .load())

    shaped = raw.select(
        F.col("value").cast("string").alias("payload"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        F.to_date(F.current_timestamp()).alias("ingest_date"),
    )

    writer = (shaped.writeStream
              .format("iceberg")
              .outputMode("append")
              .option("checkpointLocation", str(checkpoint))
              # ingest_date comes from wall-clock time, so a micro-batch that
              # straddles midnight carries two partition values. Without fanout
              # the Iceberg sink requires each task's rows to arrive sorted by
              # partition and fails otherwise; fanout keeps one writer open per
              # partition instead, which buys that robustness for the price of
              # at most two open writers per task.
              .option("fanout-enabled", "true"))

    if args.trigger == "once":
        writer = writer.trigger(availableNow=True)
        print(f"[bronze] draining '{args.topic}' into {table} (availableNow)")
    else:
        interval = normalise_trigger(args.trigger)
        writer = writer.trigger(processingTime=interval)
        print(f"[bronze] streaming '{args.topic}' into {table} every {interval} -- Ctrl-C to stop")

    query = writer.toTable(table)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[bronze] stopping cleanly (checkpoint is safe)...")
        query.stop()

    total = spark.table(table).count()
    print(f"\n[bronze] {table} now holds {total:,} rows")
    spark.sql(f"""SELECT snapshot_id, committed_at, operation,
                         summary['added-records'] AS added
                  FROM {table}.snapshots ORDER BY committed_at DESC""").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.common import config as C  # noqa: E402
from src.common.spark import CATALOG, build  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

TABLE = f"{CATALOG}.bronze.reviews_raw"
CHECKPOINT = C.CHECKPOINTS / "bronze_reviews"


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


def ensure_table(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
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

    spark = build("bronze-reviews")

    if args.reset or args.reset_only:
        print(f"[bronze] RESET: dropping {TABLE} and {CHECKPOINT}")
        spark.sql(f"DROP TABLE IF EXISTS {TABLE} PURGE")
        if CHECKPOINT.exists():
            import shutil
            shutil.rmtree(CHECKPOINT)

    ensure_table(spark)
    CHECKPOINT.mkdir(parents=True, exist_ok=True)

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
              .option("checkpointLocation", str(CHECKPOINT))
              .option("fanout-enabled", "true"))

    if args.trigger == "once":
        writer = writer.trigger(availableNow=True)
        print(f"[bronze] draining '{args.topic}' into {TABLE} (availableNow)")
    else:
        interval = normalise_trigger(args.trigger)
        writer = writer.trigger(processingTime=interval)
        print(f"[bronze] streaming '{args.topic}' into {TABLE} every {interval} -- Ctrl-C to stop")

    query = writer.toTable(TABLE)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[bronze] stopping cleanly (checkpoint is safe)...")
        query.stop()

    total = spark.table(TABLE).count()
    print(f"\n[bronze] {TABLE} now holds {total:,} rows")
    spark.sql(f"""SELECT snapshot_id, committed_at, operation,
                         summary['added-records'] AS added
                  FROM {TABLE}.snapshots ORDER BY committed_at DESC""").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()

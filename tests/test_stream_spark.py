"""The streaming path's two claims, on a plain local Spark session (ticket 15).

No Kafka, no Iceberg, no ledger: the topic is stood in for by a JSON file source, which is
enough to exercise the parts that matter -- the validation the stream calls, the watermark it
runs under, and what the aggregates do when either of those is wrong.

Two trip cases are the point of the file:

* an **unsorted** input makes `numRowsDroppedByWatermark` non-zero, which is what the control
  run's `zero_natural_drops` constituent would catch;
* a **diverged** validation function makes the projection disagree with the batch aggregate,
  which is what `STREAM_RECONCILE` would catch -- the digest constituent is the cheap version
  of the same check, and this is the expensive one that shows the consequence.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from src.spark import silver
from src.spark import stream_product_month as S

pytestmark = pytest.mark.usefixtures("spark")

WATERMARK = "30 days"
DAY_MS = 86_400_000
BASE_MS = 1_600_000_000_000          # 2020-09-13, comfortably inside the valid range


def _review(user: str, asin: str, ts: int, *, rating: float = 5.0, text: str = "ok",
            verified: bool = True) -> dict:
    return {"user_id": user, "parent_asin": asin, "asin": asin, "timestamp": ts,
            "rating": rating, "title": "t", "text": text, "helpful_vote": 0,
            "verified_purchase": verified, "images": []}


def _write_files(root: Path, groups: list[list[dict]]) -> Path:
    """One file per micro-batch, so `maxFilesPerTrigger=1` replays them in a known order.

    Spark's file source orders by modification time, not by name, and three files written in
    the same second tie -- which silently reverses the replay. The mtimes are set apart on
    purpose: the difference between a sorted and an unsorted replay is the thing under test,
    so it may not be left to how fast the disk was.
    """
    root.mkdir(parents=True, exist_ok=True)
    for i, group in enumerate(groups):
        path = root / f"{i:04d}.json"
        path.write_text("\n".join(json.dumps({"payload": json.dumps(r)}) for r in group) + "\n")
        os.utime(path, (1_700_000_000 + i * 60, 1_700_000_000 + i * 60))
    return root


def _run_stream(spark, src: Path, tmp_path: Path, *, validate=None) -> dict:
    """Drain the files through the streaming path; return the counts the job would record."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    raw = (spark.readStream
           .schema(StructType([StructField("payload", StringType())]))
           .option("maxFilesPerTrigger", 1)
           .json(str(src)))
    records = raw.select("payload",
                         F.lit(0).alias("kafka_partition"),
                         F.lit(0).cast("long").alias("kafka_offset"),
                         F.current_timestamp().alias("ingested_at"))
    typed = (S.TYPE_ROWS((validate or S.VALIDATE)(records)))
    event_ts = F.timestamp_millis(F.col("timestamp_ms"))
    reviews = typed.select(
        "review_id", "parent_asin", "user_id", "rating", "verified_purchase",
        event_ts.alias("event_ts"),
        F.to_date(F.date_trunc("month", event_ts)).alias("review_month"),
        silver.word_count_col(F.col("text")).alias("text_word_count"))
    stream = S.deduplicated(reviews, watermark=WATERMARK)

    out = tmp_path / "batches"
    seen = {"rows": 0}

    def write_batch(batch, batch_id: int) -> None:
        batch = batch.persist()
        try:
            seen["rows"] += batch.count()
            (S.batch_aggregates(batch).toPandas()
             .to_json(out / f"{batch_id:04d}.json", orient="records", lines=True))
        finally:
            batch.unpersist()

    out.mkdir(parents=True, exist_ok=True)
    query = (stream.writeStream.outputMode("append")
             .option("checkpointLocation", str(tmp_path / "ck"))
             .foreachBatch(write_batch).trigger(availableNow=True).start())
    query.awaitTermination()
    counts = S.progress_counts(query, spark=spark)
    counts["unique_review_ids"] = seen["rows"]
    counts["aggregates"] = [json.loads(line) for f in sorted(out.glob("*.json"))
                            for line in f.read_text().splitlines() if line.strip()]
    return counts


def _summed(counts: dict) -> dict[tuple[str, str], dict]:
    """The projection: per-batch contributions summed, keyed by (product, month)."""
    total: dict[tuple[str, str], dict] = {}
    for row in counts["aggregates"]:
        key = (row["parent_asin"], row["month"])
        acc = total.setdefault(key, dict.fromkeys(S.SUMMED, 0))
        for c in S.SUMMED:
            acc[c] += row[c]
    return total


# ------------------------------------------------------- validation is shared ----

def test_the_streaming_path_calls_silvers_validation_and_not_a_copy():
    assert S.VALIDATE is silver.parse_and_validate
    assert S.TYPE_ROWS is silver.typed_valid_rows


def test_the_recorded_digest_moves_when_the_shared_behaviour_moves(monkeypatch):
    """An import check would pass a wrapper; the digest is what the gate actually compares."""
    before = S.validation_digest()
    assert before == S.validation_digest()

    def patched(bronze):                       # a fork, not a drift
        return silver.parse_and_validate(bronze)

    monkeypatch.setattr(S, "SHARED_WITH_SILVER", (patched, *S.SHARED_WITH_SILVER[1:]))
    assert S.validation_digest() != before


def test_the_streaming_path_types_rows_exactly_as_silver_does(spark, tmp_path):
    """Same payloads, two entry points, identical typed rows."""
    from pyspark.sql import functions as F

    rows = [_review("u1", "p1", BASE_MS), _review("u2", "p2", BASE_MS + DAY_MS, rating=2.0)]
    records = spark.createDataFrame(
        [(json.dumps(r), 0, 0) for r in rows], "payload string, kafka_partition int, "
        "kafka_offset long").withColumn("ingested_at", F.current_timestamp())
    through_stream = S.review_rows(records).select("review_id", "rating").collect()
    through_silver = (silver.typed_valid_rows(silver.parse_and_validate(records))
                      .select("review_id", "rating").collect())
    assert sorted(map(tuple, through_stream)) == sorted(map(tuple, through_silver))


# --------------------------------------------------------------- the watermark ----

def test_a_sorted_replay_drops_nothing_and_deduplicates_exactly(spark, tmp_path):
    """The control run's shape in miniature: key collisions removed, no natural drops."""
    batches = [
        [_review("u1", "p1", BASE_MS), _review("u1", "p1", BASE_MS)],      # same key twice
        [_review("u2", "p1", BASE_MS + 40 * DAY_MS)],
        [_review("u3", "p2", BASE_MS + 80 * DAY_MS)],
    ]
    counts = _run_stream(spark, _write_files(tmp_path / "in", batches), tmp_path)
    assert counts["natural_drops"] == 0
    assert counts["unique_review_ids"] == 3          # 4 rows in, one duplicate removed
    assert counts["records_read"] == 4


def test_an_unsorted_replay_makes_natural_drops_non_zero(spark, tmp_path):
    """The trip case: event order is the premise, and losing it is what the gate must see.

    The same four rows as above, replayed with the oldest batch last. The watermark advances
    past it while it is still waiting, and Spark drops it -- so `zero_natural_drops` fails and
    the control run cannot pass on an unsorted topic.
    """
    batches = [
        [_review("u3", "p2", BASE_MS + 80 * DAY_MS)],
        [_review("u2", "p1", BASE_MS + 40 * DAY_MS)],
        [_review("u1", "p1", BASE_MS)],                                    # 80 days late
        [_review("u4", "p3", BASE_MS + 120 * DAY_MS)],
    ]
    counts = _run_stream(spark, _write_files(tmp_path / "in", batches), tmp_path)
    assert counts["natural_drops"] > 0
    assert counts["unique_review_ids"] < counts["records_read"]


# ------------------------------------------------------------ reconciliation ----

def _batch_gold(spark, rows: list[dict]) -> dict[tuple[str, str], dict]:
    """What gold would hold for the same reviews: batch silver, then gold's aggregation."""
    from pyspark.sql import functions as F

    records = spark.createDataFrame(
        [(json.dumps(r), 0, i) for i, r in enumerate(rows)],
        "payload string, kafka_partition int, kafka_offset long"
    ).withColumn("ingested_at", F.current_timestamp())
    classified = silver.classify_collisions(
        silver.typed_valid_rows(silver.parse_and_validate(records)))
    event_ts = F.timestamp_millis(F.col("timestamp_ms"))
    survivors = classified.filter("is_survivor").select(
        "parent_asin", "rating", "verified_purchase", "user_id",
        F.to_date(F.date_trunc("month", event_ts)).alias("review_month"),
        silver.word_count_col(F.col("text")).alias("text_word_count"))
    from src.spark.gold import monthly_aggregates
    return {(r["parent_asin"], r["month"]): {c: r[c] for c in S.SUMMED}
            for r in monthly_aggregates(survivors).collect()}


def test_the_projection_reconciles_with_the_batch_aggregate(spark, tmp_path):
    rows = [_review("u1", "p1", BASE_MS, rating=5.0),
            _review("u1", "p1", BASE_MS, rating=5.0),           # exact key collision
            _review("u2", "p1", BASE_MS + DAY_MS, rating=1.0, text=""),
            _review("u3", "p2", BASE_MS + 40 * DAY_MS, rating=4.0, verified=False)]
    counts = _run_stream(spark, _write_files(tmp_path / "in", [rows]), tmp_path)
    assert _summed(counts) == _batch_gold(spark, rows)


def test_a_diverged_validation_function_breaks_reconciliation(spark, tmp_path):
    """The trip case for `STREAM_RECONCILE`: a stream that disagrees about what is valid.

    The fork here is the smallest one that still changes an aggregate -- a rating bound the
    stream widened -- and it is exactly the kind of drift a copied parser acquires. The
    projection then carries a review batch silver rejected, and the product-month they share
    stops matching.
    """
    from pyspark.sql import functions as F

    def forked(bronze):
        """Silver's validation with the invalid-rating rule removed."""
        out = silver.parse_and_validate(bronze)
        return out.withColumn("reject_reason",
                              F.when(F.col("reject_reason") == "invalid_rating", None)
                               .otherwise(F.col("reject_reason")))

    rows = [_review("u1", "p1", BASE_MS, rating=5.0),
            _review("u2", "p1", BASE_MS + DAY_MS, rating=9.0)]     # batch rejects this one
    clean = _run_stream(spark, _write_files(tmp_path / "clean", [rows]), tmp_path / "a")
    diverged = _run_stream(spark, _write_files(tmp_path / "forked", [rows]), tmp_path / "b",
                           validate=forked)
    gold = _batch_gold(spark, rows)
    assert _summed(clean) == gold
    assert _summed(diverged) != gold


def test_a_micro_batch_that_never_ran_leaves_the_projection_short(spark, tmp_path):
    """Why `reviews_on_projection` is compared with a tally taken while the stream ran."""
    start = time.time()
    batches = [[_review("u1", "p1", BASE_MS)], [_review("u2", "p1", BASE_MS + DAY_MS)]]
    counts = _run_stream(spark, _write_files(tmp_path / "in", batches), tmp_path)
    assert counts["micro_batches"] >= 2
    assert sum(r["review_count"] for r in counts["aggregates"]) == counts["unique_review_ids"]
    assert time.time() - start < 300

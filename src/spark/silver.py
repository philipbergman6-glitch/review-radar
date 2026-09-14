"""Job `silver`: bronze payloads -> validated, deduplicated, catalogue-enriched reviews.

ADR-0007 in one paragraph: a bounded batch pinned to one bronze snapshot. Every payload is
parsed against the explicit review schema and validated under a fixed precedence of four
reject reasons (unparsable_json → missing_key_field → invalid_rating →
timestamp_out_of_range). Valid rows get `review_id = SHA-256(canonical(user_id,
parent_asin, timestamp_ms))`; rows sharing an id form a key-collision group, classified
exact (all retained fields agree), conflicting (same rating, something else differs;
survivor = highest helpful_vote, longest text, lowest canonical hash) or unresolvable
(rating disagreement; nobody survives). Survivors are left-joined to the PostgreSQL
catalogue read over JDBC and written to `silver.reviews`; every reject and every collision
row is kept in its own table. The three writes each commit atomically and carry this run's
id in their snapshot summary; the run ledger row records all three snapshots (ADR-0008).

The pure pieces -- `parse_and_validate`, `classify_collisions`, `review_id_col`,
`survivor_hash_col`, `word_count_col` -- take and return DataFrames and are tested on a
plain local Spark session in tests/test_silver_spark.py against the Python reference in
src/common/canonical.py.

Run:
    ./run.sh python -m src.spark.silver                       # full scope, current bronze snapshot
    ./run.sh python -m src.spark.silver --scope sample        # the sample topic's bronze table
    ./run.sh python -m src.spark.silver --bronze-snapshot-id 1672093859820484049
"""
from __future__ import annotations

import argparse
import time
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

from src.common import config as C
from src.common import runs
from src.common.console import line_buffered_stdout
from src.common.spark import CATALOG, build
from src.gates import silver as gate
from src.gates.silver import REJECT_REASONS
from src.spark.bronze import names_for

line_buffered_stdout()

# ---------------------------------------------------------------- constants ----
# Timestamps before this are impossible for Amazon reviews (named, not job time).
TS_MIN_UTC = "1995-01-01T00:00:00Z"
TS_MIN_MS = 788_918_400_000
NULL_MARKER_HEX = "FFFFFFFF"

RETAINED_FIELDS = ("rating", "title", "text", "verified_purchase", "helpful_vote", "asin", "images")

# Python's str.split() whitespace, as a Java regex class: \s plus the Unicode spaces
# `isspace` recognises. The Spark/Python parity test pins this.
PY_WHITESPACE = "[\\s\\x1c-\\x1f\\x85\\xa0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000]+"

# Parsed loosely on purpose: numeric fields as strings so a non-numeric rating or timestamp
# is *visible* to validation rather than silently nulled by a typed parse.
LOOSE_REVIEW_SCHEMA = StructType([
    StructField("rating", StringType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
    StructField("images", ArrayType(StructType([
        StructField("small_image_url", StringType()),
        StructField("medium_image_url", StringType()),
        StructField("large_image_url", StringType()),
        StructField("attachment_type", StringType()),
    ]))),
    StructField("asin", StringType()),
    StructField("parent_asin", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("helpful_vote", StringType()),
    StructField("verified_purchase", BooleanType()),
    StructField("_corrupt", StringType()),
])


# ------------------------------------------------------- canonical encoding ----
def _enc(c: Column) -> Column:
    """4-byte big-endian UTF-8 length + bytes; null = 0xFFFFFFFF (src/common/canonical.py)."""
    return F.when(c.isNull(), F.unhex(F.lit(NULL_MARKER_HEX))).otherwise(
        F.concat(F.unhex(F.lpad(F.hex(F.octet_length(c)), 8, "0")), F.encode(c, "UTF-8")))


def review_id_col(user_id: Column, parent_asin: Column, timestamp_ms: Column) -> Column:
    return F.sha2(F.concat(_enc(F.trim(user_id)), _enc(F.trim(parent_asin)),
                           _enc(timestamp_ms.cast("string"))), 256)


def images_json_col(images: Column) -> Column:
    # Sorted keys, nulls dropped (to_json's default), array order kept.
    sorted_struct = F.transform(images, lambda x: F.struct(
        x["attachment_type"].alias("attachment_type"),
        x["large_image_url"].alias("large_image_url"),
        x["medium_image_url"].alias("medium_image_url"),
        x["small_image_url"].alias("small_image_url")))
    return F.when(images.isNull(), F.lit(None).cast("string")).otherwise(F.to_json(sorted_struct))


def survivor_hash_col(rating: Column, title: Column, text: Column, verified: Column,
                      helpful_vote: Column, asin: Column, images_json: Column) -> Column:
    return F.sha2(F.concat(
        _enc(rating.cast("int").cast("string")),
        _enc(title),
        _enc(text),
        _enc(F.when(verified.isNull(), None).when(verified, "true").otherwise("false")),
        _enc(helpful_vote.cast("int").cast("string")),
        _enc(F.trim(asin)),
        _enc(images_json)), 256)


def word_count_col(text: Column) -> Column:
    """len(text.split()) in Python terms; null/blank -> 0."""
    tokens = F.filter(F.split(F.coalesce(text, F.lit("")), PY_WHITESPACE), lambda t: t != "")
    return F.size(tokens)


# ------------------------------------------------------------ validation ----
def parse_and_validate(bronze: DataFrame) -> DataFrame:
    """Add `p` (parsed struct), typed fields and `reject_reason` (null = valid).

    Expects bronze columns payload, kafka_partition, kafka_offset, ingested_at.
    """
    parsed = F.from_json(F.col("payload"), LOOSE_REVIEW_SCHEMA,
                         {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt"})
    df = bronze.withColumn("p", parsed)
    rating_d = F.col("p.rating").cast("double")
    ts_str = F.col("p.timestamp")
    ts_is_int = ts_str.rlike("^-?[0-9]{1,19}$")
    ts_ms = F.when(ts_is_int, ts_str.cast("long"))
    ingested_ms = (F.col("ingested_at").cast("double") * 1000).cast("long")

    blank = lambda c: c.isNull() | (F.trim(c) == "")
    df = (df
          .withColumn("unparsable", F.col("p").isNull() | F.col("p._corrupt").isNotNull())
          .withColumn("missing_key", blank(F.col("p.user_id")) | blank(F.col("p.parent_asin"))
                      | blank(ts_str))
          .withColumn("rating_ok", rating_d.isNotNull() & (rating_d >= 1) & (rating_d <= 5)
                      & (rating_d == F.floor(rating_d)))
          .withColumn("ts_ms", ts_ms)
          .withColumn("ts_unparsable", ~ts_is_int)
          .withColumn("ts_below_min", ts_ms < F.lit(TS_MIN_MS))
          .withColumn("ts_after_ingest", ts_ms > ingested_ms))
    reason = (F.when(F.col("unparsable"), "unparsable_json")
              .when(F.col("missing_key"), "missing_key_field")
              .when(~F.col("rating_ok"), "invalid_rating")
              .when(F.col("ts_unparsable") | F.col("ts_below_min") | F.col("ts_after_ingest"),
                    "timestamp_out_of_range"))
    diagnostics = F.when(
        reason == "timestamp_out_of_range",
        F.map_from_arrays(
            F.array(F.lit("timestamp"), F.lit("below_1995"), F.lit("after_ingest"), F.lit("unparsable")),
            F.array(ts_str, F.coalesce(F.col("ts_below_min"), F.lit(False)).cast("string"),
                    F.coalesce(F.col("ts_after_ingest"), F.lit(False)).cast("string"),
                    F.col("ts_unparsable").cast("string")))
    ).when(reason == "invalid_rating",
           F.map_from_arrays(F.array(F.lit("rating")), F.array(F.coalesce(F.col("p.rating"), F.lit("null")))))
    return (df.withColumn("reject_reason", reason)
              .withColumn("reject_diagnostics", diagnostics)
              .drop("unparsable", "missing_key", "rating_ok", "ts_unparsable",
                    "ts_below_min", "ts_after_ingest"))


def typed_valid_rows(validated: DataFrame) -> DataFrame:
    """Project valid rows to typed review columns plus review_id and the survivor hash."""
    v = validated.filter(F.col("reject_reason").isNull())
    images_json = images_json_col(F.col("p.images"))
    v = v.select(
        F.col("payload"), F.col("kafka_partition"), F.col("kafka_offset"),
        F.trim(F.col("p.user_id")).alias("user_id"),
        F.trim(F.col("p.parent_asin")).alias("parent_asin"),
        F.when(F.trim(F.col("p.asin")) == "", None).otherwise(F.trim(F.col("p.asin"))).alias("asin"),
        F.col("ts_ms").alias("timestamp_ms"),
        F.col("p.rating").cast("double").cast("int").alias("rating"),
        F.col("p.title").alias("title"),
        F.col("p.text").alias("text"),
        F.col("p.verified_purchase").alias("verified_purchase"),
        F.col("p.helpful_vote").cast("double").cast("int").alias("helpful_vote"),
        F.col("p.images").alias("images"),
        images_json.alias("images_json"),
    )
    return (v.withColumn("review_id", review_id_col(F.col("user_id"), F.col("parent_asin"),
                                                    F.col("timestamp_ms")))
             .withColumn("canonical_row_hash", survivor_hash_col(
                 F.col("rating"), F.col("title"), F.col("text"), F.col("verified_purchase"),
                 F.col("helpful_vote"), F.col("asin"), F.col("images_json"))))


# ------------------------------------------------------------ collisions ----
def classify_collisions(valid: DataFrame) -> DataFrame:
    """Add group_size, collision_class, differing_fields, selection_rank/reason, is_survivor."""
    w = Window.partitionBy("review_id")
    order = Window.partitionBy("review_id").orderBy(
        F.col("helpful_vote").desc_nulls_last(),
        F.coalesce(F.length("text"), F.lit(0)).desc(),
        F.col("canonical_row_hash").asc())

    def differs(col: str, expr: Column | None = None) -> Column:
        c = expr if expr is not None else F.col(col)
        c = c.cast("string") if col == "verified_purchase" else c
        return ((F.min(c).over(w) != F.max(c).over(w))
                | ((F.count(c).over(w) != F.col("group_size")) & (F.count(c).over(w) != 0)))

    df = valid.withColumn("group_size", F.count("*").over(w))
    diff_cols = [F.when(differs(f, F.col("images_json") if f == "images" else None), F.lit(f))
                 for f in RETAINED_FIELDS]
    df = df.withColumn("differing_fields", F.array_compact(F.array(*diff_cols)))
    df = df.withColumn("collision_class", F.when(F.col("group_size") == 1, None)
                       .when(F.array_contains("differing_fields", "rating"), "unresolvable")
                       .when(F.size("differing_fields") == 0, "exact")
                       .otherwise("conflicting"))
    df = (df.withColumn("selection_rank", F.row_number().over(order))
            .withColumn("_next_hv", F.lead("helpful_vote").over(order))
            .withColumn("_next_len", F.lead(F.coalesce(F.length("text"), F.lit(0))).over(order)))
    reason = (F.when(F.col("group_size") == 1, "only_row")
              .when(F.col("collision_class") == "unresolvable", "no_survivor")
              .when(F.col("selection_rank") > 1, "not_selected")
              .when(F.col("collision_class") == "exact", "exact_duplicate")
              .when(~F.col("helpful_vote").eqNullSafe(F.col("_next_hv")), "highest_helpful_vote")
              .when(F.coalesce(F.length("text"), F.lit(0)) != F.col("_next_len"), "longest_text")
              .otherwise("lowest_canonical_hash"))
    df = df.withColumn("selection_reason", reason)
    df = df.withColumn("is_survivor", (F.col("selection_rank") == 1)
                       & (F.coalesce(F.col("collision_class"), F.lit("")) != "unresolvable"))
    return df.drop("_next_hv", "_next_len")


def collision_counts(classified: DataFrame) -> dict[str, int]:
    groups = (classified.filter("group_size > 1")
              .groupBy("review_id").agg(F.first("collision_class").alias("cls"), F.count("*").alias("n")))
    agg = groups.agg(
        F.count("*").alias("groups"),
        F.sum(F.when(F.col("cls") == "exact", 1).otherwise(0)).alias("exact"),
        F.sum(F.when(F.col("cls") == "conflicting", 1).otherwise(0)).alias("conflicting"),
        F.sum(F.when(F.col("cls") == "unresolvable", 1).otherwise(0)).alias("unresolvable"),
        F.sum("n").alias("table_rows")).first()
    g, e, c, u, t = (int(agg[k] or 0) for k in ("groups", "exact", "conflicting", "unresolvable", "table_rows"))
    return {"collision_groups": g, "exact_groups": e, "conflicting_groups": c,
            "unresolvable_groups": u, "collision_table_rows": t,
            "collision_rows_removed": t - g + u}


# --------------------------------------------------------------- catalogue ----
def read_catalogue(spark: SparkSession) -> tuple[DataFrame, str, int]:
    """Products over JDBC; requires exactly one catalogue_load_id. Returns (df, load_id, rows)."""
    query = ("(SELECT parent_asin, title AS product_title, main_category, store, price, "
             "catalogue_load_id::text AS catalogue_load_id FROM products) AS p")
    df = (spark.read.format("jdbc")
          .option("url", C.PG_JDBC_URL).option("dbtable", query)
          .option("user", C.PG_USER).option("password", C.PG_PASSWORD)
          .option("driver", "org.postgresql.Driver").load()).cache()
    rows = df.count()
    ids = [r[0] for r in df.select("catalogue_load_id").distinct().collect()]
    if rows == 0 or len(ids) != 1:
        raise RuntimeError(f"products must hold exactly one catalogue_load_id over >0 rows; "
                           f"found {len(ids)} ids over {rows} rows. Run make catalogue first.")
    return df, ids[0], rows


def enrich(survivors: DataFrame, products: DataFrame) -> DataFrame:
    return survivors.join(F.broadcast(products.drop("catalogue_load_id")), on="parent_asin", how="left")


# ------------------------------------------------------------------ tables ----
def silver_names(topic: str) -> dict[str, str]:
    """reviews.raw -> silver.reviews; reviews.raw.sample -> silver.reviews_sample; reviews.eos -> _eos."""
    slug = names_for(topic)[0].rsplit(".", 1)[1].removeprefix("reviews_").removeprefix("raw").strip("_")
    suffix = "" if slug == "" else f"_{slug}"
    return {"reviews": f"{CATALOG}.silver.reviews{suffix}",
            "rejects": f"{CATALOG}.silver.rejects{suffix}",
            "collisions": f"{CATALOG}.silver.review_collisions{suffix}"}


def ensure_tables(spark: SparkSession, names: dict[str, str]) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.silver")
    props = ("TBLPROPERTIES ('write.format.default'='parquet', "
             "'write.parquet.compression-codec'='zstd', 'format-version'='2')")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['reviews']} (
        review_id STRING, parent_asin STRING, asin STRING, user_id STRING,
        event_ts TIMESTAMP, review_month DATE, rating INT, title STRING, text STRING,
        text_word_count INT, verified_purchase BOOLEAN, helpful_vote INT, image_count INT,
        product_title STRING, main_category STRING, store STRING, price DECIMAL(10,2),
        kafka_partition INT, kafka_offset BIGINT, source_bronze_snapshot_id BIGINT, run_id STRING
    ) USING iceberg PARTITIONED BY (review_month) {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['rejects']} (
        payload STRING, reject_reason STRING, diagnostics MAP<STRING, STRING>,
        kafka_partition INT, kafka_offset BIGINT, ingested_at TIMESTAMP,
        source_bronze_snapshot_id BIGINT, run_id STRING
    ) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['collisions']} (
        collision_group_id STRING, collision_class STRING, differing_fields ARRAY<STRING>,
        is_survivor BOOLEAN, selection_rank INT, selection_reason STRING,
        canonical_row_hash STRING, payload STRING, kafka_partition INT, kafka_offset BIGINT,
        source_bronze_snapshot_id BIGINT, run_id STRING
    ) USING iceberg {props}""")


def current_snapshot_id(spark: SparkSession, table: str) -> int:
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"{table} does not exist; drain its topic with src.spark.bronze first")
    row = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").first()
    if row is None:
        raise RuntimeError(f"{table} has no snapshot yet")
    return int(row[0])


def write_replace(df: DataFrame, table: str, run_id: str) -> int:
    """Atomically replace the table's contents; return the new snapshot id (stamped run_id)."""
    (df.writeTo(table).option("snapshot-property.run_id", run_id).overwrite(F.lit(True)))
    spark = df.sparkSession
    snap = spark.sql(f"SELECT snapshot_id, summary['run_id'] AS rid FROM {table}.snapshots "
                     "ORDER BY committed_at DESC LIMIT 1").first()
    if snap is None or snap["rid"] != run_id:
        raise RuntimeError(f"{table}: newest snapshot is not stamped with run {run_id}")
    return int(snap["snapshot_id"])


def digest(df: DataFrame, *cols: str) -> str:
    """Order-independent content digest for rerun comparison."""
    row = df.agg(F.bit_xor(F.xxhash64(*cols)).alias("x"), F.count("*").alias("n")).first()
    return f"{row['n']}:{(row['x'] or 0) & 0xFFFFFFFFFFFFFFFF:016x}"


# -------------------------------------------------------------------- main ----
def run_silver(spark: SparkSession, *, topic: str, scope: str, category: str,
               bronze_snapshot_id: int | None, verify_rerun: bool) -> dict[str, Any]:
    t0 = time.time()
    bronze_table = names_for(topic)[0]
    names = silver_names(topic)
    snapshot_id = bronze_snapshot_id or current_snapshot_id(spark, bronze_table)
    products, load_id, catalogue_rows = read_catalogue(spark)

    run = runs.start("silver", runs.SILVER_SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"bronze": {"table": bronze_table, "snapshot_id": snapshot_id},
                             "catalogue": {"table": "products", "catalogue_load_id": load_id}},
                     params={"ts_min_utc": TS_MIN_UTC, "reject_precedence": list(REJECT_REASONS),
                             "survivor_rule": "helpful_vote desc, text length desc, canonical hash asc",
                             "join": "left broadcast on parent_asin"})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        ensure_tables(spark, names)
        bronze = spark.read.option("snapshot-id", snapshot_id).table(bronze_table)
        bronze_rows = bronze.count()

        validated = parse_and_validate(bronze).cache()
        rejects = validated.filter(F.col("reject_reason").isNotNull())
        reason_counts = {r: 0 for r in REJECT_REASONS}
        for row in rejects.groupBy("reject_reason").count().collect():
            reason_counts[row["reject_reason"]] = int(row["count"])
        ts_diag = rejects.filter("reject_reason = 'timestamp_out_of_range'").agg(
            F.sum(F.when(F.col("reject_diagnostics")["below_1995"] == "true", 1).otherwise(0)).alias("b"),
            F.sum(F.when(F.col("reject_diagnostics")["after_ingest"] == "true", 1).otherwise(0)).alias("a")).first()
        reject_rows = sum(reason_counts.values())

        classified = classify_collisions(typed_valid_rows(validated)).cache()
        counts.update(collision_counts(classified))
        survivors = classified.filter("is_survivor")
        survivor_rows = survivors.count()

        enriched = enrich(survivors, products)
        silver = enriched.select(
            "review_id", "parent_asin", "asin", "user_id",
            F.timestamp_millis(F.col("timestamp_ms")).alias("event_ts"),
            F.to_date(F.date_trunc("month", F.timestamp_millis(F.col("timestamp_ms")))).alias("review_month"),
            "rating", "title", "text",
            word_count_col(F.col("text")).alias("text_word_count"),
            "verified_purchase", "helpful_vote",
            F.coalesce(F.size("images"), F.lit(0)).alias("image_count"),
            "product_title", "main_category", "store", F.col("price").cast("decimal(10,2)").alias("price"),
            "kafka_partition", "kafka_offset",
            F.lit(snapshot_id).alias("source_bronze_snapshot_id"), F.lit(run.run_id).alias("run_id"),
        ).cache()
        silver_rows = silver.count()
        unmatched = silver.filter(F.col("product_title").isNull())
        counts["unmatched_review_rows"] = unmatched.count()
        counts["unmatched_parent_asins"] = unmatched.select("parent_asin").distinct().count()
        counts["review_id_distinct"] = silver.select("review_id").distinct().count()
        counts["reject_reasons"] = reason_counts
        counts["catalogue_rows_read"] = catalogue_rows
        counts["join_cardinality_ok"] = silver_rows == survivor_rows
        counts["timestamp_below_1995"] = int(ts_diag["b"] or 0)
        counts["timestamp_after_ingest"] = int(ts_diag["a"] or 0)

        # ---- writes: rejects, collisions, then reviews; each atomic, each stamped ----
        reject_out = rejects.select(
            "payload", "reject_reason", F.col("reject_diagnostics").alias("diagnostics"),
            "kafka_partition", "kafka_offset", "ingested_at",
            F.lit(snapshot_id).alias("source_bronze_snapshot_id"), F.lit(run.run_id).alias("run_id"))
        outputs["silver.rejects"] = {"table": names["rejects"],
                                     "snapshot_id": write_replace(reject_out, names["rejects"], run.run_id)}
        coll_out = classified.filter("group_size > 1").select(
            F.col("review_id").alias("collision_group_id"), "collision_class", "differing_fields",
            "is_survivor", "selection_rank", "selection_reason", "canonical_row_hash", "payload",
            "kafka_partition", "kafka_offset",
            F.lit(snapshot_id).alias("source_bronze_snapshot_id"), F.lit(run.run_id).alias("run_id"))
        outputs["silver.review_collisions"] = {
            "table": names["collisions"],
            "snapshot_id": write_replace(coll_out, names["collisions"], run.run_id)}
        outputs["silver.reviews"] = {"table": names["reviews"],
                                     "snapshot_id": write_replace(silver, names["reviews"], run.run_id)}

        counts["digests"] = {
            "reviews": digest(silver, "review_id", "rating", "title", "text", "helpful_vote",
                              "product_title", "review_month"),
            "rejects": digest(reject_out, "payload", "reject_reason"),
            "collisions": digest(coll_out, "collision_group_id", "canonical_row_hash",
                                 "is_survivor", "collision_class", "selection_rank"),
        }
        counts["elapsed_s"] = round(time.time() - t0, 1)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=bronze_rows, records_out=silver_rows, records_rejected=reject_rows,
                 outputs=outputs, counts=counts)

    # ---- the printed gate (ADR-0007 §7) ----
    # The verdict is pure (src/gates/silver.py); the job only hands it what it counted. The
    # artefact is written by `make gate-silver`, which re-derives all of this from the pinned
    # snapshots -- a job must not publish a reproducibility claim about itself.
    gate.verdict(
        scope=scope, bronze_snapshot=snapshot_id, load_id=load_id, catalogue_rows=catalogue_rows,
        records_in=bronze_rows, records_rejected=reject_rows, records_out=silver_rows,
        counts=counts, reason_counts=reason_counts,
        collisions={"groups": counts["collision_groups"], "exact": counts["exact_groups"],
                    "conflicting": counts["conflicting_groups"],
                    "unresolvable": counts["unresolvable_groups"],
                    "table_rows": counts["collision_table_rows"],
                    "removed": counts["collision_rows_removed"]}).emit()

    if verify_rerun:
        prev = previous_matching_run(run.run_id, snapshot_id, load_id, category, scope)
        if prev is None:
            print("SILVER_RERUN previous_run=none identical=n/a "
                  "(run again with the same bronze snapshot and catalogue load to compare)")
        else:
            same = prev["counts"].get("digests") == counts["digests"] and \
                (prev["records_in"], prev["records_out"], prev["records_rejected"]) == \
                (bronze_rows, silver_rows, reject_rows)
            print(f"SILVER_RERUN previous_run={prev['run_id']} identical={'true' if same else 'false'}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def previous_matching_run(this_run_id: str, snapshot_id: int, load_id: str, category: str,
                          scope: str) -> dict[str, Any] | None:
    from src.common.pg import connect
    with connect() as conn:
        row = conn.execute(
            """SELECT run_id, records_in, records_out, records_rejected, counts FROM pipeline_runs
               WHERE job_name='silver' AND status='success' AND category=%s AND data_scope=%s
                 AND run_id <> %s
                 AND (inputs->'bronze'->>'snapshot_id')::bigint = %s
                 AND inputs->'catalogue'->>'catalogue_load_id' = %s
               ORDER BY started_at DESC LIMIT 1""",
            (category, scope, this_run_id, snapshot_id, load_id)).fetchone()
    if row is None:
        return None
    return {"run_id": str(row[0]), "records_in": row[1], "records_out": row[2],
            "records_rejected": row[3], "counts": row[4]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--topic", default=None, help="bronze source topic (default from --scope)")
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--bronze-snapshot-id", type=int, default=None,
                    help="pin to this bronze snapshot (default: the table's current snapshot)")
    ap.add_argument("--verify-rerun", action="store_true",
                    help="after the run, compare digests with the previous run on the same inputs")
    args = ap.parse_args()
    topic = args.topic or (C.TOPIC_REVIEWS if args.scope == "full" else f"{C.TOPIC_REVIEWS}.sample")

    spark = build("silver-reviews")
    try:
        run_silver(spark, topic=topic, scope=args.scope, category=args.category,
                   bronze_snapshot_id=args.bronze_snapshot_id, verify_rerun=args.verify_rerun)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""Phase 0 EDA: profile the raw data with Spark so the insights we chase later
are grounded in what the dataset actually contains, not in guesses.

Run:  ./run.sh python scripts/profile_data.py --category All_Beauty
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import CATEGORY, DATA_RAW
from src.common.schemas import META_SCHEMA, REVIEW_SCHEMA


def banner(t: str) -> None:
    print(f"\n{'=' * 78}\n{t}\n{'=' * 78}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default=CATEGORY)
    args = ap.parse_args()

    spark = (SparkSession.builder
             .master("local[6]")
             .appName("profile")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    reviews = spark.read.schema(REVIEW_SCHEMA).json(str(DATA_RAW / f"{args.category}.jsonl")).cache()
    meta = spark.read.schema(META_SCHEMA).json(str(DATA_RAW / f"meta_{args.category}.jsonl")).cache()

    banner("REVIEW SCHEMA")
    reviews.printSchema()
    banner("META SCHEMA")
    meta.printSchema()

    n_rev, n_meta = reviews.count(), meta.count()
    banner("VOLUME")
    print(f"reviews          : {n_rev:,}")
    print(f"products (meta)  : {n_meta:,}")
    print(f"distinct users   : {reviews.select('user_id').distinct().count():,}")
    print(f"distinct products: {reviews.select('parent_asin').distinct().count():,}")

    banner("TEXT QUALITY (drives the AI component)")
    t = reviews.select(
        F.length("text").alias("chars"),
        F.size(F.split(F.trim(F.col("text")), r"\s+")).alias("words"),
    )
    t.select(
        F.mean("chars").alias("mean_chars"), F.expr("percentile_approx(chars, 0.5)").alias("p50_chars"),
        F.expr("percentile_approx(chars, 0.9)").alias("p90_chars"), F.max("chars").alias("max_chars"),
        F.mean("words").alias("mean_words"),
    ).show(truncate=False)
    empty = reviews.filter((F.col("text").isNull()) | (F.trim("text") == "")).count()
    print(f"empty-text reviews: {empty:,} ({empty / n_rev:.2%})")
    long_enough = t.filter(F.col("words") >= 20).count()
    print(f"reviews >= 20 words (worth embedding): {long_enough:,} ({long_enough / n_rev:.1%})")

    banner("RATING DISTRIBUTION (class imbalance for the AI step)")
    reviews.groupBy("rating").count().orderBy("rating").withColumn(
        "pct", F.round(100 * F.col("count") / n_rev, 2)).show()

    banner("TIME RANGE (windowing for drift / anomaly detection)")
    ts = reviews.withColumn("ts", (F.col("timestamp") / 1000).cast("timestamp"))
    ts.select(F.min("ts").alias("first"), F.max("ts").alias("last")).show(truncate=False)
    ts.groupBy(F.year("ts").alias("yr")).count().orderBy("yr").show(30)

    banner("VERIFIED PURCHASE / HELPFULNESS")
    reviews.groupBy("verified_purchase").count().show()
    reviews.select(F.mean("helpful_vote").alias("mean_helpful"),
                   F.max("helpful_vote").alias("max_helpful")).show()

    banner("REVIEW VOLUME PER PRODUCT (skew -> partitioning talking point)")
    per = reviews.groupBy("parent_asin").count()
    per.select(F.mean("count").alias("mean"),
               F.expr("percentile_approx(count, 0.5)").alias("p50"),
               F.expr("percentile_approx(count, 0.99)").alias("p99"),
               F.max("count").alias("max")).show()
    print("products with >= 50 reviews (candidates for trend analysis):",
          f"{per.filter(F.col('count') >= 50).count():,}")

    banner("META COMPLETENESS (join quality)")
    for c in ["title", "price", "store", "description", "features", "categories", "details"]:
        if c in meta.columns:
            nn = meta.filter(F.col(c).isNotNull()).count()
            print(f"{c:14s} non-null: {nn:>8,} ({nn / n_meta:6.1%})")

    joined = reviews.select("parent_asin").distinct().join(
        meta.select("parent_asin").distinct(), "parent_asin", "left_semi").count()
    d = reviews.select("parent_asin").distinct().count()
    print(f"\nreview products matched in meta: {joined:,}/{d:,} ({joined / d:.1%})")

    banner("DUPLICATES (dedupe step in silver)")
    exact = reviews.groupBy("user_id", "parent_asin", "timestamp").count().filter(F.col("count") > 1).count()
    print(f"(user_id, parent_asin, timestamp) collisions: {exact:,}")

    spark.stop()


if __name__ == "__main__":
    main()

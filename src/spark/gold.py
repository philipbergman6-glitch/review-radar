"""Job `gold`: silver reviews -> product_month spine -> evaluation points -> decline episodes.

Reads `silver.reviews` pinned to the snapshot the latest successful silver run recorded
(ADR-0008: never a table's current snapshot), aggregates each product's reviews by calendar
month, expands every product to its full calendar spine (empty months kept, ADR-0001), and
applies the decline rule in src/gold/rule.py per product. Three tables are replaced
atomically and stamped with this run's id:

  gold.product_month      one row per (product, month) on the spine
  gold.evaluation_points  one row per (product, month) the rule evaluated
  gold.decline_episodes   one row per episode (condition start -> alert -> closure)

Only products that could ever be evaluable are materialised: a product with fewer than
`min_reviews` reviews in total can never fill a window. Every other product is counted and
excluded with the reason `below_min_reviews`.

While conf/decline_rule.toml is `provisional`, no point on or after `holdout_start` is
evaluated; the printed GOLD_ANALYTICAL line then covers the development period only.

Run:  ./run.sh python -m src.spark.gold [--scope full|sample] [--verify-rerun]
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.spark import CATALOG, build
from src.gates import gold as gate
from src.gold.rule import CLOSED_BY, RULE_PATH, Rule, evaluate, load_rule
from src.spark.silver import digest, write_replace

sys.stdout.reconfigure(line_buffering=True)

LONG_TEXT_WORDS = 20  # the vector-cohort predicate, frozen in RR-06

POINT_SCHEMA = StructType([
    StructField("parent_asin", StringType()), StructField("point_month", StringType()),
    StructField("in_holdout", BooleanType()),
    StructField("baseline_start", StringType()), StructField("baseline_end", StringType()),
    StructField("recent_start", StringType()), StructField("recent_end", StringType()),
    StructField("baseline_reviews", IntegerType()), StructField("baseline_active_months", IntegerType()),
    StructField("baseline_mean", DoubleType()), StructField("baseline_neg_share", DoubleType()),
    StructField("baseline_verified_reviews", IntegerType()), StructField("baseline_verified_mean", DoubleType()),
    StructField("baseline_nonempty_text", IntegerType()), StructField("baseline_long_text", IntegerType()),
    StructField("recent_reviews", IntegerType()), StructField("recent_active_months", IntegerType()),
    StructField("recent_mean", DoubleType()), StructField("recent_neg_share", DoubleType()),
    StructField("recent_verified_reviews", IntegerType()), StructField("recent_verified_mean", DoubleType()),
    StructField("recent_nonempty_text", IntegerType()), StructField("recent_long_text", IntegerType()),
    StructField("evaluable", BooleanType()), StructField("unevaluable_reason", StringType()),
    StructField("condition", BooleanType()), StructField("drop", DoubleType()),
    StructField("persistence_run", IntegerType()), StructField("alert", BooleanType()),
    StructField("episode_key", StringType()),
])
EPISODE_SCHEMA = StructType([
    StructField("parent_asin", StringType()), StructField("episode_id", StringType()),
    StructField("condition_started_at", StringType()), StructField("alert_triggered_at", StringType()),
    StructField("alert_complete_month", StringType()), StructField("last_supported_at", StringType()),
    StructField("episode_closed_at", StringType()), StructField("closed_by", StringType()),
    StructField("in_holdout", BooleanType()),
    StructField("evaluable_points", IntegerType()), StructField("supported_points", IntegerType()),
    StructField("baseline_mean_at_alert", DoubleType()), StructField("recent_mean_at_alert", DoubleType()),
    StructField("drop_at_alert", DoubleType()), StructField("max_drop", DoubleType()),
])
SPINE_COLS = ["month", "review_count", "rating_sum", "neg_count", "verified_count",
              "verified_rating_sum", "nonempty_text_count", "long_text_count"]


# --------------------------------------------------------------- the spine ----
def monthly_aggregates(reviews: DataFrame) -> DataFrame:
    """One row per (parent_asin, month) that has at least one review."""
    return (reviews
            .withColumn("month", F.date_format("review_month", "yyyy-MM"))
            .groupBy("parent_asin", "month")
            .agg(F.count("*").alias("review_count"),
                 F.sum("rating").cast("double").alias("rating_sum"),
                 F.sum(F.when(F.col("rating") <= 2, 1).otherwise(0)).alias("neg_count"),
                 F.sum(F.when(F.col("verified_purchase"), 1).otherwise(0)).alias("verified_count"),
                 F.sum(F.when(F.col("verified_purchase"), F.col("rating")).otherwise(0))
                  .cast("double").alias("verified_rating_sum"),
                 F.sum(F.when(F.col("text_word_count") > 0, 1).otherwise(0)).alias("nonempty_text_count"),
                 F.sum(F.when(F.col("text_word_count") >= LONG_TEXT_WORDS, 1).otherwise(0))
                  .alias("long_text_count"),
                 F.countDistinct("user_id").alias("distinct_users")))


def calendar_spine(monthly: DataFrame) -> DataFrame:
    """Expand to every month from each product's first to last review; empty months zeroed."""
    bounds = monthly.groupBy("parent_asin").agg(F.min("month").alias("first"), F.max("month").alias("last"))
    months = bounds.select(
        "parent_asin",
        F.explode(F.expr("transform(sequence(to_date(concat(first,'-01')), to_date(concat(last,'-01')), "
                         "interval 1 month), d -> date_format(d, 'yyyy-MM'))")).alias("month"))
    spine = months.join(monthly, on=["parent_asin", "month"], how="left")
    zero_cols = ["review_count", "rating_sum", "neg_count", "verified_count", "verified_rating_sum",
                 "nonempty_text_count", "long_text_count", "distinct_users"]
    spine = spine.fillna({c: 0 for c in zero_cols})
    return (spine
            .withColumn("mean_rating", F.when(F.col("review_count") > 0, F.col("rating_sum") / F.col("review_count")))
            .withColumn("neg_share", F.when(F.col("review_count") > 0, F.col("neg_count") / F.col("review_count")))
            .withColumn("verified_mean", F.when(F.col("verified_count") > 0,
                                                F.col("verified_rating_sum") / F.col("verified_count")))
            .withColumn("month_date", F.to_date(F.concat(F.col("month"), F.lit("-01")))))


# ---------------------------------------------------------- the rule, per product ----
def _spine_rows(pdf: pd.DataFrame) -> list[dict[str, Any]]:
    pdf = pdf.sort_values("month")
    return pdf[SPINE_COLS].to_dict("records")


def points_udf(rule: Rule):
    def f(pdf: pd.DataFrame) -> pd.DataFrame:
        asin = pdf["parent_asin"].iloc[0]
        points, _ = evaluate(_spine_rows(pdf), rule)
        if not points:
            return pd.DataFrame({c.name: pd.Series(dtype="object") for c in POINT_SCHEMA})
        out = pd.DataFrame(points)
        out.insert(0, "parent_asin", asin)
        return out[[c.name for c in POINT_SCHEMA]]
    return f


def episodes_udf(rule: Rule):
    def f(pdf: pd.DataFrame) -> pd.DataFrame:
        asin = pdf["parent_asin"].iloc[0]
        _, episodes = evaluate(_spine_rows(pdf), rule)
        if not episodes:
            return pd.DataFrame({c.name: pd.Series(dtype="object") for c in EPISODE_SCHEMA})
        out = pd.DataFrame(episodes)
        out.insert(0, "parent_asin", asin)
        out.insert(1, "episode_id", asin + ":" + out["condition_started_at"])
        return out[[c.name for c in EPISODE_SCHEMA]]
    return f


def evaluate_products(spine: DataFrame, rule: Rule) -> tuple[DataFrame, DataFrame]:
    grouped = spine.select("parent_asin", *SPINE_COLS).groupBy("parent_asin")
    return (grouped.applyInPandas(points_udf(rule), POINT_SCHEMA),
            grouped.applyInPandas(episodes_udf(rule), EPISODE_SCHEMA))


# ------------------------------------------------------------------ tables ----
def gold_names(scope: str) -> dict[str, str]:
    suffix = "" if scope == "full" else f"_{scope}"
    return {"product_month": f"{CATALOG}.gold.product_month{suffix}",
            "evaluation_points": f"{CATALOG}.gold.evaluation_points{suffix}",
            "decline_episodes": f"{CATALOG}.gold.decline_episodes{suffix}"}


def ensure_tables(spark: SparkSession, names: dict[str, str]) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    props = ("TBLPROPERTIES ('write.format.default'='parquet', "
             "'write.parquet.compression-codec'='zstd', 'format-version'='2')")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['product_month']} (
        parent_asin STRING, month STRING, month_date DATE, review_count INT, rating_sum DOUBLE,
        mean_rating DOUBLE, neg_count INT, neg_share DOUBLE, verified_count INT,
        verified_rating_sum DOUBLE, verified_mean DOUBLE, nonempty_text_count INT,
        long_text_count INT, distinct_users INT,
        source_silver_run_id STRING, source_silver_snapshot_id BIGINT, run_id STRING
    ) USING iceberg PARTITIONED BY (years(month_date)) {props}""")
    point_cols = ", ".join(f"{f.name} {_sql_type(f.dataType)}" for f in POINT_SCHEMA)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['evaluation_points']} (
        {point_cols}, rule_config_hash STRING, source_silver_run_id STRING, run_id STRING
    ) USING iceberg {props}""")
    ep_cols = ", ".join(f"{f.name} {_sql_type(f.dataType)}" for f in EPISODE_SCHEMA)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['decline_episodes']} (
        {ep_cols}, rule_config_hash STRING, source_silver_run_id STRING, run_id STRING
    ) USING iceberg {props}""")


def _sql_type(t) -> str:
    return {StringType: "STRING", BooleanType: "BOOLEAN", IntegerType: "INT",
            DoubleType: "DOUBLE", LongType: "BIGINT"}[type(t)]


# -------------------------------------------------------------------- main ----
def run_gold(spark: SparkSession, *, scope: str, category: str, verify_rerun: bool) -> dict[str, Any]:
    t0 = time.time()
    rule = load_rule()
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if silver_run is None:
        raise RuntimeError(f"no successful silver run for {category}/{scope}; run make silver first")
    src = silver_run["outputs"]["silver.reviews"]
    names = gold_names(scope)

    run = runs.start("gold", runs.GOLD_SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"silver": {"run_id": silver_run["run_id"], "table": src["table"],
                                        "snapshot_id": src["snapshot_id"]},
                             "rule": {"path": str(RULE_PATH.relative_to(C.PROJECT_ROOT)),
                                      "config_hash": rule.config_hash, "status": rule.status}},
                     params=rule.as_params())
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        ensure_tables(spark, names)
        reviews = (spark.read.option("snapshot-id", src["snapshot_id"]).table(src["table"])
                   .select("parent_asin", "review_month", "rating", "verified_purchase",
                           "text_word_count", "user_id"))
        silver_rows = reviews.count()
        totals = reviews.groupBy("parent_asin").count()
        products_total = totals.count()
        eligible = totals.filter(F.col("count") >= rule.min_reviews).select("parent_asin")
        products_materialised = eligible.count()

        monthly = monthly_aggregates(reviews.join(eligible, "parent_asin"))
        spine = calendar_spine(monthly).cache()
        stamp = {"source_silver_run_id": F.lit(silver_run["run_id"]),
                 "source_silver_snapshot_id": F.lit(int(src["snapshot_id"])).cast("long"),
                 "run_id": F.lit(run.run_id)}
        spine_out = spine.select(
            "parent_asin", "month", "month_date",
            F.col("review_count").cast("int"), "rating_sum", "mean_rating",
            F.col("neg_count").cast("int"), "neg_share", F.col("verified_count").cast("int"),
            "verified_rating_sum", "verified_mean", F.col("nonempty_text_count").cast("int"),
            F.col("long_text_count").cast("int"), F.col("distinct_users").cast("int"),
            *[v.alias(k) for k, v in stamp.items()])

        agg = spine.agg(F.count("*").alias("rows"), F.sum("review_count").alias("reviews"),
                        F.sum(F.when(F.col("review_count") > 0, 1).otherwise(0)).alias("active")).first()
        bounds = spine.groupBy("parent_asin").agg(F.min("month_date").alias("f"), F.max("month_date").alias("l"))
        expected_rows = int(bounds.select(F.sum(F.months_between("l", "f").cast("int") + 1)).first()[0] or 0)
        counts.update({
            "products_total": products_total, "products_materialised": products_materialised,
            "products_below_min_reviews": products_total - products_materialised,
            "product_months": int(agg["rows"]), "product_months_expected": expected_rows,
            "active_product_months": int(agg["active"]), "reviews_on_spine": int(agg["reviews"]),
            "silver_rows": silver_rows,
        })

        points, episodes = evaluate_products(spine, rule)
        points = points.withColumn("rule_config_hash", F.lit(rule.config_hash)) \
            .withColumn("source_silver_run_id", F.lit(silver_run["run_id"])) \
            .withColumn("run_id", F.lit(run.run_id)).cache()
        episodes = episodes.withColumn("rule_config_hash", F.lit(rule.config_hash)) \
            .withColumn("source_silver_run_id", F.lit(silver_run["run_id"])) \
            .withColumn("run_id", F.lit(run.run_id)).cache()

        pa = points.agg(
            F.count("*").alias("points"),
            F.sum(F.when(F.col("evaluable"), 1).otherwise(0)).alias("evaluable"),
            F.sum(F.when(F.col("condition"), 1).otherwise(0)).alias("condition_true"),
            F.sum(F.when(F.col("alert"), 1).otherwise(0)).alias("alerts"),
            F.sum(F.when(F.col("in_holdout"), 1).otherwise(0)).alias("holdout_points"),
            F.sum(F.when(F.col("in_holdout") & F.col("evaluable"), 1).otherwise(0)).alias("holdout_evaluable"),
            F.sum(F.when(F.col("in_holdout") & F.col("alert"), 1).otherwise(0)).alias("holdout_alerts"),
            F.countDistinct(F.when(F.col("evaluable"), F.col("parent_asin"))).alias("evaluable_products"),
            F.countDistinct(F.when(F.col("in_holdout") & F.col("evaluable"), F.col("parent_asin")))
             .alias("holdout_eligible_products"),
            F.countDistinct(F.when(F.col("in_holdout") & F.col("alert"), F.col("parent_asin")))
             .alias("holdout_alerted_products")).first()
        reasons = {r["unevaluable_reason"]: int(r["count"]) for r in
                   points.filter(F.col("unevaluable_reason").isNotNull())
                   .groupBy("unevaluable_reason").count().collect()}
        ea = episodes.agg(F.count("*").alias("episodes"),
                          *[F.sum(F.when(F.col("closed_by") == c, 1).otherwise(0)).alias(c) for c in CLOSED_BY],
                          F.sum(F.when(F.col("in_holdout"), 1).otherwise(0)).alias("holdout_episodes")).first()
        counts.update({k: int(pa[k] or 0) for k in pa.asDict()})
        counts["unevaluable_reasons"] = reasons
        counts["episodes"] = int(ea["episodes"] or 0)
        counts["episodes_by_closure"] = {c: int(ea[c] or 0) for c in CLOSED_BY}
        counts["holdout_episodes"] = int(ea["holdout_episodes"] or 0)

        outputs["gold.product_month"] = {"table": names["product_month"],
                                         "snapshot_id": write_replace(spine_out, names["product_month"], run.run_id)}
        outputs["gold.evaluation_points"] = {"table": names["evaluation_points"],
                                             "snapshot_id": write_replace(points, names["evaluation_points"], run.run_id)}
        outputs["gold.decline_episodes"] = {"table": names["decline_episodes"],
                                            "snapshot_id": write_replace(episodes, names["decline_episodes"], run.run_id)}
        counts["digests"] = {
            "product_month": digest(spine_out, "parent_asin", "month", "review_count", "rating_sum",
                                    "verified_count", "long_text_count"),
            "evaluation_points": digest(points, "parent_asin", "point_month", "evaluable", "condition",
                                        "alert", "episode_key"),
            "decline_episodes": digest(episodes, "episode_id", "alert_triggered_at", "closed_by",
                                       "episode_closed_at"),
        }
        counts["elapsed_s"] = round(time.time() - t0, 1)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=silver_rows, records_out=counts["product_months"],
                 records_rejected=0, outputs=outputs, counts=counts)
    print_lines(rule, counts, run_id=run.run_id, scope=scope, category=category)

    if verify_rerun:
        prev = previous_matching_run(run.run_id, silver_run["run_id"], rule.config_hash, category, scope)
        if prev is None:
            print("GOLD_RERUN previous_run=none identical=n/a")
        else:
            same = prev["counts"].get("digests") == counts["digests"]
            print(f"GOLD_RERUN previous_run={prev['run_id']} identical={'true' if same else 'false'}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def print_lines(rule: Rule, c: dict[str, Any], *, run_id: str, scope: str,
                category: str = C.CATEGORY) -> None:
    """Print gold's constituent lines and both verdicts, then publish them to `eval/`.

    Gold's gate lives in the job rather than in a separate script because it re-derives
    nothing: every constituent is a count the job just made over data it just wrote. The
    decisions themselves are pure and testable in `src/gates/gold.py`.
    """
    analytical = gate.analytical(c, rule_status=rule.status, holdout_start=rule.holdout_start,
                                 config_hash=rule.config_hash)
    v = gate.verdict(c, run_id=run_id, scope=scope, rule_status=rule.status)
    for line in gate.constituent_lines(c):
        print(line)
    print(analytical.terminal)
    print(v.terminal)

    common = {"protocol_hash": rule.config_hash, "pipeline_run_id": run_id, "scope": scope}
    E.record(v, capability="gold", phase="P3 Gold", kind="reproducibility",
             population={"name": f"{category}/gold.product_month", "n": c["product_months"],
                         "products_materialised": c["products_materialised"],
                         "silver_rows": c["silver_rows"]}, **common)
    E.record(analytical, capability="gold_analytical", phase="P3 Gold", kind="quality",
             population=gate.analytical_population(c, rule_status=rule.status),
             notes=[(f"decline rule {rule.status} at {RULE_PATH.name}; holdout starts "
                     f"{rule.holdout_start}")], **common)


def previous_matching_run(this_run_id: str, silver_run_id: str, config_hash: str, category: str,
                          scope: str) -> dict[str, Any] | None:
    from src.common.pg import connect
    with connect() as conn:
        row = conn.execute(
            """SELECT run_id, counts FROM pipeline_runs
               WHERE job_name='gold' AND status='success' AND category=%s AND data_scope=%s
                 AND run_id <> %s AND inputs->'silver'->>'run_id' = %s
                 AND inputs->'rule'->>'config_hash' = %s
               ORDER BY started_at DESC LIMIT 1""",
            (category, scope, this_run_id, silver_run_id, config_hash)).fetchone()
    return None if row is None else {"run_id": str(row[0]), "counts": row[1]}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--verify-rerun", action="store_true")
    args = ap.parse_args()
    spark = build("gold-decline")
    try:
        run_gold(spark, scope=args.scope, category=args.category, verify_rerun=args.verify_rerun)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

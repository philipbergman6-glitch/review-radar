"""Job `theme_samples`: decline candidates, their matched controls, and the discovery draw.

P6 may only look at reviews this job has assigned to a named sample. It reads the gold
evaluation points and episodes pinned to the latest successful gold run, keeps the pre-2020
episodes that are *text-characterisable*, matches each one against controls that never alert
under the locked rule (src/gold/controls.py, pure), and lands two Iceberg tables:

  gold.matched_controls            one row per (episode, control, rank) with its match stats
  gold.theme_sample_assignments    one row per (sample_name, review) that P6 may read

Disjointness is enforced against the assignment table itself: a review already assigned to
one sample can never be drawn into another (ADR-0003 -- discovery, development, training pool
and audit must not overlap). The discovery draw is seeded and stratified (70% rated <= 3) and
depends on nothing but (seed, salt, review ids), so it reproduces exactly.

Run:  ./run.sh python -m src.spark.theme_samples --sample discovery [--scope full|sample]
"""
from __future__ import annotations

import argparse
import sys
import time
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import runs
from src.common.spark import CATALOG, build
from src.gold.controls import SAMPLING_PATH, load_protocol, match_controls, text_characterisable
from src.gold.rule import RULE_PATH, load_rule

sys.stdout.reconfigure(line_buffering=True)

SAMPLE_NAMES = ("discovery", "development", "training_pool", "audit", "inference")


def table_names(scope: str) -> dict[str, str]:
    suffix = "" if scope == "full" else f"_{scope}"
    return {"controls": f"{CATALOG}.gold.matched_controls{suffix}",
            "assignments": f"{CATALOG}.gold.theme_sample_assignments{suffix}"}


def ensure_tables(spark: SparkSession, names: dict[str, str]) -> None:
    props = ("TBLPROPERTIES ('write.format.default'='parquet', "
             "'write.parquet.compression-codec'='zstd', 'format-version'='2')")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['controls']} (
        episode_id STRING, candidate_asin STRING, point_month STRING, control_asin STRING,
        control_rank INT, baseline_mean_diff DOUBLE, abs_log2_volume_ratio DOUBLE,
        candidate_baseline_mean DOUBLE, candidate_baseline_reviews INT,
        candidate_recent_mean DOUBLE, candidate_recent_reviews INT,
        control_baseline_mean DOUBLE, control_baseline_reviews INT,
        control_recent_mean DOUBLE, control_recent_reviews INT,
        baseline_start STRING, baseline_end STRING, recent_start STRING, recent_end STRING,
        protocol_config_hash STRING, rule_config_hash STRING,
        source_gold_run_id STRING, run_id STRING
    ) USING iceberg {props}""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {names['assignments']} (
        sample_name STRING, review_id STRING, parent_asin STRING, role STRING,
        episode_id STRING, rating INT, text_word_count INT, stratum STRING, draw_key DOUBLE,
        protocol_config_hash STRING, source_silver_run_id STRING, source_gold_run_id STRING,
        run_id STRING, assigned_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (sample_name) {props}""")


# ------------------------------------------------------------------ selection ----
def candidate_points(spark: SparkSession, points, episodes, protocol) -> list[dict[str, Any]]:
    """Pre-2020 alerting episodes joined to the evaluation point that triggered them."""
    ep = episodes.filter(~F.col("in_holdout")).select(
        "episode_id", "parent_asin", F.col("alert_triggered_at").alias("point_month"),
        "condition_started_at", "episode_closed_at", "closed_by", "max_drop")
    pt = points.filter(~F.col("in_holdout"))
    joined = ep.join(pt, on=["parent_asin", "point_month"], how="inner")
    missing = ep.count() - joined.count()
    if missing:
        raise RuntimeError(f"{missing} pre-2020 episode(s) have no evaluation point at their alert month")
    return [r.asDict() for r in joined.collect()]


def control_pool(points, episodes) -> tuple[list[dict[str, Any]], int]:
    """Evaluable pre-2020 points on products that never alert anywhere on their spine."""
    alerting = episodes.select("parent_asin").distinct()
    pool = (points.filter(~F.col("in_holdout") & F.col("evaluable"))
            .join(alerting, on="parent_asin", how="left_anti"))
    return [r.asDict() for r in pool.collect()], alerting.count()


def match_all(candidates: list[dict[str, Any]], pool: list[dict[str, Any]], protocol,
              ) -> tuple[list[dict[str, Any]], dict[str, int]]:
    by_month: dict[str, list[dict[str, Any]]] = {}
    for r in pool:
        by_month.setdefault(r["point_month"], []).append(r)
    rows: list[dict[str, Any]] = []
    dropped = {"not_text_characterisable": 0, "no_matching_control": 0}
    matched = 0
    for cand in sorted(candidates, key=lambda c: c["episode_id"]):
        if not text_characterisable(cand, protocol.matching):
            dropped["not_text_characterisable"] += 1
            continue
        controls = match_controls(cand, by_month.get(cand["point_month"], []), protocol.matching)
        if not controls:
            dropped["no_matching_control"] += 1
            continue
        matched += 1
        for c in controls:
            rows.append({
                "episode_id": cand["episode_id"], "candidate_asin": cand["parent_asin"],
                "point_month": cand["point_month"],
                "candidate_baseline_mean": float(cand["baseline_mean"]),
                "candidate_baseline_reviews": int(cand["baseline_reviews"]),
                "candidate_recent_mean": float(cand["recent_mean"]),
                "candidate_recent_reviews": int(cand["recent_reviews"]),
                "baseline_start": cand["baseline_start"], "baseline_end": cand["baseline_end"],
                "recent_start": cand["recent_start"], "recent_end": cand["recent_end"], **c})
    return rows, {"matched_candidates": matched,
                  "dropped_not_text_characterisable": dropped["not_text_characterisable"],
                  "dropped_no_matching_control": dropped["no_matching_control"]}


def window_reviews(spark: SparkSession, silver, control_rows: list[dict[str, Any]], protocol):
    """Every silver review inside a matched pair's baseline..recent span, tagged by role."""
    spans: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in control_rows:
        for asin, role in ((r["candidate_asin"], "candidate"), (r["control_asin"], "control")):
            key = (asin, role, r["episode_id"])
            spans[key] = {"parent_asin": asin, "role": role, "episode_id": r["episode_id"],
                          "window_start": r["baseline_start"], "window_end": r["recent_end"]}
    span_df = spark.createDataFrame(list(spans.values()))
    rev = (silver.filter(F.length(F.trim(F.coalesce(F.col("text"), F.lit("")))) > 0)
           .withColumn("month", F.date_format("review_month", "yyyy-MM"))
           .select("review_id", "parent_asin", "month", "rating", "text_word_count"))
    joined = (rev.join(F.broadcast(span_df), on="parent_asin", how="inner")
              .filter((F.col("month") >= F.col("window_start")) & (F.col("month") <= F.col("window_end"))))
    # One row per review: a review may sit in several episodes' spans; keep the first
    # (role, episode_id) by sort order so the draw frame has no duplicated review ids.
    w = joined.groupBy("review_id").agg(
        F.first("parent_asin").alias("parent_asin"), F.first("rating").alias("rating"),
        F.first("text_word_count").alias("text_word_count"),
        F.min(F.concat_ws("\x1f", F.col("role"), F.col("episode_id"))).alias("role_episode"))
    return (w.withColumn("role", F.split("role_episode", "\x1f").getItem(0))
            .withColumn("episode_id", F.split("role_episode", "\x1f").getItem(1))
            .drop("role_episode"))


# ----------------------------------------------------------------- the run ----
def run_theme_samples(spark: SparkSession, *, sample: str, scope: str, category: str) -> dict[str, Any]:
    t0 = time.time()
    if sample != "discovery":
        raise NotImplementedError(
            f"sample {sample!r} is drawn by a later P6 session: development, audit, training_pool "
            "and inference frames all depend on the frozen theme terms (ADR-0003)")
    protocol, rule = load_protocol(), load_rule()
    if protocol.holdout_start != rule.holdout_start:
        raise RuntimeError(f"protocol.holdout_start {protocol.holdout_start} != "
                           f"rule.holdout_start {rule.holdout_start}")
    gold_run = runs.latest_success("gold", category=category, data_scope=scope)
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if gold_run is None or silver_run is None:
        raise RuntimeError("theme_samples needs one successful gold and one successful silver run")
    pts, eps = gold_run["outputs"]["gold.evaluation_points"], gold_run["outputs"]["gold.decline_episodes"]
    slv = silver_run["outputs"]["silver.reviews"]
    names = table_names(scope)
    ensure_tables(spark, names)

    run = runs.start("theme_samples", runs.THEME_SAMPLES_SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"gold": {"run_id": gold_run["run_id"],
                                      "points_table": pts["table"], "points_snapshot_id": pts["snapshot_id"],
                                      "episodes_table": eps["table"], "episodes_snapshot_id": eps["snapshot_id"]},
                             "silver": {"run_id": silver_run["run_id"], "table": slv["table"],
                                        "snapshot_id": slv["snapshot_id"]},
                             "protocol": {"path": str(SAMPLING_PATH.relative_to(C.PROJECT_ROOT)),
                                          "status": protocol.status, "config_hash": protocol.config_hash},
                             "rule": {"path": str(RULE_PATH.relative_to(C.PROJECT_ROOT)),
                                      "config_hash": rule.config_hash, "status": rule.status}},
                     params={"sample": sample, "seed": protocol.seed,
                             "controls_per_candidate": protocol.matching.controls_per_candidate,
                             "discovery_size": protocol.discovery.size})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"protocol_config_hash": protocol.config_hash[:12]}
    try:
        points = spark.read.option("snapshot-id", pts["snapshot_id"]).table(pts["table"])
        episodes = spark.read.option("snapshot-id", eps["snapshot_id"]).table(eps["table"])
        silver = spark.read.option("snapshot-id", slv["snapshot_id"]).table(slv["table"])

        candidates = candidate_points(spark, points, episodes, protocol)
        pool, alerting = control_pool(points, episodes)
        control_rows, match_counts = match_all(candidates, pool, protocol)
        counts.update({"candidate_episodes": len(candidates), "alerting_products_excluded": alerting,
                       "control_rows": len(control_rows),
                       "distinct_control_products": len({r["control_asin"] for r in control_rows}),
                       **match_counts})
        if not control_rows:
            raise RuntimeError("no candidate episode kept a matched control; the protocol cannot proceed")

        ctl_df = (spark.createDataFrame(control_rows)
                  .withColumn("protocol_config_hash", F.lit(protocol.config_hash))
                  .withColumn("rule_config_hash", F.lit(rule.config_hash))
                  .withColumn("source_gold_run_id", F.lit(gold_run["run_id"]))
                  .withColumn("run_id", F.lit(run.run_id)))
        ctl_cols = [f.name for f in spark.table(names["controls"]).schema.fields]
        (ctl_df.select(*ctl_cols).sortWithinPartitions("episode_id", "control_rank")
         .writeTo(names["controls"]).option("snapshot-property.run_id", run.run_id).overwritePartitions())
        outputs["gold.matched_controls"] = {"table": names["controls"],
                                            "snapshot_id": _newest_snapshot(spark, names["controls"], run.run_id)}

        frame = window_reviews(spark, silver, control_rows, protocol)
        counts["window_reviews"] = frame.count()
        eligible = frame.filter(F.col("text_word_count") >= protocol.discovery.min_text_words)
        already = (spark.table(names["assignments"]).select("review_id").distinct()
                   if spark.catalog.tableExists(names["assignments"]) else None)
        if already is not None:
            eligible = eligible.join(already, on="review_id", how="left_anti")
        rows = [r.asDict() for r in eligible.collect()]
        counts["eligible_window_reviews"] = len(rows)

        from src.gold.controls import draw_discovery, draw_key
        picked = draw_discovery(rows, protocol.discovery, protocol.seed)
        if len(picked) < protocol.discovery.size:
            raise RuntimeError(f"discovery draw short: {len(picked)} of {protocol.discovery.size} "
                               "(a stratum could not be filled under max_per_product)")
        for r in picked:
            r["draw_key"] = draw_key(r["review_id"], protocol.seed, protocol.discovery.salt)
        counts.update({
            "discovery_rows": len(picked),
            "discovery_low_rated": sum(1 for r in picked if r["stratum"] == "low"),
            "discovery_high_rated": sum(1 for r in picked if r["stratum"] == "high"),
            "discovery_distinct_products": len({r["parent_asin"] for r in picked}),
            "discovery_from_candidates": sum(1 for r in picked if r["role"] == "candidate"),
            "discovery_from_controls": sum(1 for r in picked if r["role"] == "control")})

        asg = (spark.createDataFrame(picked)
               .withColumn("sample_name", F.lit(sample))
               .withColumn("protocol_config_hash", F.lit(protocol.config_hash))
               .withColumn("source_silver_run_id", F.lit(silver_run["run_id"]))
               .withColumn("source_gold_run_id", F.lit(gold_run["run_id"]))
               .withColumn("run_id", F.lit(run.run_id))
               .withColumn("assigned_at", F.current_timestamp()))
        asg_cols = [f.name for f in spark.table(names["assignments"]).schema.fields]
        (asg.select(*asg_cols).writeTo(names["assignments"])
         .option("snapshot-property.run_id", run.run_id).overwritePartitions())
        snap = _newest_snapshot(spark, names["assignments"], run.run_id)
        outputs["gold.theme_sample_assignments"] = {"table": names["assignments"], "snapshot_id": snap,
                                                    "sample_name": sample}
        written = (spark.read.option("snapshot-id", snap).table(names["assignments"])
                   .filter(F.col("sample_name") == sample))
        if written.count() != len(picked) or written.select("review_id").distinct().count() != len(picked):
            raise RuntimeError(f"{names['assignments']}: sample {sample} did not land {len(picked)} distinct rows")
        counts["elapsed_s"] = round(time.time() - t0, 1)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["eligible_window_reviews"], records_out=counts["discovery_rows"],
                 records_rejected=0, outputs=outputs, counts=counts)
    print(f"SAMPLES run_id={run.run_id} sample={sample} scope={scope} "
          f"protocol={protocol.status}/{protocol.config_hash[:12]} rule={rule.status}/{rule.config_hash[:12]} "
          f"candidates={counts['candidate_episodes']} matched={counts['matched_candidates']} "
          f"dropped_text={counts['dropped_not_text_characterisable']} "
          f"dropped_nocontrol={counts['dropped_no_matching_control']} "
          f"controls={counts['control_rows']} control_products={counts['distinct_control_products']} "
          f"window_reviews={counts['window_reviews']} eligible={counts['eligible_window_reviews']} "
          f"drawn={counts['discovery_rows']} low={counts['discovery_low_rated']} "
          f"high={counts['discovery_high_rated']} products={counts['discovery_distinct_products']} "
          f"from_candidates={counts['discovery_from_candidates']} "
          f"from_controls={counts['discovery_from_controls']} elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def _newest_snapshot(spark: SparkSession, table: str, run_id: str) -> int:
    row = spark.sql(f"SELECT snapshot_id, summary['run_id'] AS rid FROM {table}.snapshots "
                    "ORDER BY committed_at DESC LIMIT 1").first()
    if row is None or row["rid"] != run_id:
        raise RuntimeError(f"{table}: newest snapshot is not stamped with run {run_id}")
    return int(row["snapshot_id"])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", default="discovery", choices=SAMPLE_NAMES)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    spark = build("theme_samples", cores="local[4]", driver_memory="3g")
    try:
        run_theme_samples(spark, sample=args.sample, scope=args.scope, category=args.category)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

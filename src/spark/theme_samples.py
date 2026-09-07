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

The four post-discovery frames are RR-22's (`inference` is still blocked on the decline-rule
protocol freeze):

  development    200 pre-2020 rows, fully enriched by conf/theme-terms.json
  audit          200 post-2020 rows: 120 enriched + 80 prevalence rows scored separately
  training_pool  3,000 pre-2020 prevalence rows for the MLlib baseline (ADR-0002)

"Enriched" offers a review for labelling; it never labels one. The audit frame is drawn from
the *products* selected pre-2020, observed at or after the holdout boundary -- no decline rule
runs post-2020, so the audit set stays untouched.

Run:  ./run.sh python -m src.spark.theme_samples --sample discovery [--scope full|sample]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai.theme_terms import TERMS_PATH, load_terms, matched_terms
from src.common import config as C
from src.common import runs
from src.common.spark import CATALOG, build
from src.gold.controls import (
    SAMPLING_PATH,
    draw_discovery,
    draw_enriched,
    draw_key,
    draw_representative,
    load_protocol,
    match_controls,
    text_characterisable,
)
from src.gold.rule import RULE_PATH, load_rule

sys.stdout.reconfigure(line_buffering=True)

SAMPLE_NAMES = ("discovery", "development", "training_pool", "audit", "inference")
TAXONOMY_PATH = C.PROJECT_ROOT / "conf" / "theme-taxonomy.json"


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
           .select("review_id", "parent_asin", "month", "rating", "text_word_count", "title", "text"))
    joined = (rev.join(F.broadcast(span_df), on="parent_asin", how="inner")
              .filter((F.col("month") >= F.col("window_start")) & (F.col("month") <= F.col("window_end"))))
    # One row per review: a review may sit in several episodes' spans; keep the first
    # (role, episode_id) by sort order so the draw frame has no duplicated review ids.
    w = joined.groupBy("review_id").agg(
        F.first("parent_asin").alias("parent_asin"), F.first("rating").alias("rating"),
        F.first("text_word_count").alias("text_word_count"),
        F.first("title").alias("title"), F.first("text").alias("text"),
        F.min(F.concat_ws("\x1f", F.col("role"), F.col("episode_id"))).alias("role_episode"))
    return (w.withColumn("role", F.split("role_episode", "\x1f").getItem(0))
            .withColumn("episode_id", F.split("role_episode", "\x1f").getItem(1))
            .drop("role_episode"))


def holdout_reviews(spark: SparkSession, silver, control_rows: list[dict[str, Any]],
                    holdout_start: str):
    """Post-holdout reviews of the pre-2020 candidate and control products (RR-22).

    ADR-0003 wants the audit set drawn from "candidate and control windows", post-2020. A
    post-2020 *window* cannot exist while conf/decline_rule.toml is provisional -- the gold
    job refuses to evaluate any point at or after the holdout boundary -- so the window is
    read as the product's holdout-side span. The products were chosen from pre-2020 evidence
    alone, and nothing has looked at what they did afterwards, which is exactly the property
    the audit set needs.
    """
    roles: dict[str, str] = {}
    for r in control_rows:
        roles.setdefault(r["control_asin"], "control")
    for r in control_rows:                       # a candidate outranks a control if both apply
        roles[r["candidate_asin"]] = "candidate"
    episodes: dict[str, str] = {}
    for r in sorted(control_rows, key=lambda r: r["episode_id"]):
        episodes.setdefault(r["candidate_asin"], r["episode_id"])
        episodes.setdefault(r["control_asin"], r["episode_id"])
    role_df = spark.createDataFrame(
        [{"parent_asin": a, "role": roles[a], "episode_id": episodes[a]} for a in sorted(roles)])
    rev = (silver.filter(F.length(F.trim(F.coalesce(F.col("text"), F.lit("")))) > 0)
           .withColumn("month", F.date_format("review_month", "yyyy-MM"))
           .filter(F.col("month") >= F.lit(holdout_start))
           .select("review_id", "parent_asin", "rating", "text_word_count", "title", "text"))
    return rev.join(F.broadcast(role_df), on="parent_asin", how="inner").drop("month")


def enrichment_order(taxonomy_path) -> list[str]:
    """Theme ids, rarest discovery support first: the order the enriched quota is filled in."""
    themes = json.loads(taxonomy_path.read_text())["themes"]
    return [t["id"] for t in sorted(themes, key=lambda t: (t["discovery_support"]["low_rated_share"],
                                                           t["id"]))]


# ----------------------------------------------------------------- the frames ----
ASSIGNMENT_FIELDS = ("review_id", "parent_asin", "role", "episode_id", "rating",
                     "text_word_count", "stratum", "draw_key")


def eligible_frame(spark: SparkSession, source, names: dict[str, str], protocol, *, with_text: bool):
    """Long-enough reviews of the source frame that no earlier sample already claimed.

    The left-anti join is the disjointness rule of ADR-0003: discovery, development, the
    training pool and the audit set never overlap, and that is enforced against the
    assignment table itself rather than against a remembered list.
    """
    cols = list(ASSIGNMENT_FIELDS[:6]) + (["title", "text"] if with_text else [])
    eligible = source.filter(F.col("text_word_count") >= protocol.discovery.min_text_words)
    if spark.catalog.tableExists(names["assignments"]):
        already = spark.table(names["assignments"]).select("review_id").distinct()
        eligible = eligible.join(already, on="review_id", how="left_anti")
    return eligible.select(*cols)


def draw_frame(rows: list[dict[str, Any]], *, frame, protocol, theme_order: list[str], terms,
               ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """One post-discovery frame: enriched rows first, then the prevalence rows (RR-22)."""
    counts: dict[str, Any] = {}
    picked: list[dict[str, Any]] = []
    if frame.enriched_rows:
        if terms is None:
            raise RuntimeError("an enriched frame needs conf/theme-terms.json")
        for r in rows:
            r["matched"] = matched_terms(r.get("title"), r.get("text"), terms)
        counts["eligible_matching_any_theme"] = sum(1 for r in rows if r["matched"])
        enriched, fill = draw_enriched(
            rows, theme_order=theme_order, per_theme=frame.per_theme_quota(len(theme_order)),
            total=frame.enriched_rows, seed=protocol.seed, salt=frame.salt)
        picked.extend(enriched)
        counts["enriched_rows"] = len(enriched)
        counts["enriched_per_theme"] = fill
        counts["enriched_backfill"] = sum(1 for r in enriched if r["stratum"] == "enriched_backfill")
        counts["enriched_themes_short"] = sorted(
            t for t, n in fill.items() if n < frame.per_theme_quota(len(theme_order)))
        matched_in_theme = [r["matched"].get(r["stratum"].removeprefix("enriched_"), 0)
                            for r in enriched if r["stratum"] != "enriched_backfill"]
        counts["enriched_mean_terms_matched"] = (
            round(sum(matched_in_theme) / len(matched_in_theme), 2) if matched_in_theme else None)
    if frame.representative_rows:
        rep = draw_representative(rows, size=frame.representative_rows, seed=protocol.seed,
                                  salt=frame.salt, exclude={r["review_id"] for r in picked})
        picked.extend(rep)
        counts["representative_rows"] = len(rep)
    if len(picked) < frame.size:
        raise RuntimeError(f"{frame.name} draw short: {len(picked)} of {frame.size} rows; the "
                           "eligible frame cannot fill the protocol and must not be topped up "
                           "from another population")
    for r in picked:
        r["draw_key"] = draw_key(r["review_id"], protocol.seed, frame.salt)
        r.pop("matched", None)
        r.pop("title", None)
        r.pop("text", None)
    counts["low_rated_rows"] = sum(1 for r in picked
                                   if int(r["rating"]) <= protocol.discovery.low_rated_max_stars)
    counts["distinct_products"] = len({r["parent_asin"] for r in picked})
    counts["from_candidates"] = sum(1 for r in picked if r["role"] == "candidate")
    counts["from_controls"] = sum(1 for r in picked if r["role"] == "control")
    return picked, counts


# ----------------------------------------------------------------- the run ----
def run_theme_samples(spark: SparkSession, *, sample: str, scope: str, category: str) -> dict[str, Any]:
    t0 = time.time()
    if sample == "inference":
        raise NotImplementedError(
            "the inference frame needs conf/decline_rule.toml frozen: post-2020 candidate and "
            "control windows do not exist until the protocol freeze (ADR-0001, RR-22)")
    protocol, rule = load_protocol(), load_rule()
    if protocol.holdout_start != rule.holdout_start:
        raise RuntimeError(f"protocol.holdout_start {protocol.holdout_start} != "
                           f"rule.holdout_start {rule.holdout_start}")
    frame = None if sample == "discovery" else protocol.frames[sample]
    terms = theme_order = None
    if frame is not None:
        if not protocol.frozen:
            raise RuntimeError(f"the {sample} frame may only be drawn from a frozen sampling "
                               f"protocol; conf/theme_sampling.toml is {protocol.status!r}")
        theme_order = enrichment_order(TAXONOMY_PATH)
        if frame.enriched_rows:
            terms = load_terms()
    gold_run = runs.latest_success("gold", category=category, data_scope=scope)
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if gold_run is None or silver_run is None:
        raise RuntimeError("theme_samples needs one successful gold and one successful silver run")
    pts, eps = gold_run["outputs"]["gold.evaluation_points"], gold_run["outputs"]["gold.decline_episodes"]
    slv = silver_run["outputs"]["silver.reviews"]
    names = table_names(scope)
    ensure_tables(spark, names)

    params: dict[str, Any] = {"sample": sample, "seed": protocol.seed,
                              "controls_per_candidate": protocol.matching.controls_per_candidate,
                              "discovery_size": protocol.discovery.size}
    if frame is not None:
        params.update({"frame_size": frame.size, "frame_enriched_rows": frame.enriched_rows,
                       "frame_representative_rows": frame.representative_rows,
                       "frame_salt": frame.salt, "frame_config_hash": frame.config_hash})
    if terms is not None:
        params.update({"theme_terms_path": str(TERMS_PATH.relative_to(C.PROJECT_ROOT)),
                       "theme_terms_version": terms.version, "theme_terms_hash": terms.file_hash})
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
                     params=params)
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"protocol_config_hash": protocol.config_hash[:12]}
    if frame is not None:
        counts["frame_config_hash"] = frame.config_hash[:12]
    if terms is not None:
        counts["theme_terms_hash"] = terms.file_hash[:12]
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

        if sample == "audit":
            source = holdout_reviews(spark, silver, control_rows, protocol.holdout_start)
        else:
            source = window_reviews(spark, silver, control_rows, protocol)
        counts["source_reviews"] = source.count()
        eligible = eligible_frame(spark, source, names, protocol,
                                  with_text=frame is not None and bool(frame.enriched_rows))
        rows = [r.asDict() for r in eligible.collect()]
        counts["eligible_reviews"] = len(rows)

        if frame is None:
            picked = draw_discovery(rows, protocol.discovery, protocol.seed)
            if len(picked) < protocol.discovery.size:
                raise RuntimeError(f"discovery draw short: {len(picked)} of {protocol.discovery.size} "
                                   "(a stratum could not be filled under max_per_product)")
            for r in picked:
                r["draw_key"] = draw_key(r["review_id"], protocol.seed, protocol.discovery.salt)
            counts.update({
                "drawn_rows": len(picked),
                "low_rated_rows": sum(1 for r in picked if r["stratum"] == "low"),
                "high_rated_rows": sum(1 for r in picked if r["stratum"] == "high"),
                "distinct_products": len({r["parent_asin"] for r in picked}),
                "from_candidates": sum(1 for r in picked if r["role"] == "candidate"),
                "from_controls": sum(1 for r in picked if r["role"] == "control")})
        else:
            picked, frame_counts = draw_frame(rows, frame=frame, protocol=protocol,
                                              theme_order=theme_order, terms=terms)
            counts.update(frame_counts)
            counts["drawn_rows"] = len(picked)

        asg = (spark.createDataFrame([{k: r[k] for k in ASSIGNMENT_FIELDS} for r in picked])
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
        overlap = (spark.read.option("snapshot-id", snap).table(names["assignments"])
                   .groupBy("review_id").agg(F.countDistinct("sample_name").alias("n"))
                   .filter(F.col("n") > 1).count())
        if overlap:
            raise RuntimeError(f"{names['assignments']}: {overlap} review(s) are assigned to two samples")
        counts["elapsed_s"] = round(time.time() - t0, 1)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["eligible_reviews"], records_out=counts["drawn_rows"],
                 records_rejected=0, outputs=outputs, counts=counts)
    extra = ""
    if frame is not None:
        fill = counts.get("enriched_per_theme") or {}
        extra = (f"frame={frame.config_hash[:12]} enriched={counts.get('enriched_rows', 0)} "
                 f"backfill={counts.get('enriched_backfill', 0)} "
                 f"representative={counts.get('representative_rows', 0)} "
                 f"matched_any={counts.get('eligible_matching_any_theme', 0)} "
                 f"mean_terms={counts.get('enriched_mean_terms_matched')} "
                 f"short={','.join(counts.get('enriched_themes_short') or []) or 'none'} "
                 + " ".join(f"fill_{k}={v}" for k, v in sorted(fill.items())) + " ")
    print(f"SAMPLES run_id={run.run_id} sample={sample} scope={scope} "
          f"protocol={protocol.status}/{protocol.config_hash[:12]} rule={rule.status}/{rule.config_hash[:12]} "
          f"candidates={counts['candidate_episodes']} matched={counts['matched_candidates']} "
          f"dropped_text={counts['dropped_not_text_characterisable']} "
          f"dropped_nocontrol={counts['dropped_no_matching_control']} "
          f"controls={counts['control_rows']} control_products={counts['distinct_control_products']} "
          f"source_reviews={counts['source_reviews']} eligible={counts['eligible_reviews']} "
          f"{extra}drawn={counts['drawn_rows']} low_rated={counts['low_rated_rows']} "
          f"products={counts['distinct_products']} from_candidates={counts['from_candidates']} "
          f"from_controls={counts['from_controls']} elapsed_s={counts['elapsed_s']}")
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

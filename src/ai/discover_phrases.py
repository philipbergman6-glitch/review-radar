"""Job `theme_labels_llm`, budget line `discovery`: free complaint phrases from the 600-review
seeded discovery sample (ADR-0003 "Taxonomy and human labels", RR-19).

This is the step *before* a taxonomy exists: the model is given no theme list and extracts the
things each reviewer complains about, as an exact quote plus its own short aspect phrase. The
phrases are merged by hand into named themes afterwards (docs/theme-taxonomy/merge-table.csv),
preserving phrase counts, so the taxonomy is discovered from pre-2020 reviews rather than
invented.

Only `title` and `text` reach the model, and the model runs on this machine, so no review text
leaves it. Every attempt -- raw response, validation error, token counts -- is cached in
`gold.discovery_phrases` keyed by the local idempotency key, so a rerun resumes from cache and
never re-infers work already done.

Run:  ./run.sh python -m src.ai.discover_phrases [--scope full] [--limit N] [--chunk 25]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai import ollama
from src.ai.labels import DISCOVERY_SCHEMA, idempotency_key, load_spec, validate_discovery
from src.common import config as C
from src.common import runs
from src.common.spark import CATALOG, build

sys.stdout.reconfigure(line_buffering=True)

BUDGET_LINE = "discovery"
LABEL_SOURCE = "local_llm"


def table_name(scope: str) -> str:
    return f"{CATALOG}.gold.discovery_phrases" + ("" if scope == "full" else f"_{scope}")


def ensure_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        idempotency_key STRING NOT NULL, source_review_id STRING NOT NULL,
        budget_line STRING NOT NULL, label_source STRING NOT NULL, model_id STRING NOT NULL,
        label_spec_version STRING NOT NULL, prompt_version STRING NOT NULL,
        inference_config_hash STRING NOT NULL, api_mode STRING NOT NULL,
        label_status STRING NOT NULL,
        complaints ARRAY<STRUCT<quote: STRING, aspect: STRING>>,
        attempt_count INT NOT NULL,
        attempts ARRAY<STRUCT<attempt_no: INT, raw_response: STRING, validation_error: STRING,
                              input_tokens: INT, output_tokens: INT, duration_s: DOUBLE>>,
        source_silver_run_id STRING, run_id STRING, created_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (budget_line, prompt_version)
    TBLPROPERTIES ('write.format.default'='parquet', 'write.parquet.compression-codec'='zstd',
                   'format-version'='2')""")


def select_reviews(spark: SparkSession, *, scope: str, sample_run: dict[str, Any],
                   silver_run: dict[str, Any]) -> list[dict[str, Any]]:
    """The assigned discovery reviews, with the two fields the model is allowed to see."""
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    silver_out = silver_run["outputs"]["silver.reviews"]
    asg = (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
           .filter(F.col("sample_name") == BUDGET_LINE)
           .select("review_id", "parent_asin", "role", "rating", "draw_key"))
    silver = (spark.read.option("snapshot-id", silver_out["snapshot_id"]).table(silver_out["table"])
              .select("review_id", "title", "text"))
    rows = [r.asDict() for r in asg.join(silver, on="review_id", how="inner").collect()]
    missing = asg.count() - len(rows)
    if missing:
        raise RuntimeError(f"{missing} assigned discovery review(s) are absent from the pinned silver snapshot")
    rows.sort(key=lambda r: (r["draw_key"], r["review_id"]))
    return rows


def merge_chunk(spark: SparkSession, table: str, rows: list[dict[str, Any]]) -> None:
    """MERGE on the idempotency key: an existing terminal row is updated, never duplicated."""
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=spark.table(table).schema)
    df.createOrReplaceTempView("_incoming_labels")
    spark.sql(f"""
        MERGE INTO {table} t USING _incoming_labels s
        ON t.idempotency_key = s.idempotency_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def run_discovery(spark: SparkSession, *, scope: str, category: str, limit: int | None,
                  chunk: int) -> dict[str, Any]:
    t0 = time.time()
    spec = load_spec()
    config_hash = spec.config_hash("discovery", DISCOVERY_SCHEMA)
    prompt = spec.prompts["discovery"]
    sample_run = runs.latest_success("theme_samples", category=category, data_scope=scope)
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if sample_run is None or silver_run is None:
        raise RuntimeError("discovery needs one successful theme_samples and one successful silver run")
    table = table_name(scope)
    ensure_table(spark, table)

    run = runs.start("theme_labels_llm", runs.THEME_LABELS_SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"samples": {"run_id": sample_run["run_id"],
                                         "table": sample_run["outputs"]["gold.theme_sample_assignments"]["table"],
                                         "snapshot_id": sample_run["outputs"]["gold.theme_sample_assignments"]["snapshot_id"],
                                         "sample_name": BUDGET_LINE},
                             "silver": {"run_id": silver_run["run_id"],
                                        "table": silver_run["outputs"]["silver.reviews"]["table"],
                                        "snapshot_id": silver_run["outputs"]["silver.reviews"]["snapshot_id"]},
                             "spec": {"path": "conf/theme-label-spec.json",
                                      "version": spec.label_spec_version, "model_id": spec.model_id,
                                      "prompt_version": prompt.version, "config_hash": config_hash}},
                     params={"budget_line": BUDGET_LINE, "label_source": LABEL_SOURCE,
                             "api_mode": spec.api_mode, "inference": spec.inference, "chunk": chunk})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"inference_config_hash": config_hash[:12]}
    try:
        selected = select_reviews(spark, scope=scope, sample_run=sample_run, silver_run=silver_run)
        if limit:
            selected = selected[:limit]
        cached = {r["idempotency_key"] for r in
                  spark.table(table).filter(
                      (F.col("inference_config_hash") == config_hash)
                      & (F.col("label_status") != "api_failed")).select("idempotency_key").collect()}
        pending, hits = [], 0
        for r in selected:
            key = idempotency_key(source_review_id=r["review_id"], label_source=LABEL_SOURCE,
                                  model_id=spec.model_id, label_spec_version=spec.label_spec_version,
                                  prompt_version=prompt.version, inference_config_hash=config_hash)
            if key in cached:
                hits += 1
            else:
                pending.append((key, r))
        counts.update({"reviews_selected": len(selected), "cache_hits": hits,
                       "inferences_run": len(pending)})
        print(f"[discovery] {len(selected)} assigned, {hits} cached, {len(pending)} to infer "
              f"with {spec.model_id} (~{len(pending) * 6.7 / 60:.0f} min at 6.7 s/review)")

        tally = {"succeeded": 0, "parse_failed": 0, "api_failed": 0}
        phrases_total = retried = empty_lists = 0
        aspects: set[str] = set()
        buffer: list[dict[str, Any]] = []
        started = time.time()
        for i, (key, r) in enumerate(pending, start=1):
            payload = json.dumps({"title": r["title"], "text": r["text"]}, ensure_ascii=False)
            res = ollama.infer(spec, system=prompt.text, prompt=payload, schema=DISCOVERY_SCHEMA,
                               validate=lambda obj, rr=r: validate_discovery(
                                   obj, title=rr["title"], text=rr["text"], limits=spec.limits))
            tally[res.status] += 1
            retried += 1 if res.attempt_count > 1 else 0
            complaints = [{"quote": c["quote"], "aspect": c["aspect"]}
                          for c in (res.parsed or {}).get("complaints", [])]
            phrases_total += len(complaints)
            empty_lists += 1 if res.status == "succeeded" and not complaints else 0
            aspects.update(c["aspect"].strip().casefold() for c in complaints)
            buffer.append({
                "idempotency_key": key, "source_review_id": r["review_id"], "budget_line": BUDGET_LINE,
                "label_source": LABEL_SOURCE, "model_id": spec.model_id,
                "label_spec_version": spec.label_spec_version, "prompt_version": prompt.version,
                "inference_config_hash": config_hash, "api_mode": spec.api_mode,
                "label_status": res.status, "complaints": complaints,
                "attempt_count": res.attempt_count,
                "attempts": [{"attempt_no": a.attempt_no, "raw_response": a.raw_response,
                              "validation_error": a.validation_error, "input_tokens": a.input_tokens,
                              "output_tokens": a.output_tokens, "duration_s": a.duration_s}
                             for a in res.attempts],
                "source_silver_run_id": silver_run["run_id"], "run_id": run.run_id,
                "created_at": datetime.now(tz=UTC),
            })
            if len(buffer) >= chunk:
                merge_chunk(spark, table, buffer)
                buffer = []
                rate = (time.time() - started) / i
                print(f"[discovery] {i}/{len(pending)} done, {rate:.1f}s/review, "
                      f"ok={tally['succeeded']} parse_failed={tally['parse_failed']} "
                      f"api_failed={tally['api_failed']}, eta {(len(pending) - i) * rate / 60:.0f} min")
        merge_chunk(spark, table, buffer)

        snap = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        outputs["gold.discovery_phrases"] = {"table": table, "budget_line": BUDGET_LINE,
                                             "snapshot_id": int(snap["snapshot_id"]) if snap else None}
        landed = spark.table(table).filter(F.col("inference_config_hash") == config_hash)
        counts.update({
            "succeeded": tally["succeeded"], "parse_failed": tally["parse_failed"],
            "api_failed": tally["api_failed"], "retried_inferences": retried,
            "phrases_total": phrases_total, "distinct_aspects": len(aspects),
            "empty_complaint_lists": empty_lists,
            "table_rows_for_config": landed.count(),
            "distinct_keys_for_config": landed.select("idempotency_key").distinct().count(),
            "elapsed_s": round(time.time() - t0, 1),
            "seconds_per_review": round((time.time() - started) / len(pending), 2) if pending else None})
        if counts["table_rows_for_config"] != counts["distinct_keys_for_config"]:
            raise RuntimeError(f"{table}: {counts['table_rows_for_config']} rows but "
                               f"{counts['distinct_keys_for_config']} distinct idempotency keys")
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["reviews_selected"], records_out=counts["succeeded"],
                 records_rejected=counts["parse_failed"] + counts["api_failed"],
                 outputs=outputs, counts=counts)
    print(f"DISCOVERY run_id={run.run_id} scope={scope} model={spec.model_id} "
          f"prompt={prompt.version} config={config_hash[:12]} selected={counts['reviews_selected']} "
          f"cached={counts['cache_hits']} inferred={counts['inferences_run']} "
          f"ok={counts['succeeded']} parse_failed={counts['parse_failed']} "
          f"api_failed={counts['api_failed']} retried={counts['retried_inferences']} "
          f"phrases={counts['phrases_total']} aspects={counts['distinct_aspects']} "
          f"empty={counts['empty_complaint_lists']} rows={counts['table_rows_for_config']} "
          f"s_per_review={counts['seconds_per_review']} elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--limit", type=int, default=None, help="execution setting: infer at most N pending reviews")
    ap.add_argument("--chunk", type=int, default=25, help="execution setting: rows per MERGE")
    args = ap.parse_args()
    spark = build("theme_labels_llm", cores="local[4]", driver_memory="3g")
    try:
        run_discovery(spark, scope=args.scope, category=args.category, limit=args.limit, chunk=args.chunk)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

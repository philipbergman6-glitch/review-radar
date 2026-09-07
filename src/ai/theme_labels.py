"""`gold.review_theme_labels`: one row per logical inference, whoever produced it (ADR-0003).

The table is shared by three writers with three provenances, and keeping them in one table
with one schema is the point: a query can never mistake one for another, because
`label_source` is on every row.

  local_llm        the system under test -- `qwen3:8b`, and `llama3.2:3b` as the comparison
  agent_reference  P6's ground truth, written blind by the in-session agent (RR-21)
  human            reserved for Philip's 50-row adjudication subset; nothing else may use it

Aggregations must filter on `inference_config_hash` as well as `label_source`, so rows from
a superseded prompt version never mix into a count.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai.labels import LABEL_SOURCES, LABEL_STATUSES
from src.common.spark import CATALOG

TABLE = "gold.review_theme_labels"


def table_name(scope: str) -> str:
    return f"{CATALOG}.{TABLE}" + ("" if scope == "full" else f"_{scope}")


def ensure_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        idempotency_key STRING NOT NULL, source_review_id STRING NOT NULL,
        budget_line STRING NOT NULL, label_source STRING NOT NULL, model_id STRING NOT NULL,
        label_spec_version STRING NOT NULL, prompt_version STRING NOT NULL,
        inference_config_hash STRING NOT NULL, api_mode STRING NOT NULL,
        label_status STRING NOT NULL,
        themes ARRAY<STRUCT<theme_id: STRING, evidence_quote: STRING>>,
        other_present BOOLEAN, other_phrase STRING, abstain BOOLEAN,
        overall_sentiment STRING, label_confidence STRING,
        attempt_count INT NOT NULL,
        attempts ARRAY<STRUCT<attempt_no: INT, provider_request_id: STRING, raw_response: STRING,
                              validation_error: STRING, input_tokens: INT, output_tokens: INT,
                              estimated_cost_usd: DECIMAL(12,6), duration_s: DOUBLE,
                              completed_at: TIMESTAMP>>,
        source_silver_run_id STRING, run_id STRING, created_at TIMESTAMP NOT NULL
    ) USING iceberg PARTITIONED BY (budget_line, label_source, prompt_version)
    TBLPROPERTIES ('write.format.default'='parquet', 'write.parquet.compression-codec'='zstd',
                   'format-version'='2')""")


def merge_chunk(spark: SparkSession, table: str, rows: list[dict[str, Any]]) -> None:
    """MERGE on the idempotency key: an existing terminal row is updated, never duplicated."""
    if not rows:
        return
    df = spark.createDataFrame(rows, schema=spark.table(table).schema)
    df.createOrReplaceTempView("_incoming_theme_labels")
    spark.sql(f"""
        MERGE INTO {table} t USING _incoming_theme_labels s
        ON t.idempotency_key = s.idempotency_key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")


def label_row(*, idempotency_key: str, source_review_id: str, budget_line: str, label_source: str,
              model_id: str, label_spec_version: str, prompt_version: str,
              inference_config_hash: str, api_mode: str, status: str,
              parsed: dict[str, Any] | None, attempts: list[dict[str, Any]],
              source_silver_run_id: str | None, run_id: str) -> dict[str, Any]:
    """One table row. `status` is the *terminal* status; an abstention is not a failure.

    A row with no parsed answer keeps every label column null rather than a plausible default:
    a `parse_failed` row that stored `themes = []` would be counted as "the model saw no
    complaint", which is a different claim from "the model produced nothing usable".
    """
    if label_source not in LABEL_SOURCES:
        raise ValueError(f"label_source {label_source!r} is not one of {LABEL_SOURCES}")
    if status not in LABEL_STATUSES:
        raise ValueError(f"label_status {status!r} is not one of {LABEL_STATUSES}")
    ok = parsed is not None
    return {
        "idempotency_key": idempotency_key, "source_review_id": source_review_id,
        "budget_line": budget_line, "label_source": label_source, "model_id": model_id,
        "label_spec_version": label_spec_version, "prompt_version": prompt_version,
        "inference_config_hash": inference_config_hash, "api_mode": api_mode,
        "label_status": status,
        "themes": [{"theme_id": t["theme_id"], "evidence_quote": t["evidence_quote"]}
                   for t in parsed["themes"]] if ok else None,
        "other_present": parsed["other"]["present"] if ok else None,
        "other_phrase": parsed["other"]["phrase"] if ok else None,
        "abstain": parsed["abstain"] if ok else None,
        "overall_sentiment": parsed["overall_sentiment"] if ok else None,
        "label_confidence": parsed["label_confidence"] if ok else None,
        "attempt_count": len(attempts), "attempts": attempts,
        "source_silver_run_id": source_silver_run_id, "run_id": run_id,
        "created_at": datetime.now(tz=UTC),
    }


def terminal_status(status: str, parsed: dict[str, Any] | None) -> str:
    """`succeeded` and `model_abstained` are both valid answers; ADR-0003 keeps them apart."""
    if status == "succeeded" and parsed is not None and parsed.get("abstain") is True:
        return "model_abstained"
    return status


def cached_keys(spark: SparkSession, table: str, config_hash: str, label_source: str) -> set[str]:
    """Terminal rows already written for this configuration. `api_failed` is retried, not cached."""
    if not spark.catalog.tableExists(table):
        return set()
    rows = (spark.table(table)
            .filter((F.col("inference_config_hash") == config_hash)
                    & (F.col("label_source") == label_source)
                    & (F.col("label_status") != "api_failed"))
            .select("idempotency_key").collect())
    return {r["idempotency_key"] for r in rows}

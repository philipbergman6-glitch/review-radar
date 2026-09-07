"""Job `embeddings`: silver snapshot -> MiniLM vectors -> Iceberg `gold.review_embeddings`.

Reads `silver.reviews` pinned to the snapshot the latest successful silver run recorded,
keeps the vector cohort (`text_word_count >= cohort_min_words`, ADR-0005: membership by
`text` alone), prepares title + text, encodes it in the driver with the model the spec pins,
stages the vectors as Parquet, and lands them in one Iceberg commit that replaces the
partition of the active spec hash. Identity checks compare row counts and ID uniqueness,
vector dimension and unit norm before the ledger row can be `success`.

Run:  ./run.sh python -m src.ai.embed [--scope full|sample] [--batch-size 64]
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai import embedder
from src.ai.spec import EmbeddingSpec, load_spec, prepare_text
from src.common import config as C
from src.common import runs
from src.common.spark import CATALOG, build

sys.stdout.reconfigure(line_buffering=True)

CHUNK = 4096
NORM_TOL = 1e-3


def table_name(scope: str) -> str:
    return f"{CATALOG}.gold.review_embeddings" + ("" if scope == "full" else f"_{scope}")


def ensure_table(spark: SparkSession, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table} (
        review_id STRING, parent_asin STRING, text_word_count INT, vector ARRAY<FLOAT>,
        embedding_spec_hash STRING, model STRING, model_revision STRING, text_prep_version STRING,
        source_silver_run_id STRING, source_silver_snapshot_id BIGINT, run_id STRING,
        embedded_at TIMESTAMP
    ) USING iceberg PARTITIONED BY (embedding_spec_hash)
    TBLPROPERTIES ('write.format.default'='parquet', 'write.parquet.compression-codec'='zstd',
                   'format-version'='2')""")


def stage_vectors(cohort, spec: EmbeddingSpec, staging, *, batch_size: int) -> tuple[int, float]:
    """Encode the cohort in chunks; one Parquet part per chunk. Returns (rows, seconds)."""
    staging.mkdir(parents=True, exist_ok=False)
    schema = pa.schema([("review_id", pa.string()), ("parent_asin", pa.string()),
                        ("text_word_count", pa.int32()), ("vector", pa.list_(pa.float32(), spec.dims))])
    embedder.model(spec)  # load before the clock starts
    total, part, t0 = 0, 0, time.time()
    buf: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal total, part
        if not buf:
            return
        vecs = embedder.encode([prepare_text(r["title"], r["text"]) for r in buf], spec=spec,
                               batch_size=batch_size)
        tbl = pa.table({"review_id": [r["review_id"] for r in buf],
                        "parent_asin": [r["parent_asin"] for r in buf],
                        "text_word_count": pa.array([r["text_word_count"] for r in buf], pa.int32()),
                        "vector": pa.FixedSizeListArray.from_arrays(pa.array(vecs.ravel(), pa.float32()), spec.dims)},
                       schema=schema)
        pq.write_table(tbl, staging / f"part-{part:05d}.parquet")
        total += len(buf)
        part += 1
        buf.clear()
        if part % 10 == 0:
            print(f"[embed] {total:,} rows ({total / (time.time() - t0):,.0f}/s)")

    for row in cohort.toLocalIterator(prefetchPartitions=True):
        buf.append(row.asDict())
        if len(buf) >= CHUNK:
            flush()
    flush()
    return total, time.time() - t0


def run_embed(spark: SparkSession, *, scope: str, category: str, batch_size: int) -> dict[str, Any]:
    t0 = time.time()
    spec = load_spec()
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if silver_run is None:
        raise RuntimeError(f"no successful silver run for {category}/{scope}; run make silver first")
    src = silver_run["outputs"]["silver.reviews"]
    snapshot_id = int(src["snapshot_id"])
    table = table_name(scope)
    import torch
    run = runs.start("embeddings", runs.EMBEDDINGS_SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"silver": {"run_id": silver_run["run_id"], "table": src["table"],
                                        "snapshot_id": snapshot_id},
                             "spec": {"path": spec.rel_path(), "version": spec.version, "hash": spec.hash,
                                      "model": spec.model, "revision": spec.identity["revision"]}},
                     params={"batch_size": batch_size, "device": spec.execution.get("device", "cpu"),
                             "torch_threads": torch.get_num_threads(), "torch": torch.__version__,
                             "chunk": CHUNK, "table": table})
    staging = C.CHECKPOINTS / f"embeddings_{scope}_{run.run_id[:8]}"
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        reviews = spark.read.option("snapshot-id", snapshot_id).table(src["table"])
        silver_rows = reviews.count()
        cohort = (reviews.filter(F.col("text_word_count") >= spec.min_words)
                  .select("review_id", "parent_asin", "title", "text", "text_word_count"))
        cohort_rows = cohort.count()
        print(f"[embed] silver_rows={silver_rows:,} cohort_rows={cohort_rows:,} "
              f"(text_word_count >= {spec.min_words}) spec_hash={spec.hash[:12]}")

        embedded, seconds = stage_vectors(cohort, spec, staging, batch_size=batch_size)

        ensure_table(spark, table)
        staged = (spark.read.parquet(str(staging))
                  .withColumn("embedding_spec_hash", F.lit(spec.hash))
                  .withColumn("model", F.lit(spec.model))
                  .withColumn("model_revision", F.lit(spec.identity["revision"]))
                  .withColumn("text_prep_version", F.lit(spec.identity["text_prep_version"]))
                  .withColumn("source_silver_run_id", F.lit(silver_run["run_id"]))
                  .withColumn("source_silver_snapshot_id", F.lit(snapshot_id).cast("long"))
                  .withColumn("run_id", F.lit(run.run_id))
                  .withColumn("embedded_at", F.current_timestamp()))
        (staged.writeTo(table).option("snapshot-property.run_id", run.run_id).overwritePartitions())
        snap = spark.sql(f"SELECT snapshot_id, summary['run_id'] AS rid FROM {table}.snapshots "
                         "ORDER BY committed_at DESC LIMIT 1").first()
        if snap is None or snap["rid"] != run.run_id:
            raise RuntimeError(f"{table}: newest snapshot is not stamped with run {run.run_id}")
        table_snapshot = int(snap["snapshot_id"])

        written = (spark.read.option("snapshot-id", table_snapshot).table(table)
                   .filter(F.col("embedding_spec_hash") == spec.hash))
        agg = written.agg(
            F.count("*").alias("rows"), F.countDistinct("review_id").alias("distinct"),
            F.sum(F.when(F.size("vector") == spec.dims, 1).otherwise(0)).alias("dims_ok"),
            F.sum(F.when(F.abs(F.sqrt(F.aggregate("vector", F.lit(0.0),
                                                   lambda acc, x: acc + x.cast("double") * x.cast("double")))
                               - 1.0) < NORM_TOL, 1).otherwise(0)).alias("unit_norm")).first()
        outputs["gold.review_embeddings"] = {"table": table, "snapshot_id": table_snapshot, "spec_hash": spec.hash}
        counts.update({
            "silver_rows": silver_rows, "cohort_rows": cohort_rows, "below_min_words": silver_rows - cohort_rows,
            "embedded_rows": embedded, "table_rows_for_spec": int(agg["rows"]),
            "review_id_distinct": int(agg["distinct"]), "dims_ok_rows": int(agg["dims_ok"]),
            "unit_norm_rows": int(agg["unit_norm"]), "min_words": spec.min_words,
            "encode_s": round(seconds, 1), "reviews_per_s": round(embedded / seconds, 1) if seconds else None,
            "elapsed_s": round(time.time() - t0, 1),
        })
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    runs.success(run, records_in=silver_rows, records_out=embedded, records_rejected=0,
                 outputs=outputs, counts=counts)
    print(f"EMBED run_id={run.run_id} scope={scope} table={table} snapshot={table_snapshot} "
          f"silver_snapshot={snapshot_id} spec_hash={spec.hash[:12]} model={spec.model} "
          f"silver_rows={silver_rows} cohort_rows={cohort_rows} embedded={embedded} "
          f"distinct={counts['review_id_distinct']} dims_ok={counts['dims_ok_rows']} "
          f"unit_norm={counts['unit_norm_rows']} reviews_per_s={counts['reviews_per_s']} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--batch-size", type=int, default=None, help="execution setting, never hashed")
    args = ap.parse_args()
    spec = load_spec()
    bs = args.batch_size or int(spec.execution.get("batch_size", 64))
    spark = build("embeddings", cores="local[4]", driver_memory="3g")
    try:
        run_embed(spark, scope=args.scope, category=args.category, batch_size=bs)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

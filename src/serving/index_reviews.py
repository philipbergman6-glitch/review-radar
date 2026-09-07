"""Job `search_index_reviews`: silver snapshot -> review search index generation -> alias.

Reads `silver.reviews` pinned to the snapshot the latest successful silver run recorded,
keeps every row with non-empty text, validates each document against
conf/es/reviews.contract.json, bulk-loads a fresh generation, runs the analyzer tests on
it, checks the count identity, and only then swaps the `reviews` alias to it.

Run:  ./run.sh python -m src.serving.index_reviews [--scope full|sample] [--prune]
"""
from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Iterator
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config as C
from src.common import runs
from src.common.spark import build
from src.serving import projection as P
from src.serving.contract import Contract, load_contract, run_analyzer_tests

sys.stdout.reconfigure(line_buffering=True)

DOC_COLS = ["review_id", "parent_asin", "asin", "user_id", "event_ts", "review_month", "rating",
            "title", "text", "text_word_count", "verified_purchase", "helpful_vote", "image_count",
            "product_title", "main_category", "store", "price"]


def alias_for(contract: Contract, scope: str) -> str:
    return contract.alias if scope == "full" else f"{contract.alias}_{scope}"


def documents(df: DataFrame, *, silver_snapshot_id: int, silver_run_id: str,
              run_id: str) -> Iterator[dict[str, Any]]:
    for row in df.toLocalIterator(prefetchPartitions=True):
        d = row.asDict()
        doc = {k: d[k] for k in DOC_COLS if d[k] is not None}
        doc["event_ts"] = d["event_ts"].isoformat()
        doc["review_month"] = d["review_month"].isoformat()
        if "price" in doc:
            doc["price"] = float(doc["price"])
        doc["source_silver_snapshot_id"] = silver_snapshot_id
        doc["source_silver_run_id"] = silver_run_id
        doc["source_run_id"] = run_id
        yield doc


def run_index(spark: SparkSession, *, scope: str, category: str, prune: bool) -> dict[str, Any]:
    t0 = time.time()
    contract = load_contract("reviews")
    alias = alias_for(contract, scope)
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if silver_run is None:
        raise RuntimeError(f"no successful silver run for {category}/{scope}; run make silver first")
    src = silver_run["outputs"]["silver.reviews"]
    snapshot_id = int(src["snapshot_id"])

    run = runs.start("search_index_reviews", runs.SEARCH_REVIEWS_SPEC_VERSION, category=category,
                     data_scope=scope,
                     inputs={"silver": {"run_id": silver_run["run_id"], "table": src["table"],
                                        "snapshot_id": snapshot_id},
                             "contract": {"path": contract.rel_path(), "version": contract.version,
                                          "hash": contract.hash}},
                     params={"alias": alias, "bulk_chunk": P.BULK_CHUNK})
    es = P.client()
    index = P.generation_name(alias, snapshot_id, run.run_id)
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        reviews = spark.read.option("snapshot-id", snapshot_id).table(src["table"]).select(*DOC_COLS)
        silver_rows = reviews.count()
        with_text = reviews.filter(F.col("text_word_count") > 0)
        excluded = silver_rows - with_text.count()

        P.create_generation(es, contract, index)
        sent = P.bulk_load(es, contract, index, documents(
            with_text, silver_snapshot_id=snapshot_id, silver_run_id=silver_run["run_id"],
            run_id=run.run_id))
        es_count = P.count(es, index)
        tests = run_analyzer_tests(es, index, contract)
        failed_tests = [t for t in tests if not t["ok"]]
        if failed_tests:
            raise RuntimeError(f"analyzer tests failed on {index}: {failed_tests}")
        if es_count != sent:
            raise RuntimeError(f"{index}: es_count {es_count} != docs sent {sent}")
        previous = P.swap_alias(es, alias, index)
        pruned = P.prune_generations(es, alias, keep=1) if prune else []

        outputs["es.reviews"] = {"index": index, "alias": alias, "doc_count": es_count}
        counts.update({
            "silver_rows": silver_rows, "excluded_empty_text": excluded, "docs_sent": sent,
            "es_count": es_count, "previous_generations": previous, "pruned_generations": pruned,
            "contract_tests_total": len(tests), "contract_tests_passed": len(tests) - len(failed_tests),
            "analyzers": list(contract.analyzers), "contract_hash": contract.hash,
            "elapsed_s": round(time.time() - t0, 1),
        })
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=silver_rows, records_out=sent, records_rejected=0,
                 outputs=outputs, counts=counts)
    print(f"SEARCH_INDEX_REVIEWS run_id={run.run_id} scope={scope} alias={alias} index={index} "
          f"silver_snapshot={snapshot_id} silver_rows={silver_rows} excluded_empty_text={excluded} "
          f"docs={es_count} contract_tests={counts['contract_tests_passed']}/{counts['contract_tests_total']} "
          f"contract_hash={contract.hash[:12]} previous={','.join(previous) or 'none'} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--prune", action="store_true", help="delete old generations, keep one")
    args = ap.parse_args()
    spark = build("search-index-reviews", cores="local[4]", driver_memory="3g")
    try:
        run_index(spark, scope=args.scope, category=args.category, prune=args.prune)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

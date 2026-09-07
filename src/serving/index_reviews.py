"""Job `search_index_reviews`: silver snapshot -> review search index generation -> alias.

Reads `silver.reviews` pinned to the snapshot the latest successful silver run recorded,
keeps every row with non-empty text, validates each document against
conf/es/reviews.contract.json, bulk-loads a fresh generation, runs the analyzer tests on
it, checks the count identity, and only then swaps the `reviews` alias to it.

With `--with-embeddings` (P5) the generation also carries `text_vector` and
`embedding_spec_hash` for every row of `gold.review_embeddings` pinned to the latest
successful embeddings run, which must have been built from the same silver snapshot; the
ledger row is then spec version 2 and its identity requires every embedding row to have
become a vector-bearing document.

Run:  ./run.sh python -m src.serving.index_reviews [--scope full|sample] [--prune] [--with-embeddings]
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
              run_id: str, spec_hash: str | None = None, vector_docs: list[int] | None = None
              ) -> Iterator[dict[str, Any]]:
    for row in df.toLocalIterator(prefetchPartitions=True):
        d = row.asDict()
        doc = {k: d[k] for k in DOC_COLS if d[k] is not None}
        doc["event_ts"] = d["event_ts"].isoformat()
        doc["review_month"] = d["review_month"].isoformat()
        if "price" in doc:
            doc["price"] = float(doc["price"])
        if spec_hash is not None and d.get("vector") is not None:
            doc["text_vector"] = [float(x) for x in d["vector"]]
            doc["embedding_spec_hash"] = spec_hash
            vector_docs[0] += 1
        doc["source_silver_snapshot_id"] = silver_snapshot_id
        doc["source_silver_run_id"] = silver_run_id
        doc["source_run_id"] = run_id
        yield doc


def embeddings_input(spark: SparkSession, *, category: str, scope: str, silver_snapshot_id: int
                     ) -> tuple[dict[str, Any], DataFrame]:
    """The latest successful embeddings run, pinned; hard-fails if built from another silver snapshot."""
    emb_run = runs.latest_success("embeddings", category=category, data_scope=scope)
    if emb_run is None:
        raise RuntimeError(f"no successful embeddings run for {category}/{scope}; run make embed first")
    out = emb_run["outputs"]["gold.review_embeddings"]
    if int(emb_run["inputs"]["silver"]["snapshot_id"]) != silver_snapshot_id:
        raise RuntimeError(f"embeddings run {emb_run['run_id']} was built from silver snapshot "
                           f"{emb_run['inputs']['silver']['snapshot_id']}, index reads {silver_snapshot_id}")
    vectors = (spark.read.option("snapshot-id", int(out["snapshot_id"])).table(out["table"])
               .filter(F.col("embedding_spec_hash") == out["spec_hash"]).select("review_id", "vector"))
    ident = {"run_id": emb_run["run_id"], "table": out["table"], "snapshot_id": int(out["snapshot_id"]),
             "spec_hash": out["spec_hash"]}
    return ident, vectors


def run_index(spark: SparkSession, *, scope: str, category: str, prune: bool,
              with_embeddings: bool = False) -> dict[str, Any]:
    t0 = time.time()
    contract = load_contract("reviews")
    alias = alias_for(contract, scope)
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if silver_run is None:
        raise RuntimeError(f"no successful silver run for {category}/{scope}; run make silver first")
    src = silver_run["outputs"]["silver.reviews"]
    snapshot_id = int(src["snapshot_id"])

    inputs: dict[str, Any] = {"silver": {"run_id": silver_run["run_id"], "table": src["table"],
                                         "snapshot_id": snapshot_id},
                              "contract": {"path": contract.rel_path(), "version": contract.version,
                                           "hash": contract.hash}}
    vectors = None
    if with_embeddings:
        inputs["embeddings"], vectors = embeddings_input(spark, category=category, scope=scope,
                                                          silver_snapshot_id=snapshot_id)
    spec_version = "2" if with_embeddings else runs.SEARCH_REVIEWS_SPEC_VERSION
    run = runs.start("search_index_reviews", spec_version, category=category, data_scope=scope,
                     inputs=inputs, params={"alias": alias, "bulk_chunk": P.BULK_CHUNK,
                                            "with_embeddings": with_embeddings})
    es = P.client()
    index = P.generation_name(alias, snapshot_id, run.run_id)
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        reviews = spark.read.option("snapshot-id", snapshot_id).table(src["table"]).select(*DOC_COLS)
        silver_rows = reviews.count()
        with_text = reviews.filter(F.col("text_word_count") > 0)
        excluded = silver_rows - with_text.count()

        spec_hash, embedding_rows, vector_docs = None, 0, [0]
        if vectors is not None:
            spec_hash = inputs["embeddings"]["spec_hash"]
            embedding_rows = vectors.count()
            with_text = with_text.join(vectors, on="review_id", how="left")

        P.create_generation(es, contract, index)
        sent = P.bulk_load(es, contract, index, documents(
            with_text, silver_snapshot_id=snapshot_id, silver_run_id=silver_run["run_id"],
            run_id=run.run_id, spec_hash=spec_hash, vector_docs=vector_docs))
        es_count = P.count(es, index)
        vector_docs_es = P.count_with_vector(es, index) if vectors is not None else 0
        if vectors is not None and not (embedding_rows == vector_docs[0] == vector_docs_es):
            raise RuntimeError(f"{index}: embedding_rows {embedding_rows} / vector docs sent {vector_docs[0]} "
                               f"/ vector docs in ES {vector_docs_es} disagree")
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
        if vectors is not None:
            counts.update({"embedding_rows": embedding_rows, "vector_docs_sent": vector_docs[0],
                           "vector_docs_es": vector_docs_es, "embedding_spec_hash": spec_hash})
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=silver_rows, records_out=sent, records_rejected=0,
                 outputs=outputs, counts=counts)
    print(f"SEARCH_INDEX_REVIEWS run_id={run.run_id} scope={scope} alias={alias} index={index} "
          f"silver_snapshot={snapshot_id} silver_rows={silver_rows} excluded_empty_text={excluded} "
          f"docs={es_count} contract_tests={counts['contract_tests_passed']}/{counts['contract_tests_total']} "
          f"contract_hash={contract.hash[:12]} previous={','.join(previous) or 'none'} "
          f"vector_docs={counts.get('vector_docs_es', 0)} spec_hash={(spec_hash or 'none')[:12]} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--prune", action="store_true", help="delete old generations, keep one")
    ap.add_argument("--with-embeddings", action="store_true",
                    help="populate text_vector from the latest embeddings run (spec version 2)")
    args = ap.parse_args()
    spark = build("search-index-reviews", cores="local[4]", driver_memory="3g")
    try:
        run_index(spark, scope=args.scope, category=args.category, prune=args.prune,
                  with_embeddings=args.with_embeddings)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

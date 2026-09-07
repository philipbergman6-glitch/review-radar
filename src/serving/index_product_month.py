"""Job `search_index_product_month`: gold snapshot -> `product_month` projection -> alias.

Reads `gold.product_month` pinned to the snapshot the latest successful gold run recorded,
builds generation `product_month_s<gold_snapshot_id>_<run8>` under
conf/es/product_month.contract.json, checks the count identity, and swaps the alias.
Every document carries `source_gold_snapshot_id`; the lineage gate asserts the alias's
snapshot equals the ledger's (ADR-0004, RR-16). Kibana reads the alias only.

Run:  ./run.sh python -m src.serving.index_product_month [--scope full|sample] [--prune]
"""
from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Iterator
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from src.common import config as C
from src.common import runs
from src.common.spark import build
from src.serving import projection as P
from src.serving.contract import Contract, load_contract

sys.stdout.reconfigure(line_buffering=True)

DOC_COLS = ["parent_asin", "month", "month_date", "review_count", "rating_sum", "mean_rating",
            "neg_count", "neg_share", "verified_count", "verified_rating_sum", "verified_mean",
            "nonempty_text_count", "long_text_count", "distinct_users", "source_silver_snapshot_id"]


def alias_for(contract: Contract, scope: str) -> str:
    return contract.alias if scope == "full" else f"{contract.alias}_{scope}"


def documents(df: DataFrame, *, gold_snapshot_id: int, gold_run_id: str,
              run_id: str) -> Iterator[dict[str, Any]]:
    for row in df.toLocalIterator(prefetchPartitions=True):
        d = row.asDict()
        doc = {k: d[k] for k in DOC_COLS if d[k] is not None}
        doc["product_month_id"] = f"{d['parent_asin']}|{d['month']}"
        doc["month_date"] = d["month_date"].isoformat()
        doc["source_gold_snapshot_id"] = gold_snapshot_id
        doc["source_gold_run_id"] = gold_run_id
        doc["source_run_id"] = run_id
        yield doc


def run_index(spark: SparkSession, *, scope: str, category: str, prune: bool) -> dict[str, Any]:
    t0 = time.time()
    contract = load_contract("product_month")
    alias = alias_for(contract, scope)
    gold_run = runs.latest_success("gold", category=category, data_scope=scope)
    if gold_run is None:
        raise RuntimeError(f"no successful gold run for {category}/{scope}; run make gold first")
    src = gold_run["outputs"]["gold.product_month"]
    snapshot_id = int(src["snapshot_id"])

    run = runs.start("search_index_product_month", runs.SEARCH_PRODUCT_MONTH_SPEC_VERSION,
                     category=category, data_scope=scope,
                     inputs={"gold": {"run_id": gold_run["run_id"], "table": src["table"],
                                      "snapshot_id": snapshot_id},
                             "contract": {"path": contract.rel_path(), "version": contract.version,
                                          "hash": contract.hash}},
                     params={"alias": alias, "bulk_chunk": P.BULK_CHUNK})
    es = P.client()
    index = P.generation_name(alias, snapshot_id, run.run_id)
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        pm = spark.read.option("snapshot-id", snapshot_id).table(src["table"]).select(*DOC_COLS)
        gold_rows = pm.count()
        P.create_generation(es, contract, index)
        sent = P.bulk_load(es, contract, index, documents(
            pm, gold_snapshot_id=snapshot_id, gold_run_id=gold_run["run_id"], run_id=run.run_id))
        es_count = P.count(es, index)
        if es_count != sent:
            raise RuntimeError(f"{index}: es_count {es_count} != docs sent {sent}")
        stamped = P.source_snapshot_of(es, index, "source_gold_snapshot_id")
        if stamped != snapshot_id:
            raise RuntimeError(f"{index}: documents carry source_gold_snapshot_id {stamped}, expected {snapshot_id}")
        previous = P.swap_alias(es, alias, index)
        pruned = P.prune_generations(es, alias, keep=1) if prune else []
        outputs["es.product_month"] = {"index": index, "alias": alias, "doc_count": es_count}
        counts.update({"gold_rows": gold_rows, "docs_sent": sent, "es_count": es_count,
                       "previous_generations": previous, "pruned_generations": pruned,
                       "contract_hash": contract.hash, "elapsed_s": round(time.time() - t0, 1)})
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=gold_rows, records_out=sent, records_rejected=0,
                 outputs=outputs, counts=counts)
    print(f"SEARCH_INDEX_PRODUCT_MONTH run_id={run.run_id} scope={scope} alias={alias} index={index} "
          f"gold_snapshot={snapshot_id} gold_rows={gold_rows} docs={es_count} "
          f"contract_hash={contract.hash[:12]} previous={','.join(previous) or 'none'} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--prune", action="store_true")
    args = ap.parse_args()
    spark = build("search-index-product-month", cores="local[4]", driver_memory="2g")
    try:
        run_index(spark, scope=args.scope, category=args.category, prune=args.prune)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

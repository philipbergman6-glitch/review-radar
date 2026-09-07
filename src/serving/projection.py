"""Build a serving projection generation and reach it by an atomic alias swap (ADR-0004).

A generation is a physical index `<alias>_s<source_snapshot_id>_<run_id[:8]>` created from a
contract, bulk-loaded from a document iterator (every document validated first), refreshed,
counted against the expected total, and only then made the alias target. The previous
generation stays on disk until `prune_generations` removes it. Readers use the alias only.
"""
from __future__ import annotations

import time
from collections.abc import Iterable, Iterator
from typing import Any

from elasticsearch import Elasticsearch, helpers

from src.common import config as C
from src.serving.contract import Contract, validate_doc

BULK_CHUNK = 2000


def client() -> Elasticsearch:
    return Elasticsearch(C.ES_HOST, request_timeout=120, max_retries=3, retry_on_timeout=True)


def generation_name(alias: str, source_snapshot_id: int, run_id: str) -> str:
    return f"{alias}_s{source_snapshot_id}_{run_id[:8]}"


def alias_targets(es: Elasticsearch, alias: str) -> list[str]:
    if not es.indices.exists_alias(name=alias):
        return []
    return sorted(es.indices.get_alias(name=alias))


def create_generation(es: Elasticsearch, contract: Contract, index: str) -> None:
    if es.indices.exists(index=index):
        raise RuntimeError(f"generation {index} already exists; a run id is never reused")
    body = contract.body()
    # Bulk-load settings: no refresh until we ask; restored before the alias swap.
    settings = {**body["settings"], "refresh_interval": "-1"}
    es.indices.create(index=index, settings=settings, mappings=body["mappings"])


def _actions(contract: Contract, docs: Iterable[dict[str, Any]], index: str) -> Iterator[dict[str, Any]]:
    for doc in docs:
        validate_doc(contract, doc)
        yield {"_op_type": "index", "_index": index, "_id": doc[contract.id_field], "_source": doc}


def bulk_load(es: Elasticsearch, contract: Contract, index: str,
              docs: Iterable[dict[str, Any]], *, log_every: int = 100_000) -> int:
    """Index every document; hard-fail on the first bulk item error. Returns docs sent."""
    sent = 0
    t0 = time.time()
    for ok, item in helpers.streaming_bulk(es, _actions(contract, docs, index), chunk_size=BULK_CHUNK,
                                           raise_on_error=True, raise_on_exception=True,
                                           request_timeout=120):
        if not ok:
            raise RuntimeError(f"bulk item failed: {item}")
        sent += 1
        if sent % log_every == 0:
            print(f"[index] {index}: {sent:,} docs ({sent / (time.time() - t0):,.0f}/s)", flush=True)
    es.indices.put_settings(index=index, settings={"refresh_interval": "1s"})
    es.indices.refresh(index=index)
    return sent


def count(es: Elasticsearch, index: str) -> int:
    return int(es.count(index=index)["count"])


def swap_alias(es: Elasticsearch, alias: str, new_index: str) -> list[str]:
    """Atomically point `alias` at `new_index`; returns the generations it left."""
    previous = alias_targets(es, alias)
    actions = [{"remove": {"index": p, "alias": alias}} for p in previous]
    actions.append({"add": {"index": new_index, "alias": alias}})
    es.indices.update_aliases(actions=actions)
    return previous


def generations(es: Elasticsearch, alias: str) -> list[str]:
    return sorted(es.indices.get(index=f"{alias}_s*", ignore_unavailable=True).keys())


def prune_generations(es: Elasticsearch, alias: str, *, keep: int = 1) -> list[str]:
    """Delete generations that are not the alias target, keeping the `keep` newest others."""
    live = set(alias_targets(es, alias))
    others = [g for g in generations(es, alias)
              if g not in live]
    by_age = sorted(others, key=lambda g: es.indices.get_settings(index=g)[g]["settings"]["index"]["creation_date"])
    doomed = by_age[:max(0, len(by_age) - keep)]
    for g in doomed:
        es.indices.delete(index=g)
    return doomed


def source_snapshot_of(es: Elasticsearch, index: str, field: str) -> int | None:
    """The single source snapshot id every document in `index` carries, or None if mixed/empty."""
    resp = es.search(index=index, size=0, aggs={"s": {"terms": {"field": field, "size": 2}}})
    buckets = resp["aggregations"]["s"]["buckets"]
    if len(buckets) != 1:
        return None
    return int(buckets[0]["key"])

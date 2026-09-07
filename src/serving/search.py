"""Retrieval over the review search index: one function per system, ranks 1-based.

`SYSTEMS` names every retriever the judgement set is evaluated on. Search (P4) has the
two BM25 analyzers; Embeddings (P5) adds kNN and the client-side hybrid here. Each system
returns hits `{review_id, rank, score, source}` so the judging CLI and the evaluation never
touch Elasticsearch response shapes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from elasticsearch import Elasticsearch

DISPLAY_FIELDS = ["review_id", "parent_asin", "title", "text", "rating", "product_title",
                  "store", "review_month", "verified_purchase"]

ANALYZER_FIELDS = {
    "review_plain": ["text", "title"],
    "review_english": ["text.stemmed", "title.stemmed"],
}


@dataclass(frozen=True)
class Hit:
    review_id: str
    rank: int
    score: float
    source: dict[str, Any]


def bm25(es: Elasticsearch, alias: str, query: str, *, analyzer: str, size: int = 10,
         filter_ids: list[str] | None = None) -> list[Hit]:
    """`multi_match` over text and title in one analyzer family; ties broken by review id."""
    fields = ANALYZER_FIELDS[analyzer]
    must: dict[str, Any] = {"multi_match": {"query": query, "fields": [f"{fields[0]}^2", fields[1]],
                                            "type": "best_fields"}}
    body: dict[str, Any] = {"query": must}
    if filter_ids is not None:
        body["query"] = {"bool": {"must": must, "filter": {"ids": {"values": filter_ids}}}}
    resp = es.search(index=alias, size=size, query=body["query"], _source=DISPLAY_FIELDS,
                     sort=[{"_score": "desc"}, {"review_id": "asc"}])
    return [Hit(h["_source"]["review_id"], i + 1, float(h["_score"]), h["_source"])
            for i, h in enumerate(resp["hits"]["hits"])]


SYSTEMS: dict[str, dict[str, Any]] = {
    "bm25_plain": {"kind": "bm25", "analyzer": "review_plain", "phase": "search"},
    "bm25_stemmed": {"kind": "bm25", "analyzer": "review_english", "phase": "search"},
}


def run_system(es: Elasticsearch, alias: str, system: str, query: str, *, size: int = 10) -> list[Hit]:
    spec = SYSTEMS[system]
    if spec["kind"] == "bm25":
        return bm25(es, alias, query, analyzer=spec["analyzer"], size=size)
    raise ValueError(f"unknown system kind {spec['kind']!r} for {system}")

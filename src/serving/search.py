"""Retrieval over the review search index: one function per system, ranks 1-based.

`SYSTEMS` names every retriever the judgement set is evaluated on. Search (P4) has the
two BM25 analyzers; Embeddings (P5) adds approximate kNN over `text_vector`, the
client-side RRF hybrid, and the cohort-restricted BM25 / hybrid for the controlled table. Each system
returns hits `{review_id, rank, score, source}` so the judging CLI and the evaluation never
touch Elasticsearch response shapes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from elasticsearch import Elasticsearch

DISPLAY_FIELDS = ["review_id", "parent_asin", "title", "text", "rating", "product_title",
                  "store", "review_month", "verified_purchase"]

ANALYZER_FIELDS = {
    "review_plain": ["text", "title"],
    "review_english": ["text.stemmed", "title.stemmed"],
}


VECTOR_FIELD = "text_vector"
COHORT_FILTER: dict[str, Any] = {"exists": {"field": VECTOR_FIELD}}
KNN_K = 50
KNN_NUM_CANDIDATES = 200
RRF_K = 60
RRF_WINDOW = 50
TIE_SORT = [{"_score": "desc"}, {"review_id": "asc"}]


@dataclass(frozen=True)
class Hit:
    review_id: str
    rank: int
    score: float
    source: dict[str, Any]
    parts: dict[str, int] = field(default_factory=dict)   # hybrid: rank in each leg


def _hits(resp: dict[str, Any]) -> list[Hit]:
    return [Hit(h["_source"]["review_id"], i + 1, float(h["_score"]), h["_source"])
            for i, h in enumerate(resp["hits"]["hits"])]


def bm25(es: Elasticsearch, alias: str, query: str, *, analyzer: str, size: int = 10,
         filter_ids: list[str] | None = None, cohort_only: bool = False) -> list[Hit]:
    """`multi_match` over text and title in one analyzer family; ties broken by review id."""
    fields = ANALYZER_FIELDS[analyzer]
    must: dict[str, Any] = {"multi_match": {"query": query, "fields": [f"{fields[0]}^2", fields[1]],
                                            "type": "best_fields"}}
    filters: list[dict[str, Any]] = []
    if filter_ids is not None:
        filters.append({"ids": {"values": filter_ids}})
    if cohort_only:
        filters.append(COHORT_FILTER)
    q = {"bool": {"must": must, "filter": filters}} if filters else must
    resp = es.search(index=alias, size=size, query=q, _source=DISPLAY_FIELDS, sort=TIE_SORT)
    return _hits(resp)


def knn(es: Elasticsearch, alias: str, query_vector: list[float], *, size: int = 10, k: int = KNN_K,
        num_candidates: int = KNN_NUM_CANDIDATES, exclude_ids: list[str] | None = None) -> list[Hit]:
    """Approximate kNN (HNSW) over the vector cohort; ties broken by review id."""
    if k < size or num_candidates < k:
        raise ValueError(f"need num_candidates {num_candidates} >= k {k} >= size {size}")
    body: dict[str, Any] = {"field": VECTOR_FIELD, "query_vector": query_vector, "k": k,
                            "num_candidates": num_candidates}
    if exclude_ids:
        body["filter"] = {"bool": {"must_not": {"ids": {"values": exclude_ids}}}}
    resp = es.search(index=alias, size=size, knn=body, _source=DISPLAY_FIELDS, sort=TIE_SORT)
    return _hits(resp)


def exact_knn(es: Elasticsearch, alias: str, query_vector: list[float], *, size: int = 10,
              exclude_ids: list[str] | None = None) -> list[Hit]:
    """Brute-force cosine over every vector-bearing document: the recall baseline for `knn`."""
    q: dict[str, Any] = {"bool": {"filter": [COHORT_FILTER]}}
    if exclude_ids:
        q["bool"]["must_not"] = {"ids": {"values": exclude_ids}}
    script = {"script_score": {"query": q, "script": {
        "source": f"cosineSimilarity(params.qv, '{VECTOR_FIELD}') + 1.0", "params": {"qv": query_vector}}}}
    resp = es.search(index=alias, size=size, query=script, _source=DISPLAY_FIELDS, sort=TIE_SORT)
    return _hits(resp)


def rrf(legs: dict[str, list[str]], *, k: int = RRF_K, window: int = RRF_WINDOW) -> list[tuple[str, float, dict[str, int]]]:
    """Unweighted reciprocal rank fusion over the top-`window` of each leg (ADR-0005).

    score(d) = sum over legs containing d of 1 / (k + rank_leg(d)); ties broken by review id
    ascending, never by any leg's rank. Returns (review_id, score, {leg: rank}) sorted.
    """
    scores: dict[str, float] = {}
    parts: dict[str, dict[str, int]] = {}
    for leg, ids in legs.items():
        for rank, rid in enumerate(ids[:window], start=1):
            scores[rid] = scores.get(rid, 0.0) + 1.0 / (k + rank)
            parts.setdefault(rid, {})[leg] = rank
    return [(rid, scores[rid], parts[rid]) for rid in sorted(scores, key=lambda r: (-scores[r], r))]


def hybrid(es: Elasticsearch, alias: str, query: str, query_vector: list[float], *, analyzer: str,
           size: int = 10, cohort_only: bool = False) -> list[Hit]:
    """BM25 leg + kNN leg, each top-`RRF_WINDOW`, fused client-side by `rrf`."""
    lex = bm25(es, alias, query, analyzer=analyzer, size=RRF_WINDOW, cohort_only=cohort_only)
    vec = knn(es, alias, query_vector, size=RRF_WINDOW, k=KNN_K, num_candidates=KNN_NUM_CANDIDATES)
    sources = {h.review_id: h.source for h in lex + vec}
    fused = rrf({"bm25": [h.review_id for h in lex], "knn": [h.review_id for h in vec]})
    return [Hit(rid, i + 1, score, sources[rid], parts) for i, (rid, score, parts) in enumerate(fused[:size])]


# ------------------------------------------------------------ scoped retrieval (P7) ----
# A P7 question is about one product between two months, and its answer may only cite reviews
# from inside those bounds (ADR-0006). So the scope is a *filter on both legs before fusion*,
# not a post-filter on the fused list: filtering afterwards would let a window with few
# in-scope reviews come back short because the top 50 of each leg were spent elsewhere, and
# "the retriever found nothing" would then be an artefact of the fusion window.
#
# `review_month` is `date_trunc('month')` in silver (`src/spark/silver.py:372`), so it is always
# the first of the month and an inclusive `YYYY-MM-01` range is exactly the declared window.
def scope_filter(*, parent_asin: str, start: str, end: str) -> list[dict[str, Any]]:
    """The ES filter clauses for one product and one inclusive `YYYY-MM` window."""
    return [{"term": {"parent_asin": parent_asin}},
            {"range": {"review_month": {"gte": f"{start}-01", "lte": f"{end}-01",
                                        "format": "yyyy-MM-dd"}}}]


def scoped_hybrid(es: Elasticsearch, alias: str, query: str, query_vector: list[float], *,
                  parent_asin: str, start: str, end: str, analyzer: str = "review_english",
                  size: int = 10, rrf_window: int = RRF_WINDOW, knn_k: int = KNN_K,
                  num_candidates: int = KNN_NUM_CANDIDATES) -> list[Hit]:
    """The production hybrid, both legs restricted to one product-window before fusion."""
    filters = scope_filter(parent_asin=parent_asin, start=start, end=end)
    fields = ANALYZER_FIELDS[analyzer]
    lex_q = {"bool": {"must": {"multi_match": {"query": query, "fields": [f"{fields[0]}^2", fields[1]],
                                               "type": "best_fields"}},
                      "filter": filters}}
    lex = _hits(es.search(index=alias, size=rrf_window, query=lex_q, _source=DISPLAY_FIELDS,
                          sort=TIE_SORT))
    knn_body = {"field": VECTOR_FIELD, "query_vector": query_vector, "k": knn_k,
                "num_candidates": num_candidates, "filter": {"bool": {"filter": filters}}}
    vec = _hits(es.search(index=alias, size=rrf_window, knn=knn_body, _source=DISPLAY_FIELDS,
                          sort=TIE_SORT))
    sources = {h.review_id: h.source for h in lex + vec}
    fused = rrf({"bm25": [h.review_id for h in lex], "knn": [h.review_id for h in vec]},
                window=rrf_window)
    return [Hit(rid, i + 1, score, sources[rid], parts)
            for i, (rid, score, parts) in enumerate(fused[:size])]


# `table` says which evaluation table a system belongs to (ADR-0005: never merged):
# production = ranked over every review in the alias; controlled = restricted to the vector cohort.
SYSTEMS: dict[str, dict[str, Any]] = {
    "bm25_plain": {"kind": "bm25", "analyzer": "review_plain", "phase": "search", "table": "production"},
    "bm25_stemmed": {"kind": "bm25", "analyzer": "review_english", "phase": "search", "table": "production"},
    "knn": {"kind": "knn", "phase": "embeddings", "table": "both"},
    "hybrid": {"kind": "hybrid", "analyzer": "review_english", "phase": "embeddings", "table": "production"},
    "bm25_cohort": {"kind": "bm25", "analyzer": "review_english", "cohort_only": True,
                    "phase": "embeddings", "table": "controlled"},
    "hybrid_cohort": {"kind": "hybrid", "analyzer": "review_english", "cohort_only": True,
                      "phase": "embeddings", "table": "controlled"},
}


def systems_in(phase: str) -> list[str]:
    return [s for s, v in SYSTEMS.items() if v["phase"] == phase]


def run_system(es: Elasticsearch, alias: str, system: str, query: str, *, size: int = 10,
               query_vector: list[float] | None = None) -> list[Hit]:
    spec = SYSTEMS[system]
    if spec["kind"] == "bm25":
        return bm25(es, alias, query, analyzer=spec["analyzer"], size=size,
                    cohort_only=spec.get("cohort_only", False))
    if query_vector is None:
        from src.ai.embedder import encode_query  # loads the model on first use
        query_vector = encode_query(query)
    if spec["kind"] == "knn":
        return knn(es, alias, query_vector, size=size)
    if spec["kind"] == "hybrid":
        return hybrid(es, alias, query, query_vector, analyzer=spec["analyzer"], size=size,
                      cohort_only=spec.get("cohort_only", False))
    raise ValueError(f"unknown system kind {spec['kind']!r} for {system}")

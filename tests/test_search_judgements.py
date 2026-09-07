"""Judgement set freeze, pooling with provenance, and completeness rules (RR-06 item 7)."""
from __future__ import annotations

import pytest

from src.serving import judgements as J


def test_frozen_set_is_10_plus_10_with_rules():
    qs = J.load_queries()
    assert len(qs.queries) == 20
    assert sum(q.stratum == "lexical" for q in qs.queries) == 10
    assert all(q.relevance_rule and q.information_need for q in qs.queries)
    assert qs.hash == J.load_queries().hash


def test_pool_merges_ranks_and_keeps_provenance(tmp_path):
    pool = tmp_path / "pool.jsonl"
    rows = [{"query_id": "L01", "review_id": "a", "ranks": {"bm25_plain": 1}},
            {"query_id": "L01", "review_id": "b", "ranks": {"bm25_plain": 2, "bm25_stemmed": 1}}]
    assert J.add_to_pool(rows, round_name="search", set_hash="h", path=pool) == 2
    # second round adds a new system rank for an existing doc and one new doc
    rows2 = [{"query_id": "L01", "review_id": "a", "ranks": {"knn": 3}},
             {"query_id": "L01", "review_id": "b", "ranks": {"bm25_plain": 2}},
             {"query_id": "L01", "review_id": "c", "ranks": {"knn": 1}}]
    assert J.add_to_pool(rows2, round_name="embeddings", set_hash="h", path=pool) == 2
    merged = J.load_pool(pool)
    assert merged[("L01", "a")]["ranks"] == {"bm25_plain": 1, "knn": 3}
    assert merged[("L01", "a")]["pool_round"] == "search"
    assert merged[("L01", "c")]["pool_round"] == "embeddings"


def test_completeness_never_scores_unjudged_or_cannot_judge(tmp_path):
    pool = tmp_path / "pool.jsonl"
    jpath = tmp_path / "j.jsonl"
    J.add_to_pool([{"query_id": "Q", "review_id": r, "ranks": {"s": i + 1}} for i, r in enumerate("abc")],
                  round_name="search", set_hash="h", path=pool)
    J.record_judgement("Q", "a", "relevant", judge="t", pool_round="search", set_hash="h", path=jpath)
    J.record_judgement("Q", "b", "cannot_judge", judge="t", pool_round="search", set_hash="h", path=jpath)
    comp = J.completeness(J.load_pool(pool), J.load_judgements(jpath), systems=["s"])
    assert comp["Q"] == {"pooled": 3, "judged": 1, "cannot_judge": 1, "unjudged": 1, "complete": False}
    J.record_judgement("Q", "b", "not_relevant", judge="t", pool_round="search", set_hash="h", path=jpath)
    J.record_judgement("Q", "c", "not_relevant", judge="t", pool_round="search", set_hash="h", path=jpath)
    comp = J.completeness(J.load_pool(pool), J.load_judgements(jpath), systems=["s"])
    assert comp["Q"]["complete"] is True


def test_bad_state_rejected(tmp_path):
    with pytest.raises(ValueError):
        J.record_judgement("Q", "a", "maybe", judge="t", pool_round="r", set_hash="h", path=tmp_path / "j")

"""Client-side RRF and the kNN argument contract (ADR-0005). No Elasticsearch."""
from __future__ import annotations

import pytest

from src.serving import search as S


def test_rrf_scores_and_orders_by_reciprocal_rank():
    fused = S.rrf({"bm25": ["a", "b", "c"], "knn": ["c", "a", "d"]}, k=60)
    ids = [rid for rid, _, _ in fused]
    assert ids == ["a", "c", "b", "d"]
    a = {rid: (score, parts) for rid, score, parts in fused}["a"]
    assert a[0] == pytest.approx(1 / 61 + 1 / 62)
    assert a[1] == {"bm25": 1, "knn": 2}


def test_rrf_ties_break_by_review_id_not_by_leg():
    # x is bm25 rank 1 only, y is knn rank 1 only: equal scores, 'x' < 'y' wins regardless of leg order
    fused = S.rrf({"knn": ["y"], "bm25": ["x"]})
    assert [rid for rid, _, _ in fused] == ["x", "y"]
    fused = S.rrf({"bm25": ["y"], "knn": ["x"]})
    assert [rid for rid, _, _ in fused] == ["x", "y"]


def test_rrf_window_truncates_each_leg():
    fused = S.rrf({"bm25": ["a", "b", "c"], "knn": []}, window=2)
    assert [rid for rid, _, _ in fused] == ["a", "b"]


def test_knn_rejects_inconsistent_k_and_candidates():
    with pytest.raises(ValueError):
        S.knn(None, "reviews", [0.0] * 384, size=10, k=5, num_candidates=200)
    with pytest.raises(ValueError):
        S.knn(None, "reviews", [0.0] * 384, size=10, k=50, num_candidates=20)


def test_systems_declare_phase_and_table():
    assert S.systems_in("search") == ["bm25_plain", "bm25_stemmed"]
    assert set(S.systems_in("embeddings")) == {"knn", "hybrid", "bm25_cohort", "hybrid_cohort"}
    assert all(v["table"] in ("production", "controlled", "both") for v in S.SYSTEMS.values())
    assert S.KNN_K == 50 and S.KNN_NUM_CANDIDATES == 200 and S.RRF_K == 60 and S.RRF_WINDOW == 50

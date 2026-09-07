"""Mapping contract: loads, hashes, and the indexer-side validation half (ADR-0004)."""
from __future__ import annotations

import pytest

from src.serving.contract import ContractViolation, load_contract, validate_doc


@pytest.fixture(scope="module")
def reviews():
    return load_contract("reviews")


@pytest.fixture(scope="module")
def product_month():
    return load_contract("product_month")


def test_contracts_are_strict(reviews, product_month):
    for c in (reviews, product_month):
        assert c.mappings["dynamic"] == "strict"
        assert c.id_field in c.fields
        assert set(c.required) <= c.fields


def test_hash_is_stable_and_distinct(reviews, product_month):
    assert reviews.hash == load_contract("reviews").hash
    assert reviews.hash != product_month.hash
    assert len(reviews.hash) == 64


def test_reviews_declares_vector_field_and_two_analyzers(reviews):
    vec = reviews.mappings["properties"]["text_vector"]
    assert (vec["type"], vec["dims"], vec["similarity"]) == ("dense_vector", 384, "cosine")
    assert vec["index_options"]["type"] == "int8_hnsw"
    assert set(reviews.analyzers) == {"review_plain", "review_english"}
    assert reviews.mappings["properties"]["text"]["analyzer"] == "review_plain"
    assert reviews.mappings["properties"]["text"]["fields"]["stemmed"]["analyzer"] == "review_english"


def _doc(**over):
    d = {"review_id": "r1", "parent_asin": "B0", "user_id": "u", "event_ts": "2020-01-01T00:00:00",
         "review_month": "2020-01-01", "rating": 5, "text": "fine", "text_word_count": 1,
         "verified_purchase": True, "source_silver_snapshot_id": 1, "source_silver_run_id": "s",
         "source_run_id": "x"}
    d.update(over)
    return d


def test_validate_accepts_minimal_doc(reviews):
    validate_doc(reviews, _doc())


def test_validate_rejects_missing_required(reviews):
    with pytest.raises(ContractViolation, match="missing required fields \\['text'\\]"):
        validate_doc(reviews, _doc(text=None))


def test_validate_rejects_unknown_field(reviews):
    with pytest.raises(ContractViolation, match="outside the contract \\['sentiment'\\]"):
        validate_doc(reviews, _doc(sentiment="pos"))


def test_analyzer_cases_name_declared_analyzers(reviews):
    assert reviews.analyzer_tests
    for case in reviews.analyzer_tests:
        assert case["analyzer"] in reviews.analyzers

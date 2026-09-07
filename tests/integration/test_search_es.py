"""Mapping contract against the live Elasticsearch: strict rejection and analyzer cases.

Skipped when ES is unreachable. Uses a throwaway index and deletes it afterwards.
"""
from __future__ import annotations

import uuid

import pytest

from src.serving import projection as P
from src.serving.contract import load_contract, run_analyzer_tests


@pytest.fixture(scope="module")
def es():
    try:
        client = P.client()
        client.info()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Elasticsearch unreachable: {exc}")
    return client


@pytest.fixture(scope="module")
def probe(es):
    contract = load_contract("reviews")
    name = f"pytest_reviews_{uuid.uuid4().hex[:8]}"
    P.create_generation(es, contract, name)
    yield contract, name
    es.indices.delete(index=name)


def test_analyzer_cases_pass(es, probe):
    contract, name = probe
    results = run_analyzer_tests(es, name, contract)
    assert all(r["ok"] for r in results), [r for r in results if not r["ok"]]


def test_strict_mapping_rejects_unknown_field(es, probe):
    _, name = probe
    from elasticsearch import BadRequestError
    with pytest.raises(BadRequestError, match="strict_dynamic_mapping_exception"):
        es.index(index=name, id="x", document={"review_id": "x", "unexpected": 1})

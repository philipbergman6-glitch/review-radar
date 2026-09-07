"""Embedding identity (ADR-0005): hash covers the identity only; text preparation is fixed."""
from __future__ import annotations

import json

import pytest

from src.ai import spec as S
from src.common import runs
from src.common.runs import validate_finish


def test_spec_hash_ignores_execution_settings(tmp_path):
    raw = json.loads(S.SPEC_PATH.read_text())
    a = S.load_spec()
    raw["execution_defaults"]["batch_size"] = 8
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(raw))
    b = S.load_spec(p)
    assert a.hash == b.hash
    raw["identity"]["max_seq_length"] = 128
    p.write_text(json.dumps(raw))
    assert S.load_spec(p).hash != a.hash


def test_spec_pins_model_revision_and_es_contract_dims():
    s = S.load_spec()
    assert s.model == "sentence-transformers/all-MiniLM-L6-v2"
    assert len(s.identity["revision"]) == 40
    assert s.dims == 384 and s.min_words == 20


def test_spec_rejects_non_cosine_or_wrong_dims(tmp_path):
    raw = json.loads(S.SPEC_PATH.read_text())
    raw["identity"]["dims"] = 768
    p = tmp_path / "spec.json"
    p.write_text(json.dumps(raw))
    with pytest.raises(ValueError):
        S.load_spec(p)


def test_prepare_text_collapses_whitespace_and_optional_title():
    assert S.prepare_text("  Nice  ", "a \n b") == "Nice. a b"
    assert S.prepare_text(None, "a  b") == "a b"
    assert S.prepare_text("   ", "x") == "x"


EMB_OUT = {"gold.review_embeddings": {"table": "lake.gold.review_embeddings", "snapshot_id": 1, "spec_hash": "h"}}


def _counts(**over):
    c = {"silver_rows": 100, "cohort_rows": 60, "below_min_words": 40, "embedded_rows": 60,
         "table_rows_for_spec": 60, "review_id_distinct": 60, "dims_ok_rows": 60, "unit_norm_rows": 60,
         "min_words": 20, "elapsed_s": 1.0, "reviews_per_s": 60.0}
    c.update(over)
    return c


def test_embeddings_identity_holds():
    fails = validate_finish("embeddings", runs.EMBEDDINGS_SPEC_VERSION,
                            records={"records_in": 100, "records_out": 60, "records_rejected": 0},
                            outputs=EMB_OUT, counts=_counts(), raise_=False)
    assert fails == []


def test_embeddings_identity_names_a_norm_or_duplicate_failure():
    fails = validate_finish("embeddings", runs.EMBEDDINGS_SPEC_VERSION,
                            records={"records_in": 100, "records_out": 60, "records_rejected": 0},
                            outputs=EMB_OUT, counts=_counts(unit_norm_rows=59), raise_=False)
    assert any("unit_norm_rows" in f for f in fails)
    fails = validate_finish("embeddings", runs.EMBEDDINGS_SPEC_VERSION,
                            records={"records_in": 100, "records_out": 60, "records_rejected": 0},
                            outputs=EMB_OUT, counts=_counts(below_min_words=39), raise_=False)
    assert any("below_min_words" in f for f in fails)


def test_search_reviews_v2_requires_vector_identity():
    counts = {"silver_rows": 10, "excluded_empty_text": 1, "docs_sent": 9, "es_count": 9,
              "previous_generations": [], "contract_tests_total": 7, "contract_tests_passed": 7,
              "analyzers": [], "contract_hash": "c", "embedding_rows": 5, "vector_docs_sent": 5,
              "vector_docs_es": 4, "embedding_spec_hash": "h"}
    fails = validate_finish("search_index_reviews", "2",
                            records={"records_in": 10, "records_out": 9, "records_rejected": 0},
                            outputs={"es.reviews": {"index": "i", "alias": "a", "doc_count": 9}},
                            counts=counts, raise_=False)
    assert any("vector_docs_es" in f for f in fails)
    counts["vector_docs_es"] = 5
    assert validate_finish("search_index_reviews", "2",
                           records={"records_in": 10, "records_out": 9, "records_rejected": 0},
                           outputs={"es.reviews": {"index": "i", "alias": "a", "doc_count": 9}},
                           counts=counts, raise_=False) == []

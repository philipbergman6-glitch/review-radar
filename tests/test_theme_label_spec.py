"""The labelling spec, its identity hash, and the validation that never repairs (ADR-0003, RR-19)."""
from __future__ import annotations

import json

import pytest

from src.ai.labels import (
    DISCOVERY_SCHEMA,
    idempotency_key,
    load_spec,
    quote_is_evidence,
    validate_discovery,
)
from src.common import runs


@pytest.fixture(scope="module")
def spec():
    return load_spec()


def test_spec_is_local_and_deterministic(spec):
    assert spec.host == "ollama" and spec.api_mode == "local"
    assert spec.model_id == "qwen3:8b"                  # RR-19
    assert spec.comparison_model_id == "llama3.2:3b"    # ADR-0003's comparison row
    assert float(spec.inference["temperature"]) == 0.0


def test_config_hash_covers_prompt_schema_and_validation_limits(spec):
    base = spec.config_hash("discovery", DISCOVERY_SCHEMA)
    other_schema = json.loads(json.dumps(DISCOVERY_SCHEMA))
    other_schema["properties"]["complaints"]["maxItems"] = 4
    assert spec.config_hash("discovery", other_schema) != base
    loose = load_spec()
    loose.limits["discovery_quote_max_words"] = 99
    assert loose.config_hash("discovery", DISCOVERY_SCHEMA) != base


def test_idempotency_key_is_stable_and_field_boundaries_cannot_collide(spec):
    args = {"label_source": "local_llm", "model_id": "qwen3:8b", "label_spec_version": "1",
            "prompt_version": "discovery-v1", "inference_config_hash": "abc"}
    assert idempotency_key(source_review_id="r1", **args) == idempotency_key(source_review_id="r1", **args)
    assert idempotency_key(source_review_id="r1", **args) != idempotency_key(source_review_id="r2", **args)
    # "ab" + "c" and "a" + "bc" must not hash alike: the tuple is length-prefixed.
    a = idempotency_key(source_review_id="ab", **{**args, "model_id": "c"})
    b = idempotency_key(source_review_id="a", **{**args, "model_id": "bc"})
    assert a != b


def test_idempotency_key_refuses_an_unknown_label_source():
    with pytest.raises(ValueError, match="label_source"):
        idempotency_key(source_review_id="r1", label_source="guesswork", model_id="m",
                        label_spec_version="1", prompt_version="p", inference_config_hash="h")


def test_quote_must_appear_in_the_review():
    title, text = "Leaks", "The liquid leaked out of the pump nozel every time"
    assert quote_is_evidence("liquid leaked out", title, text)
    assert quote_is_evidence("LIQUID   LEAKED  out", title, text)      # whitespace and case only
    assert not quote_is_evidence("liquid leaks out of the nozzle", title, text)  # normalised = invented


def test_validate_discovery_names_every_failure(spec):
    title, text = "Bad", "Overpriced with low quality and it leaks in transit"
    ok = {"complaints": [{"quote": "Overpriced with low quality", "aspect": "price"}]}
    assert validate_discovery(ok, title=title, text=text, limits=spec.limits) == []

    invented = {"complaints": [{"quote": "leaks during shipping", "aspect": "leakage"}]}
    assert any("not present" in f for f in validate_discovery(invented, title=title, text=text,
                                                              limits=spec.limits))
    long_aspect = {"complaints": [{"quote": "it leaks in transit",
                                   "aspect": "the product leaks while being shipped"}]}
    assert any("aspect has" in f for f in validate_discovery(long_aspect, title=title, text=text,
                                                             limits=spec.limits))
    repeated = {"complaints": [{"quote": "it leaks in transit", "aspect": "leakage"},
                               {"quote": "it leaks in transit", "aspect": "shipping"}]}
    assert any("repeats" in f for f in validate_discovery(repeated, title=title, text=text,
                                                          limits=spec.limits))
    assert validate_discovery({"complaints": []}, title=title, text=text, limits=spec.limits) == []
    assert validate_discovery({"themes": []}, title=title, text=text, limits=spec.limits)


def test_theme_labels_contract_identity_balances_cache_and_status():
    c = runs.contract_for("theme_labels_llm", runs.THEME_LABELS_SPEC_VERSION)
    assert c is not None
    counts = {"reviews_selected": 100, "cache_hits": 40, "inferences_run": 60, "succeeded": 55,
              "parse_failed": 4, "api_failed": 1, "table_rows_for_config": 95,
              "distinct_keys_for_config": 95}
    assert c.identity({"records_out": 55, "records_rejected": 5}, counts) == []
    assert any("cache_hits" in f for f in
               c.identity({"records_out": 55, "records_rejected": 5}, {**counts, "cache_hits": 41}))
    assert any("distinct_keys_for_config" in f for f in
               c.identity({"records_out": 55, "records_rejected": 5},
                          {**counts, "distinct_keys_for_config": 94}))

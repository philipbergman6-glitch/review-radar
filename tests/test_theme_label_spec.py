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
from src.common.config import PROJECT_ROOT


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
    c = runs.contract_for("theme_labels_llm", runs.THEME_DISCOVERY_SPEC_VERSION)
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


def test_the_labelling_contract_counts_an_abstention_as_an_answer_not_a_failure():
    """ADR-0003: `abstain=true` is a deliberate decline, neither a parse nor an API failure."""
    c = runs.contract_for("theme_labels_llm", runs.THEME_LABELS_SPEC_VERSION)
    assert c is not None
    counts = {"reviews_selected": 100, "cache_hits": 40, "inferences_run": 60, "succeeded": 50,
              "model_abstained": 5, "parse_failed": 4, "api_failed": 1,
              "table_rows_for_config": 95, "distinct_keys_for_config": 95}
    assert c.identity({"records_out": 55, "records_rejected": 5}, counts) == []
    # An abstention folded into the failures would misreport the failure coverage.
    assert any("records_out" in f for f in
               c.identity({"records_out": 50, "records_rejected": 10}, counts))


def test_the_reference_contract_refuses_a_partially_labelled_frame():
    """RR-21: a rejected reference label is re-labelled, never stored as a gap in ground truth."""
    c = runs.contract_for("theme_labels_reference", runs.THEME_REFERENCE_SPEC_VERSION)
    assert c is not None
    good = {"reviews_selected": 200, "labels_submitted": 200, "accepted": 200, "rejected": 0,
            "table_rows_for_config": 200, "distinct_keys_for_config": 200}
    assert c.identity({"records_out": 200}, good) == []
    short = {**good, "labels_submitted": 199, "accepted": 199}
    assert any("reviews_selected" in f for f in c.identity({"records_out": 199}, short))
    bad = {**good, "accepted": 198, "rejected": 2}
    assert any("failed validation" in f for f in c.identity({"records_out": 198}, bad))


def test_agent_reference_is_a_distinct_label_source_from_human_and_local_llm():
    """RR-21: agent ground truth must never be indistinguishable from Philip's labels."""
    keys = {src: idempotency_key(source_review_id="r1", label_source=src, model_id="m",
                                 label_spec_version="1", prompt_version="v1",
                                 inference_config_hash="h")
            for src in ("agent_reference", "human", "local_llm")}
    assert len(set(keys.values())) == 3


def test_the_frozen_prompt_block_names_a_real_prompt_and_the_score_it_was_selected_on(spec):
    """ADR-0003 line 27, ticket 06: the freeze is a claim the repo must be able to re-derive.

    `scripts/gate_themes.py` trusts this block for the audit run's prompt identity, so a block
    that names a version the spec does not have, or a macro-F1 the committed artefact does not
    show, would let the audit be scored against a prompt nobody selected.
    """
    frozen = spec.raw["frozen_prompt"]
    assert spec.prompts[frozen["name"]].version == frozen["version"]
    assert frozen["freeze_commit"] and frozen["decided_in"] == "docs/decisions/theme-prompt-freeze.md"

    selection = json.loads((PROJECT_ROOT / "eval" / "themes" / "selection-development.json").read_text())
    assert selection["winner"]["version"] == frozen["version"]
    assert selection["audit_evidence"]["untouched"], "the freeze is void if the audit set was open"

    scored = json.loads((PROJECT_ROOT / "eval" / "themes"
                         / f"score-development-{frozen['version']}-"
                           f"{spec.model_id.replace(':', '_')}.json").read_text())
    assert scored["overall"]["macro_f1"] == frozen["selected_on"]["value"]
    # The score that selected the prompt was over the whole frame, not the rows it answered.
    assert scored["reference_rows"] == scored["system_rows"] == scored["overall"]["reviews"]

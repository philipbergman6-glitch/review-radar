"""The evaluation artefact contract and the table assembled from it (ADR-0011, ticket 01).

Two seams are tested here and nowhere else:

* **the contract** -- `conf/eval-artifact.schema.json` accepts a well-formed artefact and
  names a failure for every required field, so gates that write artefacts inherit one
  validated shape;
* **the renderer's hard-fail** -- a capability that is neither a number nor a written
  reason must make `make eval-table` exit non-zero. Silence is the failure mode the whole
  ticket exists to make impossible.

No Spark, no Elasticsearch, no Postgres: `build_rows` takes already-loaded artefacts.
"""
from __future__ import annotations

import copy
import json

import pytest

from src.common import evaluation as E


# ------------------------------------------------------------------- fixtures ----
def artifact(**over) -> dict:
    doc = {
        "artifact_version": "1",
        "capability": "themes_quality",
        "phase": "P6 Themes",
        "kind": "quality",
        "gate_name": "THEMES_QUALITY",
        "protocol_hash": "0f1e2d3c4b5a6978",
        "model": "qwen3:8b",
        "prompt": "label-v4",
        "population": {"name": "development", "n": 200},
        "pipeline_run_id": "8a1d2f4e-0000-4000-8000-000000000001",
        "scope": "full",
        "status": "FAIL",
        "metric": {"name": "macro_f1", "value": 0.4298, "threshold": 0.70,
                   "direction": "gte", "interval": [0.354, 0.484]},
        "constituents": ["THEMES_QUALITY macro_f1=0.4298 bar=0.70 verdict=FAIL"],
        "created_at": "2026-09-10T12:00:00+00:00",
    }
    doc.update(over)
    return {k: v for k, v in doc.items() if v is not E.OMIT}


def capability(**over) -> E.Capability:
    base = {"id": "themes_quality", "phase": "P6 Themes", "gate_name": "THEMES_QUALITY",
            "kind": "quality", "status": "declared", "cut_reason": None, "jobs": ()}
    base.update(over)
    return E.Capability(**base)


# -------------------------------------------------------------- the contract ----
def test_a_well_formed_artefact_validates():
    assert E.validate_artifact(artifact()) == []


@pytest.mark.parametrize("field", [
    "artifact_version", "capability", "phase", "kind", "gate_name", "protocol_hash",
    "model", "prompt", "population", "pipeline_run_id", "scope", "status", "created_at",
])
def test_every_required_field_is_declared_and_its_absence_is_named(field):
    doc = artifact()
    doc.pop(field)
    fails = E.validate_artifact(doc)
    assert fails, f"{field} may be omitted"
    assert any(field in f for f in fails), fails


def test_the_verdict_vocabulary_is_closed():
    assert E.validate_artifact(artifact(status="ALMOST")) != []


def test_scope_is_full_or_sample_and_nothing_else():
    assert E.validate_artifact(artifact(scope="partial")) != []


def test_a_capability_that_did_not_run_must_carry_a_written_reason():
    without = artifact(status="NOT_RUN", metric=E.OMIT, pipeline_run_id=None)
    assert E.validate_artifact(without) != []
    with_reason = artifact(status="NOT_RUN", metric=E.OMIT, pipeline_run_id=None,
                           cut_reason="not reached by submission date")
    assert E.validate_artifact(with_reason) == []


def test_a_capability_that_ran_must_carry_a_number_and_a_run_id():
    assert E.validate_artifact(artifact(metric=E.OMIT)) != []
    assert E.validate_artifact(artifact(pipeline_run_id=None)) != []


def test_a_metric_carries_its_threshold_beside_its_value():
    doc = artifact()
    doc["metric"].pop("threshold")
    assert E.validate_artifact(doc) != []


def test_reproducibility_never_reports():
    repro = artifact(capability="silver", kind="reproducibility", gate_name="SILVER_GATE",
                     model=None, prompt=None, status="REPORTED",
                     metric={"name": "constituents_ok", "value": 7, "threshold": 7, "direction": "gte"})
    assert E.validate_artifact(repro) != []
    assert E.validate_artifact({**repro, "status": "PASS"}) == []


def test_an_unknown_field_is_a_failure_not_a_shrug():
    assert E.validate_artifact(artifact(macro_f1=0.99)) != []


# ------------------------------------------------------------------ the chain ----
def test_the_declared_chain_covers_every_phase_and_both_tracks():
    chain = E.load_chain()
    phases = {c.phase for c in chain}
    for expected in ("P2 Silver", "P3 Gold", "P4 Search", "P5 Embeddings", "P6 Themes",
                     "P7 RAG", "P8 Stream", "Lineage track", "Deliverables track"):
        assert expected in phases, f"{expected} is not declared"
    assert len({c.id for c in chain}) == len(chain), "capability ids are not unique"


def test_every_cut_capability_carries_a_reason():
    for c in E.load_chain():
        if c.status == "cut":
            assert c.cut_reason and c.cut_reason.strip(), c.id


def test_a_cut_capability_without_a_reason_is_rejected_at_load(tmp_path):
    bad = tmp_path / "chain.toml"
    bad.write_text('version = "1"\n[[capability]]\nid = "rag"\nphase = "P7 RAG"\n'
                   'gate_name = "RAG_GATE"\nkind = "reproducibility"\nstatus = "cut"\n')
    with pytest.raises(ValueError, match="cut_reason"):
        E.load_chain(bad)


def test_every_declared_capability_names_its_artefact_under_eval():
    for c in E.load_chain():
        assert c.artifact_path.as_posix() == f"eval/{c.id}/gate.json"


# --------------------------------------------------------------- the renderer ----
def test_a_missing_artefact_that_is_not_declared_cut_fails_the_command():
    rows, errors = E.build_rows([capability()], {"themes_quality": None})
    assert errors and "themes_quality" in errors[0]
    assert rows[0].verdict == "MISSING"


def test_a_capability_declared_cut_renders_not_run_and_does_not_fail():
    cut = capability(id="themes_repeat_kappa", status="cut",
                     cut_reason="the labeller is deterministic")
    rows, errors = E.build_rows([cut], {"themes_repeat_kappa": None})
    assert errors == []
    assert rows[0].verdict == "NOT_RUN"
    assert "deterministic" in rows[0].note


def test_a_sample_scope_artefact_renders_not_run_never_pass():
    doc = artifact(capability="silver", kind="reproducibility", gate_name="SILVER_GATE",
                   model=None, prompt=None, scope="sample", status="PASS",
                   metric={"name": "constituents_ok", "value": 7, "threshold": 7, "direction": "gte"})
    rows, errors = E.build_rows(
        [capability(id="silver", phase="P2 Silver", gate_name="SILVER_GATE",
                    kind="reproducibility")], {"silver": doc})
    assert errors == []
    assert rows[0].verdict == "NOT_RUN"
    assert "sample" in rows[0].note


def test_a_full_scope_artefact_renders_its_own_verdict():
    rows, errors = E.build_rows([capability()], {"themes_quality": artifact()})
    assert errors == []
    assert rows[0].verdict == "FAIL"
    assert rows[0].value == "0.4298" and rows[0].threshold == "0.70"


def test_an_artefact_filed_under_the_wrong_capability_fails_the_command():
    _, errors = E.build_rows([capability()], {"themes_quality": artifact(capability="rag")})
    assert errors and any("capability" in e for e in errors)


def test_an_artefact_that_does_not_validate_fails_the_command():
    _, errors = E.build_rows([capability()], {"themes_quality": artifact(status="ALMOST")})
    assert errors and any("status" in e for e in errors)


def test_a_cut_capability_that_also_has_an_artefact_is_a_contradiction():
    cut = capability(id="rag", status="cut", cut_reason="not reached by submission date")
    _, errors = E.build_rows([cut], {"rag": artifact(capability="rag")})
    assert errors and any("cut" in e for e in errors)


def test_reproducibility_and_quality_are_visually_separated():
    chain = [capability(id="silver", phase="P2 Silver", gate_name="SILVER_GATE",
                        kind="reproducibility"),
             capability()]
    docs = {"silver": artifact(capability="silver", kind="reproducibility",
                               gate_name="SILVER_GATE", model=None, prompt=None, status="PASS",
                               metric={"name": "constituents_ok", "value": 7, "threshold": 7,
                                       "direction": "gte"}),
            "themes_quality": artifact()}
    rows, errors = E.build_rows(chain, docs)
    out = E.render(rows, errors)
    assert "Reproducibility" in out and "Quality" in out
    assert out.index("SILVER_GATE") < out.index("Quality") < out.index("THEMES_QUALITY")
    assert "EVAL_TABLE=OK" in out


def test_the_rendered_table_names_its_errors_and_says_so_in_the_terminal_line():
    rows, errors = E.build_rows([capability()], {"themes_quality": None})
    out = E.render(rows, errors)
    assert "EVAL_TABLE=INCOMPLETE" in out
    assert "themes_quality" in out


def test_the_schema_file_is_the_only_source_of_the_contract():
    schema = json.loads(E.SCHEMA_PATH.read_text())
    assert schema["$id"] == "review-radar/eval-artifact/v1"
    # The module validates against the file; mutating the loaded copy must not leak.
    before = copy.deepcopy(E.load_schema())
    E.validate_artifact(artifact())
    assert E.load_schema() == before

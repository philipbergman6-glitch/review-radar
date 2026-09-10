"""The classifier as a scored system (ticket 08): its identity, and the two freezes around it.

Three systems are compared on the same held-out set, and each is a set of rows in
`gold.review_theme_labels` picked out by an identity. The labeller's identity is a prompt and
a model; the classifier's is a fitted model's spec hash. If the scorer resolved the
classifier's rows with the *labeller's* identity it would find none, and a score over no rows
is the silence the evaluation spine exists to make impossible -- so the resolution is a
function with a test rather than a branch in a script.

The two freezes: cuts are fitted once on development, and audit is scored only after the
development number is on disk.
"""
from __future__ import annotations

import pytest

from src.ai.classifier_spec import (
    LABEL_SOURCE,
    MODEL_ID,
    classifier_identity,
    development_score_path,
    llm_identity,
    parse_classifier_spec,
    require_development_first,
    require_frozen_cuts,
    require_frozen_labeller,
)
from src.ai.labels import decoding_schema, idempotency_key, load_spec, load_taxonomy

TAX = load_taxonomy()
CUTS = {t: 0.5 for t in TAX.ids}


def spec_doc(**over):
    doc = {"classifier": "mllib_theme_baseline", "spec_version": "1", "spec_hash": "a" * 64,
           "model_path": "data/models/theme_classifier/aaaaaaaaaaaa",
           "taxonomy_version": TAX.version, "taxonomy_hash": TAX.file_hash,
           "training_labels_config": "b" * 64, "thresholds": dict(CUTS),
           "thresholds_fitted_on": "development"}
    doc.update(over)
    return doc


# ------------------------------------------------------------------- the identity ----
def test_the_classifier_is_identified_by_its_spec_hash_not_by_a_prompt():
    ident = classifier_identity(parse_classifier_spec(spec_doc()))
    assert (ident.label_source, ident.model_id) == (LABEL_SOURCE, MODEL_ID)
    assert ident.prompt_version == "classifier-v1"
    assert ident.inference_config_hash == "a" * 64
    assert ident.artefact_stem == "classifier-v1-" + "a" * 12


def test_the_labellers_artefact_stem_is_the_one_the_themes_gate_looks_for():
    """`scripts/gate_themes.py` builds the audit score path as `<prompt version>-<model>`.

    The stem moved into a function when the classifier needed one of its own; if it changed
    shape here, the gate would silently read `artefact=missing` and print FAIL.
    """
    spec = load_spec()
    ident = llm_identity(spec, prompt_name=spec.frozen.name, model_id=spec.model_id,
                         schema=decoding_schema(TAX.ids), taxonomy_hash=TAX.file_hash)
    assert ident.artefact_stem == f"{spec.frozen.version}-{spec.model_id.replace(':', '_')}"


def test_the_two_systems_can_never_collide_on_a_row():
    """Same review, same table: only the identity keeps the two systems' rows apart."""
    spec = load_spec()
    llm = llm_identity(spec, prompt_name=spec.frozen.name, model_id=spec.model_id,
                       schema=decoding_schema(TAX.ids), taxonomy_hash=TAX.file_hash)
    clf = classifier_identity(parse_classifier_spec(spec_doc()))
    assert llm.inference_config_hash != clf.inference_config_hash
    assert llm.label_source != clf.label_source
    keys = {idempotency_key(source_review_id="R1", label_source=i.label_source,
                            model_id=i.model_id, label_spec_version="2",
                            prompt_version=i.prompt_version,
                            inference_config_hash=i.inference_config_hash)
            for i in (llm, clf)}
    assert len(keys) == 2


# ------------------------------------------------------------------- the spec file ----
def test_a_spec_missing_a_theme_cut_is_rejected():
    short = {t: 0.5 for t in TAX.ids[:-1]}
    with pytest.raises(ValueError, match="cut"):
        parse_classifier_spec(spec_doc(thresholds=short))


def test_a_cut_outside_zero_to_one_is_rejected():
    with pytest.raises(ValueError, match="cut"):
        parse_classifier_spec(spec_doc(thresholds={**CUTS, TAX.ids[0]: 1.5}))


def test_an_unfitted_spec_parses_and_carries_no_cuts():
    """--train writes the spec before --fit-thresholds fills it in; that state is legal."""
    spec = parse_classifier_spec(spec_doc(thresholds=None, thresholds_fitted_on=None))
    assert spec.thresholds is None


# ------------------------------------------------------- the pool that trained it ----
def test_the_pool_defaults_to_the_frozen_labellers_labels():
    assert require_frozen_labeller(None, frozen_config_hash="d" * 64) == "d" * 64


def test_a_hash_that_is_not_the_frozen_prompts_is_refused():
    """A stale paste is the realistic failure: the rows differ only by this column."""
    with pytest.raises(ValueError, match="frozen prompt"):
        require_frozen_labeller("e" * 64, frozen_config_hash="d" * 64)


def test_naming_the_frozen_hash_explicitly_is_allowed():
    assert require_frozen_labeller("d" * 64, frozen_config_hash="d" * 64) == "d" * 64


# ------------------------------------------------------------------- the two freezes ----
def test_scoring_without_fitted_cuts_is_refused():
    spec = parse_classifier_spec(spec_doc(thresholds=None, thresholds_fitted_on=None))
    with pytest.raises(ValueError, match="no frozen"):
        require_frozen_cuts(spec, taxonomy_hash=TAX.file_hash)


def test_cuts_fitted_on_anything_but_development_are_refused():
    """A cut fitted on audit is a threshold tuned against the holdout, whatever it is called."""
    spec = parse_classifier_spec(spec_doc(thresholds_fitted_on="audit"))
    with pytest.raises(ValueError, match="development"):
        require_frozen_cuts(spec, taxonomy_hash=TAX.file_hash)


def test_cuts_fitted_against_another_taxonomy_are_refused():
    spec = parse_classifier_spec(spec_doc())
    with pytest.raises(ValueError, match="taxonomy"):
        require_frozen_cuts(spec, taxonomy_hash="c" * 64)


def test_a_frozen_spec_scores():
    require_frozen_cuts(parse_classifier_spec(spec_doc()), taxonomy_hash=TAX.file_hash)


def test_audit_is_refused_until_this_model_has_a_development_score(tmp_path):
    ident = classifier_identity(parse_classifier_spec(spec_doc()))
    with pytest.raises(ValueError, match="development"):
        require_development_first("audit", identity=ident, scores_dir=tmp_path)
    development_score_path(ident, scores_dir=tmp_path).write_text("{}")
    require_development_first("audit", identity=ident, scores_dir=tmp_path)


def test_a_refit_does_not_inherit_the_previous_models_development_score(tmp_path):
    """The stem carries the spec hash, so a retrained classifier starts with no number."""
    old = classifier_identity(parse_classifier_spec(spec_doc()))
    development_score_path(old, scores_dir=tmp_path).write_text("{}")
    refit = classifier_identity(parse_classifier_spec(spec_doc(spec_hash="f" * 64)))
    with pytest.raises(ValueError, match="development"):
        require_development_first("audit", identity=refit, scores_dir=tmp_path)


def test_development_never_waits_on_itself(tmp_path):
    ident = classifier_identity(parse_classifier_spec(spec_doc()))
    require_development_first("development", identity=ident, scores_dir=tmp_path)

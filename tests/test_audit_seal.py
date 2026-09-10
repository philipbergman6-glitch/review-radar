"""The audit set is opened once, under freezes that cannot move afterwards (ticket 09).

Every assertion here is about a *refusal*. The numbers P6 publishes are only worth anything if
the systems that produced them were frozen before the held-out set was read, and that is a
claim about what the repo will not do rather than about what it computes. So:

* `require_audit_unopened` refuses a second pass, and says what the first one found.
* `require_measurement_goes_through_the_pass` refuses a single system's audit score, which is
  how the set would otherwise be opened three times, one disappointing number at a time.
* `fingerprint` changes when *any* frozen thing changes -- the prompt, a classifier cut, a star
  threshold, the taxonomy, or the pass rule itself -- and `freezes_moved` names which.

The last one is the load-bearing test. A fingerprint that missed one of its inputs would still
be a hash, still be stored, still be compared, and would still pass the gate while the thing it
was supposed to protect had moved underneath it.
"""
from __future__ import annotations

import copy
import json

import pytest

from src.ai.audit_seal import (
    MACRO_F1_BAR,
    PUBLISHED_SYSTEM,
    SYSTEMS,
    build_seal,
    collect_freezes,
    fingerprint,
    freezes_moved,
    load_star_spec,
    require_audit_unopened,
    require_measurement_goes_through_the_pass,
    seal_matches_artefacts,
    star_identity,
)
from src.ai.classifier_spec import load_classifier_spec, score_artefact_name
from src.ai.labels import load_spec, load_taxonomy


def freezes() -> dict:
    """The real frozen material where it exists, standing in for what ticket 08 has not run."""
    spec, tax = load_spec(), load_taxonomy()
    try:
        clf = load_classifier_spec()
    except ValueError:
        pytest.skip("the classifier is not trained yet; the shape under test is the fingerprint")
    return collect_freezes(spec=spec, tax=tax, clf=clf, star=load_star_spec())


def synthetic_freezes() -> dict:
    """A freeze structure of the same shape, so the fingerprint is testable before the runs.

    Deliberately not built by `collect_freezes`: these tests are about what the fingerprint
    covers, and building the input with the function under test would let a dropped field pass
    unnoticed on both sides.
    """
    return {
        "taxonomy": {"version": "1", "hash": "t" * 64, "themes": 10},
        "llm": {"prompt_name": "label_v5", "prompt_version": "label-v5",
                "freeze_commit": "c" * 40, "decided_in": "ticket 06", "model_id": "qwen3:8b",
                "label_spec_version": "1", "inference_config_hash": "h" * 64},
        "classifier": {"spec_version": "1", "spec_hash": "s" * 64,
                       "training_labels_config": "h" * 64, "thresholds_fitted_on": "development",
                       "thresholds": {"delivery": 0.41, "packaging": 0.38}},
        "star_only": {"version": "1", "fitted_on": "development",
                      "thresholds": {"delivery": 2, "packaging": 3}},
        "pass_rule": {"macro_f1_bar": 0.70, "min_supported_recall_bar": 0.50},
    }


def systems(macro: float = 0.74, recall: float = 0.61) -> list[dict]:
    return [{"system": name, "artefact": f"score-audit-{name}.json",
             "artefact_sha256": "a" * 64, "run_ids": [], "reviews": 200,
             "macro_f1": macro if name == PUBLISHED_SYSTEM else 0.5,
             "bootstrap_95": [0.7, 0.8],
             "min_supported_recall": recall if name == PUBLISHED_SYSTEM else 0.4,
             "supported_themes": 8, "failure_rate": 0.0}
            for name in SYSTEMS]


# ------------------------------------------------------------- opened exactly once ----
def test_an_unopened_audit_set_is_not_refused(tmp_path):
    require_audit_unopened(tmp_path / "audit-seal.json")


def test_a_second_pass_is_refused_and_quotes_the_first(tmp_path):
    path = tmp_path / "audit-seal.json"
    seal = build_seal(freezes=synthetic_freezes(), systems=systems(), scope="full",
                      reference_rows=200, git_commit_sha="d" * 40,
                      opened_at="2026-09-11T06:00:00+00:00")
    path.write_text(json.dumps(seal))
    with pytest.raises(ValueError) as exc:
        require_audit_unopened(path)
    # The refusal has to carry the first pass's numbers: an operator who reruns it is usually
    # someone who wants to see the result again, and sending them to the file is the answer.
    assert "already opened" in str(exc.value)
    assert "0.74" in str(exc.value)


def test_a_failing_verdict_is_sealed_just_like_a_passing_one(tmp_path):
    """A miss is reported, not re-run. The seal must exist either way, or it invites a retry."""
    seal = build_seal(freezes=synthetic_freezes(), systems=systems(macro=0.41), scope="full",
                      reference_rows=200, git_commit_sha="d" * 40, opened_at="2026-09-11T06:00:00Z")
    assert seal["verdict"]["passed"] is False
    assert seal["verdict"]["macro_f1"] == 0.41
    path = tmp_path / "audit-seal.json"
    path.write_text(json.dumps(seal))
    with pytest.raises(ValueError):
        require_audit_unopened(path)


def test_the_seal_must_carry_all_three_systems():
    partial = [s for s in systems() if s["system"] != "star_only"]
    with pytest.raises(ValueError, match="scores exactly"):
        build_seal(freezes=synthetic_freezes(), systems=partial, scope="full",
                   reference_rows=200, git_commit_sha="d" * 40, opened_at="2026-09-11T06:00:00Z")


def test_a_bar_missed_by_recall_alone_still_fails():
    """Both halves of ADR-0003's rule bind. A macro-F1 over the bar does not buy a bad theme."""
    seal = build_seal(freezes=synthetic_freezes(), systems=systems(macro=0.90, recall=0.12),
                      scope="full", reference_rows=200, git_commit_sha="d" * 40,
                      opened_at="2026-09-11T06:00:00Z")
    assert seal["verdict"]["macro_f1"] > MACRO_F1_BAR
    assert seal["verdict"]["passed"] is False


# ------------------------------------------------- one system at a time is refused ----
@pytest.mark.parametrize("command", ["score_themes.py", "baseline_star_only.py --score"])
def test_a_single_system_may_not_score_audit(command):
    with pytest.raises(ValueError) as exc:
        require_measurement_goes_through_the_pass("audit", command=command)
    assert "make audit-once" in str(exc.value)


def test_development_is_never_refused():
    require_measurement_goes_through_the_pass("development", command="score_themes.py")


# ---------------------------------------------------- the fingerprint covers it all ----
def test_the_fingerprint_is_stable_for_unchanged_freezes():
    assert fingerprint(synthetic_freezes()) == fingerprint(synthetic_freezes())


MOVES = [
    ("the prompt", ["llm", "prompt_version"], "label-v4"),
    ("the prompt's config hash", ["llm", "inference_config_hash"], "z" * 64),
    ("the model", ["llm", "model_id"], "llama3.2:3b"),
    ("a classifier cut", ["classifier", "thresholds", "delivery"], 0.37),
    ("the classifier model", ["classifier", "spec_hash"], "z" * 64),
    ("the pool the classifier learned from", ["classifier", "training_labels_config"], "z" * 64),
    ("a star threshold", ["star_only", "thresholds", "delivery"], 1),
    ("the taxonomy", ["taxonomy", "hash"], "z" * 64),
    ("the pass bar", ["pass_rule", "macro_f1_bar"], 0.60),
    ("the recall bar", ["pass_rule", "min_supported_recall_bar"], 0.30),
]


@pytest.mark.parametrize("what,path,value", MOVES, ids=[m[0] for m in MOVES])
def test_every_frozen_thing_is_inside_the_fingerprint(what, path, value):
    """Each of these, moved alone, must change the fingerprint and be named by `freezes_moved`.

    The pass bar is in the list on purpose. Lowering the bar after a disappointing audit is the
    same act as refitting a cut -- it makes the published claim untrue -- and a fingerprint over
    the systems but not the rule would let it through silently.
    """
    before = synthetic_freezes()
    after = copy.deepcopy(before)
    node = after
    for key in path[:-1]:
        node = node[key]
    assert node[path[-1]] != value, "the test's 'after' value must differ from the frozen one"
    node[path[-1]] = value

    assert fingerprint(after) != fingerprint(before)
    moved = freezes_moved(before, after)
    assert len(moved) == 1, moved
    assert ".".join(path) in moved[0]


def test_unmoved_freezes_report_nothing():
    assert freezes_moved(synthetic_freezes(), synthetic_freezes()) == []


def test_a_freeze_that_disappears_is_named_not_ignored():
    after = copy.deepcopy(synthetic_freezes())
    del after["star_only"]["thresholds"]["delivery"]
    moved = freezes_moved(synthetic_freezes(), after)
    assert moved == ["star_only.thresholds.delivery was 2, absent today"]


# ------------------------------------------------------- the artefacts, not just the hash ----
def test_a_changed_score_file_is_caught(tmp_path):
    seal = build_seal(freezes=synthetic_freezes(), systems=systems(), scope="full",
                      reference_rows=200, git_commit_sha="d" * 40, opened_at="2026-09-11T06:00:00Z")
    assert len(seal_matches_artefacts(seal, scores_dir=tmp_path)) == len(SYSTEMS)  # none on disk
    for s in seal["systems"]:
        (tmp_path / s["artefact"]).write_text("{}")
    fails = seal_matches_artefacts(seal, scores_dir=tmp_path)
    assert len(fails) == len(SYSTEMS)
    assert all("has changed since the audit was sealed" in f for f in fails)


# ------------------------------------------------------------------ the star floor ----
def test_the_star_floor_has_no_inference_identity():
    """It predicts from a rating and a frozen cut, so it has no config hash to pretend to."""
    identity = star_identity({"version": "1"})
    assert identity.inference_config_hash == ""
    assert score_artefact_name(sample="audit", stem=identity.artefact_stem) == \
        "score-audit-star_only.json"


def test_a_star_floor_fitted_on_audit_is_refused(tmp_path):
    path = tmp_path / "theme-star-baseline.json"
    path.write_text(json.dumps({"version": "1", "fitted_on": "audit", "thresholds": {},
                                "taxonomy_hash": "t" * 64}))
    with pytest.raises(ValueError, match="not a floor"):
        load_star_spec(path)


# ------------------------------------------------------ the real configs, as they stand ----
def test_the_real_freezes_fingerprint_when_they_are_all_present():
    """Skips until ticket 08's classifier exists; asserts the real shape once it does."""
    f = freezes()
    assert set(f) == {"taxonomy", "llm", "classifier", "star_only", "pass_rule"}
    assert f["llm"]["prompt_version"] == load_spec().frozen.version
    assert f["classifier"]["thresholds_fitted_on"] == "development"
    assert len(fingerprint(f)) == 64

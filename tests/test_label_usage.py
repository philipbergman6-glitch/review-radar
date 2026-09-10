"""The asymmetry (ticket 07): a failed row is dropped from training, scored as empty in eval.

These tests exist to stop the two readings from being quietly unified. Both directions are
asserted on the *same* row, because the claim is not "training drops failures" or "scoring
keeps them" -- it is that one row means two different things depending on which question is
being asked of it.
"""
from __future__ import annotations

import pytest

from src.ai.label_usage import (
    TRAINABLE_STATUSES,
    UNANSWERED_STATUSES,
    for_evaluation,
    for_training,
    theme_ids,
    training_census,
)
from src.ai.labels import LABEL_STATUSES

FAILED_ROW = {"label_status": "parse_failed", "themes": None}
EMPTY_ROW = {"label_status": "succeeded", "themes": []}
LABELLED_ROW = {"label_status": "succeeded",
                "themes": [{"theme_id": "overpriced", "evidence_quote": "way too expensive"},
                           {"theme_id": "does_not_work", "evidence_quote": "never turned on"}]}


# ------------------------------------------------------------------ the asymmetry ----
@pytest.mark.parametrize("status", UNANSWERED_STATUSES)
def test_an_unanswered_row_is_dropped_from_training(status):
    assert for_training({"label_status": status, "themes": None}) is None


@pytest.mark.parametrize("status", UNANSWERED_STATUSES)
def test_the_same_unanswered_row_scores_as_an_empty_prediction(status):
    assert for_evaluation({"label_status": status, "themes": None}) == set()


def test_no_label_and_no_themes_are_the_same_to_the_scorer_and_different_to_training():
    """The whole point. Two rows, indistinguishable downstream of `for_evaluation`."""
    assert for_evaluation(FAILED_ROW) == for_evaluation(EMPTY_ROW) == set()
    # ... and not at all the same to the trainer: one is dropped, the other teaches "no themes".
    assert for_training(FAILED_ROW) is None
    assert for_training(EMPTY_ROW) == set()


def test_a_dropped_row_is_never_confused_with_a_negative_example():
    """A drop is None, never an empty set: `if for_training(row):` would merge the two."""
    dropped = for_training(FAILED_ROW)
    assert dropped is not set()
    assert dropped is None


# ------------------------------------------------------------------- the easy cases ----
def test_a_labelled_row_carries_the_same_themes_to_both():
    expected = {"overpriced", "does_not_work"}
    assert for_training(LABELLED_ROW) == expected
    assert for_evaluation(LABELLED_ROW) == expected


def test_an_abstention_is_an_answer_and_trains_as_no_themes():
    """The model applied the contract and declined; that is a valid empty answer, not a gap."""
    row = {"label_status": "model_abstained", "themes": []}
    assert for_training(row) == set()
    assert for_evaluation(row) == set()


def test_every_terminal_status_is_either_trainable_or_unanswered():
    assert set(TRAINABLE_STATUSES) | set(UNANSWERED_STATUSES) == set(LABEL_STATUSES)
    assert not set(TRAINABLE_STATUSES) & set(UNANSWERED_STATUSES)


# --------------------------------------------------------------------- hard failure ----
@pytest.mark.parametrize("fn", [for_training, for_evaluation])
def test_an_unknown_status_raises_rather_than_defaulting(fn):
    with pytest.raises(ValueError, match="label_status"):
        fn({"label_status": "timed_out", "themes": None})


def test_a_trainable_row_with_null_themes_raises():
    """`succeeded` with no themes array is a contradiction; silently training on it is worse."""
    with pytest.raises(ValueError, match="no themes array"):
        for_training({"label_status": "succeeded", "themes": None})


# -------------------------------------------------------------------- theme_ids ----
def test_theme_ids_accepts_row_objects_as_well_as_dicts():
    class Row:
        theme_id = "overpriced"

    assert theme_ids([Row()]) == {"overpriced"}


def test_theme_ids_of_nothing_is_empty():
    assert theme_ids([]) == set()


# --------------------------------------------------------------------- the census ----
def test_the_census_reports_what_training_kept_and_what_it_dropped_by_name():
    statuses = ["succeeded"] * 7 + ["model_abstained", "parse_failed", "parse_failed", "api_failed"]
    c = training_census(statuses)
    assert c == {"pool_rows": 11, "training_rows": 8, "dropped_rows": 3,
                 "by_status": {"succeeded": 7, "model_abstained": 1, "parse_failed": 2,
                               "api_failed": 1},
                 "dropped_by_status": {"parse_failed": 2, "api_failed": 1},
                 "drop_rate": 0.2727}


def test_the_census_of_an_empty_pool_does_not_divide_by_zero():
    assert training_census([])["drop_rate"] == 0.0


def test_the_census_rejects_an_unknown_status():
    with pytest.raises(ValueError, match="label_status"):
        training_census(["succeeded", "exploded"])

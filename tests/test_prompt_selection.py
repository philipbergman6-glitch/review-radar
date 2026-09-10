"""The rule that picks the frozen prompt (docs/decisions/theme-prompt-freeze.md, ADR-0003).

The rule was committed before the numbers it selects on. These tests are how it stays that
way: they state the rule in cases, away from Spark and away from any real score, so that
changing the rule to suit an outcome breaks a test rather than passing quietly.
"""
from __future__ import annotations

import pytest

from src.ai.prompt_selection import (
    TIE_EPSILON,
    Candidate,
    intervals_overlap,
    require_comparable,
    select,
)


def candidate(name: str, macro_f1: float, *, failure_rate: float = 0.0,
              interval: tuple[float, float] = (0.0, 1.0)) -> Candidate:
    return Candidate(name=name, version=f"label-v{name[-1]}", macro_f1=macro_f1,
                     interval=interval, failure_rate=failure_rate, reviews=200)


def test_the_higher_macro_f1_wins_and_the_reason_names_the_margin():
    v4, v5 = candidate("label_v4", 0.4298), candidate("label_v5", 0.4633)
    winner, reason = select([v4, v5])
    assert winner is v5
    assert "0.0335" in reason


def test_the_wider_interval_does_not_win_on_its_upper_bound():
    """ADR-0003 line 32 makes the bootstrap interval context; rule 4 keeps it out of the choice."""
    steady = candidate("label_v4", 0.50, interval=(0.48, 0.52))
    lucky = candidate("label_v5", 0.45, interval=(0.20, 0.90))
    assert select([steady, lucky])[0] is steady


def test_a_gap_under_the_epsilon_is_a_tie_and_goes_to_the_cheaper_system():
    v4 = candidate("label_v4", 0.4300, failure_rate=0.295)
    v5 = candidate("label_v5", 0.4300 + TIE_EPSILON / 2, failure_rate=0.250)
    winner, reason = select([v4, v5])
    assert winner is v5 and "tie" in reason and "0.25" in reason
    # ...and the tie is on the gap, not on who scored higher: flip the failure rates and the
    # lower-scoring prompt wins, because within the epsilon the numbers are not distinguishable.
    v4_cheap = candidate("label_v4", 0.4300, failure_rate=0.10)
    assert select([v4_cheap, v5])[0] is v4_cheap


def test_a_tie_on_both_score_and_cost_goes_to_the_earlier_version():
    """Rule 5(c): the later prompt bought nothing, so it does not get frozen for existing."""
    v4 = candidate("label_v4", 0.43, failure_rate=0.25)
    v5 = candidate("label_v5", 0.43, failure_rate=0.25)
    winner, reason = select([v5, v4])
    assert winner is v4 and "earlier version" in reason


def test_selection_refuses_an_unscored_candidate_rather_than_ranking_it_last():
    """A None macro-F1 means the configuration was never scored. Ranking it is a silent drop."""
    with pytest.raises(ValueError, match="not scored"):
        select([candidate("label_v4", 0.43), Candidate("label_v5", "label-v5", None, (None, None),
                                                       0.25, 200)])


def test_selection_needs_at_least_one_candidate():
    with pytest.raises(ValueError, match="no candidates"):
        select([])


def test_overlapping_intervals_are_overlapping_including_at_a_shared_endpoint():
    assert intervals_overlap((0.383, 0.5145), (0.339, 0.4075))
    assert intervals_overlap((0.40, 0.50), (0.50, 0.60))          # touching is not separation
    assert not intervals_overlap((0.42, 0.51), (0.34, 0.41))
    assert not intervals_overlap((0.34, 0.41), (0.42, 0.51))      # order does not matter


def test_an_unbounded_interval_cannot_be_declared_separated():
    assert intervals_overlap((None, None), (0.339, 0.4075))


def test_comparability_is_enforced_on_every_setting_that_moves_a_score():
    base = {"reviews": 200, "min_support": 10, "seed": 20260907, "bootstrap_draws": 1000,
            "taxonomy_hash": "abc", "reference_source": "agent_reference"}
    require_comparable({"a": base, "b": dict(base)})
    for field, other in [("reviews", 199), ("min_support", 5), ("seed", 1),
                         ("bootstrap_draws", 100), ("taxonomy_hash", "def"),
                         ("reference_source", "human")]:
        with pytest.raises(ValueError, match=field):
            require_comparable({"a": base, "b": {**base, field: other}})


def test_a_missing_setting_is_a_mismatch_not_an_assumed_default():
    base = {"reviews": 200, "min_support": 10, "seed": 20260907, "bootstrap_draws": 1000,
            "taxonomy_hash": "abc", "reference_source": "agent_reference"}
    incomplete = {k: v for k, v in base.items() if k != "bootstrap_draws"}
    with pytest.raises(ValueError, match="bootstrap_draws"):
        require_comparable({"a": base, "b": incomplete})

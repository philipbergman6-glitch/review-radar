"""Scoring the theme labeller (ADR-0003, RR-21): what the headline number is allowed to mean."""
from __future__ import annotations

from src.ai.theme_scoring import (
    bootstrap_macro_f1,
    failure_coverage,
    macro_f1,
    other_agreement,
    score_themes,
)

THEMES = ["a", "b", "c"]


def test_a_review_the_system_could_not_answer_scores_as_a_miss_not_an_exclusion():
    """Dropping the failures would measure the model when it answers, not the pipeline."""
    ref = {"r1": {"a"}, "r2": {"a"}, "r3": {"a"}}
    partial = {"r1": {"a"}}                     # r2 and r3 ended parse_failed: no row at all
    s = {x.theme_id: x for x in score_themes(ref, partial, THEMES, min_support=1)}
    assert (s["a"].tp, s["a"].fn, s["a"].support) == (1, 2, 3)
    assert s["a"].recall == 1 / 3


def test_an_unsupported_theme_is_scored_but_excluded_from_macro_f1():
    ref = {f"r{i}": {"a"} for i in range(10)} | {"r99": {"b"}}
    sys_ = dict(ref)
    scores = score_themes(ref, sys_, THEMES, min_support=10)
    by = {x.theme_id: x for x in scores}
    assert by["a"].supported and not by["b"].supported
    assert by["b"].f1 == 1.0                    # still measured and printed
    assert macro_f1(scores) == 1.0              # but averaged over 'a' alone


def test_macro_f1_is_none_when_no_theme_clears_support():
    ref = {"r1": {"a"}}
    assert macro_f1(score_themes(ref, ref, THEMES, min_support=10)) is None


def test_a_theme_the_system_never_predicts_scores_zero_not_undefined():
    ref = {f"r{i}": {"a"} for i in range(10)}
    scores = score_themes(ref, {}, THEMES, min_support=10)
    a = next(s for s in scores if s.theme_id == "a")
    assert a.precision is None and a.recall == 0.0 and a.f1 == 0.0
    assert macro_f1(scores) == 0.0


def test_the_bootstrap_resamples_products_and_is_seeded():
    ref = {f"r{i}": ({"a"} if i % 2 else set()) for i in range(40)}
    sys_ = {k: v for k, v in ref.items() if k != "r1"}
    products = {f"r{i}": f"p{i // 4}" for i in range(40)}
    first = bootstrap_macro_f1(ref, sys_, THEMES, products, min_support=5, seed=7, draws=50)
    again = bootstrap_macro_f1(ref, sys_, THEMES, products, min_support=5, seed=7, draws=50)
    assert first == again
    lo, hi = first
    assert lo is not None and hi is not None and lo <= hi


def test_failure_coverage_counts_an_absent_row_and_keeps_abstention_separate():
    statuses = {"r1": "succeeded", "r2": "model_abstained", "r3": "parse_failed"}
    cov = failure_coverage(statuses, ["r1", "r2", "r3", "r4"])
    assert cov["absent"] == 1 and cov["model_abstained"] == 1
    assert cov["failure_rate"] == 0.5          # parse_failed + absent, not the abstention
    assert cov["abstention_rate"] == 0.25


def test_other_agreement_treats_a_missing_system_row_as_absent_not_as_agreement():
    ref = {"r1": True, "r2": False}
    assert other_agreement(ref, {"r1": True, "r2": False}, ["r1", "r2"])["agreement"] == 1.0
    assert other_agreement(ref, {}, ["r1", "r2"])["agreement"] == 0.5

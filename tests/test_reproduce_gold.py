"""The independent gold reproduction must reach the rule and the spine on its own (ticket 03).

`scripts/reproduce_gold.py` imports nothing from `src/spark/gold.py` or `src/gold/rule.py` --
it re-derives the aggregates, the evaluation points and the episodes from the words in
`src/gold/rule.py`'s docstring and the values in `conf/decline_rule.toml`. This file is the
one place the two implementations are allowed to meet: it runs both over the same synthetic
spines and asserts they agree, which is the reproduction claim at unit scale, cheap enough to
run on every commit and long before the full pipeline is stood up.

It also pins the reconciliation itself: a perturbed slice must fail, and a comparison that
compared nothing must never read PASS.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

import pytest

from scripts import reproduce_gold as R
from src.common import evaluation as E
from src.gates import gold as gold_gate
from src.gold.rule import evaluate as reference_evaluate
from src.gold.rule import load_rule as load_reference_rule

RUN_ID = "8a1d2f4e-0000-4000-8000-00000000000d"
HASH = "0f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c"


# ============================================================ the rule, re-derived ====
def random_spine(rng: random.Random, n: int, start: str = "2010-01") -> list[dict]:
    """A complete calendar spine of `n` months, in the shape both implementations take."""
    rows = []
    for i in range(n):
        count = rng.choice([0, 0, 1, 3, 8, 12, 20])
        # A plausible rating sum: `count` ratings drawn 1..5, so the mean can drift enough
        # for the delta to bite on some spines and not on others.
        ratings = [rng.randint(1, 5) for _ in range(count)]
        rows.append({
            "month": R.ix_month(R.month_ix(start) + i),
            "review_count": count,
            "rating_sum": float(sum(ratings)),
            "neg_count": sum(1 for r in ratings if r <= 2),
            "verified_count": sum(1 for _ in ratings if rng.random() < 0.8),
            "verified_rating_sum": float(sum(ratings) * 0.8),
            "nonempty_text_count": sum(1 for _ in ratings if rng.random() < 0.9),
            "long_text_count": sum(1 for _ in ratings if rng.random() < 0.4),
        })
    return rows


TOL = R.load_rule_spec()["metric_tolerance"]


def assert_agree(mine: list[dict], theirs: list[dict], fields, what: str) -> None:
    """Field by field, with the tolerance the config declares.

    The two implementations sum each window in a different order -- the reference over prefix
    sums, this one over the window itself -- so their means differ in the last bit or two of a
    double. That is what `[reproduction] metric_tolerance` is for, and comparing here on any
    other terms would be comparing something the gate does not.
    """
    assert len(mine) == len(theirs), f"{what}: {len(mine)} vs {len(theirs)}"
    for m, t in zip(mine, theirs, strict=True):
        bad = {f: (m[f], t[f]) for f in fields if not R.same(m[f], t[f], TOL)}
        assert not bad, f"{what}: {bad}"


@pytest.mark.parametrize("status", ["provisional", "frozen"])
def test_the_reimplemented_rule_agrees_with_the_reference_on_random_spines(status):
    reference = load_reference_rule()
    if status == "frozen":
        reference = type(reference)(**{**reference.as_params(), "status": "frozen"})
    mine = dict(R.load_rule_spec(), status=status)
    rng = random.Random(20260910)
    seen_alerts = 0
    for _ in range(300):
        spine = random_spine(rng, n=rng.randint(13, 60))
        ref_points, ref_episodes = reference_evaluate(spine, reference)
        my_points, my_episodes = R.evaluate_product(spine, mine)
        assert_agree(my_points, ref_points, R.POINT_FIELDS, "points")
        assert_agree(my_episodes, ref_episodes,
                     ("condition_started_at", *R.EPISODE_FIELDS), "episodes")
        seen_alerts += len(ref_episodes)
    assert seen_alerts > 0, "the spines never alerted, so the episode machinery went untested"


def test_max_drop_is_measured_from_the_alert_and_the_other_span_is_reported():
    """gold's docstring opens an episode at the first supporting point; its code measures
    `max_drop` from the alert. The reproduction reconciles against the published span and
    counts the difference on its own line rather than failing the gate over it."""
    mine = R.load_rule_spec()
    rng = random.Random(20260910)
    divergent = 0
    for _ in range(300):
        _, episodes = R.evaluate_product(random_spine(rng, n=rng.randint(13, 60)), mine)
        divergent += sum(1 for e in episodes
                         if not R.same(e["max_drop"], e["max_drop_from_episode_start"], TOL))
    assert divergent > 0, ("the two spans never diverged, so the MAX_DROP_SPAN line is "
                           "measuring nothing -- if gold's code changed, delete it")


def test_the_max_drop_span_line_reports_and_never_blocks():
    episodes = {"A:2011-01": {"max_drop": 0.5, "max_drop_from_episode_start": 0.9},
                "B:2011-01": {"max_drop": 0.4, "max_drop_from_episode_start": 0.4}}
    line = R.max_drop_span_line(episodes, TOL)
    assert line == "GOLD_REPRO MAX_DROP_SPAN episodes=2 differing=1 verdict=REPORTED"
    assert "max_drop_from_episode_start" not in R.EPISODE_FIELDS
    assert "max_drop" in R.EPISODE_FIELDS, "the published field is still reconciled"


def test_the_spine_is_every_month_between_the_first_and_last_review():
    rows = [
        {"parent_asin": "P", "month": "2011-01", "rating": 5, "verified_purchase": True,
         "text_word_count": 30, "user_id": "u1"},
        {"parent_asin": "P", "month": "2011-01", "rating": 1, "verified_purchase": False,
         "text_word_count": 0, "user_id": "u1"},
        {"parent_asin": "P", "month": "2011-04", "rating": 2, "verified_purchase": True,
         "text_word_count": 5, "user_id": "u2"},
    ]
    spine = R.product_spine(rows)
    assert [s["month"] for s in spine] == ["2011-01", "2011-02", "2011-03", "2011-04"]
    first = spine[0]
    assert first["review_count"] == 2 and first["rating_sum"] == 6.0
    assert first["neg_count"] == 1 and first["verified_count"] == 1
    assert first["verified_rating_sum"] == 5.0
    assert first["nonempty_text_count"] == 1 and first["long_text_count"] == 1
    assert first["distinct_users"] == 1
    assert spine[1]["review_count"] == 0 and spine[1]["rating_sum"] == 0.0


def test_a_product_below_min_reviews_is_never_materialised():
    rule = R.load_rule_spec()
    rows = [{"parent_asin": "SMALL", "month": "2011-01", "rating": 5, "verified_purchase": True,
             "text_word_count": 3, "user_id": f"u{i}"} for i in range(rule["min_reviews"] - 1)]
    rows += [{"parent_asin": "BIG", "month": "2011-01", "rating": 5, "verified_purchase": True,
              "text_word_count": 3, "user_id": f"u{i}"} for i in range(rule["min_reviews"])]
    d = R.derive(rows, rule)
    assert d.products_total == 2 and d.products_materialised == 1
    assert {asin for asin, _ in d.spine} == {"BIG"}


# ================================================================= reconciliation ====
def _side(**over):
    base = {("P", "2011-01"): {"review_count": 2, "rating_sum": 6.0},
            ("P", "2011-02"): {"review_count": 0, "rating_sum": 0.0}}
    for k, v in over.items():
        key = ("P", k.replace("_", "-"))
        base[key] = {**base[key], **v}
    return base


def test_a_clean_reconciliation_names_what_it_compared():
    c = R.reconcile("product_month", _side(), _side(), ("review_count", "rating_sum"))
    assert c.ok and c.compared == 2
    assert c.line == ("GOLD_REPRO compare=product_month compared=2 only_mine=0 only_theirs=0 "
                      "fields=2 mismatched=none")


def test_a_perturbed_slice_fails_reconciliation_and_names_the_field():
    theirs = _side(**{"2011_01": {"review_count": 3}})
    c = R.reconcile("product_month", _side(), theirs, ("review_count", "rating_sum"))
    assert not c.ok and c.mismatched == (("review_count", 1),)
    assert "mismatched=review_count:1" in c.line


def test_a_key_present_on_one_side_only_fails_reconciliation():
    mine = dict(_side()) | {("P", "2011-03"): {"review_count": 1, "rating_sum": 5.0}}
    c = R.reconcile("product_month", mine, _side(), ("review_count", "rating_sum"))
    assert not c.ok and c.n_only_mine == 1 and c.n_only_theirs == 0
    assert c.compared == 2, "only the shared keys are field-compared"


def test_floats_agree_within_the_configured_tolerance_and_not_beyond_it():
    near = _side(**{"2011_01": {"rating_sum": 6.0 + 1e-12}})
    far = _side(**{"2011_01": {"rating_sum": 6.0 + 1e-3}})
    fields = ("rating_sum",)
    assert R.reconcile("m", _side(), near, fields, tol=1e-9).ok
    assert not R.reconcile("m", _side(), far, fields, tol=1e-9).ok


def test_a_null_on_one_side_only_is_a_mismatch_not_a_match():
    theirs = _side(**{"2011_02": {"rating_sum": None}})
    c = R.reconcile("m", _side(), theirs, ("rating_sum",), tol=1e-9)
    assert not c.ok and c.mismatched == (("rating_sum", 1),)


# ========================================================================= verdict ====
def passing_checks():
    return [("rule_config_hash", True), ("products_materialised", True),
            ("product_month_keys", True), ("product_month_fields", True),
            ("product_month_non_empty", True), ("evaluation_points_keys", True),
            ("evaluation_points_fields", True), ("evaluation_points_non_empty", True),
            ("decline_episodes_keys", True), ("decline_episodes_fields", True)]


def test_the_repro_terminal_line_names_its_scope_and_its_constituent_count():
    v = gold_gate.repro(scope="full", checks=passing_checks())
    assert v.terminal == ("GOLD_REPRO_GATE scope=full checks=10 failed=none "
                          "GOLD_REPRO_GATE=PASS")
    assert v.passed and v.metric == {"name": "constituents_ok", "value": 10, "threshold": 10,
                                     "direction": "eq"}


@pytest.mark.parametrize("i", range(10))
def test_every_repro_constituent_can_trip_the_gate(i):
    checks = passing_checks()
    name = checks[i][0]
    checks[i] = (name, False)
    v = gold_gate.repro(scope="full", checks=checks)
    assert not v.passed and v.failed == (name,)
    assert f"failed={name}" in v.terminal and v.terminal.endswith("GOLD_REPRO_GATE=FAIL")


def test_a_reproduction_that_compared_nothing_cannot_pass():
    empty = R.reconcile("product_month", {}, {}, ("review_count",))
    assert empty.ok, "with no keys on either side there is nothing to disagree about"
    checks = R.comparison_checks(empty, require_non_empty=True)
    assert ("product_month_non_empty", False) in checks
    v = gold_gate.repro(scope="full", checks=checks)
    assert not v.passed, "a vacuous comparison must never read PASS"


def test_episodes_may_legitimately_be_empty():
    empty = R.reconcile("decline_episodes", {}, {}, ("closed_by",))
    assert all(ok for _, ok in R.comparison_checks(empty, require_non_empty=False))


def test_the_repro_artefact_validates_and_is_declared_in_the_chain():
    v = gold_gate.repro(scope="full", checks=passing_checks(),
                        constituents=["GOLD_REPRO compare=product_month compared=2"])
    doc = E.build_artifact(v, capability="gold_repro", phase="P3 Gold", kind="reproducibility",
                           protocol_hash=HASH,
                           population={"name": "gold.product_month", "n": 2400,
                                       "silver_snapshot_id": 7788},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert json.dumps(doc)  # the artefact is serialisable as written
    declared = {c.id: c for c in E.load_chain()}
    assert declared["gold_repro"].gate_name == "GOLD_REPRO_GATE"
    assert declared["gold_repro"].kind == "reproducibility"


def test_the_reproduction_never_imports_the_implementation_it_reproduces():
    text = Path(R.__file__).read_text()
    for banned in ("src.gold.rule", "src.spark.gold", "from src.gold", "from src.spark.gold"):
        assert banned not in text, f"the reproduction imports {banned}; it must re-derive"

"""Calibration must measure the rule the project ships, and its bars must be able to fail.

Three claims are pinned here, because each one is a way the calibration could print a
confident number that means nothing:

* **The evaluators agree.** Every figure `scripts/calibrate_gold.py` prints comes from the
  array evaluator in `src/gold/calibration.py`. If that has drifted from `src/gold/rule.py`,
  the placebo rate and the power belong to a rule nobody runs. The script cross-checks on
  real spines at runtime; this checks the same thing on random ones, on every commit.
* **The null is a null.** A blocked permutation that changed a product's volume, or its
  length, would not be the same product with the trend removed -- it would be a different
  portfolio, and the placebo rate would be measuring that instead.
* **The injection is an injection.** It must move the mean by the size it claims, using only
  relabelled rows, without inventing a review or touching one that was never five stars.

Plus the selection rule and the verdict: a gate that cannot fail is not a gate (audit F3), so
both bars are exercised in both directions.
"""
from __future__ import annotations

import json
import random

import pytest

from src.common import evaluation as E
from src.gates import calibration as gate
from src.gold import calibration as K
from src.gold.rule import Rule, month_str
from src.gold.rule import evaluate as reference_evaluate

RUN_ID = "8a1d2f4e-0000-4000-8000-0000000000ca"
HASH = "1c2d3e4f5a6b7c8d1c2d3e4f5a6b7c8d1c2d3e4f"

BASE = K.Config(baseline_months=6, recent_months=6, delta=0.3, persistence=2, min_reviews=10,
                min_active_months=3, recovery_points=2, max_gap=3)


def as_reference(c: K.Config) -> Rule:
    """The same configuration as `src/gold/rule.py` takes it, with the holdout out of reach."""
    return Rule(status="frozen", holdout_start="9999-12", baseline_months=c.baseline_months,
                recent_months=c.recent_months, delta=c.delta, persistence=c.persistence,
                max_gap=c.max_gap, recovery_points=c.recovery_points,
                min_reviews=c.min_reviews, min_active_months=c.min_active_months,
                unevaluable_resets_persistence=c.unevaluable_resets_persistence,
                config_hash=HASH, metric_tolerance=0.0)


def random_spine(rng: random.Random, n: int) -> tuple[list[float], list[float]]:
    counts = [float(rng.choice([0, 0, 1, 3, 8, 12, 20, 40])) for _ in range(n)]
    sums = [sum(rng.randint(1, 5) for _ in range(int(c))) * 1.0 for c in counts]
    return counts, sums


def as_rule_spine(counts, sums, start: int = 24000) -> list[dict]:
    return [{"month": month_str(start + i), "review_count": int(counts[i]),
             "rating_sum": sums[i], "neg_count": 0, "verified_count": 0,
             "verified_rating_sum": 0.0, "nonempty_text_count": 0, "long_text_count": 0}
            for i in range(len(counts))]


# ================================================== the three evaluators are one rule ====
@pytest.mark.parametrize("config", [
    BASE,
    K.Config(12, 12, 0.5, 3, 20, 3, 2, 3),
    K.Config(6, 6, 1.0, 4, 40, 3, 1, 0),
    K.Config(6, 12, 0.2, 2, 10, 1, 3, 6),
])
def test_scalar_and_array_evaluators_match_the_rule_module(config: K.Config) -> None:
    """`rule.evaluate`, `alerts_and_points` and `Portfolio.scan` must count the same."""
    rng = random.Random(1234)
    spines = [random_spine(rng, rng.randrange(config.baseline_months + config.recent_months,
                                              90)) for _ in range(120)]
    ref = as_reference(config)
    ref_points = ref_alerts = 0
    sca_points = sca_alerts = 0
    for counts, sums in spines:
        points, _, alerts = K.alerts_and_points(counts, sums, config)
        sca_points += points
        sca_alerts += len(alerts)
        pts, _ = reference_evaluate(as_rule_spine(counts, sums), ref)
        ref_points += len(pts)
        ref_alerts += sum(1 for p in pts if p["alert"])
    arr = K.Portfolio(spines).scan(config)
    assert (ref_points, ref_alerts) == (sca_points, sca_alerts)
    assert (ref_points, ref_alerts) == (arr["points"], arr["alerts"])


def test_alert_indices_are_the_months_the_rule_fires_on() -> None:
    """The scalar evaluator's alert positions must be the reference's, not just its count."""
    rng = random.Random(99)
    for _ in range(40):
        counts, sums = random_spine(rng, rng.randrange(20, 70))
        _, _, alerts = K.alerts_and_points(counts, sums, BASE)
        pts, _ = reference_evaluate(as_rule_spine(counts, sums), as_reference(BASE))
        fired = [i + BASE.baseline_months - 1 for i, p in enumerate(pts) if p["alert"]]
        assert alerts == fired


# ============================================================== the null is a null ====
def test_block_permutation_keeps_the_product_and_removes_only_the_order() -> None:
    rng = random.Random(5)
    counts, sums = random_spine(rng, 60)
    pc, ps = K.block_permute(counts, sums, block=6, rng=random.Random(11))
    assert len(pc) == len(counts) and len(ps) == len(sums)
    assert sorted(pc) == sorted(counts), "volume must survive the permutation"
    assert sum(ps) == pytest.approx(sum(sums)), "the stars must survive it too"
    assert sorted(zip(pc, ps, strict=True)) == sorted(zip(counts, sums, strict=True)), \
        "a month's count and its rating sum must move together or the means are invented"


def test_a_short_spine_is_returned_untouched_rather_than_truncated() -> None:
    counts, sums = [1.0, 2.0], [4.0, 8.0]
    pc, ps = K.block_permute(counts, sums, block=6, rng=random.Random(1))
    assert (pc, ps) == (counts, sums)


def test_the_placebo_rate_falls_when_the_rule_is_made_stricter() -> None:
    rng = random.Random(21)
    spines = [random_spine(rng, rng.randrange(30, 80)) for _ in range(150)]
    loose, tight = BASE, K.config_from(BASE.as_rule_fields(), delta=1.5, persistence=4)
    out = K.placebo(spines, [loose, tight], replicates=3, block=6, seed=7,
                    surveillance_months=100)
    assert out[tight.key].alerts <= out[loose.key].alerts
    assert out[loose.key].alerts_per_month == out[loose.key].alerts / (3 * 100)


# ========================================================= the injection is an injection ====
def five_star_product(n_months: int = 60, per_month: int = 12) -> K.Product:
    months = [m for m in range(n_months) for _ in range(per_month)]
    return K.Product("p", n_months, tuple(months), tuple([5] * len(months)), start=24000)


def test_injection_moves_the_mean_by_the_size_it_claims() -> None:
    p = five_star_product()
    stars = K.inject(p.months, p.ratings, step_index=30, target_drop=0.5, mechanism="severe",
                     ramp_months=1, rng=random.Random(3))
    post = [s for m, s in zip(p.months, stars, strict=True) if m >= 30]
    assert sum(post) / len(post) == pytest.approx(4.5, abs=0.05)


def test_injection_invents_no_reviews_and_lowers_only_five_star_rows() -> None:
    rng = random.Random(8)
    months = [m for m in range(60) for _ in range(10)]
    ratings = [rng.choice([1, 2, 3, 4, 5, 5, 5]) for _ in months]
    out = K.inject(months, ratings, step_index=20, target_drop=0.3, mechanism="moderate",
                   ramp_months=6, rng=random.Random(4))
    assert len(out) == len(ratings), "a relabelling may not add or drop a row"
    for before, after in zip(ratings, out, strict=True):
        if before != 5:
            assert after == before, "only five-star rows are relabelled"
        else:
            assert after in (5, 3), "the moderate mechanism relabels to three stars"


def test_a_ramp_reaches_the_terminal_effect_and_does_not_overshoot_it() -> None:
    """The target is the level the ramp climbs to, not the average of the climb.

    Sizing the relabels over the post-step period as a whole makes `target_drop` the mean drop
    across it, so a ramp starting from zero has to end well below 5 - target to average out --
    and the tail ends up injected harder than the step it is meant to be a gentler version of.
    """
    p = five_star_product(n_months=60, per_month=20)
    stars = K.inject(p.months, p.ratings, step_index=20, target_drop=0.5, mechanism="severe",
                     ramp_months=6, rng=random.Random(3))
    by_month: dict[int, list[int]] = {}
    for m, s in zip(p.months, stars, strict=True):
        by_month.setdefault(m, []).append(s)
    # Pooled over the tail, not month by month: a month holds a whole number of reviews, so
    # each one lands just above or just below the level and the carry averages them out.
    tail = [s for m in range(30, 60) for s in by_month[m]]
    assert sum(tail) / len(tail) == pytest.approx(4.5, abs=0.05), "the tail sits at 5 - 0.5"
    early = sum(by_month[20]) / len(by_month[20])
    assert early > 4.5, "the first month of a ramp must not already be at the terminal level"


def test_both_injection_shapes_are_monotone_in_the_size_of_the_decline() -> None:
    """A larger decline must be caught at least as often -- for the step and for the ramp.

    Deliberately *not* asserting that the step beats the ramp. It does not, always: a sharp
    step is absorbed into the trailing baseline within B months, so it can hold the condition
    for fewer consecutive points than a gradual decline of the same terminal size, and a rule
    with `persistence = 3` then catches the ramp more often. That is a property of a
    trailing-baseline rule, not a defect, and RR-09 round 5 is why it costs nothing here: the
    bar is stated at the step and the ramp is reported beside it.
    """
    rng = random.Random(23)
    products = []
    for i in range(80):
        months = [m for m in range(60) for _ in range(rng.randrange(8, 16))]
        products.append(K.Product(f"p{i}", 60, tuple(months),
                                  tuple(rng.choices([4, 5], [.2, .8], k=len(months))),
                                  start=24000))
    out = K.power_grid(products, [BASE], reference=BASE, effects=[0.2, 0.5, 1.0],
                       mechanisms=["severe"], ramps=[1, 6], window_points=6, seed=4,
                       replicates=1)
    for ramp in (1, 6):
        powers = [out[(BASE.key, "severe", e, ramp)].power for e in (0.2, 0.5, 1.0)]
        assert powers == sorted(powers), f"ramp={ramp} power must not fall as the decline grows"


def test_nothing_before_the_step_is_touched() -> None:
    p = five_star_product()
    stars = K.inject(p.months, p.ratings, step_index=40, target_drop=0.5, mechanism="severe",
                     ramp_months=1, rng=random.Random(3))
    assert all(s == 5 for m, s in zip(p.months, stars, strict=True) if m < 40)


def test_a_bigger_injected_decline_is_detected_at_least_as_often() -> None:
    rng = random.Random(17)
    products = []
    for i in range(60):
        months = [m for m in range(48) for _ in range(rng.randrange(6, 14))]
        products.append(K.Product(f"p{i}", 48, tuple(months),
                                  tuple(rng.choices([3, 4, 5], [.1, .2, .7], k=len(months))),
                                  start=24000))
    out = K.power_grid(products, [BASE], reference=BASE, effects=[0.2, 1.0],
                       mechanisms=["severe"], ramps=[1], window_points=6, seed=2,
                       replicates=1)
    assert out[(BASE.key, "severe", 1.0, 1)].power >= out[(BASE.key, "severe", 0.2, 1)].power


def test_power_never_counts_a_product_that_was_already_alerting() -> None:
    """Delivered is the denominator, and it excludes prior alerts -- so it cannot exceed eligible."""
    products = []
    for i in range(40):
        months = [m for m in range(48) for _ in range(10)]
        # A product already in free fall: the rule fires on it with nothing injected.
        ratings = [5 if m < 24 else 1 for m in months]
        products.append(K.Product(f"p{i}", 48, tuple(months), tuple(ratings), start=24000))
    out = K.power_grid(products, [BASE], reference=BASE, effects=[0.3], mechanisms=["severe"],
                       ramps=[1], window_points=6, seed=2, replicates=1)
    r = out[(BASE.key, "severe", 0.3, 1)]
    assert r.delivered <= r.eligible
    assert r.detected <= r.delivered


# ==================================================================== detection window ====
def test_detection_is_counted_in_evaluable_points_not_calendar_months() -> None:
    evaluable = [10, 14, 19, 25, 40, 51, 60, 71]
    # Calendar month 25 is 15 months after the step but only the 4th evaluable point.
    assert K.detected_within([25], from_index=10, window_points=6, evaluable_at=evaluable) == 3
    # The 6th evaluable point is the last one inside the window; the 7th is outside it.
    assert K.detected_within([51], from_index=10, window_points=6, evaluable_at=evaluable) == 5
    assert K.detected_within([60], from_index=10, window_points=6, evaluable_at=evaluable) is None
    assert K.detected_within([71], from_index=10, window_points=6, evaluable_at=evaluable) is None


# ========================================================================= selection ====
def placebo_at(c: K.Config, alerts_per_month: float) -> K.PlaceboResult:
    return K.PlaceboResult(config=c, replicates=1, alerts=int(alerts_per_month * 100),
                           points=1000, evaluable=500, surveillance_months=100)


def power_at(c: K.Config, value: float) -> K.PowerResult:
    n = 1000
    return K.PowerResult(config=c, mechanism="severe", effect=0.3, ramp_months=6, eligible=n,
                         delivered=n, detected=int(value * n), delays=(2,) * int(value * n))


def test_only_configurations_under_the_ceiling_are_admissible() -> None:
    loose = K.config_from(BASE.as_rule_fields(), delta=0.3)
    tight = K.config_from(BASE.as_rule_fields(), delta=1.0)
    sel = K.select([loose, tight],
                   {loose.key: placebo_at(loose, 9.0), tight.key: placebo_at(tight, 0.4)},
                   {loose.key: power_at(loose, 0.99), tight.key: power_at(tight, 0.10)},
                   ceiling_alerts_per_month=1.0, power_target=0.8)
    assert sel.config == tight, "the higher-power rule is inadmissible and must not win"
    assert sel.admissible == 1


def test_no_admissible_configuration_chooses_nothing_and_says_why() -> None:
    c = BASE
    sel = K.select([c], {c.key: placebo_at(c, 5.0)}, {c.key: power_at(c, 0.95)},
                   ceiling_alerts_per_month=1.0, power_target=0.8)
    assert sel.config is None
    assert "ceiling" in sel.reason and sel.admissible == 0


def test_a_tie_on_power_falls_to_the_more_conservative_rule() -> None:
    a = K.config_from(BASE.as_rule_fields(), delta=0.5)
    b = K.config_from(BASE.as_rule_fields(), delta=1.0)
    sel = K.select([a, b], {a.key: placebo_at(a, 0.2), b.key: placebo_at(b, 0.2)},
                   {a.key: power_at(a, 0.9), b.key: power_at(b, 0.9)},
                   ceiling_alerts_per_month=1.0, power_target=0.8)
    assert sel.config == b


# ============================================================================ verdict ====
def verdict_for(*, placebo_rate: float, power: float, fastpath_ok: bool = True):
    c = BASE
    sel = K.select([c], {c.key: placebo_at(c, placebo_rate)}, {c.key: power_at(c, power)},
                   ceiling_alerts_per_month=1.0, power_target=0.8)
    return gate.verdict(selection=sel, placebo_by_key={c.key: placebo_at(c, placebo_rate)},
                        primary_power=power_at(c, power) if sel.config else None,
                        constituents=["GOLD_PLACEBO ..."], ceiling=1.0, power_target=0.8,
                        fastpath_ok=fastpath_ok, scope="full")


def test_the_gate_passes_only_when_both_bars_hold_and_the_evaluator_agrees() -> None:
    assert verdict_for(placebo_rate=0.5, power=0.9).status == "PASS"
    assert verdict_for(placebo_rate=5.0, power=0.9).status == "FAIL", "ceiling breached"
    assert verdict_for(placebo_rate=0.5, power=0.4).status == "FAIL", "power short"
    assert verdict_for(placebo_rate=0.5, power=0.9, fastpath_ok=False).status == "FAIL", \
        "the numbers describe a rule the project does not run"


def test_the_verdict_prints_its_number_beside_its_bar() -> None:
    v = verdict_for(placebo_rate=0.5, power=0.9)
    assert v.metric["name"] == "detection_power_at_primary_effect"
    assert v.metric["threshold"] == 0.8 and v.metric["direction"] == "gte"
    assert len(v.metric["interval"]) == 2
    assert "verdict=PASS" in v.terminal and "power=0.9000" in v.terminal


def test_the_artefact_validates_against_the_frozen_contract() -> None:
    v = verdict_for(placebo_rate=0.5, power=0.9)
    doc = E.build_artifact(v, capability="gold_calibration", phase="P3 Gold", kind="quality",
                           protocol_hash=HASH, population={"name": "pre-2020 products", "n": 5880},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert json.loads(json.dumps(doc))["gate_name"] == "GOLD_CALIBRATION"


def test_a_failed_calibration_still_publishes_a_number() -> None:
    """FAIL is a result, not an absence: the table must show what the rule actually did."""
    v = verdict_for(placebo_rate=0.5, power=0.4)
    doc = E.build_artifact(v, capability="gold_calibration", phase="P3 Gold", kind="quality",
                           protocol_hash=HASH, population={"name": "pre-2020 products", "n": 5880},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert doc["status"] == "FAIL" and doc["metric"]["value"] == 0.4


# ============================================================================== grid ====
def test_calibration_may_only_vary_the_fields_the_grid_names() -> None:
    with pytest.raises(ValueError, match="may only vary"):
        K.config_from(BASE.as_rule_fields(), holdout_start="2021-01")


def test_wilson_brackets_the_point_estimate() -> None:
    lo, hi = K.wilson(80, 100)
    assert lo < 0.8 < hi
    assert K.wilson(0, 0) == (0.0, 0.0)

"""The decline rule over synthetic spines (src/gold/rule.py)."""
from __future__ import annotations

import pytest

from src.gold import rule as R

RULE = R.Rule(status="frozen", holdout_start="2020-01", baseline_months=3, recent_months=3,
              delta=0.5, persistence=2, max_gap=1, recovery_points=2, min_reviews=6,
              min_active_months=2, unevaluable_resets_persistence=True, config_hash="x",
              metric_tolerance=1e-9)


def month(m: str, n: int, mean: float, verified: int | None = None) -> dict:
    return {"month": m, "review_count": n, "rating_sum": n * mean, "neg_count": 0,
            "verified_count": n if verified is None else verified,
            "verified_rating_sum": (n if verified is None else verified) * mean,
            "nonempty_text_count": n, "long_text_count": n // 2}


def spine(means: list[float | None], start: str = "2015-01", n: int = 4) -> list[dict]:
    months = R.spine_months(start, R.month_str(R.month_index(start) + len(means) - 1))
    return [month(m, 0 if v is None else n, 0.0 if v is None else v) for m, v in zip(months, means)]


def test_load_rule_reads_the_repo_config_and_hashes_it():
    r = R.load_rule()
    assert r.status in ("provisional", "frozen")
    assert len(r.config_hash) == 64
    assert r.baseline_months >= 1 and r.recent_months >= 1


def test_month_arithmetic():
    assert R.month_str(R.month_index("2019-12") + 1) == "2020-01"
    assert R.spine_months("2019-11", "2020-02") == ["2019-11", "2019-12", "2020-01", "2020-02"]


def test_incomplete_spine_is_rejected():
    s = spine([4.5, 4.5, 4.5, 4.5, 4.5, 4.5])
    del s[2]
    with pytest.raises(ValueError):
        R.evaluate(s, RULE)


def test_flat_product_has_points_but_no_alert():
    pts, eps = R.evaluate(spine([4.5] * 10), RULE)
    assert len(pts) == 10 - 3 - 3 + 1
    assert all(p["evaluable"] for p in pts)
    assert not any(p["alert"] for p in pts)
    assert eps == []
    assert pts[0]["baseline_start"] == "2015-01" and pts[0]["point_month"] == "2015-03"
    assert pts[0]["recent_end"] == "2015-06"


def test_step_decline_alerts_at_the_second_true_point_then_the_baseline_absorbs_it():
    # Months 1-6 at 4.5, months 7-12 at 3.5. Point 2015-05: recent 06..08 = (4.5,3.5,3.5)
    # -> drop .67 true (run 1). Point 2015-06: baseline 4.5, recent 3.5 -> drop 1.0, run 2
    # -> alert. With trailing baselines a permanent step stops satisfying the condition once
    # the baseline is in the low regime, so K false points close the episode as "recovery":
    # that is RR-09's definition (K evaluable false points), not a claim the rating rose.
    pts, eps = R.evaluate(spine([4.5] * 6 + [3.5] * 6), RULE)
    by = {p["point_month"]: p for p in pts}
    assert by["2015-05"]["condition"] is True      # recent 06..08 mean = (4.5+3.5+3.5)/3 = 3.83 -> drop .67
    assert by["2015-06"]["alert"] is True
    assert len(eps) == 1
    ep = eps[0]
    assert ep["condition_started_at"] == "2015-05"
    assert ep["alert_triggered_at"] == "2015-06"
    assert ep["alert_complete_month"] == "2015-09"
    assert ep["closed_by"] == "recovery"
    assert ep["last_supported_at"] == "2015-07"
    assert ep["episode_closed_at"] == "2015-09"
    assert ep["drop_at_alert"] == pytest.approx(1.0)
    assert ep["in_holdout"] is False
    assert by["2015-05"]["episode_key"] == "2015-05" and by["2015-06"]["episode_key"] == "2015-05"


def test_unevaluable_point_resets_persistence():
    # A true point, then an empty month inside the recent window makes the next point
    # unevaluable (active months < 2 in recent), so the run restarts.
    # Point 2015-03: recent 04..06 = (4.5,4.5,3.4) -> drop .37, false. Point 2015-04: recent
    # 05..07 = (4.5,3.4,empty) -> 8 reviews, 2 active months, mean 3.95 -> true, run 1.
    # Point 2015-05: recent 06..08 = (3.4,empty,empty) -> 4 reviews -> unevaluable, run reset.
    means = [4.5, 4.5, 4.5, 4.5, 4.5, 3.4, None, None, 3.4, 3.4, 3.4, 3.4]
    pts, _ = R.evaluate(spine(means), RULE)
    by = {p["point_month"]: p for p in pts}
    assert by["2015-03"]["condition"] is False
    assert by["2015-04"]["condition"] is True and by["2015-04"]["persistence_run"] == 1
    assert by["2015-05"]["evaluable"] is False
    assert by["2015-05"]["persistence_run"] == 0
    assert by["2015-05"]["unevaluable_reason"] in ("recent_reviews", "recent_active_months")


def test_recovery_closes_after_k_false_points_and_a_new_episode_can_start():
    means = [4.5] * 4 + [3.0] * 4 + [4.5] * 6 + [3.0] * 4
    _, eps = R.evaluate(spine(means), RULE)
    assert [e["closed_by"] for e in eps] == ["recovery", "end_of_data"]
    first = eps[0]
    assert first["episode_closed_at"] > first["last_supported_at"]
    assert eps[1]["condition_started_at"] > first["episode_closed_at"]


def test_gap_closes_an_open_episode():
    rule = R.Rule(**{**RULE.as_params(), "max_gap": 0})
    means = [4.5] * 4 + [3.0] * 3 + [None] * 3 + [3.0] * 4
    _, eps = R.evaluate(spine(means), rule)
    assert eps[0]["closed_by"] == "gap"


def test_holdout_points_are_withheld_until_frozen():
    provisional = R.Rule(**{**RULE.as_params(), "status": "provisional"})
    means = [4.5] * 12
    pts_p, _ = R.evaluate(spine(means, start="2019-06"), provisional)
    pts_f, _ = R.evaluate(spine(means, start="2019-06"), RULE)
    assert all(p["point_month"] < "2020-01" for p in pts_p)
    assert any(p["in_holdout"] for p in pts_f)
    assert len(pts_f) > len(pts_p)


def test_window_statistics_are_sums_over_the_window():
    s = [month("2015-01", 2, 5.0), month("2015-02", 4, 4.0), month("2015-03", 2, 3.0),
         month("2015-04", 3, 2.0), month("2015-05", 3, 2.0), month("2015-06", 3, 2.0)]
    pts, _ = R.evaluate(s, RULE)
    p = pts[0]
    assert p["baseline_reviews"] == 8
    assert p["baseline_mean"] == pytest.approx((10 + 16 + 6) / 8)
    assert p["recent_mean"] == pytest.approx(2.0)
    assert p["baseline_long_text"] == 1 + 2 + 1

"""Matching and the discovery draw (ADR-0003, RR-19): deterministic, and refusing bad input."""
from __future__ import annotations

import pytest

from src.common import runs
from src.gold.controls import (
    Discovery,
    draw_discovery,
    draw_key,
    load_protocol,
    match_controls,
    text_characterisable,
)


def point(asin: str, *, mean: float, vol: int, evaluable: bool = True, condition: bool = False,
          text: int = 50, long_text: int = 20, month: str = "2016-06") -> dict:
    return {"parent_asin": asin, "point_month": month, "baseline_mean": mean, "baseline_reviews": vol,
            "recent_mean": mean - 0.1, "recent_reviews": vol, "evaluable": evaluable,
            "condition": condition, "baseline_nonempty_text": text, "recent_nonempty_text": text,
            "baseline_long_text": long_text, "recent_long_text": long_text}


@pytest.fixture(scope="module")
def protocol():
    return load_protocol()


def test_protocol_hash_covers_the_matching_and_discovery_tables(protocol):
    assert len(protocol.config_hash) == 64
    assert protocol.matching.controls_per_candidate >= 1
    assert 0 < protocol.discovery.low_rated_share < 1


def test_text_characterisable_needs_both_windows(protocol):
    m = protocol.matching
    assert text_characterisable(point("A", mean=4.0, vol=40), m)
    thin = point("A", mean=4.0, vol=40, text=m.min_nonempty_text_per_window - 1)
    assert not text_characterisable(thin, m)
    short = point("A", mean=4.0, vol=40, long_text=m.min_long_text_per_window - 1)
    assert not text_characterisable(short, m)


def test_matching_ranks_by_rating_then_volume_then_asin(protocol):
    cand = point("CAND", mean=4.0, vol=40)
    pool = [point("FAR", mean=4.4, vol=40), point("NEAR", mean=4.05, vol=40),
            point("NEARBIG", mean=4.05, vol=78)]
    got = match_controls(cand, pool, protocol.matching)
    assert [c["control_asin"] for c in got] == ["NEAR", "NEARBIG"]  # two per candidate
    assert got[0]["baseline_mean_diff"] == pytest.approx(0.05)


def test_matching_excludes_alerting_shape_and_out_of_band_products(protocol):
    cand = point("CAND", mean=4.0, vol=40)
    pool = [point("CONDITION_TRUE", mean=4.0, vol=40, condition=True),
            point("UNEVALUABLE", mean=4.0, vol=40, evaluable=False),
            point("TOO_DIFFERENT", mean=3.0, vol=40),
            point("TOO_SMALL", mean=4.0, vol=10),
            point("TOO_BIG", mean=4.0, vol=400),
            point("NO_TEXT", mean=4.0, vol=40, text=0, long_text=0)]
    assert match_controls(cand, pool, protocol.matching) == []


def test_matching_refuses_a_pool_from_another_month(protocol):
    cand = point("CAND", mean=4.0, vol=40, month="2016-06")
    with pytest.raises(ValueError, match="point_month"):
        match_controls(cand, [point("OTHER", mean=4.0, vol=40, month="2017-01")], protocol.matching)


def test_draw_key_is_stable_and_salted():
    assert draw_key("r1", 7, "discovery") == draw_key("r1", 7, "discovery")
    assert draw_key("r1", 7, "discovery") != draw_key("r1", 7, "audit")
    assert 0.0 <= draw_key("r1", 7, "discovery") < 1.0


def test_discovery_draw_hits_the_stratum_quota_and_the_product_cap():
    rows = [{"review_id": f"r{i}", "parent_asin": f"p{i % 7}", "rating": 1 if i % 2 else 5}
            for i in range(400)]
    d = Discovery(size=100, low_rated_share=0.7, low_rated_max_stars=3, max_per_product=20,
                  min_text_words=5, salt="discovery")
    picked = draw_discovery(rows, d, seed=1)
    assert len(picked) == 100
    assert sum(1 for r in picked if r["stratum"] == "low") == 70
    per_product = {}
    for r in picked:
        per_product[r["parent_asin"]] = per_product.get(r["parent_asin"], 0) + 1
    assert max(per_product.values()) <= 20
    assert [r["review_id"] for r in picked] == [r["review_id"] for r in draw_discovery(rows, d, seed=1)]
    assert [r["review_id"] for r in picked] != [r["review_id"] for r in draw_discovery(rows, d, seed=2)]


def test_discovery_draw_reports_a_shortfall_by_returning_fewer_rows():
    """Two low-rated products under a cap of 5 cannot fill a quota of 70; the job hard-fails."""
    rows = [{"review_id": f"r{i}", "parent_asin": f"p{i % 2}", "rating": 1} for i in range(400)]
    d = Discovery(size=100, low_rated_share=0.7, low_rated_max_stars=3, max_per_product=5,
                  min_text_words=5, salt="discovery")
    picked = draw_discovery(rows, d, seed=1)
    assert len(picked) == 10 < d.size


def test_theme_samples_contract_identity_catches_a_lost_candidate():
    c = runs.contract_for("theme_samples", runs.THEME_SAMPLES_SPEC_VERSION)
    assert c is not None
    good = {"candidate_episodes": 10, "matched_candidates": 7, "dropped_no_matching_control": 1,
            "dropped_not_text_characterisable": 2, "discovery_rows": 5, "discovery_low_rated": 3,
            "discovery_high_rated": 2}
    assert c.identity({"records_out": 5}, good) == []
    bad = {**good, "matched_candidates": 6}
    assert any("candidate_episodes" in f for f in c.identity({"records_out": 5}, bad))
    assert any("discovery_rows" in f for f in c.identity({"records_out": 4}, good))

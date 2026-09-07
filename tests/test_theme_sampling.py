"""Matching and the discovery draw (ADR-0003, RR-19): deterministic, and refusing bad input."""
from __future__ import annotations

import pytest

from src.common import runs
from src.gold.controls import (
    Discovery,
    draw_discovery,
    draw_enriched,
    draw_key,
    draw_representative,
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
            "dropped_not_text_characterisable": 2, "drawn_rows": 5, "eligible_reviews": 40,
            "low_rated_rows": 3, "high_rated_rows": 2}
    assert c.identity({"records_out": 5, "records_in": 40}, good) == []
    bad = {**good, "matched_candidates": 6}
    assert any("candidate_episodes" in f for f in c.identity({"records_out": 5, "records_in": 40}, bad))
    assert any("drawn_rows" in f for f in c.identity({"records_out": 4, "records_in": 40}, good))


def test_theme_samples_identity_accepts_an_enriched_frame_and_rejects_a_bad_split():
    """v2 must validate a frame with no low/high strata at all (RR-22)."""
    c = runs.contract_for("theme_samples", runs.THEME_SAMPLES_SPEC_VERSION)
    base = {"candidate_episodes": 10, "matched_candidates": 7, "dropped_no_matching_control": 1,
            "dropped_not_text_characterisable": 2, "eligible_reviews": 900, "drawn_rows": 200,
            "enriched_rows": 120, "representative_rows": 80}
    assert c.identity({"records_out": 200, "records_in": 900}, base) == []
    bad = {**base, "representative_rows": 70}
    assert any("representative_rows" in f for f in c.identity({"records_out": 200, "records_in": 900}, bad))
    # A drawn row that no eligible row could have supplied is a lost-frame bug, not a rounding one.
    over = {**base, "eligible_reviews": 100}
    assert any("eligible_reviews" in f for f in c.identity({"records_out": 200, "records_in": 100}, over))


# --------------------------------------------------------------- the RR-22 frames ----
def _rows(n: int, matched=None):
    return [{"review_id": f"r{i}", "parent_asin": f"p{i % 7}", "role": "candidate",
             "episode_id": "e", "rating": (i % 5) + 1, "text_word_count": 30,
             "matched": (matched or {})(i) if matched else {}} for i in range(n)]


def test_draw_enriched_fills_the_rarest_theme_first_and_never_reuses_a_review():
    """A review matching two themes is consumed by whichever theme picks first."""
    rows = _rows(40, matched=lambda i: {"rare": 2, "common": 1} if i < 5 else {"common": 1})
    picked, fill = draw_enriched(rows, theme_order=["rare", "common"], per_theme=5, total=10,
                                 seed=1, salt="development")
    assert fill == {"rare": 5, "common": 5}
    assert len(picked) == 10
    assert len({r["review_id"] for r in picked}) == 10
    assert {r["stratum"] for r in picked} == {"enriched_rare", "enriched_common"}


def test_draw_enriched_prefers_reviews_matching_more_of_the_theme_terms():
    rows = _rows(20, matched=lambda i: {"t": 3 if i < 2 else 1})
    picked, _ = draw_enriched(rows, theme_order=["t"], per_theme=2, total=2, seed=1, salt="s")
    assert {r["review_id"] for r in picked} == {"r0", "r1"}


def test_draw_enriched_reports_a_short_theme_and_backfills_the_frame():
    rows = _rows(30, matched=lambda i: {"rare": 1} if i < 2 else {"common": 1})
    picked, fill = draw_enriched(rows, theme_order=["rare", "common"], per_theme=5, total=10,
                                 seed=1, salt="s")
    assert fill["rare"] == 2                       # the shortfall is visible, not absorbed
    assert len(picked) == 10
    assert sum(1 for r in picked if r["stratum"] == "enriched_backfill") == 3


def test_draw_representative_is_seeded_and_excludes_the_enriched_rows():
    rows = _rows(50)
    first = draw_representative(rows, size=10, seed=7, salt="audit", exclude=set())
    assert first == draw_representative(list(reversed(rows)), size=10, seed=7, salt="audit",
                                        exclude=set())
    taken = {r["review_id"] for r in first[:4]}
    second = draw_representative(rows, size=10, seed=7, salt="audit", exclude=taken)
    assert not taken & {r["review_id"] for r in second}
    assert all(r["stratum"] == "representative" for r in second)


def test_the_protocol_declares_every_frame_and_each_carries_its_own_hash():
    p = load_protocol()
    assert p.frozen, "the post-discovery frames may only be drawn from a frozen protocol"
    assert sorted(p.frames) == ["audit", "development", "training_pool"]
    assert p.frames["audit"].enriched_rows + p.frames["audit"].representative_rows == 200
    assert p.frames["development"].representative_rows == 0
    assert p.frames["training_pool"].enriched_rows == 0
    hashes = {f.config_hash for f in p.frames.values()} | {p.config_hash}
    assert len(hashes) == 4, "a frame that shares another's hash cannot be told apart in lineage"
    assert p.frames["audit"].per_theme_quota(10) == 12

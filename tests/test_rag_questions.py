"""The frozen P7 question set's pure logic (ticket 11, ADR-0006, RR-17).

Every check here has a case that would trip it. That is the audit's F3 rule, and it matters most
for `check_manifest`: a manifest checker that cannot fail is a rubber stamp on whatever the
builder happened to produce, and the thirty questions are the one artefact nobody gets to edit
after an answer exists.
"""
from __future__ import annotations

import json

import pytest

from src.ai import rag_questions as rq

# --------------------------------------------------------------------- ranking ----


def test_bootstrap_lower_bound_is_below_the_point_estimate():
    baseline, recent = [5.0] * 10 + [4.0] * 10, [2.0] * 10 + [1.0] * 10
    lb = rq.bootstrap_lower_bound(baseline, recent, seed=1, draws=500, quantile=0.025)
    point = sum(baseline) / len(baseline) - sum(recent) / len(recent)
    assert lb is not None
    assert lb < point


def test_bootstrap_lower_bound_punishes_a_thin_window():
    """The same mean drop over four reviews must rank below the same drop over four hundred."""
    thin = rq.bootstrap_lower_bound([5.0, 3.0], [3.0, 1.0], seed=7, draws=800, quantile=0.025)
    thick = rq.bootstrap_lower_bound([5.0, 3.0] * 100, [3.0, 1.0] * 100,
                                     seed=7, draws=800, quantile=0.025)
    assert thin is not None and thick is not None
    assert thin < thick


def test_bootstrap_lower_bound_refuses_an_empty_window():
    assert rq.bootstrap_lower_bound([], [1.0], seed=1, draws=200, quantile=0.025) is None
    assert rq.bootstrap_lower_bound([1.0], [], seed=1, draws=200, quantile=0.025) is None


def test_bootstrap_lower_bound_is_deterministic_in_the_seed():
    a = rq.bootstrap_lower_bound([5.0, 4.0, 3.0], [1.0, 2.0], seed=99, draws=300, quantile=0.025)
    b = rq.bootstrap_lower_bound([5.0, 4.0, 3.0], [1.0, 2.0], seed=99, draws=300, quantile=0.025)
    assert a == b


def _spec(**over):
    base = {"version": "t", "status": "provisional", "seed": 1, "bootstrap_draws": 200,
            "lower_bound_quantile": 0.025,
            "templates": {"product_scoped": "", "temporal": ""},
            "scan_terms": {"does_not_work": ["does not work"]},
            "absent_attributes": [], "out_of_domain": [], "refusal_reasons": {},
            "spec_hash": "h"}
    base.update(over)
    return rq.QuestionSpec(**base)


def _episode(eid, asin, rank_hint=""):
    return {"episode_id": eid, "candidate_asin": asin, "point_month": "2017-01",
            "baseline_start": "2016-08", "baseline_end": "2017-01",
            "recent_start": "2017-02", "recent_end": "2017-07"}


def test_rank_candidates_orders_by_lower_bound_and_breaks_ties_on_id():
    episodes = [_episode("e1", "B"), _episode("e2", "A")]
    ratings = {"e1": {"baseline": [5.0] * 8, "recent": [1.0] * 8},
               "e2": {"baseline": [5.0] * 8, "recent": [1.0] * 8}}
    ranked = rq.rank_candidates(episodes, ratings, _spec())
    assert [e["decline_rank"] for e in ranked] == [1, 2]
    # Identical data, so the lower bounds differ only by the per-episode seed; whatever the order,
    # it must be a function of the data and not of the input list's order.
    again = rq.rank_candidates(list(reversed(episodes)), ratings, _spec())
    assert [e["episode_id"] for e in ranked] == [e["episode_id"] for e in again]


def test_rank_candidates_drops_an_episode_with_no_ratings():
    ranked = rq.rank_candidates([_episode("e1", "A")], {}, _spec())
    assert ranked == []


# ----------------------------------------------------------------- slot picking ----
def _ranked(n):
    out = []
    for i in range(1, n + 1):
        e = _episode(f"e{i}", f"CAND{i}")
        out.append({**e, "decline_rank": i, "decline_lower_bound": 10.0 - i})
    return out


def _controls(n, per=2):
    return {f"e{i}": [{"control_asin": f"CTL{i}_{j}", "control_rank": j, "baseline_mean_diff": 0.1 * j}
                      for j in range(1, per + 1)] for i in range(1, n + 1)}


def test_select_slots_takes_five_candidates_with_their_nearest_control():
    draw = rq.select_slots(_ranked(6), _controls(6))
    assert draw["complete"] and draw["candidates_selected"] == 5
    controls = [s for s in draw["slots"] if s["slot_role"] == "control"]
    assert all(s["control_rank"] == 1 for s in controls)
    assert len({s["parent_asin"] for s in draw["slots"]}) == 10


def test_select_slots_never_reuses_a_product_across_slots():
    shared = {f"e{i}": [{"control_asin": "SHARED", "control_rank": 1, "baseline_mean_diff": 0.1},
                        {"control_asin": f"CTL{i}", "control_rank": 2, "baseline_mean_diff": 0.2}]
              for i in range(1, 7)}
    draw = rq.select_slots(_ranked(6), shared)
    asins = [s["parent_asin"] for s in draw["slots"]]
    assert len(asins) == len(set(asins))
    assert asins.count("SHARED") == 1


def test_select_slots_walks_past_an_ineligible_candidate_and_records_why():
    eligible = {f"CAND{i}" for i in range(1, 7)} | {f"CTL{i}_1" for i in range(1, 7)}
    eligible.discard("CAND1")
    draw = rq.select_slots(_ranked(6), _controls(6), eligible=eligible)
    assert draw["complete"]
    assert "CAND1" not in {s["parent_asin"] for s in draw["slots"]}
    assert {"episode_id": "e1", "reason": "candidate_not_eligible"} in draw["skipped"]


def test_select_slots_reports_an_incomplete_draw_rather_than_relaxing():
    draw = rq.select_slots(_ranked(3), _controls(3))
    assert not draw["complete"]
    assert draw["candidates_selected"] == 3


# --------------------------------------------------------------- evidence scan ----
def test_scan_review_is_word_bounded():
    pats = rq.compile_terms({"t": ["work"]})
    assert rq.scan_review("it does not work", pats) == {"t": [r"\bwork\b"]}
    assert rq.scan_review("network issues", pats) == {}


def test_scan_review_tolerates_whitespace_inside_a_phrase():
    pats = rq.compile_terms({"t": ["fell apart"]})
    assert rq.scan_review("it fell\n  apart", pats)


def test_scan_review_matches_praise_too_which_is_why_validation_exists():
    """The scan is recall-oriented; this match is a false positive a human must reject."""
    pats = rq.compile_terms({"arrived_damaged": ["leaked"]})
    assert rq.scan_review("Nothing leaked, unlike the last one I bought", pats)


def test_probe_regex_matches_the_same_thing_as_scan_review():
    import re
    terms = ["fell apart", "no effect"]
    pattern = re.compile(rq.probe_regex(terms))
    pats = rq.compile_terms({"t": terms})
    for text in ("it fell apart", "no effect at all", "nothing here"):
        assert bool(pattern.search(text.lower())) == bool(rq.scan_review(text, pats))


def _validated(baseline: int, recent: int):
    return ([{"window": "baseline", "complaint_bearing": True} for _ in range(baseline)]
            + [{"window": "recent", "complaint_bearing": True} for _ in range(recent)])


def test_answerability_needs_the_bar_in_every_window():
    assert rq.answerability(_validated(3, 3), windows=["baseline", "recent"])["verdict"] == "answerable"
    assert rq.answerability(_validated(9, 2), windows=["baseline", "recent"])["verdict"] == "excluded_band"


def test_answerability_calls_zero_everywhere_unanswerable():
    assert rq.answerability([], windows=["baseline", "recent"])["verdict"] == "unanswerable"


def test_answerability_excludes_the_one_to_two_band():
    assert rq.answerability(_validated(2, 0), windows=["baseline", "recent"])["verdict"] == "excluded_band"


# -------------------------------------------------------------- manifest checks ----
def _question(qid, family, *, answerability="answerable", stratum=None, asin="A",
              windows=("scope",), support=3, role="candidate"):
    key = {"required_propositions": ["something"] if answerability == "answerable" else [],
           "acceptable_themes": ["does_not_work"],
           "supporting_review_ids": ([{"review_id": f"r{i}", "window": w}
                                      for w in windows for i in range(support)]
                                     if answerability == "answerable" else []),
           "forbidden_claims": list(rq.FORBIDDEN_CLAIMS),
           "expected_refusal_reason": None if answerability == "answerable" else "nothing to cite"}
    return {"question_id": qid, "family": family, "stratum": stratum,
            "answerability": answerability, "parent_asin": asin, "slot_role": role,
            "scope": {"windows": {w: {"start": "2016-01", "end": "2016-06"} for w in windows}},
            "answer_key": key}


def _manifest():
    qs = []
    for i in range(10):
        asin = f"P{i}"
        qs.append(_question(f"product_scoped-{i:02d}", "product_scoped", asin=asin))
        qs.append(_question(f"temporal-{i:02d}", "temporal", asin=asin,
                            windows=("baseline", "recent")))
    for stratum, n in rq.UNANSWERABLE_STRATA.items():
        for i in range(n):
            qs.append(_question(f"{stratum}-{i:02d}", "product_scoped",
                                answerability="unanswerable", stratum=stratum, asin="P0"))
    return {"questions": qs}


def test_a_well_formed_manifest_has_no_violations():
    assert rq.check_manifest(_manifest()) == []


def test_manifest_rejects_the_wrong_total():
    m = _manifest()
    m["questions"].pop()
    assert any("expected 30 questions" in v for v in rq.check_manifest(m))


def test_manifest_rejects_families_that_cover_different_products():
    m = _manifest()
    next(q for q in m["questions"] if q["family"] == "temporal")["parent_asin"] = "OTHER"
    assert any("same ten products" in v for v in rq.check_manifest(m))


def test_manifest_rejects_a_window_short_of_support():
    m = _manifest()
    q = next(q for q in m["questions"] if q["family"] == "temporal")
    q["answer_key"]["supporting_review_ids"] = [
        s for s in q["answer_key"]["supporting_review_ids"] if s["window"] != "recent"][:3] + [
        {"review_id": "x", "window": "recent"}]
    assert any("carries 1 supporting reviews" in v for v in rq.check_manifest(m))


def test_manifest_rejects_a_key_that_drops_the_forbidden_claims():
    m = _manifest()
    m["questions"][0]["answer_key"]["forbidden_claims"] = []
    assert any("forbidden claims" in v for v in rq.check_manifest(m))


def test_manifest_rejects_an_unanswerable_question_carrying_evidence():
    m = _manifest()
    q = next(q for q in m["questions"] if q["answerability"] == "unanswerable")
    q["answer_key"]["supporting_review_ids"] = [{"review_id": "r", "window": "scope"}]
    assert any("carries supporting reviews" in v for v in rq.check_manifest(m))


def test_manifest_rejects_an_answerable_question_carrying_a_refusal_reason():
    m = _manifest()
    m["questions"][0]["answer_key"]["expected_refusal_reason"] = "no"
    assert any("carries a refusal reason" in v for v in rq.check_manifest(m))


def test_manifest_rejects_a_wrong_stratum_split():
    m = _manifest()
    next(q for q in m["questions"] if q["stratum"] == "out_of_domain")["stratum"] = "absent_attribute"
    violations = rq.check_manifest(m)
    assert any("absent_attribute questions" in v for v in violations)
    assert any("out_of_domain questions" in v for v in violations)


# ------------------------------------------------------------ the real artefacts ----
def test_the_committed_spec_loads_and_hashes():
    spec = rq.load_spec()
    assert set(spec.templates) >= set(rq.FAMILIES) | set(rq.UNANSWERABLE_STRATA)
    assert len(spec.spec_hash) == 64


def test_the_committed_question_set_satisfies_every_invariant():
    if not rq.MANIFEST_PATH.exists():
        pytest.skip("the question set has not been frozen yet")
    manifest = json.loads(rq.MANIFEST_PATH.read_text())
    assert rq.check_manifest(manifest) == []
    assert manifest["status"] == "frozen"
    assert manifest["spec_hash"] == rq.load_spec().spec_hash


def test_the_frozen_questions_never_leak_the_theme_shift_direction():
    """ADR-0006 hides direction from generator and judge; these fields are what they read.

    `forbidden_claims` is excluded on purpose -- it says the words in order to ban them, and a
    blanket scan over the whole file would flag the ban itself.
    """
    if not rq.MANIFEST_PATH.exists():
        pytest.skip("the question set has not been frozen yet")
    manifest = json.loads(rq.MANIFEST_PATH.read_text())
    words = ("increase", "decrease", "rose", "more common", "less common", "trend",
             "theme shift", "theme_shift", "worsen", "improved")
    for q in manifest["questions"]:
        key = q["answer_key"]
        readable = " ".join([q["question"], *key["required_propositions"],
                             *key["acceptable_themes"],
                             key["expected_refusal_reason"] or ""]).lower()
        for word in words:
            assert word not in readable, f"{q['question_id']} mentions {word!r}"

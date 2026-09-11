"""P6's close-out: the gate split, the withheld interval, and the agreement number (ticket 10).

Four things are asserted here, each of them a claim ticket 10 makes about what may be
published:

* **`THEMES_GATE` is reproducibility only, and every constituent can still trip it.** The
  macro-F1 is not among them: a quality number that can fail a phase gate is a standing
  invitation to retune a frozen protocol.
* **`THEMES_QUALITY` fails on the audit numbers and blocks nothing.** The bar is 0.70 and the
  measured value is 0.4583; the verdict reads FAIL and the phase is still built.
* **The representative stratum publishes no interval.** Its point estimate is undefined, and
  the sealed artefact's interval beside it is an artefact of the resampler.
* **The agreement number is per-theme and overall with Wilson intervals**, and its denominator
  is the frozen draw rather than whichever rows happened to be labelled.

No Spark, no Ollama, no filesystem beyond the two config files the contract lives in.
"""
from __future__ import annotations

import pytest

from scripts.agreement_subset import pick, quotas
from src.ai.agreement import agreement_report
from src.ai.theme_scoring import system_report
from src.common import evaluation as E
from src.gates import themes as gate

RUN_ID = "e39584c3-9022-4417-8677-291166af7ec3"


# ------------------------------------------------------------------------ fixtures ----
def repro_facts(**over):
    facts = {
        "taxonomy": {"version": "1", "themes": 10, "file_hash": "0cc29c374779aa",
                     "recorded_hash": "0cc29c374779aa", "unchanged": True, "ok": True},
        "protocol": {"status": "frozen", "config_hash": "8a6d50fbc7fbaa",
                     "sizes": {"audit": 200}, "expected": {"audit": 200}, "overlap": 0,
                     "ok": True},
        "prompt": {"frozen": True, "name": "label_v5", "version": "label-v5",
                   "freeze_commit": "67926ed92bde", "in_spec": True, "audit_used": "label-v5",
                   "frozen_before_audit": True, "ok": True},
        "reference": {"table_present": True, "rows": 200, "distinct": 200, "expected": 200,
                      "human_rows": 0, "ok": True},
        "audit": {"present": True, "run_id": RUN_ID, "model_id": "qwen3:8b",
                  "config": "b84a2ce22193aa", "rows": 200, "expected": 200, "terminal": 200,
                  "succeeded": 173, "model_abstained": 0, "parse_failed": 27, "api_failed": 0,
                  "ok": True},
        "seal": {"present": True, "freezes_readable": True, "opened_at": "2026-09-11T06:26:28+00:00",
                 "commit": "aafaad58cc", "sealed": "f39ce4f57abd", "today": "f39ce4f57abd",
                 "systems": [], "moved": [], "artefacts_changed": [], "verdict_passed": False,
                 "ok": True},
    }
    facts.update(over)
    return facts


def overall(macro, ci, *, min_recall=0.4667, reviews=200, supported=9, per_theme=None):
    return {"subset": "overall", "reviews": reviews, "macro_f1": macro, "bootstrap_95": ci,
            "supported_themes": [f"t{i}" for i in range(supported)],
            "min_supported_recall": min_recall,
            "per_theme": per_theme or [
                {"theme_id": "does_not_work", "support": 28, "predicted": 53, "tp": 19, "fp": 34,
                 "fn": 9, "precision": 0.358, "recall": 0.679, "f1": 0.469, "supported": True},
                {"theme_id": "wrong_size_or_fit", "support": 8, "predicted": 35, "tp": 3, "fp": 32,
                 "fn": 5, "precision": 0.086, "recall": 0.375, "f1": 0.140, "supported": False}],
            "coverage": {"succeeded": 173, "model_abstained": 0, "parse_failed": 27,
                         "api_failed": 0, "absent": 0, "failure_rate": 0.135,
                         "abstention_rate": 0.0}}


def audit_score(**over):
    doc = {
        "min_support": 10,
        "overall": overall(0.4582513400987497, [0.36467415604739545, 0.5223414876717782]),
        "enriched": {"subset": "enriched", "reviews": 120, "macro_f1": 0.48627559886869776,
                     "bootstrap_95": [0.37006779174522486, 0.5576272071154857],
                     "supported_themes": ["t"] * 9, "min_supported_recall": 0.4,
                     "per_theme": [], "coverage": {"failure_rate": 0.2}},
        "representative": {"subset": "representative", "reviews": 80, "macro_f1": None,
                           "bootstrap_95": [0.4444444444444445, 0.8695652173913044],
                           "supported_themes": [], "min_supported_recall": None,
                           "per_theme": [], "coverage": {"failure_rate": 0.0375}},
    }
    doc.update(over)
    return doc


SYSTEMS = {
    "llm": {"overall": overall(0.4583, [0.3647, 0.5223])},
    "classifier": {"overall": overall(0.3899, [0.3074, 0.4639])},
    "star_only": {"overall": overall(0.2968, [0.2612, 0.3493])},
}
DEVELOPMENT = {"llm": overall(0.4633, [0.3830, 0.5145]),
               "star_only": overall(0.3656, [0.3392, 0.4075])}


# ================================================== THEMES_GATE is reproducibility ====
def test_the_gate_carries_six_reproducibility_constituents_and_no_score():
    v = gate.verdict(repro_facts(), scope="full")
    assert [name for name, _ in v.checks] == ["taxonomy", "protocol", "prompt", "reference",
                                              "audit_run", "seal"]
    assert not any("THEMES_SCORE" in line for line in v.lines), (
        "the macro-F1 is THEMES_QUALITY's; a quality number inside a blocking gate is an "
        "invitation to retune a frozen protocol (ADR-0011)")
    assert v.terminal == ("THEMES_GATE scope=full constituents_ok=6/6 failed=none "
                          "kind=reproducibility THEMES_GATE=PASS")


@pytest.mark.parametrize("constituent", ["taxonomy", "protocol", "prompt", "reference", "audit",
                                         "seal"])
def test_every_constituent_can_trip_the_gate(constituent):
    facts = repro_facts()
    facts[constituent] = {**facts[constituent], "ok": False}
    v = gate.verdict(facts, scope="full")
    assert v.status == "FAIL"
    assert v.terminal.endswith("THEMES_GATE=FAIL")


def test_a_missing_seal_names_the_command_that_opens_the_set():
    v = gate.verdict(repro_facts(seal={"present": False, "ok": False}), scope="full")
    assert "make audit-once" in v.constituents[-1]
    assert v.status == "FAIL"


# =================================================== THEMES_QUALITY reports, never blocks ====
def test_quality_fails_against_the_unmoved_bar_and_says_it_does_not_block():
    v = gate.quality(audit_score(), scope="full", systems=SYSTEMS, development=DEVELOPMENT)
    assert v.status == "FAIL"
    assert "blocks=false" in v.terminal
    assert "macro_f1=0.4583 bar=0.7" in v.terminal
    assert v.metric == {"name": "macro_f1", "value": 0.4583, "threshold": 0.7,
                        "direction": "gte", "interval": [0.3647, 0.5223]}


def test_the_quality_verdict_publishes_a_valid_artefact_even_on_a_fail():
    v = gate.quality(audit_score(), scope="full", systems=SYSTEMS, development=DEVELOPMENT)
    doc = E.build_artifact(v, capability="themes_quality", phase="P6 Themes", kind="quality",
                           protocol_hash="8a6d50fbc7fbaa", population={"name": "audit", "n": 200},
                           pipeline_run_id=RUN_ID, scope="full",
                           notes=gate.quality_notes(audit_score(), systems=SYSTEMS,
                                                    development=DEVELOPMENT))
    assert E.validate_artifact(doc) == []
    assert doc["status"] == "FAIL"


def test_an_unscored_audit_set_is_not_run_with_a_reason_rather_than_a_zero():
    v = gate.quality(None, scope="full")
    assert v.status == "NOT_RUN"
    assert "make audit-once" in (v.cut_reason or "")


# ============================================ the representative stratum has no interval ====
def test_the_representative_stratum_withholds_its_interval_and_says_why():
    lines = gate.stratum_lines(audit_score())
    rep = next(line for line in lines if line.startswith("THEMES_SUBSET representative"))
    assert "macro_f1=NOT_RUN" in rep and "bootstrap95=withheld" in rep
    assert "0.8695" not in rep and "0.4444" not in rep, (
        "the sealed artefact's interval is an artefact of the resampler averaging over the "
        "draws that happened to contain a supported theme")
    assert "no theme reaches min_support" in rep


def test_the_enriched_stratum_still_reports_its_interval():
    enriched = next(line for line in gate.stratum_lines(audit_score())
                    if line.startswith("THEMES_SUBSET enriched"))
    assert "macro_f1=0.4863" in enriched and "bootstrap95=[0.3701,0.5576]" in enriched


def test_a_future_pass_produces_no_interval_where_the_point_estimate_is_undefined():
    """The reporting fix above is for the sealed artefacts; this is the fix for the next pass."""
    reference = {"r1": set(), "r2": {"a"}}
    report = system_report(subset="representative", reference=reference, system={"r2": {"a"}},
                           theme_ids=["a", "b"], product_of={"r1": "p1", "r2": "p2"},
                           min_support=10, seed=1, draws=50)
    assert report["macro_f1"] is None
    assert report["bootstrap_95"] == [None, None]


# ======================================================= the three-way comparison ====
def test_the_comparison_states_the_separation_the_holdout_produced():
    line = gate.comparison_line(SYSTEMS, sample="audit", development=DEVELOPMENT)
    assert "llm_vs_star=disjoint" in line
    assert "classifier_vs_star=overlapping" in line
    assert "development_llm_vs_star=overlapping" in line


def test_the_finding_names_the_reversal_rather_than_the_development_story():
    finding = gate.comparison_finding(SYSTEMS, development=DEVELOPMENT)
    assert "disjoint" in finding
    assert "holdout reverses" in finding
    assert "not distinguishable" in finding, "the MLlib arm's honest reading goes in too"


def test_an_overlapping_holdout_would_report_the_overlap_instead():
    systems = {**SYSTEMS, "llm": {"overall": overall(0.32, [0.28, 0.38])}}
    finding = gate.comparison_finding(systems, development=DEVELOPMENT)
    assert "still overlaps" in finding and "J-shaped corpus" in finding


def test_a_missing_interval_is_never_half_an_interval():
    systems = {**SYSTEMS, "classifier": {"overall": overall(0.39, [None, 0.46])}}
    line = gate.comparison_line(systems, sample="audit")
    assert "classifier=0.39 none" in line
    assert "classifier_vs_star=none" in line


# ================================================== THEMES_AGREEMENT (RR-21) ====
def agreement_fixture():
    theme_ids = ["a", "b", "c"]
    reviews = [f"r{i}" for i in range(10)]
    agent = {"r0": {"a"}, "r1": {"a", "b"}, "r2": {"b"}, "r3": set(), "r4": {"c"},
             "r5": set(), "r6": {"a"}, "r7": set(), "r8": {"b"}, "r9": set()}
    human = {"r0": {"a"}, "r1": {"a"}, "r2": {"b"}, "r3": set(), "r4": {"c"},
             "r5": {"a"}, "r6": {"a"}, "r7": set(), "r8": {"b"}, "r9": set()}
    return agreement_report(agent=agent, human=human, theme_ids=theme_ids, review_ids=reviews)


def test_agreement_is_per_theme_and_overall_with_wilson_intervals():
    r = agreement_fixture()
    assert r["n"] == 30 and r["reviews"] == 10 and r["themes"] == 3
    assert r["agreed"] == 28 and r["agreement"] == pytest.approx(28 / 30)
    lo, hi = r["wilson_95"]
    assert 0 < lo < r["agreement"] < hi < 1
    assert [t["theme_id"] for t in r["per_theme"]] == ["a", "b", "c"]
    assert all(t["wilson_95"][0] <= t["agreement"] <= t["wilson_95"][1] for t in r["per_theme"])


def test_a_theme_neither_annotator_labels_reports_no_kappa_rather_than_zero():
    r = agreement_report(agent={"r0": set()}, human={"r0": set()}, theme_ids=["a"],
                         review_ids=["r0"])
    assert r["per_theme"][0]["agreement"] == 1.0
    assert r["per_theme"][0]["kappa"] is None, (
        "perfect agreement about nothing is not agreement no better than chance")


def test_a_review_missing_from_one_side_counts_as_no_themes_not_as_a_smaller_denominator():
    r = agreement_report(agent={"r0": {"a"}}, human={}, theme_ids=["a"], review_ids=["r0", "r1"])
    assert r["reviews"] == 2 and r["n"] == 2
    assert r["per_theme"][0]["agent_only"] == 1 and r["per_theme"][0]["human_only"] == 0


def test_the_agreement_verdict_is_reported_and_carries_its_interval():
    v = gate.agreement(agreement_fixture(), scope="full")
    assert v.status == "REPORTED"
    assert "blocks=false" in v.terminal and "verdict=REPORTED" in v.terminal
    assert v.metric["threshold"] is None and len(v.metric["interval"]) == 2


def test_an_unlabelled_subset_publishes_not_run_with_its_reason():
    v = gate.agreement(None, scope="full")
    assert v.status == "NOT_RUN"
    assert "hand-labelled" in (v.cut_reason or "")
    doc = E.build_artifact(v, capability="themes_agreement", phase="P6 Themes", kind="quality",
                           protocol_hash="8a6d50fbc7fbaa",
                           population={"name": "audit adjudication subset", "n": 50},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []


# ====================================================== the frozen draw of the 50 ====
THEMES = [f"t{i}" for i in range(10)]


def test_the_fifty_are_split_between_both_halves_of_the_audit_frame():
    q = quotas(THEMES, enriched=120, representative=80, total=50)
    assert sum(q.values()) == 50
    assert q["representative"] == 20
    assert all(q[f"enriched_{t}"] == 3 for t in THEMES), (
        "every theme gets a floor of hand-labelled reviews rather than whatever a flat "
        "random 50 happened to produce")


def test_the_draw_fills_each_stratum_by_seeded_key_and_reproduces():
    rows = ([{"review_id": f"e{t}-{i}", "stratum": f"enriched_{t}"} for t in THEMES
             for i in range(12)]
            + [{"review_id": f"rep-{i}", "stratum": "representative"} for i in range(80)])
    q = quotas(THEMES, enriched=120, representative=80, total=50)
    first, shortfall = pick(rows, q=q, seed=20260907, total=50)
    again, _ = pick(list(reversed(rows)), q=q, seed=20260907, total=50)
    assert len(first) == 50 and not shortfall
    assert {r["review_id"] for r in first} == {r["review_id"] for r in again}


def test_a_short_stratum_is_released_to_a_final_pass_and_reported():
    rows = ([{"review_id": "e0-0", "stratum": "enriched_t0"}]
            + [{"review_id": f"rep-{i}", "stratum": "representative"} for i in range(80)])
    q = quotas(THEMES, enriched=120, representative=80, total=50)
    picked, shortfall = pick(rows, q=q, seed=1, total=50)
    assert shortfall["enriched_t0"] == 2
    assert len(picked) == 50, "the shortfall is released, never topped up from a neighbour"
    assert len({r["review_id"] for r in picked}) == 50

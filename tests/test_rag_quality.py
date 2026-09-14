"""`RAG_QUALITY`: four bars over fixed denominators, judged by Philip, blocking nothing (ticket 13).

Asserted with nothing running -- no Postgres, no Elasticsearch, no Ollama:

* **the denominators never move.** A judged subset, an unanswered question, a refusal: every
  one scores as a failure over the manifest's 20 and 10, never as a row that leaves the count.
* **the derived outcomes cannot be judged away.** A refusal on an answerable question is a
  false refusal whatever the judge writes; an uncited claim caps `fully` to `partially`; a
  refusal that carries a claim is a failed abstention on an unanswerable question.
* **each target has a case that trips it**, and a miss prints FAIL beside its unmoved bar.
* **the judgement contract is strict in both directions**: an axis on a refusal is rejected,
  a missing axis on an answered question is rejected, and a row judged against another
  answers run is rejected.
* **the artefact validates** against `conf/eval-artifact.schema.json`, in both the NOT_RUN
  and the scored shape, and never as a reproducibility kind.
* **the quality verdict never touches `RAG_GATE`**: scoring the same facts with and without
  judgements leaves the reproducibility verdict byte-identical.
"""
from __future__ import annotations

import pytest

from src.common import evaluation as E
from src.gates import rag as gate
from src.gates import rag_quality as Q

WINDOWS = {"baseline": {"start": "2016-01", "end": "2016-06"},
           "recent": {"start": "2017-01", "end": "2017-06"}}
RUN = "run-2"


def question(qid, *, answerable=True, stratum=None, support=("r1", "r3")):
    return {"question_id": qid, "question": "What complaints …", "parent_asin": "B000",
            "family": "temporal" if qid.startswith("temporal") else "product_scoped",
            "answerability": "answerable" if answerable else "unanswerable",
            "stratum": stratum, "slot_role": "candidate",
            "retrieval": {"mode": "hybrid_per_window", "top_k": 10, "per_window_k": 5},
            "scope": {"windows": dict(WINDOWS)},
            "answer_key": {"required_propositions": ["one complaint per window"] if answerable else [],
                           "acceptable_themes": ["poor_build_quality"] if answerable else [],
                           "forbidden_claims": ["prevalence", "direction"],
                           "supporting_review_ids": [{"review_id": r, "window": "baseline",
                                                      "month": "2016-03", "themes": []}
                                                     for r in support],
                           "expected_refusal_reason": None if answerable else "nothing in scope"}}


def retrieved(*rows):
    return [{"cite_id": f"R{i + 1}", "review_id": r, "window": w, "rank": i + 1,
             "review_month": m, "title": "t", "text": "x"}
            for i, (r, w, m) in enumerate(rows)]


BASE = retrieved(("r1", "baseline", "2016-03"), ("r2", "baseline", "2016-05"),
                 ("r3", "recent", "2017-02"), ("r4", "recent", "2017-04"))


def answered(qid, *, cites=(("R1", "baseline"),), extra_uncited=False, rows=None):
    claims = [{"claim": "one cited review describes a broken clip",
               "citations": [{"cite_id": c, "window": w} for c, w in cites]}]
    if extra_uncited:
        claims.append({"claim": "no such complaint appears in the baseline", "citations": []})
    return {"question_id": qid, "status": "succeeded",
            "parsed": {"refused": False, "refusal_reason": None, "subject": "s",
                       "subject_supported": True, "claims": claims},
            "retrieved": list(BASE if rows is None else rows)}


def refused(qid, *, with_claim=False, rows=None):
    claims = ([{"claim": "hedge", "citations": []}] if with_claim else [])
    return {"question_id": qid, "status": "succeeded",
            "parsed": {"refused": True, "refusal_reason": "nothing in scope", "subject": "s",
                       "subject_supported": False, "claims": claims},
            "retrieved": list(BASE if rows is None else rows)}


def malformed(qid):
    return {"question_id": qid, "status": "parse_failed", "parsed": None, "retrieved": list(BASE)}


def judgement(qid, *, g="fully", a="fully", forbidden=False, reason=None, uncertain=False,
              secondary=None, note=None, kind="answered"):
    row = {"question_id": qid, "answers_run_id": RUN, "reviewed": True, "uncertain": uncertain,
           "secondary_label": secondary, "note": note,
           "groundedness": None, "adequacy": None, "forbidden_claim_present": None,
           "refusal_reason_matches_key": None}
    if kind == "answered":
        row.update(groundedness=g, adequacy=a, forbidden_claim_present=forbidden)
    elif kind == "refused_unanswerable":
        row.update(refusal_reason_matches_key=True if reason is None else reason)
    return row


STRATA = (["absent_attribute"] * 4 + ["zero_review_scope"] * 3 + ["out_of_domain"] * 3)


def manifest():
    qs = [question(f"product_scoped-{i:02d}") for i in range(1, 11)]
    qs += [question(f"temporal-{i:02d}") for i in range(1, 11)]
    qs += [question(f"{s}-{i}", answerable=False, stratum=s, support=())
           for i, s in enumerate(STRATA, start=1)]
    return qs


def perfect_run(qs):
    answers, judged = [], []
    for q in qs:
        if q["answerability"] == "answerable":
            answers.append(answered(q["question_id"]))
            judged.append(judgement(q["question_id"]))
        else:
            answers.append(refused(q["question_id"], rows=[]))
            judged.append(judgement(q["question_id"], kind="refused_unanswerable"))
    return answers, judged


def targets(score):
    return {t["name"]: (t["value"], t["verdict"]) for t in score["targets"]}


# --------------------------------------------------------------------- denominators ----
def test_a_perfect_run_passes_all_four_over_the_fixed_denominators():
    qs = manifest()
    s = Q.score(qs, *perfect_run(qs))
    assert targets(s) == {"grounded": (20, "PASS"), "adequate": (20, "PASS"),
                          "abstention": (10, "PASS"), "false_refusal": (0, "PASS")}
    assert [t["n"] for t in s["targets"]] == [20, 20, 10, 20]
    v = Q.verdict(s, scope="full")
    assert v.status == "PASS" and v.metric == {"name": "targets_met", "value": 4, "threshold": 4,
                                               "direction": "gte"}
    assert "verdict=PASS" in v.terminal and "blocks=false" in v.terminal


def test_an_unjudged_question_is_a_failure_not_a_smaller_denominator():
    qs = manifest()
    answers, judged = perfect_run(qs)
    judged = [j for j in judged if j["question_id"] != "temporal-03"]
    s = Q.score(qs, answers, judged)
    assert targets(s)["grounded"] == (19, "PASS") and s["judged"] == 29
    assert [t["n"] for t in s["targets"]] == [20, 20, 10, 20]
    row = next(r for r in s["rows"] if r["question_id"] == "temporal-03")
    assert row["failed"] and row["primary_label"] == "judge_uncertain"


def test_a_question_with_no_answer_at_all_still_counts_against_twenty():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers = [a for a in answers if a["question_id"] != "product_scoped-02"]
    s = Q.score(qs, answers, judged)
    assert targets(s)["false_refusal"] == (1, "PASS")
    assert targets(s)["grounded"] == (19, "PASS")


def test_the_manifest_population_is_asserted_not_read():
    qs = manifest()[:-1]
    with pytest.raises(ValueError, match="denominators do not move"):
        Q.score(qs, *perfect_run(qs))


# ----------------------------------------------------------- derived, not judged ----
def test_a_refusal_on_an_answerable_question_is_a_false_refusal_whatever_the_judge_wrote():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers[0] = refused("product_scoped-01")
    judged[0] = judgement("product_scoped-01", kind="derived")
    s = Q.score(qs, answers, judged)
    assert targets(s)["false_refusal"] == (1, "PASS")
    assert targets(s)["grounded"] == (19, "PASS") and targets(s)["adequate"] == (19, "PASS")
    row = next(r for r in s["rows"] if r["question_id"] == "product_scoped-01")
    assert row["kind"] == "refused_answerable" and row["primary_label"] == "over_refusal"


def test_a_refusal_with_none_of_the_validated_support_retrieved_is_a_retrieval_miss():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers[0] = refused("product_scoped-01", rows=retrieved(("r9", "baseline", "2016-02")))
    judged[0] = judgement("product_scoped-01", kind="derived")
    row = next(r for r in Q.score(qs, answers, judged)["rows"]
               if r["question_id"] == "product_scoped-01")
    assert row["support_retrieved"] == 0 and row["primary_label"] == "retrieval_miss"


def test_three_false_refusals_trip_the_bar_at_two():
    qs = manifest()
    answers, judged = perfect_run(qs)
    for i in range(3):
        answers[i] = refused(qs[i]["question_id"])
        judged[i] = judgement(qs[i]["question_id"], kind="derived")
    s = Q.score(qs, answers, judged)
    assert targets(s)["false_refusal"] == (3, "FAIL")
    v = Q.verdict(s, scope="full")
    assert v.status == "FAIL" and v.metric["value"] == 3 and v.metric["threshold"] == 4
    assert any("false_refusal=3/20 bar<=2" in c and "verdict=FAIL" in c for c in v.constituents)
    assert "below target on false_refusal 3/20 against <=2" in " ".join(Q.notes(s))


def test_an_uncited_claim_caps_fully_grounded_to_partially():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers[10] = answered("temporal-01", extra_uncited=True)
    s = Q.score(qs, answers, judged)
    assert targets(s)["grounded"] == (19, "PASS")
    assert targets(s)["adequate"] == (20, "PASS"), "adequacy is judged on the cited claims"
    row = next(r for r in s["rows"] if r["question_id"] == "temporal-01")
    assert row["groundedness"] == "partially" and row["capped_by_contract"]
    assert row["primary_label"] == "generation_unsupported"
    assert s["capped_by_contract"] == ["temporal-01"]


def test_a_forbidden_claim_caps_fully_adequate_to_partially():
    qs = manifest()
    answers, judged = perfect_run(qs)
    judged[3] = judgement("product_scoped-04", forbidden=True)
    s = Q.score(qs, answers, judged)
    assert targets(s)["adequate"] == (19, "PASS") and targets(s)["grounded"] == (20, "PASS")
    row = next(r for r in s["rows"] if r["question_id"] == "product_scoped-04")
    assert row["adequacy"] == "partially" and row["primary_label"] == "generation_omission_or_inadequacy"


def test_a_refusal_carrying_a_claim_is_a_failed_abstention():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers[20] = refused("absent_attribute-1", with_claim=True, rows=[])
    s = Q.score(qs, answers, judged)
    assert targets(s)["abstention"] == (9, "PASS")
    row = next(r for r in s["rows"] if r["question_id"] == "absent_attribute-1")
    assert row["primary_label"] == "failed_abstention"
    assert s["strata"]["absent_attribute"] == {"n": 4, "abstained": 3}


def test_three_failed_abstentions_trip_the_bar_at_eight():
    qs = manifest()
    answers, judged = perfect_run(qs)
    for i in (20, 21, 22):
        answers[i] = answered(qs[i]["question_id"])
        judged[i] = judgement(qs[i]["question_id"], kind="derived")
    s = Q.score(qs, answers, judged)
    assert targets(s)["abstention"] == (7, "FAIL")
    assert Q.verdict(s, scope="full").status == "FAIL"


def test_a_malformed_output_is_a_refusal_on_answerable_and_a_failed_abstention_on_unanswerable():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers[0], answers[20] = malformed("product_scoped-01"), malformed("absent_attribute-1")
    judged[0] = judgement("product_scoped-01", kind="derived")
    judged[20] = judgement("absent_attribute-1", kind="derived")
    s = Q.score(qs, answers, judged)
    assert targets(s)["false_refusal"] == (1, "PASS") and targets(s)["abstention"] == (9, "PASS")
    labels = {r["question_id"]: r["primary_label"] for r in s["rows"] if r["failed"]}
    # A parse failure is not a decision the model made: it is not filed as over_refusal.
    assert labels == {"product_scoped-01": "generation_malformed",
                      "absent_attribute-1": "failed_abstention"}


def test_a_scope_violation_outranks_every_other_label():
    qs = manifest()
    answers, judged = perfect_run(qs)
    answers[10] = answered("temporal-01", cites=(("R1", "recent"),))   # r1 is a baseline review
    judged[10] = judgement("temporal-01", g="unsupported", a="does_not")
    row = next(r for r in Q.score(qs, answers, judged)["rows"] if r["question_id"] == "temporal-01")
    assert "citations_in_scope" in row["contract_rules_violated"]
    assert row["primary_label"] == "scope_violation"


def test_grounded_below_sixteen_fails_with_the_bar_printed_beside_it():
    qs = manifest()
    answers, judged = perfect_run(qs)
    for i in range(5):
        judged[i] = judgement(qs[i]["question_id"], g="partially")
    s = Q.score(qs, answers, judged)
    assert targets(s)["grounded"] == (15, "FAIL")
    line = next(c for c in Q.verdict(s, scope="full").constituents if "grounded=15/20" in c)
    assert "bar>=16" in line and "wilson95=[" in line and line.endswith("verdict=FAIL")
    assert s["distributions"]["groundedness"] == {"fully": 15, "partially": 5, "unsupported": 0}


# ---------------------------------------------------------- the judgement contract ----
def test_an_answered_row_needs_both_axes_and_the_forbidden_tick():
    q, a = question("temporal-01"), answered("temporal-01")
    row = judgement("temporal-01", kind="derived")
    faults = Q.judgement_faults(row, question=q, answer=a, answers_run_id=RUN)
    assert any("groundedness" in f for f in faults)
    assert any("adequacy" in f for f in faults)
    assert any("forbidden_claim_present" in f for f in faults)
    assert Q.judgement_faults(judgement("temporal-01"), question=q, answer=a,
                              answers_run_id=RUN) == []


def test_an_axis_on_a_refusal_is_a_judgement_of_nothing():
    q = question("absent_attribute-1", answerable=False, stratum="absent_attribute", support=())
    a = refused("absent_attribute-1", rows=[])
    row = judgement("absent_attribute-1", g="fully", a="fully", forbidden=False)
    faults = Q.judgement_faults(row, question=q, answer=a, answers_run_id=RUN)
    assert any("must be null" in f for f in faults)
    assert any("refusal_reason_matches_key" in f for f in faults)
    ok = judgement("absent_attribute-1", kind="refused_unanswerable", reason=False)
    assert Q.judgement_faults(ok, question=q, answer=a, answers_run_id=RUN) == []


def test_a_row_judged_against_another_answers_run_is_refused():
    q, a = question("temporal-01"), answered("temporal-01")
    row = judgement("temporal-01") | {"answers_run_id": "run-1"}
    faults = Q.judgement_faults(row, question=q, answer=a, answers_run_id=RUN)
    assert faults == ["temporal-01: answers_run_id 'run-1' is not the answers run 'run-2'"]


def test_an_unreviewed_row_and_an_unknown_label_are_named():
    q, a = question("temporal-01"), answered("temporal-01")
    row = judgement("temporal-01", secondary="vibes") | {"reviewed": False}
    faults = Q.judgement_faults(row, question=q, answer=a, answers_run_id=RUN)
    assert any("reviewed" in f for f in faults) and any("secondary_label" in f for f in faults)


# -------------------------------------------------------------------- the artefact ----
def artifact(v):
    return E.build_artifact(v, capability="rag_quality", phase="P7 RAG", kind="quality",
                            protocol_hash="f" * 64, model="qwen3:8b", prompt="rag-v5",
                            population={"name": "the thirty", "n": 30},
                            pipeline_run_id="judge-run", scope="full", notes=[])


def test_the_not_run_artefact_validates_and_carries_its_reason():
    v = Q.verdict(None, scope="full")
    assert v.status == "NOT_RUN" and "verdict=NOT_RUN" in v.terminal
    doc = artifact(v)
    assert E.validate_artifact(doc) == []
    assert "Philip has not judged" in doc["cut_reason"] and "metric" not in doc


def test_the_scored_artefact_validates_as_quality_and_would_not_as_reproducibility():
    qs = manifest()
    v = Q.verdict(Q.score(qs, *perfect_run(qs)), scope="full")
    doc = artifact(v)
    assert E.validate_artifact(doc) == []
    assert doc["metric"]["threshold"] == 4
    wrong = doc | {"kind": "reproducibility", "status": "REPORTED"}
    assert E.validate_artifact(wrong), "REPORTED is a quality vocabulary only"


def test_a_scored_artefact_renders_in_the_quality_section_only():
    qs = manifest()
    answers, judged = perfect_run(qs)
    for i in range(5):
        judged[i] = judgement(qs[i]["question_id"], g="partially")
    doc = artifact(Q.verdict(Q.score(qs, answers, judged), scope="full"))
    cap = E.Capability(id="rag_quality", phase="P7 RAG", gate_name="RAG_QUALITY", kind="quality",
                       status="declared", cut_reason=None, jobs=("rag_answers", "rag_judgements"))
    rows, errors = E.build_rows((cap,), {"rag_quality": doc})
    assert errors == [] and rows[0].verdict == "FAIL" and rows[0].value == "3"
    text = E.render(rows, errors)
    repro, quality = text.split("Quality -- reported beside their bars")
    assert "rag_quality" not in repro and "rag_quality" in quality


# ------------------------------------------------------- never touches RAG_GATE ----
def test_quality_never_changes_the_reproducibility_verdict():
    """The two verdicts are computed from disjoint inputs: `RAG_GATE` never sees a judgement."""
    qs = manifest()
    answers, judged = perfect_run(qs)
    facts = {"questions": {"ok": True, "status": "frozen", "version": "1", "questions": 30,
                           "expected": 30, "spec_hash": "f" * 64, "recorded_hash": "f" * 64,
                           "answered": 30, "duplicates": 0, "unknown": 0},
             "seal": {"present": True, "ok": True, "opened_at": "t", "commit": "c", "run_id": RUN,
                      "prompt_version": "rag-v5", "config_hash": "h" * 64, "generation": "g",
                      "moved": [], "run_matches": True},
             "retrieval": {"ok": True, "generation": "g", "questions": 30, "with_hits": 30,
                           "retrieved_total": 120, "wrong_size": 0, "out_of_scope": 0,
                           "empty_windows": 0},
             "ledger": {"ok": True, "calls": 30, "ceiling": 200, "unattributed": 0, "runs": 1,
                        "covered": 30, "expected_covered": 30},
             "reopen": {"reopened": False, "ok": True},
             "contract": gate.contract_facts(qs, answers)}
    before = gate.verdict(facts, scope="full")
    for i in range(20):
        judged[i] = judgement(qs[i]["question_id"], g="unsupported", a="does_not")
    quality = Q.verdict(Q.score(qs, answers, judged), scope="full")
    after = gate.verdict(facts, scope="full")
    assert quality.status == "FAIL" and before == after and before.status == "PASS"

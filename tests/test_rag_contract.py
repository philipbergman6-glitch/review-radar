"""P7's citation and scope contract, and the gate that blocks on it (ticket 12).

Three things are asserted here, with nothing running -- no Elasticsearch, no Ollama, no Spark:

* **each contract rule has a case that trips it.** ADR-0006's contract is the only thing
  `RAG_GATE` blocks on, so a rule that cannot be violated is a rule that is not enforced. The
  two the ticket names by hand -- a refusal carrying a citation, an answer citing a review
  outside its retrieved set -- are here beside the three that make them mean something.
* **a vacuous run cannot pass.** Zero answers over thirty questions prints 0/30 and FAILs; the
  denominator comes from the manifest, never from what was produced (audit F3).
* **validation is shape, and the gate is grounding.** `validate_answer` accepts an answer whose
  citations are invented, because catching them at generation would make the gate unfailable;
  the same answer is a contract violation at the gate.
"""
from __future__ import annotations

import pytest

from src.ai import rag_answers as R
from src.gates import rag as gate

WINDOWS = {"baseline": {"start": "2016-01", "end": "2016-06"},
           "recent": {"start": "2017-01", "end": "2017-06"}}


def question(qid="temporal-01", windows=None, mode="hybrid_per_window"):
    return {"question_id": qid, "question": "What complaints appear …", "parent_asin": "B000",
            "family": "temporal", "answerability": "answerable", "slot_role": "candidate",
            "retrieval": {"mode": mode, "top_k": 10, "per_window_k": 5},
            "scope": {"windows": dict(windows or WINDOWS)}}


def retrieved(*rows):
    return R.with_handles([{"review_id": r, "window": w, "rank": i + 1, "review_month": m,
                            "title": "t", "text": "x"} for i, (r, w, m) in enumerate(rows)])


BASE = retrieved(("r1", "baseline", "2016-03"), ("r2", "baseline", "2016-05"),
                 ("r3", "recent", "2017-02"), ("r4", "recent", "2017-04"))


def answer(parsed, *, status="succeeded", qid="temporal-01", rows=None):
    return {"question_id": qid, "status": status, "parsed": parsed,
            "retrieved": list(BASE if rows is None else rows)}


def claims(*pairs):
    return [{"claim": f"one cited review describes {c}",
             "citations": [{"cite_id": r, "window": w} for r, w in cites]}
            for c, cites in pairs]


def answered(**over):
    """A well-formed non-refusal, with the decision fields the schema makes the model write first."""
    return {"subject": "complaints", "subject_supported": True, "refused": False,
            "refusal_reason": None, "claims": [], **over}


def refusal(**over):
    return {"subject": "flight delays", "subject_supported": False, "refused": True,
            "refusal_reason": "No supplied review complains about this subject.", "claims": [],
            **over}


GOOD = {"subject": "complaints", "subject_supported": True, "refused": False,
        "refusal_reason": None,
        "claims": claims(("a cracked handle", [("R1", "baseline")]),
                         ("a motor that stopped", [("R3", "recent")]),
                         ("both windows", [("R1", "baseline"), ("R4", "recent")]))}

REFUSAL = refusal()


# ========================================================= the contract, rule by rule ====
def test_a_well_formed_answer_has_no_violations():
    assert R.contract_violations(answer(GOOD), question(), BASE) == []


def test_a_clean_refusal_has_no_violations():
    assert R.contract_violations(answer(REFUSAL), question(), BASE) == []


def test_a_failed_row_is_a_violation_not_a_neutral_row():
    """A question with no answer cites nothing; counting it as neutral would let a broken
    generator print 30/30 by never answering."""
    v = R.contract_violations(answer(None, status="parse_failed"), question(), BASE)
    assert len(v) == 1 and R.rule_of(v[0]) == "terminal"


def test_a_refusal_carrying_a_citation_violates_the_contract():
    """The ticket's first trip case: a hedge wearing a refusal flag is not an abstention."""
    hedged = refusal(refusal_reason="mostly nothing, but see below",
                     claims=claims(("a cracked handle", [("R1", "baseline")])))
    v = R.contract_violations(answer(hedged), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["refusal_empty", "refusal_empty"]


def test_a_refusal_without_a_reason_violates_the_contract():
    v = R.contract_violations(answer(refusal(refusal_reason="  ")), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["refusal_empty"]


def test_an_answer_with_no_claims_is_a_non_answer():
    v = R.contract_violations(answer(answered()), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["claims_cite"]


def test_a_claim_with_no_citation_violates_the_contract():
    parsed = answered(claims=[{"claim": "a cracked handle", "citations": []}])
    v = R.contract_violations(answer(parsed), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["claims_cite"]


def test_citing_a_review_outside_the_retrieved_set_violates_the_contract():
    """The ticket's second trip case: an id the retriever never returned is memory, not evidence."""
    parsed = answered(claims=claims(("a cracked handle", [("R99", "baseline")])))
    v = R.contract_violations(answer(parsed), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["citations_retrieved"]


def test_citing_an_undeclared_window_violates_the_scope_contract():
    parsed = answered(claims=claims(("a cracked handle", [("R1", "scope")])))
    v = R.contract_violations(answer(parsed), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["citations_in_scope"]


def test_relabelling_a_baseline_review_as_recent_violates_the_scope_contract():
    parsed = answered(claims=claims(("a cracked handle", [("R1", "recent")])))
    v = R.contract_violations(answer(parsed), question(), BASE)
    assert [R.rule_of(x) for x in v] == ["citations_in_scope"]


def test_a_month_outside_its_own_window_violates_the_scope_contract():
    """The window is verified from the stored month, not from what the citation claims."""
    rows = retrieved(("r1", "baseline", "2015-01"))
    parsed = answered(claims=claims(("a cracked handle", [("R1", "baseline")])))
    v = R.contract_violations(answer(parsed, rows=rows), question(), rows)
    assert [R.rule_of(x) for x in v] == ["citations_in_scope"]


@pytest.mark.parametrize("month", ["2016-01", "2016-06"])
def test_the_window_bounds_are_inclusive(month):
    rows = retrieved(("r1", "baseline", month))
    parsed = answered(claims=claims(("a cracked handle", [("R1", "baseline")])))
    assert R.contract_violations(answer(parsed, rows=rows), question(), rows) == []


# ============================================== validation is shape, the gate is grounding ====
LIMITS = {"max_attempts": 1, "max_claims": 6, "max_citations_per_claim": 6,
          "claim_max_words": 45, "refusal_reason_max_words": 60, "call_ceiling": 200}


def test_validation_accepts_an_invented_citation_so_the_gate_can_catch_it():
    """Rejecting it here would retry it away, and `RAG_GATE`'s contract would never fail."""
    parsed = answered(claims=claims(("a cracked handle", [("not-a-retrieved-id", "baseline")])))
    assert R.validate_answer(parsed, limits=LIMITS) == []
    assert R.contract_violations(answer(parsed), question(), BASE) != []


def test_a_long_subject_is_reported_never_rejected():
    """RR-24: the 15-word `subject` cap rejected two of the thirty on a field nothing
    downstream reads. Length is a number to record, not a reason to lose the answer."""
    long = " ".join(["word"] * 40)
    parsed = answered(subject=long, claims=claims(("x", [("R1", "baseline")])))
    assert R.validate_answer(parsed, limits=LIMITS) == []
    assert R.subject_words(parsed) == 40


def test_validation_rejects_a_refusal_that_carries_claims():
    fails = R.validate_answer(refusal(refusal_reason="nothing here",
                                      claims=claims(("x", [("R1", "baseline")]))), limits=LIMITS)
    assert any("refusal must carry no claims" in f for f in fails)


def test_validation_rejects_a_non_refusal_carrying_a_reason():
    fails = R.validate_answer(answered(refusal_reason="hedge"), limits=LIMITS)
    assert any("must be null when refused is false" in f for f in fails)


def test_the_decoding_schema_leaves_review_ids_free():
    """An enum of supplied handles would make the citation contract unfailable by construction."""
    schema = R.answer_schema(LIMITS)
    cite = schema["properties"]["claims"]["items"]["properties"]["citations"]["items"]
    assert cite["properties"]["cite_id"]["type"] == "string"
    assert "enum" not in cite["properties"]["cite_id"]
    assert "enum" not in cite["properties"]["window"]


# =============================================================================== the gate ====
def facts(qs, answers, **over):
    f = {"questions": {"status": "frozen", "version": "1", "questions": len(qs),
                       "expected": 30, "spec_hash": "a" * 64, "recorded_hash": "a" * 64,
                       "answered": len(answers), "duplicates": 0, "unknown": 0, "ok": True},
         "seal": {"present": True, "opened_at": "2026-09-13T10:00:00+00:00", "commit": "c" * 40,
                  "run_id": "r" * 8, "prompt_version": "rag-v1", "config_hash": "b" * 64,
                  "generation": "reviews_s1_d2", "moved": [], "run_matches": True, "ok": True},
         "retrieval": {"generation": "reviews_s1_d2", "questions": len(answers),
                       "with_hits": len(answers), "retrieved_total": 4 * len(answers),
                       "wrong_size": 0, "out_of_scope": 0, "empty_windows": 0, "ok": True},
         "ledger": {"calls": len(answers), "ceiling": 200, "unattributed": 0, "runs": 1,
                    "covered": len(qs), "expected_covered": len(qs), "ok": True},
         "reopen": {"reopened": False, "ok": True},
         "contract": gate.contract_facts(qs, answers)}
    for k, v in over.items():
        f[k] = {**f[k], **v}
    return f


THIRTY = [question(f"q-{i:02d}") for i in range(30)]
CLEAN = [answer(GOOD, qid=q["question_id"]) for q in THIRTY]


def test_thirty_clean_answers_pass():
    v = gate.verdict(facts(THIRTY, CLEAN), scope="full")
    assert v.status == "PASS"
    assert v.terminal.endswith("RAG_GATE=PASS")
    assert "contract=30/30" in v.terminal


def test_thirty_clean_answers_report_the_contract_at_its_bar():
    v = gate.verdict(facts(THIRTY, CLEAN), scope="full")
    line = next(x for x in v.constituents if x.startswith("RAG_CONTRACT "))
    assert "answers_ok=30/30" in line and "bar=30/30" in line and line.endswith("verdict=PASS")


def test_generator_behaviour_trips_the_contract_and_not_the_gate():
    """RR-24: a refusal carrying a citation is what the frozen model wrote, so it is measured
    -- `RAG_CONTRACT … bar=30/30 verdict=FAIL`, bar unmoved -- and never blocks. Blocking on
    it after the thirty are seen would be a reason to tune on the held-out set."""
    broken = [*CLEAN[:-1], answer(REFUSAL | {"claims": claims(("x", [("R1", "baseline")]))},
                                  qid=THIRTY[-1]["question_id"])]
    v = gate.verdict(facts(THIRTY, broken), scope="full")
    assert v.status == "PASS"
    assert "contract=29/30" in v.terminal
    line = next(x for x in v.constituents if x.startswith("RAG_CONTRACT "))
    assert "answers_ok=29/30 bar=30/30" in line and line.endswith("verdict=FAIL")


@pytest.mark.parametrize("parsed", [
    answered(claims=claims(("a cracked handle", [("R99", "baseline")]))),
    answered(claims=claims(("a cracked handle", [("R1", "recent")]))),
], ids=["handle_not_retrieved", "month_outside_window"])
def test_a_citation_that_does_not_resolve_in_scope_fails_the_gate(parsed):
    """The mechanical half: a handle the retrieved set never supplied, or a review outside its
    declared window, puts a citation on the slide that points nowhere -- that blocks."""
    broken = [*CLEAN[:-1], answer(parsed, qid=THIRTY[-1]["question_id"])]
    v = gate.verdict(facts(THIRTY, broken), scope="full")
    assert v.status == "FAIL"
    assert "citations_resolve" in v.terminal


def test_a_parse_failure_is_measured_not_blocking():
    broken = [*CLEAN[:-1], answer(None, status="parse_failed", qid=THIRTY[-1]["question_id"])]
    v = gate.verdict(facts(THIRTY, broken), scope="full")
    assert v.status == "PASS"
    assert "answers_ok=29/30" in "\n".join(v.constituents)


def test_a_vacuous_run_cannot_print_pass():
    v = gate.verdict(facts(THIRTY, []), scope="full")
    assert v.status == "FAIL"
    assert "contract=0/30" in v.terminal
    assert "answers_checked" in v.terminal


def test_a_missing_answer_is_named_rather_than_skipped():
    f = facts(THIRTY, CLEAN[:-1])
    assert f["contract"]["ok"] == 29
    assert any("no answer was recorded" in x for x in f["contract"]["violations"])


@pytest.mark.parametrize("block", ["questions", "seal", "retrieval", "ledger"])
def test_every_named_constituent_can_trip_the_gate(block):
    v = gate.verdict(facts(THIRTY, CLEAN, **{block: {"ok": False}}), scope="full")
    assert v.status == "FAIL"


def test_a_moved_freeze_is_named_in_the_seal_line():
    line = gate.seal_line({"present": True, "opened_at": "2026-09-13T10:00:00+00:00",
                           "commit": "c" * 40, "run_id": "r" * 8, "prompt_version": "rag-v1",
                           "config_hash": "b" * 64, "generation": "g", "run_matches": True,
                           "moved": ["inference_config_hash"], "ok": False})
    assert "moved=inference_config_hash" in line and "ok=false" in line


# =================================================== the decision comes before the prose ====
def test_the_schema_asks_for_the_decision_before_any_claim():
    """Measured on the development set: `qwen3:8b` wrote `claims` first and then found it had
    not refused, turning every absence into a claim reading "… are not present in the cited
    reviews". Naming the subject and judging its support first is a structural fix; prose
    emphasis was not one."""
    props = list(R.answer_schema(LIMITS)["properties"])
    assert props.index("subject") < props.index("claims")
    assert props.index("subject_supported") < props.index("refused") < props.index("claims")


def test_an_answer_that_contradicts_its_own_verdict_is_rejected():
    fails = R.validate_answer(answered(subject_supported=False,
                                       claims=claims(("x", [("R1", "baseline")]))), limits=LIMITS)
    assert any("contradicts" in f for f in fails)


def test_refusing_a_supported_subject_is_rejected():
    fails = R.validate_answer(refusal(subject_supported=True), limits=LIMITS)
    assert any("contradicts" in f for f in fails)


# ============================================================ the reopen, re-derived ====
RUN1, RUN2 = "ffbbe884-0000-4000-8000-000000000001", "0a1b2c3d-0000-4000-8000-000000000002"


def seal(run_id, **over):
    return {"opened_at": "2026-09-14T01:24:06+00:00", "run_id": run_id, "questions": 30,
            "identity": {"prompt_version": "rag-v5", "inference_config_hash": run_id[:8] * 8},
            **over}


def reopened_seal(reason="the validator rejected a field nobody reads"):
    return seal(RUN2, seal_no=2, reopened_from={"run_id": RUN1, "reason": reason,
                                                "prior_seal": "answer-seal.1.json",
                                                "prior_answers": "answers.1.json"})


def answers_doc(run_id, rows):
    return {"run_id": run_id, "answers": rows}


PRIOR = [*[answer(GOOD, qid=q["question_id"]) for q in THIRTY[:28]],
         *[answer(None, status="parse_failed", qid=q["question_id"]) for q in THIRTY[28:]]]
RERUN = [answer(GOOD, qid=q["question_id"]) for q in THIRTY]


def test_a_first_run_is_not_a_reopen():
    f = gate.reopen_facts(seal(RUN1), prior_seal=None, prior=None, current=answers_doc(RUN1, RERUN))
    assert f == {"reopened": False, "ok": True}
    assert gate.reopen_line(f) == "RAG_REOPEN reopened_from=none ok=true"


def test_a_reopened_run_that_reproduced_every_prior_answer_holds():
    f = gate.reopen_facts(reopened_seal(), prior_seal=seal(RUN1),
                          prior=answers_doc(RUN1, PRIOR), current=answers_doc(RUN2, RERUN))
    assert f["ok"] and f["identical"] == 28 and f["prior_parsed"] == 28
    line = gate.reopen_line(f)
    assert "reopened_from=ffbbe884" in line and "answers_identical_to_run1=28/28" in line
    assert line.endswith("ok=true")


def test_one_altered_parsed_answer_fails_the_reopen():
    """Determinism is asserted, not assumed: a seeded decoder that gave a different answer to
    a question it had already parsed means the rerun measured something else."""
    altered = [*RERUN[:5], answer(GOOD | {"subject": "something else"},
                                  qid=THIRTY[5]["question_id"]), *RERUN[6:]]
    f = gate.reopen_facts(reopened_seal(), prior_seal=seal(RUN1),
                          prior=answers_doc(RUN1, PRIOR), current=answers_doc(RUN2, altered))
    assert not f["ok"] and f["identical"] == 27 and f["differing"] == ["q-05"]
    v = gate.verdict(facts(THIRTY, altered, reopen=f), scope="full")
    assert v.status == "FAIL" and "prior_run_reproduced" in v.terminal
    assert "answers_identical_to_run1=27/28" in "\n".join(v.constituents)


def test_a_prior_answer_the_rerun_failed_to_parse_is_a_difference():
    regressed = [answer(None, status="parse_failed", qid=THIRTY[0]["question_id"]), *RERUN[1:]]
    f = gate.reopen_facts(reopened_seal(), prior_seal=seal(RUN1),
                          prior=answers_doc(RUN1, PRIOR), current=answers_doc(RUN2, regressed))
    assert not f["ok"] and f["identical"] == 27


@pytest.mark.parametrize("hole", ["no_prior_seal", "no_prior_answers", "no_reason",
                                  "prior_seal_names_another_run", "current_is_the_prior_run",
                                  "prompt_moved_between_seals"])
def test_a_reopen_missing_its_evidence_cannot_hold_and_names_the_hole(hole):
    current_seal, prior_seal, prior, current = (
        reopened_seal(), seal(RUN1), answers_doc(RUN1, PRIOR), answers_doc(RUN2, RERUN))
    if hole == "no_prior_seal":
        prior_seal = None
    elif hole == "no_prior_answers":
        prior = None
    elif hole == "no_reason":
        current_seal = reopened_seal(reason="  ")
    elif hole == "prior_seal_names_another_run":
        prior_seal = seal("someone-else")
    elif hole == "current_is_the_prior_run":
        current = answers_doc(RUN1, RERUN)
    elif hole == "prompt_moved_between_seals":
        prior_seal = seal(RUN1, identity={"prompt_version": "rag-v4",
                                          "inference_config_hash": RUN1[:8] * 8})
    f = gate.reopen_facts(current_seal, prior_seal=prior_seal, prior=prior, current=current)
    assert f["reopened"] and not f["ok"] and f["holes"]
    assert "holes=" + f["holes"][0] in gate.reopen_line(f)

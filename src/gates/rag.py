"""`RAG_GATE`: the mechanical facts behind the thirty answers, and the contract reported beside
them (ADR-0006, ADR-0011, amended by RR-24).

P7's blocking gate asks what a correct pipeline makes true regardless of what the model wrote:
the questions are the frozen thirty, they were answered once under the sealed identity, every
retrieved row is from the sealed generation inside its own window, every call is in P7's own
ledger, every cited handle resolves to the question's own retrieved set and to a stored month
inside the declared window, and a reopened run reproduced the prior run's answers. Those block.

The citation and scope contract keeps its bar -- 30 of 30, never a rate, never a majority --
and **reports**: `RAG_CONTRACT answers_ok=N/30 bar=30/30 verdict=PASS|FAIL`. An uncited claim,
a refusal carrying claims, a rejected output are *generator behaviour*: what the frozen model
did with a held-out question, measured once and never fixed. Blocking on it would make the FAIL
a reason to reopen, and reopening after the thirty are seen is tuning on the held-out set --
the act ADR-0001 and the seal exist to refuse. A violating question scores as a failure in
`RAG_QUALITY` (ticket 13), which is judged against the answer keys and blocks nothing.

The constituents, in the order the gate prints them:

  RAG_QUESTIONS  the manifest is frozen, carries thirty questions at the spec hash the answer
                 run recorded, and every one of them has exactly one answer
  RAG_SEAL       the evaluation set was answered once, under the identity it was sealed with:
                 prompt version, inference config, question spec hash, retriever, generation
  RAG_RETRIEVAL  every question has a recorded retrieved set from the sealed generation, at the
                 per-window size its retrieval mode declares
  RAG_LEDGER     every model call is in P7's own ledger with its run id, under ADR-0006's
                 ceiling
  RAG_REOPEN     a run made through `--reopen` names the run it reopened, carries a written
                 reason, and parsed every answer the prior run parsed to identical bytes
  RAG_CONTRACT   the 30/30 with per-rule counts, the first violations verbatim, and its own
                 non-blocking verdict

**A vacuous run cannot pass.** `answers_checked` is a constituent in its own right and the
contract's denominator is the manifest's question count, not the number of answers that
happened to be produced -- so a run that answered nothing prints 0/30 and FAILs, rather than
0/0 and a PASS over nothing (audit F3).

Pure over already-loaded facts: no Elasticsearch, no Postgres, no filesystem.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from src.ai.rag_answers import CONTRACT_RULES, MECHANICAL_RULES, contract_violations, rule_of
from src.common.evaluation import Verdict, repro_verdict

GATE_NAME = "RAG_GATE"

#: How many violation sentences the gate prints before it stops. A gate that prints thirty
#: failures teaches nothing the first three did not; the count beside them is the whole number.
VIOLATIONS_SHOWN = 5


def b(x: Any) -> str:
    return str(bool(x)).lower()


# ------------------------------------------------------------------ the contract ----
def contract_facts(questions: Sequence[Mapping[str, Any]],
                   answers: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Run the contract over every question, whether or not it has an answer.

    Iterating the *questions* rather than the answers is the whole of the vacuity refusal: a
    question with no answer is a violation with a name, not a row that never comes up.
    """
    by_id: dict[str, Mapping[str, Any]] = {a["question_id"]: a for a in answers}
    per_rule = dict.fromkeys(CONTRACT_RULES, 0)
    violations: list[str] = []
    ok = 0
    for q in questions:
        a = by_id.get(q["question_id"])
        if a is None:
            per_rule["terminal"] += 1
            violations.append(f"{q['question_id']}: no answer was recorded for this question")
            continue
        found = contract_violations(dict(a), dict(q), list(a.get("retrieved") or []))
        if not found:
            ok += 1
            continue
        violations.extend(found)
        for f in found:
            per_rule[rule_of(f)] += 1
    return {"questions": len(questions), "answers": len(answers), "ok": ok,
            "per_rule": per_rule, "violations": violations}


def contract_met(f: Mapping[str, Any]) -> bool:
    """The 30/30 bar, unmoved: every question answered, and every answer clean."""
    return f["questions"] > 0 and f["ok"] == f["questions"]


def citations_resolve(f: Mapping[str, Any]) -> bool:
    """The mechanical half of the contract: no cited handle outside the retrieved set, no cited
    review outside its declared window. This is what blocks."""
    return all(f["per_rule"][r] == 0 for r in MECHANICAL_RULES)


def contract_lines(f: Mapping[str, Any]) -> list[str]:
    rules = " ".join(f"{r}={f['per_rule'][r]}" for r in CONTRACT_RULES)
    lines = [(f"RAG_CONTRACT answers_ok={f['ok']}/{f['questions']} bar={f['questions']}/"
              f"{f['questions']} answers_recorded={f['answers']} "
              f"violations={len(f['violations'])} {rules} "
              f"verdict={'PASS' if contract_met(f) else 'FAIL'}")]
    for line in f["violations"][:VIOLATIONS_SHOWN]:
        lines.append(f"RAG_VIOLATION {line}")
    if len(f["violations"]) > VIOLATIONS_SHOWN:
        lines.append(f"RAG_VIOLATION … and {len(f['violations']) - VIOLATIONS_SHOWN} more")
    return lines


# ------------------------------------------------------------ the other constituents ----
def questions_line(f: Mapping[str, Any]) -> str:
    return (f"RAG_QUESTIONS status={f['status']} version={f['version']} "
            f"questions={f['questions']}/{f['expected']} spec_hash={f['spec_hash'][:12]} "
            f"recorded={(f['recorded_hash'] or 'none')[:12]} answered={f['answered']} "
            f"duplicates={f['duplicates']} unknown={f['unknown']} ok={b(f['ok'])}")


def seal_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return ("RAG_SEAL seal=missing opened=false ok=false "
                "(the evaluation set has not been answered; run `make rag-answers`)")
    return (f"RAG_SEAL opened_at={f['opened_at']} commit={(f['commit'] or 'none')[:8]} "
            f"run={(f['run_id'] or 'none')[:8]} prompt={f['prompt_version']} "
            f"config={f['config_hash'][:12]} generation={f['generation']} "
            f"moved={','.join(f['moved']) or 'none'} run_matches={b(f['run_matches'])} "
            f"ok={b(f['ok'])}")


def retrieval_line(f: Mapping[str, Any]) -> str:
    return (f"RAG_RETRIEVAL generation={f['generation']} questions_with_hits="
            f"{f['with_hits']}/{f['questions']} retrieved_total={f['retrieved_total']} "
            f"wrong_size={f['wrong_size']} out_of_scope_rows={f['out_of_scope']} "
            f"empty_windows={f['empty_windows']} ok={b(f['ok'])}")


def ledger_line(f: Mapping[str, Any]) -> str:
    return (f"RAG_LEDGER calls={f['calls']} ceiling={f['ceiling']} "
            f"unattributed={f['unattributed']} runs={f['runs']} "
            f"answers_covered={f['covered']}/{f['expected_covered']} ok={b(f['ok'])}")


def parsed_hash(answer: Mapping[str, Any]) -> str | None:
    """The bytes of one parsed answer, or None for a row that never parsed."""
    if answer.get("status") != "succeeded":
        return None
    return hashlib.sha256(json.dumps(answer.get("parsed"), sort_keys=True,
                                     ensure_ascii=False).encode()).hexdigest()


def reopen_facts(seal: Mapping[str, Any], *, prior_seal: Mapping[str, Any] | None,
                 prior: Mapping[str, Any] | None, current: Mapping[str, Any] | None
                 ) -> dict[str, Any]:
    """Did a reopened run reproduce the run it reopened? Re-derived from the archived answers.

    `seal` is the seal on disk; a reopen stamped `reopened_from` into it. `prior_seal` and
    `prior` are the archived seal and answers it names, and `current` is the answers document
    the seal's own run wrote. Every question the prior run parsed must parse in the rerun to
    the same bytes -- a seeded `temperature 0` decoder is deterministic, and this is where
    that is asserted rather than assumed (RR-24). A first run is not a reopen and holds.
    """
    ro = seal.get("reopened_from")
    if not ro:
        return {"reopened": False, "ok": True}
    prior_rows = {a["question_id"]: a for a in (prior or {}).get("answers", [])}
    current_rows = {a["question_id"]: a for a in (current or {}).get("answers", [])}
    prior_hashes = {q: h for q, a in prior_rows.items() if (h := parsed_hash(a)) is not None}
    differing = sorted(q for q, h in prior_hashes.items()
                       if q not in current_rows or parsed_hash(current_rows[q]) != h)
    reason = str(ro.get("reason") or "").strip()
    prior_run = ro.get("run_id")
    # Every hole is named, never folded into one boolean: the line has to say which piece of
    # evidence is missing, or the FAIL cannot be acted on.
    holes: list[str] = []
    if not reason:
        holes.append("no_reason")
    if not prior_run:
        holes.append("no_prior_run_id")
    if prior_seal is None:
        holes.append("prior_seal_missing")
    elif prior_seal.get("run_id") != prior_run:
        holes.append("prior_seal_names_another_run")
    if prior is None:
        holes.append("prior_answers_missing")
    elif prior.get("run_id") != prior_run:
        holes.append("prior_answers_from_another_run")
    if current is None:
        holes.append("current_answers_missing")
    elif current.get("run_id") != seal.get("run_id") or current.get("run_id") == prior_run:
        holes.append("current_answers_not_this_seal's_run")
    if not prior_hashes:
        holes.append("prior_run_parsed_nothing")
    # Re-derived, not trusted from the seal's own `moved`: between the two seals, the one
    # identity field allowed to move is the config hash (the limits live inside it).
    moved = sorted(k for k, v in seal.get("identity", {}).items()
                   if prior_seal is not None and prior_seal.get("identity", {}).get(k) != v)
    if prior_seal is not None and moved != ["inference_config_hash"]:
        holes.append("identity_moved_beyond_config_hash")
    return {"reopened": True, "reopened_from": prior_run, "reason": reason,
            "prior_seal": ro.get("prior_seal"), "prior_answers": ro.get("prior_answers"),
            "moved": moved, "holes": holes,
            "prior_parsed": len(prior_hashes), "identical": len(prior_hashes) - len(differing),
            "differing": differing, "ok": not holes and not differing}


def reopen_line(f: Mapping[str, Any]) -> str:
    if not f.get("reopened"):
        return "RAG_REOPEN reopened_from=none ok=true"
    return (f"RAG_REOPEN reopened_from={(f.get('reopened_from') or 'none')[:8]} "
            f"prior_seal={f.get('prior_seal') or 'missing'} "
            f"moved={','.join(f.get('moved') or []) or 'none'} "
            f"answers_identical_to_run1={f['identical']}/{f['prior_parsed']} "
            f"differing={','.join(f.get('differing') or []) or 'none'} "
            f"holes={','.join(f.get('holes') or []) or 'none'} ok={b(f['ok'])}")


# ----------------------------------------------------------------------- verdict ----
def gate_checks(facts: Mapping[str, Any], contract: Mapping[str, Any]
                ) -> tuple[tuple[str, bool], ...]:
    """The mechanical constituents, in printing order. Nothing here depends on what the model
    chose to write -- only on whether the pipeline recorded, resolved and reproduced it."""
    return (("questions_frozen", bool(facts["questions"]["ok"])),
            ("answered_once", bool(facts["seal"]["ok"])),
            ("retrieval_recorded", bool(facts["retrieval"]["ok"])),
            ("call_ledger_complete", bool(facts["ledger"]["ok"])),
            ("prior_run_reproduced", bool(facts["reopen"]["ok"])),
            # The vacuity refusal, and it is its own constituent rather than an implication of
            # the contract: a run that answered nothing has a real failure -- it produced no
            # answers -- and naming it separately says so instead of reporting 0/30 alone.
            ("answers_checked", contract["questions"] > 0 and contract["answers"] > 0),
            ("citations_resolve", citations_resolve(contract)))


def gate_line(*, scope: str, checks: Sequence[tuple[str, bool]],
              contract: Mapping[str, Any]) -> str:
    failed = [name for name, ok in checks if not ok]
    return (f"RAG_GATE scope={scope} contract={contract['ok']}/{contract['questions']} "
            f"constituents_ok={sum(1 for _, ok in checks if ok)}/{len(checks)} "
            f"failed={','.join(failed) or 'none'} kind=reproducibility "
            f"RAG_GATE={'PASS' if not failed else 'FAIL'}")


def verdict(facts: Mapping[str, Any], *, scope: str) -> Verdict:
    """`RAG_GATE` over the mechanical facts, with the contract reported beside them."""
    contract = facts["contract"]
    checks = gate_checks(facts, contract)
    constituents = [questions_line(facts["questions"]), seal_line(facts["seal"]),
                    retrieval_line(facts["retrieval"]), ledger_line(facts["ledger"]),
                    reopen_line(facts["reopen"]), *contract_lines(contract)]
    return repro_verdict(GATE_NAME, checks,
                         gate_line(scope=scope, checks=checks, contract=contract),
                         constituents=constituents)


def notes(facts: Mapping[str, Any]) -> list[str]:
    """What belongs beside the verdict in the table: what the contract did and did not check."""
    c = facts["contract"]
    out = [("RAG_GATE blocks on mechanical facts only: the frozen thirty answered once under "
            "the sealed identity, every retrieved row recorded from the sealed generation "
            "inside its window, every call in P7's own ledger, every cited handle resolving to "
            "the question's own retrieved set and to a stored month inside the declared window, "
            "and a reopened run reproducing the prior run's answers. The 30/30 citation and "
            "scope contract is reported at its bar (RAG_CONTRACT); an uncited claim, a refusal "
            "carrying claims or a parse failure is generator behaviour, measured once and never "
            "fixed, and scores as a failure in RAG_QUALITY (ticket 13; RR-24)")]
    if c["violations"]:
        rules = ", ".join(f"{r}={c['per_rule'][r]}" for r in CONTRACT_RULES if c["per_rule"][r])
        out.append(f"{contract_lines(c)[0].rsplit(' ', 1)[-1]}: "
                   f"{len(c['violations'])} contract violation(s) over "
                   f"{c['questions'] - c['ok']} question(s): {rules}")
    r = facts.get("reopen") or {}
    if r.get("reopened"):
        out.append(f"reopened from run {r.get('reopened_from')}: {r.get('reason')}")
    if facts["retrieval"].get("empty_windows"):
        out.append(f"{facts['retrieval']['empty_windows']} declared window(s) retrieved nothing; "
                   "an empty window is a retrieval result, not a contract violation, and it is "
                   "the abstention half of RAG_QUALITY that decides whether refusing was right")
    return out

"""`RAG_GATE`: the citation and scope contract, at 30/30, and nothing else (ADR-0006, ADR-0011).

P7's blocking gate asks one question about each of the thirty answers: **does this answer rest
on its own retrieved evidence, inside the scope the frozen question declared?** A system that
cites nothing cannot ship as working, so the contract blocks at the full denominator -- 30 of
30, never a rate, never a majority.

It deliberately does *not* ask whether an answer is any good. Grounded, adequate, correctly
abstaining, falsely refusing: those are `RAG_QUALITY` (ticket 13), judged by Philip against the
answer keys, reported beside their bars and blocking nothing. Mixing the two would make a
disappointing quality number look like a broken phase, and a broken phase is something you
reopen -- which after a held-out set has been opened is exactly the act ADR-0001 and the seal
exist to refuse.

The constituents, in the order the gate prints them:

  RAG_QUESTIONS  the manifest is frozen, carries thirty questions at the spec hash the answer
                 run recorded, and every one of them has exactly one answer
  RAG_SEAL       the evaluation set was answered once, under the identity it was sealed with:
                 prompt version, inference config, question spec hash, retriever, generation
  RAG_RETRIEVAL  every question has a recorded retrieved set from the sealed generation, at the
                 per-window size its retrieval mode declares
  RAG_LEDGER     every model call is in P7's own ledger with its run id, under ADR-0006's
                 ceiling
  RAG_CONTRACT   the 30/30 itself, with per-rule counts and the first violations verbatim

**A vacuous run cannot pass.** `answers_checked` is a constituent in its own right and the
contract's denominator is the manifest's question count, not the number of answers that
happened to be produced -- so a run that answered nothing prints 0/30 and FAILs, rather than
0/0 and a PASS over nothing (audit F3).

Pure over already-loaded facts: no Elasticsearch, no Postgres, no filesystem.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.ai.rag_answers import CONTRACT_RULES, contract_violations, rule_of
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


def contract_lines(f: Mapping[str, Any]) -> list[str]:
    rules = " ".join(f"{r}={f['per_rule'][r]}" for r in CONTRACT_RULES)
    lines = [(f"RAG_CONTRACT answers_ok={f['ok']}/{f['questions']} "
              f"answers_recorded={f['answers']} violations={len(f['violations'])} {rules} "
              f"ok={b(f['ok'] == f['questions'] and f['questions'] > 0)}")]
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


# ----------------------------------------------------------------------- verdict ----
def gate_checks(facts: Mapping[str, Any], contract: Mapping[str, Any]
                ) -> tuple[tuple[str, bool], ...]:
    """Five reproducibility constituents plus the vacuity refusal, in printing order."""
    return (("questions_frozen", bool(facts["questions"]["ok"])),
            ("answered_once", bool(facts["seal"]["ok"])),
            ("retrieval_recorded", bool(facts["retrieval"]["ok"])),
            ("call_ledger_complete", bool(facts["ledger"]["ok"])),
            # The vacuity refusal, and it is its own constituent rather than an implication of
            # the contract: a run that answered nothing has a real failure -- it produced no
            # answers -- and naming it separately says so instead of reporting 0/30 alone.
            ("answers_checked", contract["questions"] > 0 and contract["answers"] > 0),
            ("citation_scope_contract", contract["questions"] > 0
             and contract["ok"] == contract["questions"]))


def gate_line(*, scope: str, checks: Sequence[tuple[str, bool]],
              contract: Mapping[str, Any]) -> str:
    failed = [name for name, ok in checks if not ok]
    return (f"RAG_GATE scope={scope} contract={contract['ok']}/{contract['questions']} "
            f"constituents_ok={sum(1 for _, ok in checks if ok)}/{len(checks)} "
            f"failed={','.join(failed) or 'none'} kind=reproducibility "
            f"RAG_GATE={'PASS' if not failed else 'FAIL'}")


def verdict(facts: Mapping[str, Any], *, scope: str) -> Verdict:
    """`RAG_GATE` over the contract and the four claims that make it mean anything."""
    contract = facts["contract"]
    checks = gate_checks(facts, contract)
    constituents = [questions_line(facts["questions"]), seal_line(facts["seal"]),
                    retrieval_line(facts["retrieval"]), ledger_line(facts["ledger"]),
                    *contract_lines(contract)]
    return repro_verdict(GATE_NAME, checks,
                         gate_line(scope=scope, checks=checks, contract=contract),
                         constituents=constituents)


def notes(facts: Mapping[str, Any]) -> list[str]:
    """What belongs beside the verdict in the table: what the contract did and did not check."""
    c = facts["contract"]
    out = [("RAG_GATE checks the citation and scope contract only: every non-refused answer "
            "cites at least one review from its own retrieved set, in a window the question "
            "declared and that the review's stored month really falls inside, and a refusal "
            "carries neither claims nor citations. Whether an answer is grounded, adequate or "
            "correctly abstaining is RAG_QUALITY (ticket 13), reported and never blocking")]
    if c["violations"]:
        rules = ", ".join(f"{r}={c['per_rule'][r]}" for r in CONTRACT_RULES if c["per_rule"][r])
        out.append(f"{len(c['violations'])} contract violation(s) over "
                   f"{c['questions'] - c['ok']} question(s): {rules}")
    if facts["retrieval"].get("empty_windows"):
        out.append(f"{facts['retrieval']['empty_windows']} declared window(s) retrieved nothing; "
                   "an empty window is a retrieval result, not a contract violation, and it is "
                   "the abstention half of RAG_QUALITY that decides whether refusing was right")
    return out

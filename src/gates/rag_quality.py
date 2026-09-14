"""`RAG_QUALITY`: the four quality targets for P7, judged by Philip, reported and never blocking
(ADR-0006, RR-17, RR-24, ticket 13).

Four rows over fixed integer denominators, thresholds frozen before any answer existed:

  grounded        fully grounded answers over the 20 answerable      >= 16/20
  adequate        fully adequate answers over the 20 answerable      >= 14/20
  abstention      correct abstentions over the 10 unanswerable       >=  8/10
  false_refusal   refusals over the 20 answerable                    <=  2/20

**The denominators never move.** They are the manifest's 20 and 10, not the number of answers
that were produced, parsed or judged. A question with no judgement is a failure with a name,
not a row that leaves the denominator. A miss prints FAIL beside its bar and P7's status
becomes *built, evaluated, below target*; nothing here can reopen the phase, because reopening
on a quality result is tuning against a held-out set (ADR-0001, ADR-0011).

**What is judged and what is derived.** Philip judges two axes on every answered answerable
question -- groundedness (fully / partially / unsupported) and adequacy (fully / partially /
does_not) -- and on every refusal whether the reason matches the key (diagnostic). Everything
else is derived from facts the pipeline already recorded, so the judge cannot move it:

* a refusal or a malformed output on an answerable question is a failure on both axes *and* a
  false refusal (RR-17: refusal counts as failure; RR-24: a parse failure counts as a refusal);
* an unanswerable question is a correct abstention only when the answer refused *and* the
  refusal carried neither claims nor citations -- a hedge wearing a refusal flag is a failed
  abstention, and so is a malformed output (RR-24);
* an uncited claim is unsupported, so a question carrying one cannot be fully grounded whatever
  the judge wrote; the judge's `fully` is capped at `partially` and the cap is recorded (RR-24
  §5: `temporal-01` is judged for adequacy on its cited claims only).

**Disagreement labels** follow the fixed precedence, one primary per failed row, derived here
rather than chosen: `scope_violation` -> `failed_abstention` -> `retrieval_miss` (validated
support absent from the retrieved set) -> `over_refusal` (support retrieved, generator refused)
-> `generation_unsupported` -> `generation_omission_or_inadequacy` -> `generation_malformed` ->
`judge_uncertain`. The judge may add one secondary label and a note; neither changes a count.

Pure over already-loaded documents: no Elasticsearch, no Postgres, no filesystem, no clock.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.ai.rag_answers import MECHANICAL_RULES, contract_violations, rule_of
from src.ai.wilson import wilson
from src.common.evaluation import Verdict

GATE_NAME = "RAG_QUALITY"

#: The four targets, in printing order: (row, numerator over which family, threshold, direction).
#: Frozen in RR-17 and ADR-0006 before the prompt was written; never moved (spec: Out of Scope).
TARGETS: tuple[tuple[str, str, int, str], ...] = (
    ("grounded", "answerable", 16, "gte"),
    ("adequate", "answerable", 14, "gte"),
    ("abstention", "unanswerable", 8, "gte"),
    ("false_refusal", "answerable", 2, "lte"),
)

GROUNDEDNESS = ("fully", "partially", "unsupported")
ADEQUACY = ("fully", "partially", "does_not")

#: The disagreement precedence, in order. One primary label per failed row.
LABELS = ("scope_violation", "failed_abstention", "retrieval_miss", "over_refusal",
          "generation_unsupported", "generation_omission_or_inadequacy",
          "generation_malformed", "judge_uncertain")

#: Population identity the manifest fixes (ADR-0006). The scorer asserts it rather than
#: reading it, because a manifest that drifted would otherwise silently move a denominator.
ANSWERABLE_N = 20
UNANSWERABLE_N = 10


def b(x: Any) -> str:
    return str(bool(x)).lower()


def _interval(k: int, n: int) -> list[float]:
    lo, hi = wilson(k, n)
    return [round(lo, 4), round(hi, 4)]


# ------------------------------------------------------------- judgement validation ----
def judgement_faults(row: Mapping[str, Any], *, question: Mapping[str, Any],
                     answer: Mapping[str, Any] | None, answers_run_id: str) -> list[str]:
    """Why one judgement row cannot be accepted. Empty when it can.

    The shape a row must take depends on what the answer did, and the checks are strict in
    both directions: an answered answerable question needs both axes; a refusal needs the
    reason judgement and *no* axis (a groundedness on a refusal is a judgement of nothing);
    a malformed output or an answered unanswerable question needs only `reviewed`, because its
    outcome is derived. Every row names the answers run it judged, so a file judged against run 1
    cannot be imported against run 2.
    """
    faults: list[str] = []
    qid = question["question_id"]
    if row.get("answers_run_id") != answers_run_id:
        faults.append(f"answers_run_id {row.get('answers_run_id')!r} is not the answers run "
                      f"{answers_run_id!r}")
    if row.get("reviewed") is not True:
        faults.append("reviewed must be true: the judge has to have read the row")
    if not isinstance(row.get("uncertain"), bool):
        faults.append("uncertain must be a boolean")
    sec = row.get("secondary_label")
    if sec is not None and sec not in LABELS:
        faults.append(f"secondary_label {sec!r} is not one of {LABELS}")
    note = row.get("note")
    if note is not None and (not isinstance(note, str) or len(note) > 1000):
        faults.append("note must be a string of at most 1000 characters, or null")
    fc = row.get("forbidden_claim_present")

    kind = row_kind(question, answer)
    g, a, r = row.get("groundedness"), row.get("adequacy"), row.get("refusal_reason_matches_key")
    if kind == "answered_answerable":
        if g not in GROUNDEDNESS:
            faults.append(f"groundedness must be one of {GROUNDEDNESS}, got {g!r}")
        if a not in ADEQUACY:
            faults.append(f"adequacy must be one of {ADEQUACY}, got {a!r}")
        if r is not None:
            faults.append("refusal_reason_matches_key must be null on an answered question")
        if not isinstance(fc, bool):
            faults.append("forbidden_claim_present must be a boolean on an answered question")
    else:
        if g is not None or a is not None:
            faults.append(f"groundedness and adequacy must be null on a {kind} row; the "
                          "outcome is derived, not judged")
        if fc is not None:
            faults.append(f"forbidden_claim_present must be null on a {kind} row")
        if kind == "refused_unanswerable":
            if not isinstance(r, bool):
                faults.append("refusal_reason_matches_key must be a boolean on a refusal of an "
                              "unanswerable question")
        elif r is not None:
            faults.append(f"refusal_reason_matches_key must be null on a {kind} row")
    return [f"{qid}: {f}" for f in faults]


def row_kind(question: Mapping[str, Any], answer: Mapping[str, Any] | None) -> str:
    """Which shape of judgement a question takes, from what its answer did."""
    answerable = question.get("answerability") == "answerable"
    if answer is None or answer.get("status") != "succeeded":
        return "malformed"
    refused = bool((answer.get("parsed") or {}).get("refused"))
    if answerable:
        return "answered_answerable" if not refused else "refused_answerable"
    return "refused_unanswerable" if refused else "answered_unanswerable"


# ---------------------------------------------------------------------- outcomes ----
def support_retrieved(question: Mapping[str, Any], answer: Mapping[str, Any] | None) -> int:
    """How many of the key's validated supporting reviews the retriever actually returned."""
    wanted = {s["review_id"] for s in question.get("answer_key", {}).get("supporting_review_ids", [])}
    got = {r.get("review_id") for r in (answer or {}).get("retrieved") or []}
    return len(wanted & got)


def outcome(question: Mapping[str, Any], answer: Mapping[str, Any] | None,
            judgement: Mapping[str, Any] | None) -> dict[str, Any]:
    """One question's scored outcome: the four booleans, the derived facts, the primary label.

    `judgement` may be None -- a question nobody judged -- and then every success flag is
    false: the denominator is the manifest's, and an unjudged row is a failure with a name.
    """
    qid = question["question_id"]
    answerable = question.get("answerability") == "answerable"
    kind = row_kind(question, answer)
    violations = (contract_violations(dict(answer), dict(question),
                                      list(answer.get("retrieved") or []))
                  if answer is not None else [f"{qid}: no answer was recorded"])
    rules = sorted({rule_of(v) for v in violations})
    parsed = (answer or {}).get("parsed") or {}
    uncited = sum(1 for c in parsed.get("claims") or [] if not c.get("citations"))
    support = support_retrieved(question, answer)
    judged = judgement is not None

    g = a = None
    capped = False
    grounded_ok = adequate_ok = abstained_ok = false_refusal = False
    if kind == "answered_answerable" and judged:
        g, a = judgement.get("groundedness"), judgement.get("adequacy")
        if uncited and g == "fully":
            g, capped = "partially", True
        if judgement.get("forbidden_claim_present") and a == "fully":
            a, capped = "partially", True
        grounded_ok, adequate_ok = g == "fully", a == "fully"
    elif kind in ("refused_answerable", "malformed") and answerable:
        false_refusal = True
    elif kind == "refused_unanswerable":
        abstained_ok = "refusal_empty" not in rules
    failed = ((answerable and (not grounded_ok or not adequate_ok or false_refusal))
              or (not answerable and not abstained_ok))
    return {
        "question_id": qid, "answerable": answerable,
        "stratum": question.get("stratum"), "family": question.get("family"),
        "kind": kind, "judged": judged,
        "groundedness": g, "adequacy": a, "capped_by_contract": capped,
        "uncited_claims": uncited, "contract_rules_violated": rules,
        "support_in_key": len(question.get("answer_key", {}).get("supporting_review_ids", [])),
        "support_retrieved": support,
        "grounded_ok": grounded_ok, "adequate_ok": adequate_ok,
        "abstained_ok": abstained_ok, "false_refusal": false_refusal,
        "refusal_reason_matches_key": (judgement or {}).get("refusal_reason_matches_key"),
        "uncertain": bool((judgement or {}).get("uncertain")),
        "secondary_label": (judgement or {}).get("secondary_label"),
        "note": (judgement or {}).get("note"),
        "failed": failed,
        "primary_label": primary_label(kind=kind, answerable=answerable, rules=rules,
                                       support=support, grounded_ok=grounded_ok,
                                       adequate_ok=adequate_ok, abstained_ok=abstained_ok,
                                       judged=judged)
        if failed else None,
    }


def primary_label(*, kind: str, answerable: bool, rules: Sequence[str], support: int,
                  grounded_ok: bool, adequate_ok: bool, abstained_ok: bool,
                  judged: bool) -> str:
    """The first label in the precedence whose condition holds for a failed row.

    `over_refusal` is a *refusal* -- the generator chose to refuse with support in front of
    it -- so a malformed output does not take it and falls through to `generation_malformed`;
    reading the precedence any other way would file a validator rejection as a decision the
    model made. An unjudged answered row has no axis to fail on, so it is `judge_uncertain`:
    the judge has not spoken.
    """
    if any(r in MECHANICAL_RULES for r in rules):
        return "scope_violation"
    if not answerable and not abstained_ok:
        return "failed_abstention"
    if answerable and support == 0:
        return "retrieval_miss"
    if kind == "refused_answerable":
        return "over_refusal"
    if kind == "answered_answerable" and judged and not grounded_ok:
        return "generation_unsupported"
    if kind == "answered_answerable" and judged and not adequate_ok:
        return "generation_omission_or_inadequacy"
    if kind == "malformed":
        return "generation_malformed"
    # Only an answered row nobody judged, or one the judge flagged, reaches here.
    return "judge_uncertain"


# ----------------------------------------------------------------------- scoring ----
def score(questions: Sequence[Mapping[str, Any]], answers: Sequence[Mapping[str, Any]],
          judgements: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The four rows over every manifest question, with intervals and the per-row detail.

    Iterates the *questions*: a question with no answer or no judgement is scored as a failure
    rather than dropped, which is the whole of the fixed-denominator rule.
    """
    by_a = {a["question_id"]: a for a in answers}
    by_j = {j["question_id"]: j for j in judgements}
    rows = [outcome(q, by_a.get(q["question_id"]), by_j.get(q["question_id"])) for q in questions]
    answerable = [r for r in rows if r["answerable"]]
    unanswerable = [r for r in rows if not r["answerable"]]
    if (len(answerable), len(unanswerable)) != (ANSWERABLE_N, UNANSWERABLE_N):
        raise ValueError(f"the manifest carries {len(answerable)} answerable and "
                         f"{len(unanswerable)} unanswerable questions; ADR-0006 fixes "
                         f"{ANSWERABLE_N} and {UNANSWERABLE_N}, and the denominators do not move")
    counts = {"grounded": sum(r["grounded_ok"] for r in answerable),
              "adequate": sum(r["adequate_ok"] for r in answerable),
              "abstention": sum(r["abstained_ok"] for r in unanswerable),
              "false_refusal": sum(r["false_refusal"] for r in answerable)}
    targets = []
    for name, family, bar, direction in TARGETS:
        n = ANSWERABLE_N if family == "answerable" else UNANSWERABLE_N
        k = counts[name]
        passed = k >= bar if direction == "gte" else k <= bar
        targets.append({"name": name, "value": k, "n": n, "threshold": bar,
                        "direction": direction, "wilson_95": _interval(k, n),
                        "verdict": "PASS" if passed else "FAIL"})
    strata: dict[str, dict[str, int]] = {}
    for r in unanswerable:
        s = strata.setdefault(str(r["stratum"]), {"n": 0, "abstained": 0})
        s["n"] += 1
        s["abstained"] += int(r["abstained_ok"])
    dist = {"groundedness": {g: sum(1 for r in answerable if r["groundedness"] == g)
                             for g in GROUNDEDNESS},
            "adequacy": {a: sum(1 for r in answerable if r["adequacy"] == a) for a in ADEQUACY},
            "refused_answerable": sum(1 for r in answerable if r["kind"] == "refused_answerable"),
            "malformed": sum(1 for r in rows if r["kind"] == "malformed")}
    labels = {lbl: sum(1 for r in rows if r["primary_label"] == lbl) for lbl in LABELS}
    return {"targets": targets, "strata": strata, "distributions": dist,
            "labels": {k: v for k, v in labels.items() if v},
            "judged": sum(r["judged"] for r in rows), "questions": len(rows),
            "capped_by_contract": [r["question_id"] for r in rows if r["capped_by_contract"]],
            "reason_matches": {"yes": sum(1 for r in unanswerable
                                          if r["refusal_reason_matches_key"] is True),
                               "no": sum(1 for r in unanswerable
                                         if r["refusal_reason_matches_key"] is False)},
            "rows": rows}


# ----------------------------------------------------------------------- verdict ----
def target_line(t: Mapping[str, Any]) -> str:
    op = ">=" if t["direction"] == "gte" else "<="
    return (f"RAG_QUALITY_TARGET {t['name']}={t['value']}/{t['n']} bar{op}{t['threshold']} "
            f"wilson95=[{t['wilson_95'][0]},{t['wilson_95'][1]}] verdict={t['verdict']}")


def strata_line(strata: Mapping[str, Mapping[str, int]]) -> str:
    parts = " ".join(f"{k}={v['abstained']}/{v['n']}" for k, v in sorted(strata.items()))
    return f"RAG_QUALITY_STRATA {parts}"


def distribution_line(s: Mapping[str, Any]) -> str:
    d = s["distributions"]
    g = " ".join(f"{k}={v}" for k, v in d["groundedness"].items())
    a = " ".join(f"{k}={v}" for k, v in d["adequacy"].items())
    return (f"RAG_QUALITY_AXES groundedness[{g}] adequacy[{a}] "
            f"refused_answerable={d['refused_answerable']} malformed={d['malformed']} "
            f"capped_by_contract={','.join(s['capped_by_contract']) or 'none'} "
            f"refusal_reason_matches_key yes={s['reason_matches']['yes']} "
            f"no={s['reason_matches']['no']}")


def labels_line(s: Mapping[str, Any]) -> str:
    failed = [r for r in s["rows"] if r["failed"]]
    parts = " ".join(f"{k}={v}" for k, v in s["labels"].items()) or "none"
    return f"RAG_QUALITY_LABELS failed_rows={len(failed)} {parts}"


def row_lines(s: Mapping[str, Any]) -> list[str]:
    out = []
    for r in s["rows"]:
        if not r["failed"]:
            continue
        out.append(f"RAG_QUALITY_ROW {r['question_id']} kind={r['kind']} "
                   f"groundedness={r['groundedness'] or 'n/a'} adequacy={r['adequacy'] or 'n/a'} "
                   f"uncited={r['uncited_claims']} support_retrieved={r['support_retrieved']}/"
                   f"{r['support_in_key']} primary={r['primary_label']} "
                   f"secondary={r['secondary_label'] or 'none'}")
    return out


def quality_line(s: Mapping[str, Any], *, scope: str) -> str:
    parts = " ".join(f"{t['name']}={t['value']}/{t['n']}" for t in s["targets"])
    met = sum(1 for t in s["targets"] if t["verdict"] == "PASS")
    return (f"RAG_QUALITY {parts} targets_met={met}/{len(s['targets'])} "
            f"judged={s['judged']}/{s['questions']} scope={scope} blocks=false "
            f"verdict={'PASS' if met == len(s['targets']) else 'FAIL'}")


NOT_JUDGED = ("the thirty answers exist under RAG_GATE=PASS but Philip has not judged them; "
              "the four targets stay unmeasured until `make rag-judge-import` accepts all "
              "thirty judgement rows (ticket 13). The bars do not move while they wait")


def verdict(s: Mapping[str, Any] | None, *, scope: str,
            cut_reason: str | None = None) -> Verdict:
    """`RAG_QUALITY`: four bars, one terminal line, no power to block.

    The artefact's single `metric` is `targets_met` against 4 -- the headline the table shows --
    and the four rows are constituents in full, each with its own threshold beside its value,
    so nothing about a target is lost to the one-number contract.
    """
    if s is None:
        return Verdict(gate_name=GATE_NAME, status="NOT_RUN", constituents=(),
                       terminal=(f"RAG_QUALITY grounded=none/{ANSWERABLE_N} "
                                 f"adequate=none/{ANSWERABLE_N} abstention=none/{UNANSWERABLE_N} "
                                 f"false_refusal=none/{ANSWERABLE_N} targets_met=0/4 judged=0/30 "
                                 f"scope={scope} blocks=false verdict=NOT_RUN"),
                       metric={"name": "targets_met", "value": 0, "threshold": 4,
                               "direction": "gte"},
                       checks=(("judged_by_philip", False),),
                       cut_reason=cut_reason or NOT_JUDGED)
    checks = tuple((f"{t['name']}_at_bar", t["verdict"] == "PASS") for t in s["targets"])
    met = sum(1 for _, ok in checks if ok)
    constituents = [*(target_line(t) for t in s["targets"]), strata_line(s["strata"]),
                    distribution_line(s), labels_line(s), *row_lines(s)]
    return Verdict(gate_name=GATE_NAME, status="PASS" if met == len(checks) else "FAIL",
                   constituents=tuple(constituents), terminal=quality_line(s, scope=scope),
                   metric={"name": "targets_met", "value": met, "threshold": len(checks),
                           "direction": "gte"},
                   checks=checks)


def notes(s: Mapping[str, Any]) -> list[str]:
    out = [("single-annotator project evaluation on a thirty-question benchmark: pragmatic "
            "targets frozen before the prompt was written, not reliability estimates; Wilson "
            "95% beside every rate, denominators fixed at the manifest's 20 and 10 (RR-17)")]
    missed = [t for t in s["targets"] if t["verdict"] == "FAIL"]
    if missed:
        out.append("below target on " + ", ".join(
            f"{t['name']} {t['value']}/{t['n']} against {'>=' if t['direction'] == 'gte' else '<='}"
            f"{t['threshold']}" for t in missed)
            + "; the bars did not move and the phase is built, evaluated, below target "
              "(ADR-0011) -- a quality miss never reopens P7")
    if s["capped_by_contract"]:
        out.append(f"{', '.join(s['capped_by_contract'])}: the judge's 'fully' was capped at "
                   "'partially' because the answer carries an uncited claim or a claim the key "
                   "forbids -- an uncited claim is unsupported (RR-24 §5)")
    if s["labels"]:
        out.append("disagreement labels by the fixed precedence: " + ", ".join(
            f"{k}={v}" for k, v in s["labels"].items()))
    return out

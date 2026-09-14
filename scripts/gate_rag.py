"""P7's blocking gate: re-derive the citation and scope contract from what was recorded.

Everything the gate decides is re-derived rather than trusted. The manifest is re-read and
re-counted, the answering identity is re-computed from `conf/rag-answer-spec.json` and compared
against the seal written when the thirty were opened, every retrieved row is re-checked against
the window it was retrieved for, and the contract is re-run over all thirty questions -- not
over the answers that happen to exist.

  RAG_GATE   **blocks**, on mechanical facts only (RR-24): the frozen manifest, the seal, the
             recorded retrieval, P7's own call ledger, a reopened run reproducing the run it
             reopened, the vacuity refusal, and every cited handle resolving in scope.
  RAG_CONTRACT  the 30/30 citation and scope contract, **reported** at its unmoved bar:
             generator behaviour is measured once and never fixed.

`RAG_QUALITY` is ticket 13's and is not printed here. Exit 0 when `RAG_GATE` passes.

Run:  ./run.sh python scripts/gate_rag.py [--scope full]   (or `make gate-rag`)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from src.ai import rag_answers
from src.ai.rag_run import EVAL_ROOT, SEAL_PATH, identity_of
from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.gates import lineage as L
from src.gates import rag as gate

ANSWERS_PATH = EVAL_ROOT / "answers.json"
LEDGER_PATH = EVAL_ROOT / "call-ledger.jsonl"
MANIFEST_PATH = C.PROJECT_ROOT / "conf" / "rag-questions.json"


def _load(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text()) if path.exists() else None


# --------------------------------------------------------------------------- facts ----
def question_facts(manifest: dict[str, Any], answers: dict[str, Any] | None) -> dict[str, Any]:
    qs = manifest.get("questions", [])
    ids = [q["question_id"] for q in qs]
    recorded = (answers or {}).get("question_spec_hash")
    got = [a["question_id"] for a in (answers or {}).get("answers", [])]
    duplicates = len(got) - len(set(got))
    unknown = sorted(set(got) - set(ids))
    return {"status": manifest.get("status"), "version": str(manifest.get("questions_version", "0")),
            "questions": len(qs), "expected": 30, "spec_hash": manifest.get("spec_hash", ""),
            "recorded_hash": recorded, "answered": len(got), "duplicates": duplicates,
            "unknown": len(unknown),
            "ok": (manifest.get("status") == "frozen" and len(qs) == 30
                   and len(set(ids)) == len(ids) and bool(manifest.get("spec_hash"))
                   and recorded == manifest.get("spec_hash") and duplicates == 0
                   and not unknown and len(got) == len(qs))}


def seal_facts(manifest: dict[str, Any], answers: dict[str, Any] | None) -> dict[str, Any]:
    """Re-derive today's answering identity and compare it with the seal, field by field.

    The re-derivation is the point: reading the seal back and agreeing with itself would check
    nothing. Editing `conf/prompts/rag-v1.txt` after the run moves `inference_config_hash` here
    and the gate says which field moved.
    """
    seal = _load(SEAL_PATH)
    if seal is None or answers is None:
        return {"present": False, "ok": False}
    sealed = seal["identity"]
    generation = sealed.get("generation", "")
    try:
        spec = rag_answers.load_spec()
        frozen = spec.frozen or {}
        name = frozen.get("name")
        prompt = spec.prompts.get(name) if name else None
        if prompt is None:
            today = {"error": "conf/rag-answer-spec.json carries no frozen prompt"}
        else:
            schema = rag_answers.answer_schema(spec.limits)
            today = identity_of(spec, prompt_version=prompt.version,
                                config_hash=spec.config_hash(name, schema, generation=generation),
                                spec_hash=manifest.get("spec_hash", ""),
                                retrieval_hash=spec.retrieval_hash(generation=generation),
                                generation=generation)
    except (ValueError, KeyError) as exc:
        today = {"error": f"{type(exc).__name__}: {exc}"}
    moved = (["spec_unreadable"] if "error" in today
             else sorted(k for k, v in today.items() if sealed.get(k) != v))
    run_matches = bool(answers.get("run_id")) and answers.get("run_id") == seal.get("run_id")
    return {"present": True, "opened_at": seal["opened_at"], "commit": seal.get("git_commit_sha"),
            "run_id": seal.get("run_id"), "prompt_version": sealed.get("prompt_version"),
            "config_hash": sealed.get("inference_config_hash", ""), "generation": generation,
            "moved": moved, "run_matches": run_matches, "today_error": today.get("error"),
            "ok": not moved and run_matches}


def retrieval_facts(manifest: dict[str, Any], answers: dict[str, Any] | None,
                    spec: rag_answers.AnswerSpec | None) -> dict[str, Any]:
    """Every retrieved row re-checked against the window it was retrieved for.

    A row outside its own window is worse than a bad citation: it means the scope filter did not
    hold, so an answer could cite an out-of-scope review and still look like it cited its own
    retrieved set.
    """
    qs = {q["question_id"]: q for q in manifest.get("questions", [])}
    rows = (answers or {}).get("answers", [])
    with_hits = wrong_size = out_of_scope = empty_windows = total = 0
    for a in rows:
        q = qs.get(a["question_id"])
        retrieved = a.get("retrieved") or []
        total += len(retrieved)
        if retrieved:
            with_hits += 1
        if q is None:
            wrong_size += 1
            continue
        windows = q["scope"]["windows"]
        mode = q["retrieval"]["mode"]
        cap = (int(spec.retrieval["temporal_per_window_k"]) if mode == "hybrid_per_window"
               else int(spec.retrieval["product_scoped_top_k"])) if spec else None
        for name, w in windows.items():
            got = [r for r in retrieved if r.get("window") == name]
            if not got:
                empty_windows += 1
            if cap is not None and len(got) > cap:
                wrong_size += 1
            out_of_scope += sum(1 for r in got
                                if not rag_answers.month_in_window(r.get("review_month"), w))
        out_of_scope += sum(1 for r in retrieved if r.get("window") not in windows)
    return {"generation": (answers or {}).get("generation", "none"), "questions": len(rows),
            "with_hits": with_hits, "retrieved_total": total, "wrong_size": wrong_size,
            "out_of_scope": out_of_scope, "empty_windows": empty_windows,
            "ok": bool(rows) and wrong_size == 0 and out_of_scope == 0}


def ledger_facts(answers: dict[str, Any] | None, ceiling: int, expected: int) -> dict[str, Any]:
    calls = [json.loads(line) for line in LEDGER_PATH.read_text().splitlines()
             if line.strip()] if LEDGER_PATH.exists() else []
    unattributed = sum(1 for c in calls if not c.get("run_id"))
    run_ids = {c.get("run_id") for c in calls if c.get("run_id")}
    covered = len({c.get("question_id") for c in calls if c.get("question_id")})
    return {"calls": len(calls), "ceiling": ceiling, "unattributed": unattributed,
            "runs": len(run_ids), "covered": covered, "expected_covered": expected,
            "ok": bool(calls) and unattributed == 0 and len(calls) <= ceiling
            and covered == expected}


def reopen_facts(answers: dict[str, Any] | None) -> dict[str, Any]:
    """The seal names what it reopened; the archived seal and answers are read from where it
    says they are, never from the current files."""
    seal = _load(SEAL_PATH)
    if seal is None:
        return {"reopened": False, "ok": True}
    ro = seal.get("reopened_from") or {}
    prior_seal = _load(EVAL_ROOT / ro["prior_seal"]) if ro.get("prior_seal") else None
    prior = _load(EVAL_ROOT / ro["prior_answers"]) if ro.get("prior_answers") else None
    return gate.reopen_facts(seal, prior_seal=prior_seal, prior=prior, current=answers)


# ----------------------------------------------------------------------------- main ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    manifest = json.loads(MANIFEST_PATH.read_text())
    answers = _load(ANSWERS_PATH)
    try:
        spec: rag_answers.AnswerSpec | None = rag_answers.load_spec()
    except ValueError:
        spec = None
    ceiling = int(spec.limits["call_ceiling"]) if spec else 200

    facts = {
        "questions": question_facts(manifest, answers),
        "seal": seal_facts(manifest, answers),
        "retrieval": retrieval_facts(manifest, answers, spec),
        "ledger": ledger_facts(answers, ceiling, len(manifest.get("questions", []))),
        "reopen": reopen_facts(answers),
        "contract": gate.contract_facts(manifest.get("questions", []),
                                        (answers or {}).get("answers", [])),
    }
    v = L.attest(gate.verdict(facts, scope=args.scope), "rag")
    v.emit()

    run_id = (answers or {}).get("run_id")
    if run_id:
        run = runs.latest_success("rag_answers", category=args.category, data_scope=args.scope,
                                  params_match={"question_set": "evaluation"})
        E.record(v, capability="rag", phase="P7 RAG", kind="reproducibility",
                 protocol_hash=manifest.get("spec_hash", ""),
                 model=(run or {}).get("params", {}).get("model_id")
                 or facts["seal"].get("prompt_version"),
                 prompt=facts["seal"].get("prompt_version"),
                 population={"name": "the thirty frozen P7 questions (20 answerable, 10 "
                                     "unanswerable in three strata)",
                             "n": len(manifest.get("questions", [])),
                             "generation": facts["retrieval"]["generation"],
                             "search_run_id": (answers or {}).get("search_run_id")},
                 pipeline_run_id=run_id, scope=args.scope, notes=gate.notes(facts))
    else:
        print("RAG_ARTEFACT not written: eval/rag/answers.json names no run id, so there is "
              "nothing to attribute the result to", file=sys.stderr)
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

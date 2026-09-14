"""Philip's judging pass over the thirty P7 answers: export, check, import (ticket 13).

  --export   assemble eval/rag/judging-worksheet.json -- every question with its answer, its
             retrieved reviews, its answer key and the mechanical facts the scorer will use
             (uncited claims, contract violations, how much validated support was retrieved).
             The worksheet hides `slot_role`, `decline_rank` and `episode_id`: a judge who knows
             which product is the headline candidate is judging the product, not the answer.
  --check    validate eval/rag/judgements.jsonl row by row without importing it; a partial
             file is fine here and named row by row.
  --import   accept all thirty rows or none, register a `rag_judgements` run in the ledger
             naming the answers run (by id and by the file's digest) and the rubric (by digest),
             and write eval/rag/judgements.json for `scripts/gate_rag.py` to score.

The judgement is HITL and stays so: nothing here writes a judgement, and the importer records
`annotator=human`. A second import over the same answers run is refused -- judged once.

Run:  ./run.sh python scripts/rag_judge.py --export | --check | --import
      (or `make rag-judge-export` / `make rag-judge-check` / `make rag-judge-import`)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.ai.rag_run import EVAL_ROOT, SEAL_PATH
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.gates import rag_quality as Q

MANIFEST_PATH = PROJECT_ROOT / "conf" / "rag-questions.json"
ANSWERS_PATH = EVAL_ROOT / "answers.json"
RUBRIC_PATH = PROJECT_ROOT / "docs" / "RAG_JUDGING.md"
WORKSHEET_PATH = EVAL_ROOT / "judging-worksheet.json"
JUDGEMENTS_JSONL = EVAL_ROOT / "judgements.jsonl"
JUDGEMENTS_PATH = EVAL_ROOT / "judgements.json"

ANNOTATOR = "philip"
PROTOCOL_VERSION = "rag-judge-v1"

#: Manifest fields the judge does not see. The question text already names the product; what
#: is withheld is where it sits in the decline ranking and why it was drawn.
HIDDEN_QUESTION_KEYS = ("slot_role", "decline_rank", "episode_id", "evidence", "probe")
KEY_FIELDS = ("required_propositions", "acceptable_themes", "forbidden_claims",
              "expected_refusal_reason")


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"missing {path.relative_to(PROJECT_ROOT)}")
    return json.loads(path.read_text())


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rel(path: Path) -> str:
    return str(path.relative_to(PROJECT_ROOT))


def load_inputs() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest, answers, seal = _load(MANIFEST_PATH), _load(ANSWERS_PATH), _load(SEAL_PATH)
    if manifest.get("status") != "frozen":
        raise SystemExit("conf/rag-questions.json is not frozen; nothing to judge")
    if answers.get("question_set") != "evaluation":
        raise SystemExit("eval/rag/answers.json is not the evaluation set")
    if answers.get("run_id") != seal.get("run_id"):
        raise SystemExit(f"answers.json is run {answers.get('run_id')} but the seal names "
                         f"{seal.get('run_id')}; judge the sealed run only")
    if answers.get("question_spec_hash") != manifest.get("spec_hash"):
        raise SystemExit("answers.json was produced against another question spec hash")
    return manifest, answers, seal


# --------------------------------------------------------------------------- export ----
def worksheet_row(q: dict[str, Any], a: dict[str, Any] | None) -> dict[str, Any]:
    o = Q.outcome(q, a, None)
    in_key = {s["review_id"] for s in q["answer_key"].get("supporting_review_ids", [])}
    retrieved = [{"cite_id": r.get("cite_id"), "window": r.get("window"),
                  "review_month": r.get("review_month"), "title": r.get("title"),
                  "text": r.get("text"), "review_id": r.get("review_id"),
                  "in_key": r.get("review_id") in in_key}
                 for r in (a or {}).get("retrieved") or []]
    parsed = (a or {}).get("parsed") or {}
    return {
        "question_id": q["question_id"], "question": q["question"],
        "family": q.get("family"), "answerability": q.get("answerability"),
        "stratum": q.get("stratum"), "windows": q["scope"]["windows"],
        "kind": o["kind"],
        "answer": {"status": (a or {}).get("status"), "refused": parsed.get("refused"),
                   "refusal_reason": parsed.get("refusal_reason"),
                   "subject": parsed.get("subject"),
                   "subject_supported": parsed.get("subject_supported"),
                   "claims": [{"claim": c.get("claim"), "citations": c.get("citations") or [],
                               "uncited": not c.get("citations")}
                              for c in parsed.get("claims") or []],
                   "validation_error": ((a or {}).get("attempts") or [{}])[-1]
                   .get("validation_error")},
        "retrieved": retrieved,
        "key": {k: q["answer_key"].get(k) for k in KEY_FIELDS},
        "facts": {"uncited_claims": o["uncited_claims"],
                  "contract_rules_violated": o["contract_rules_violated"],
                  "support_in_key": o["support_in_key"],
                  "support_retrieved": o["support_retrieved"]},
    }


def export(args: argparse.Namespace) -> None:
    manifest, answers, _ = load_inputs()
    by_a = {a["question_id"]: a for a in answers["answers"]}
    rows = [worksheet_row(q, by_a.get(q["question_id"])) for q in manifest["questions"]]
    for r in rows:
        leaked = set(r) & set(HIDDEN_QUESTION_KEYS)
        if leaked:
            raise SystemExit(f"{r['question_id']}: worksheet carries {sorted(leaked)}")
    doc = {"answers_run_id": answers["run_id"], "question_spec_hash": manifest["spec_hash"],
           "questions_version": str(manifest.get("questions_version")),
           "rubric": {"path": _rel(RUBRIC_PATH), "sha256": _sha256(RUBRIC_PATH)},
           "protocol": PROTOCOL_VERSION, "labels": list(Q.LABELS),
           "groundedness": list(Q.GROUNDEDNESS), "adequacy": list(Q.ADEQUACY),
           "created_at": datetime.now(UTC).isoformat(timespec="seconds"), "rows": rows}
    WORKSHEET_PATH.write_text(json.dumps(doc, indent=2, sort_keys=True, ensure_ascii=False) + "\n")
    kinds = {}
    for r in rows:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(f"RAG_JUDGE_EXPORT rows={len(rows)} answers_run={answers['run_id'][:8]} "
          f"{' '.join(f'{k}={v}' for k, v in sorted(kinds.items()))} "
          f"uncited_rows={sum(1 for r in rows if r['facts']['uncited_claims'])} "
          f"rubric={doc['rubric']['sha256'][:12]} out={_rel(WORKSHEET_PATH)}")


# ---------------------------------------------------------------------------- check ----
def read_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SystemExit(f"missing {_rel(path)}; export from the judging page first")
    rows = []
    for n, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SystemExit(f"line {n}: not JSON ({exc.msg})") from exc
        if not isinstance(row, dict):
            raise SystemExit(f"line {n}: not an object")
        rows.append(row)
    return rows


def validate(rows: list[dict[str, Any]], manifest: dict[str, Any], answers: dict[str, Any]
             ) -> tuple[list[str], list[str]]:
    """Every fault, by question, plus the manifest questions no row judges."""
    by_q = {q["question_id"]: q for q in manifest["questions"]}
    by_a = {a["question_id"]: a for a in answers["answers"]}
    faults: list[str] = []
    seen: set[str] = set()
    for n, row in enumerate(rows, start=1):
        qid = row.get("question_id")
        if qid not in by_q:
            faults.append(f"line {n}: question_id {qid!r} is not in the frozen manifest")
            continue
        if qid in seen:
            faults.append(f"line {n}: {qid} is judged twice")
            continue
        seen.add(qid)
        faults.extend(Q.judgement_faults(row, question=by_q[qid], answer=by_a.get(qid),
                                         answers_run_id=answers["run_id"]))
    missing = sorted(set(by_q) - seen)
    return faults, missing


def check(args: argparse.Namespace) -> None:
    manifest, answers, _ = load_inputs()
    rows = read_rows(JUDGEMENTS_JSONL)
    faults, missing = validate(rows, manifest, answers)
    for f in faults:
        print(f"RAG_JUDGE_FAULT {f}")
    print(f"RAG_JUDGE_CHECK rows={len(rows)} expected={len(manifest['questions'])} "
          f"faults={len(faults)} missing={len(missing)} "
          f"importable={str(not faults and not missing).lower()}")
    if missing:
        print(f"RAG_JUDGE_MISSING {','.join(missing)}")
    sys.exit(0 if not faults else 1)


# --------------------------------------------------------------------------- import ----
def import_judgements(args: argparse.Namespace) -> None:
    t0 = time.time()
    manifest, answers, _ = load_inputs()
    rows = read_rows(JUDGEMENTS_JSONL)
    faults, missing = validate(rows, manifest, answers)
    if faults or missing:
        for f in faults:
            print(f"RAG_JUDGE_FAULT {f}", file=sys.stderr)
        raise SystemExit(f"refusing to import: {len(faults)} fault(s), {len(missing)} question(s) "
                         f"unjudged ({','.join(missing) or 'none'}); all thirty or none")
    prior = runs.latest_success("rag_judgements", category=args.category, data_scope=args.scope,
                                params_match={"answers_run_id": answers["run_id"]})
    if prior is not None:
        raise SystemExit(f"answers run {answers['run_id']} was already judged by run "
                         f"{prior['run_id']}; the thirty are judged once (ADR-0006)")
    answers_run = runs.latest_success("rag_answers", category=args.category,
                                      data_scope=args.scope,
                                      params_match={"question_set": "evaluation"})
    if answers_run is None or answers_run["run_id"] != answers["run_id"]:
        raise SystemExit(f"the ledger's latest evaluation rag_answers run is "
                         f"{(answers_run or {}).get('run_id')}, not {answers['run_id']}")
    out = answers_run["outputs"]["eval.rag_answers"]
    if out.get("sha256") != _sha256(ANSWERS_PATH):
        raise SystemExit("eval/rag/answers.json does not match the digest its run recorded; "
                         "the judged bytes are not the sealed bytes")

    by_q = {q["question_id"]: q for q in manifest["questions"]}
    by_a = {a["question_id"]: a for a in answers["answers"]}
    kinds = {k: 0 for k in ("answered_answerable", "refused_answerable",
                            "refused_unanswerable", "answered_unanswerable", "malformed")}
    for r in rows:
        kinds[Q.row_kind(by_q[r["question_id"]], by_a.get(r["question_id"]))] += 1

    run = runs.start(
        "rag_judgements", runs.RAG_JUDGEMENTS_SPEC_VERSION,
        category=args.category, data_scope=args.scope,
        inputs={"answers": {"run_id": answers["run_id"], "path": out["path"],
                            "questions": out["questions"], "sha256": out["sha256"]},
                "questions": {"path": _rel(MANIFEST_PATH),
                              "version": str(manifest.get("questions_version")),
                              "spec_hash": manifest["spec_hash"]},
                "rubric": {"path": _rel(RUBRIC_PATH), "sha256": _sha256(RUBRIC_PATH)}},
        params={"annotator": ANNOTATOR, "label_source": "human", "protocol": PROTOCOL_VERSION,
                "api_mode": "manual", "answers_run_id": answers["run_id"],
                "judgements_file": _rel(JUDGEMENTS_JSONL)})
    doc = {"run_id": run.run_id, "answers_run_id": answers["run_id"],
           "question_spec_hash": manifest["spec_hash"], "annotator": ANNOTATOR,
           "label_source": "human", "protocol": PROTOCOL_VERSION,
           "rubric": {"path": _rel(RUBRIC_PATH), "sha256": _sha256(RUBRIC_PATH)},
           "imported_at": datetime.now(UTC).isoformat(timespec="seconds"),
           "rows": sorted(rows, key=lambda r: list(by_q).index(r["question_id"]))}
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {}
    try:
        JUDGEMENTS_PATH.write_text(json.dumps(doc, indent=2, sort_keys=True,
                                              ensure_ascii=False) + "\n")
        outputs["eval.rag_judgements"] = {"path": _rel(JUDGEMENTS_PATH), "rows": len(rows),
                                          "sha256": _sha256(JUDGEMENTS_PATH)}
        answerable = sum(1 for q in manifest["questions"] if q["answerability"] == "answerable")
        counts.update({"questions_total": len(manifest["questions"]), "answerable": answerable,
                       "unanswerable": len(manifest["questions"]) - answerable,
                       "judged": len(rows), **kinds,
                       "uncertain": sum(1 for r in rows if r.get("uncertain")),
                       "elapsed_s": round(time.time() - t0, 1)})
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise
    runs.success(run, records_in=counts["questions_total"], records_out=counts["judged"],
                 records_rejected=0, outputs=outputs, counts=counts)
    print(f"RAG_JUDGE_IMPORT run={run.run_id[:8]} answers_run={answers['run_id'][:8]} "
          f"judged={len(rows)}/{counts['questions_total']} "
          f"{' '.join(f'{k}={v}' for k, v in kinds.items())} uncertain={counts['uncertain']} "
          f"out={_rel(JUDGEMENTS_PATH)}")


# ----------------------------------------------------------------------------- main ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--export", action="store_true")
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--import", dest="do_import", action="store_true")
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    if args.export:
        export(args)
    elif args.check:
        check(args)
    else:
        import_judgements(args)


if __name__ == "__main__":
    main()

"""Job `rag_answers`: answer a frozen question set once, and record everything it rested on.

This is P7's system under test. For each question it runs the production hybrid retriever
restricted to the question's declared scope, hands the model the question and the retrieved
reviews, validates the shape of what comes back, and writes one record per question carrying
the retrieved set, the answer, the attempts and the identity hashes.

It decides nothing about whether an answer is right. The citation and scope contract is
`RAG_GATE`'s (`scripts/gate_rag.py`), and grounding, adequacy, abstention and false refusal are
`RAG_QUALITY`'s (ticket 13). A job that graded its own output would be the retriever defining
its own test, which is the failure ADR-0006 is built around.

**The evaluation set runs once.** The first run over `conf/rag-questions.json` writes
`eval/rag/answer-seal.json` -- prompt version, inference config hash, question spec hash,
retriever hash, index generation. A later evaluation run under any different identity is
refused before a single call is made: re-answering the thirty after seeing how they scored is
tuning against the held-out set, and no later freeze can undo it. Re-running under the *same*
identity is allowed and is free -- every answer is a cache hit on its idempotency key.

**Reopening (RR-24).** The thirty run once *per identity*. `--reopen "<reason>"` is the one
sanctioned second run, for a defect in the answer path that could not have been chosen on the
held-out answers -- a validator limit rejecting content nobody read. It refuses without a
written reason, refuses if the prompt, model, decoding, schema, retrieval or questions moved
(the sealed config hash must come back from today's spec with the sealed commit's limits), and
refuses if nothing moved at all. It archives the prior seal, answers and gate artefact as
`*.1.json`, writes seal 2 with the reason and the prior run's id, and `RAG_GATE` then asserts
`answers_identical_to_run1=k/k` over every question the prior run parsed. Run 1 stays the
first result on record.

Prompt development runs against a separate question set (`--questions`, `--question-set
development`), instantiated on pre-2020 windows of non-candidate products (ADR-0006). Its
answers land in their own directory and never touch the seal.

Run:  ./run.sh python -m src.ai.rag_run --questions conf/rag-questions.json
      ./run.sh python -m src.ai.rag_run --reopen "<reason>"     (once, RR-24)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from elasticsearch import Elasticsearch

from src.ai import ollama, rag_answers
from src.ai.rag_answers import AnswerSpec
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.serving import projection as P
from src.serving import search as S

sys.stdout.reconfigure(line_buffering=True)

EVAL_ROOT = PROJECT_ROOT / "eval" / "rag"
SEAL_PATH = EVAL_ROOT / "answer-seal.json"
QUESTION_SETS = ("evaluation", "development")

#: The files a reopen archives, numbered by the seal they belonged to: `answers.1.json`.
ARCHIVED = {"prior_answers": "answers.json", "prior_seal": "answer-seal.json",
            "prior_gate": "gate.json"}


# --------------------------------------------------------------------- retrieval ----
def retrieve(es: Elasticsearch, alias: str, question: dict[str, Any], spec: AnswerSpec,
             *, encode) -> list[dict[str, Any]]:
    """The scoped hybrid per declared window, concatenated in window order then rank order.

    A temporal question retrieves each window on its own (`per_window_k` each) rather than
    taking the top ten of the union: the union would let a rich baseline crowd a thin recent
    window out of the context entirely, and the answer would then contrast one window with
    silence while the retriever looked like it had found nothing. Product-scoped questions have
    one window and take `top_k`.
    """
    r = spec.retrieval
    mode = question["retrieval"]["mode"]
    per_window = (int(r["temporal_per_window_k"]) if mode == "hybrid_per_window"
                  else int(r["product_scoped_top_k"]))
    vector = encode(question["question"])
    out: list[dict[str, Any]] = []
    for name in sorted(question["scope"]["windows"]):
        w = question["scope"]["windows"][name]
        hits = S.scoped_hybrid(es, alias, question["question"], vector,
                               parent_asin=question["parent_asin"], start=w["start"],
                               end=w["end"], analyzer=r["analyzer"], size=per_window,
                               rrf_window=int(r["rrf_window"]), knn_k=int(r["knn_k"]),
                               num_candidates=int(r["knn_num_candidates"]))
        for h in hits:
            src = h.source
            out.append({"review_id": h.review_id, "window": name, "rank": h.rank,
                        "score": round(h.score, 8), "legs": h.parts,
                        "review_month": (src.get("review_month") or "")[:7] or None,
                        "title": src.get("title"), "text": src.get("text"),
                        "parent_asin": src.get("parent_asin")})
    # Handles are stamped once, here, over the whole concatenated set -- so R4 means the same
    # row in the prompt, in the stored record and at the gate.
    return rag_answers.with_handles(out)


# -------------------------------------------------------------------------- seal ----
def identity_of(spec: AnswerSpec, *, prompt_version: str, config_hash: str, spec_hash: str,
                retrieval_hash: str, generation: str) -> dict[str, Any]:
    return {"model_id": spec.model_id, "answer_spec_version": spec.answer_spec_version,
            "prompt_version": prompt_version, "inference_config_hash": config_hash,
            "question_spec_hash": spec_hash, "retrieval_hash": retrieval_hash,
            "generation": generation}


def check_seal(identity: dict[str, Any], *, path: Path = SEAL_PATH) -> dict[str, Any] | None:
    """Refuse a second evaluation run under a changed identity; return the seal if there is one."""
    if not path.exists():
        return None
    seal = json.loads(path.read_text())
    moved = {k: (seal["identity"].get(k), v) for k, v in identity.items()
             if seal["identity"].get(k) != v}
    if moved:
        detail = "; ".join(f"{k}: sealed {str(was)[:12]!r} != now {str(now)[:12]!r}"
                           for k, (was, now) in sorted(moved.items()))
        raise ValueError(
            f"the thirty questions were already answered on {seal['opened_at']} under a "
            f"different identity, and ADR-0006 lets them run once: {detail}. Re-answering them "
            "after seeing how they scored is tuning against the held-out set. Develop on the "
            "development question set instead (--question-set development).")
    return seal


def write_seal(identity: dict[str, Any], *, run_id: str, questions: int,
               path: Path = SEAL_PATH, reopened_from: dict[str, Any] | None = None,
               seal_no: int = 1) -> dict[str, Any]:
    sha, _ = runs.git_state()
    seal = {"opened_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "git_commit_sha": sha, "run_id": run_id, "questions": questions,
            "identity": identity, "seal_no": seal_no,
            "note": "the evaluation question set is answered once per identity (ADR-0006); a "
                    "later run under a different identity is refused before any call is made, "
                    "and the one sanctioned reopen (--reopen, RR-24) records its reason here"}
    if reopened_from:
        seal["reopened_from"] = reopened_from
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(seal, indent=2, sort_keys=True) + "\n")
    return seal


# ------------------------------------------------------------------------ reopen ----
@dataclass(frozen=True)
class ReopenCheck:
    """A reopen the seal allows: what it reopens, why, and the one identity field that moved."""
    reason: str
    prior_run_id: str
    seal_no: int
    moved: tuple[str, ...]


def check_reopen(seal: dict[str, Any] | None, *, reason: str, identity: dict[str, Any],
                 sealed_hash_recomputed: str) -> ReopenCheck:
    """Allow the reopen only if the sole movement is one the held-out answers could not have
    chosen -- a validator limit.

    `sealed_hash_recomputed` is today's config hash rebuilt with the *sealed commit's* limits.
    If it equals the hash the seal recorded, then everything else inside that hash -- the
    prompt text, the model, the decoding settings including the seed, the schema, the
    retriever -- is what it was when the thirty were opened, and only `limits` moved. The
    direct identity fields (prompt version, model, retrieval, questions, generation) are
    compared on their own, so the refusal names the field.
    """
    if not str(reason or "").strip():
        raise ValueError("--reopen needs a written reason: the seal records why the thirty "
                         "were opened a second time, and an unexplained reopen is a rerun")
    if seal is None:
        raise ValueError("nothing to reopen: no seal exists, so the thirty have not been "
                         "answered yet; run without --reopen")
    sealed = seal["identity"]
    moved = tuple(sorted(k for k, v in identity.items() if sealed.get(k) != v))
    other = [k for k in moved if k != "inference_config_hash"]
    if other:
        detail = "; ".join(f"{k}: sealed {str(sealed.get(k))[:12]!r} != now "
                           f"{str(identity[k])[:12]!r}" for k in other)
        raise ValueError(f"a reopen may not move the prompt, model, retrieval, questions or "
                         f"generation, and this one moved {', '.join(other)}: {detail}. That "
                         "is tuning on the held-out set (ADR-0006)")
    if not moved:
        raise ValueError("identity unchanged since the seal: a run under the same identity "
                         "needs no reopen, and a reopen that changes nothing measures nothing")
    if sealed_hash_recomputed != sealed["inference_config_hash"]:
        raise ValueError("the config hash moved for more than the limits: today's prompt text, "
                         "model, decoding settings, schema and retriever with the sealed "
                         f"commit's limits give {sealed_hash_recomputed[:12]}, but the seal "
                         f"recorded {sealed['inference_config_hash'][:12]}. Only a validator "
                         "limit may move under --reopen (RR-24)")
    return ReopenCheck(reason=reason.strip(), prior_run_id=seal["run_id"],
                       seal_no=int(seal.get("seal_no", 1)), moved=moved)


def sealed_limits(commit: str, *, path: str = "conf/rag-answer-spec.json") -> dict[str, Any]:
    """The `limits` block as committed at the seal's commit, read from git, never from disk."""
    try:
        text = subprocess.run(["git", "show", f"{commit}:{path}"], check=True,
                              capture_output=True, text=True, cwd=PROJECT_ROOT).stdout
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"cannot read {path} at the sealed commit {commit[:8]}: "
                           f"{exc.stderr.strip()}") from exc
    return json.loads(text)["limits"]


def archive_prior(out_dir: Path, *, seal_no: int) -> dict[str, str]:
    """Move seal `seal_no`'s answers, seal and gate artefact to `*.{seal_no}.json`.

    Refuses to overwrite an earlier archive and refuses when there is no prior run to archive;
    the gate artefact is optional (the gate may not have been run). Returns the archived
    names, which the new seal records so the gate can find them.
    """
    targets = {k: out_dir / f"{Path(v).stem}.{seal_no}.json" for k, v in ARCHIVED.items()}
    for k, v in ARCHIVED.items():
        if not (out_dir / v).exists() and k != "prior_gate":
            raise FileNotFoundError(f"{out_dir / v} is missing: no prior run to archive")
        if targets[k].exists():
            raise FileExistsError(f"{targets[k]} already exists; an archive is never "
                                  "overwritten")
    out: dict[str, str] = {}
    for k, v in ARCHIVED.items():
        if (out_dir / v).exists():
            shutil.move(out_dir / v, targets[k])
            out[k] = targets[k].name
    return out


# --------------------------------------------------------------------------- run ----
def _cache(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    doc = json.loads(path.read_text())
    return {a["idempotency_key"]: a for a in doc.get("answers", [])}


def run_rag_answers(*, questions_path: Path, question_set: str, scope: str, category: str,
                    prompt_name: str, model_id: str | None, out_dir: Path,
                    limit: int | None, reopen: str | None = None) -> dict[str, Any]:
    t0 = time.time()
    if reopen is not None and question_set != "evaluation":
        raise ValueError("--reopen is for the sealed evaluation set only; the development set "
                         "has no seal to reopen")
    spec = rag_answers.load_spec()
    manifest = json.loads(questions_path.read_text())
    qs = manifest["questions"]
    if question_set == "evaluation":
        frozen = spec.require_frozen(purpose="answering the thirty evaluation questions")
        if manifest.get("status") != "frozen":
            raise ValueError(f"{questions_path} is status={manifest.get('status')!r}: the "
                             "evaluation set must be frozen before it is answered")
        if prompt_name != frozen["name"]:
            raise ValueError(f"the evaluation set is answered by the frozen prompt "
                             f"{frozen['name']!r}, got {prompt_name!r}")
    prompt = spec.prompts[prompt_name]
    model = model_id or spec.model_id
    schema = rag_answers.answer_schema(spec.limits)

    es = P.client()
    alias = spec.retrieval["alias"] if scope == "full" else f"{spec.retrieval['alias']}_{scope}"
    generations = P.alias_targets(es, alias)
    if len(generations) != 1:
        raise RuntimeError(f"alias {alias} points at {len(generations)} indices; retrieval must "
                           "rest on exactly one generation")
    generation = generations[0]
    config_hash = spec.config_hash(prompt_name, schema, generation=generation, model_id=model)
    retrieval_hash = spec.retrieval_hash(generation=generation)
    identity = identity_of(spec, prompt_version=prompt.version, config_hash=config_hash,
                           spec_hash=manifest.get("spec_hash", ""),
                           retrieval_hash=retrieval_hash, generation=generation)

    reopening: ReopenCheck | None = None
    if reopen is not None:
        seal = json.loads(SEAL_PATH.read_text()) if SEAL_PATH.exists() else None
        limits_then = sealed_limits(seal["git_commit_sha"]) if seal else {}
        recomputed = spec.config_hash(prompt_name, rag_answers.answer_schema(limits_then),
                                      generation=generation, model_id=model,
                                      limits=limits_then) if seal else ""
        reopening = check_reopen(seal, reason=reopen, identity=identity,
                                 sealed_hash_recomputed=recomputed)
        if limit:
            raise ValueError("a reopen answers all thirty; --limit would leave the seal "
                             "naming a partial run")
        print(f"[rag] reopening run {reopening.prior_run_id[:8]} (seal {reopening.seal_no}): "
              f"{reopening.reason}")
    else:
        seal = check_seal(identity) if question_set == "evaluation" else None
    search_run = runs.latest_success("search_index_reviews", category=category, data_scope=scope)
    if search_run is None:
        raise RuntimeError("rag_answers needs a successful search_index_reviews run to pin the "
                           "generation it retrieved from")

    out_dir.mkdir(parents=True, exist_ok=True)
    answers_path = out_dir / "answers.json"
    ledger_path = out_dir / "call-ledger.jsonl"
    # A reopen changes the identity, so nothing is a cache hit; the archive below keeps the
    # prior answers on disk for the gate to compare against, not for reuse.
    cached = {} if reopening else _cache(answers_path)

    run = runs.start("rag_answers", runs.RAG_ANSWERS_SPEC_VERSION, category=category,
                     data_scope=scope,
                     inputs={"questions": {"path": str(questions_path.relative_to(PROJECT_ROOT)),
                                           "version": str(manifest.get("questions_version", "0")),
                                           "spec_hash": manifest.get("spec_hash", "")},
                             "spec": {"path": "conf/rag-answer-spec.json",
                                      "version": spec.answer_spec_version, "model_id": model,
                                      "prompt_version": prompt.version,
                                      "config_hash": config_hash},
                             "search": {"run_id": search_run["run_id"], "alias": alias,
                                        "generation": generation}},
                     params={"question_set": question_set, "prompt_version": prompt.version,
                             "model_id": model, "api_mode": spec.api_mode,
                             "inference": spec.inference, "retrieval_hash": retrieval_hash,
                             "question_spec_hash": manifest.get("spec_hash", ""),
                             **({"reopened_from": reopening.prior_run_id,
                                 "reopen_reason": reopening.reason} if reopening else {})})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"inference_config_hash": config_hash[:12],
                              "retrieval_hash": retrieval_hash[:12]}
    try:
        from src.ai.embedder import encode_query  # loads the model on first use
        selected = qs[:limit] if limit else qs
        tally = {"answered": 0, "refused": 0, "parse_failed": 0, "api_failed": 0}
        calls = cache_hits = retrieved_total = 0
        records: list[dict[str, Any]] = []
        ledger_rows: list[dict[str, Any]] = []
        started = time.time()
        for i, q in enumerate(selected, start=1):
            retrieved = retrieve(es, alias, q, spec, encode=encode_query)
            retrieved_total += len(retrieved)
            digest = rag_answers.retrieved_digest(retrieved)
            key = rag_answers.idempotency_key(
                question_id=q["question_id"], model_id=model,
                answer_spec_version=spec.answer_spec_version, prompt_version=prompt.version,
                inference_config_hash=config_hash, retrieved_digest=digest)
            if key in cached:
                rec = dict(cached[key])
                rec["retrieved"] = retrieved
                cache_hits += 1
            else:
                body = rag_answers.render_question(q, retrieved)
                res = ollama.infer(spec, system=prompt.text, prompt=body, schema=schema,
                                   model_id=model,
                                   validate=lambda obj: rag_answers.validate_answer(
                                       obj, limits=spec.limits))
                calls += res.attempt_count
                rec = {"question_id": q["question_id"], "idempotency_key": key,
                       "status": res.status, "parsed": res.parsed,
                       "subject_words": (rag_answers.subject_words(res.parsed)
                                         if res.parsed else None),
                       "retrieved": retrieved, "retrieved_digest": digest,
                       "prompt_version": prompt.version, "model_id": model,
                       "inference_config_hash": config_hash, "run_id": run.run_id,
                       "attempts": [{"attempt_no": a.attempt_no,
                                     "validation_error": a.validation_error,
                                     "input_tokens": a.input_tokens,
                                     "output_tokens": a.output_tokens,
                                     "duration_s": a.duration_s} for a in res.attempts]}
                ledger_rows += [{"run_id": run.run_id, "question_id": q["question_id"],
                                 "attempt_no": a.attempt_no, "model_id": model,
                                 "prompt_version": prompt.version, "api_mode": spec.api_mode,
                                 "inference_config_hash": config_hash,
                                 "input_tokens": a.input_tokens, "output_tokens": a.output_tokens,
                                 "estimated_cost_usd": 0.0, "duration_s": a.duration_s,
                                 "status": "ok" if a.validation_error is None else "rejected",
                                 "validation_error": a.validation_error,
                                 "completed_at": datetime.fromtimestamp(a.completed_at,
                                                                        tz=UTC).isoformat()}
                                for a in res.attempts]
            if rec["status"] != "succeeded":
                tally[rec["status"]] += 1
            elif (rec["parsed"] or {}).get("refused"):
                tally["refused"] += 1
            else:
                tally["answered"] += 1
            records.append(rec)
            rate = (time.time() - started) / i
            print(f"[rag] {i}/{len(selected)} {q['question_id']} retrieved={len(retrieved)} "
                  f"status={rec['status']} refused={bool((rec['parsed'] or {}).get('refused'))} "
                  f"{rate:.1f}s/q eta {(len(selected) - i) * rate / 60:.0f} min")

        ceiling = int(spec.limits["call_ceiling"])
        with ledger_path.open("a") as f:
            for row in ledger_rows:
                f.write(json.dumps(row, sort_keys=True) + "\n")
        total_calls = sum(1 for _ in ledger_path.open()) if ledger_path.exists() else 0
        if total_calls > ceiling:
            raise RuntimeError(f"P7's own call ledger holds {total_calls} calls, over ADR-0006's "
                               f"ceiling of {ceiling}")

        doc = {"question_set": question_set, "questions_path": str(questions_path.relative_to(PROJECT_ROOT)),
               "question_spec_hash": manifest.get("spec_hash", ""),
               "questions_version": str(manifest.get("questions_version", "0")),
               "run_id": run.run_id, "scope": scope, "alias": alias, "generation": generation,
               "search_run_id": search_run["run_id"], "identity": identity,
               "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
               **({"reopened_from": reopening.prior_run_id} if reopening else {}),
               "answers": records}
        archived: dict[str, str] = {}
        if reopening:
            # Only now, with every answer in hand: a reopen that failed halfway leaves run 1
            # exactly where it was, and a second attempt starts from the same seal.
            archived = archive_prior(out_dir, seal_no=reopening.seal_no)
            print(f"[rag] archived run {reopening.prior_run_id[:8]} as "
                  f"{', '.join(sorted(archived.values()))}")
        answers_path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
        sha256 = hashlib.sha256(answers_path.read_bytes()).hexdigest()
        outputs["eval.rag_answers"] = {
            "path": str(answers_path.relative_to(PROJECT_ROOT)), "questions": len(records),
            "sha256": sha256}
        counts.update({"questions_total": len(selected), "answered": tally["answered"],
                       "refused": tally["refused"], "parse_failed": tally["parse_failed"],
                       "api_failed": tally["api_failed"], "calls": calls,
                       "calls_in_ledger": total_calls, "call_ceiling": ceiling,
                       "cache_hits": cache_hits, "retrieved_total": retrieved_total,
                       "elapsed_s": round(time.time() - t0, 1)})
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["questions_total"],
                 records_out=counts["answered"] + counts["refused"],
                 records_rejected=counts["parse_failed"] + counts["api_failed"],
                 outputs=outputs, counts=counts)
    if reopening:
        seal = write_seal(identity, run_id=run.run_id, questions=len(records),
                          seal_no=reopening.seal_no + 1,
                          reopened_from={"run_id": reopening.prior_run_id,
                                         "reason": reopening.reason,
                                         "moved": list(reopening.moved), **archived})
        print(f"[rag] sealed {SEAL_PATH.relative_to(PROJECT_ROOT)} (seal {seal['seal_no']}) "
              f"at {seal['opened_at']}, reopened from {reopening.prior_run_id[:8]}")
    elif question_set == "evaluation" and seal is None:
        seal = write_seal(identity, run_id=run.run_id, questions=len(records))
        print(f"[rag] sealed {SEAL_PATH.relative_to(PROJECT_ROOT)} at {seal['opened_at']}")
    print(f"RAG_ANSWERS run_id={run.run_id} set={question_set} scope={scope} model={model} "
          f"prompt={prompt.version} config={config_hash[:12]} generation={generation} "
          f"questions={counts['questions_total']} answered={counts['answered']} "
          f"refused={counts['refused']} parse_failed={counts['parse_failed']} "
          f"api_failed={counts['api_failed']} calls={counts['calls']} "
          f"cached={counts['cache_hits']} ledger={total_calls}/{ceiling} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "counts": counts, "answers_path": answers_path}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--questions", default=str(PROJECT_ROOT / "conf" / "rag-questions.json"))
    ap.add_argument("--question-set", default="evaluation", choices=QUESTION_SETS)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--prompt", default=None, help="prompt name in conf/rag-answer-spec.json "
                                                   "(default: the frozen one, else rag_v1)")
    ap.add_argument("--model", default=None, help="override the primary model")
    ap.add_argument("--out-dir", default=None,
                    help="where answers.json and the call ledger land (default: eval/rag, or "
                         "eval/rag/dev for the development set)")
    ap.add_argument("--limit", type=int, default=None,
                    help="execution setting: answer at most N questions")
    ap.add_argument("--reopen", default=None, metavar="REASON",
                    help="the one sanctioned second run of the sealed evaluation set (RR-24): "
                         "archives the prior seal, answers and gate artefact, and records this "
                         "reason in the new seal; refused unless only a validator limit moved")
    args = ap.parse_args()
    spec = rag_answers.load_spec()
    prompt_name = args.prompt or (spec.frozen or {}).get("name") or "rag_v1"
    out = Path(args.out_dir) if args.out_dir else (
        EVAL_ROOT if args.question_set == "evaluation" else EVAL_ROOT / "dev")
    run_rag_answers(questions_path=Path(args.questions).resolve(), question_set=args.question_set,
                    scope=args.scope, category=args.category, prompt_name=prompt_name,
                    model_id=args.model, out_dir=out, limit=args.limit, reopen=args.reopen)


if __name__ == "__main__":
    main()

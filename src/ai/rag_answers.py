"""The P7 answer contract: what an answer is, and what makes one a contract violation.

Pure -- plain dicts in, plain findings out. No Elasticsearch, no Ollama, no Spark, no clock.
`src/ai/rag_run.py` does the I/O and `src/gates/rag.py` turns these findings into a verdict.

Three separations decide the whole design, and each one is easier to collapse than to keep:

* **Decoding constrains shape, never citation membership.** Ollama would happily accept an
  `enum` of the retrieved review ids and then only ever emit ids from it. That would make
  `RAG_GATE`'s citation contract unfailable by construction -- a gate that cannot fail is not
  a gate (audit F3) -- and it is the same deformation `src.ai.labels.decoding_schema` refuses
  for quote length: a rule enforced at decoding silently bends the answer instead of recording
  that the answer was wrong. So the decoding schema leaves `cite_id` and `window` as free
  strings, and membership is checked afterwards, from the stored retrieved set.

* **Validation is shape; the gate is grounding.** `validate_answer` checks that a response is
  a well-formed answer -- the keys, the types, the length limits, and that a refusal is *empty*
  rather than a hedge wearing a refusal flag. It never checks whether a cited id was retrieved
  or whether a cited review falls inside its declared window. Those are the contract
  `RAG_GATE` blocks on, so they must survive generation to reach it. A response that fails
  validation is terminal `parse_failed` -- one attempt, because an identical retry of a
  `temperature 0`, seeded decoder returned identical bytes both times (RR-24) -- and the
  contract counts that as a violation too: an answer that is not an answer cites nothing.

* **Windows are verified from stored metadata, never from the citation.** A citation names a
  window; the check reads that review's `review_month` out of the retrieved set and asks
  whether it really lies inside the bounds the frozen question declared (ADR-0006). A model
  that relabels a baseline review as recent is a scope violation, not a formatting slip.

**The citation vocabulary is a per-question handle** (`R1`, `R2`, …) carried on the retrieved
row beside its `review_id`, not the 64-hex review id itself. That is a measurement decision, not
a convenience: the first development run had `qwen3:8b` cite `65c6cea5…` for a supplied
`95c6cea5…` and cite bare list positions elsewhere, and a transcription slip in a hex string is
not the retriever-grounding failure `RAG_GATE` exists to catch. Handles are assigned per
question, so one question's handle cannot resolve inside another's, and a handle the question
never supplied resolves to nothing and is a violation exactly as an invented id was.

Nothing here knows what a *good* answer is. Grounding, adequacy, abstention correctness and
false refusal are `RAG_QUALITY` (ticket 13), judged by Philip against the answer keys -- and
the answer keys are never in scope for anything in this module.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

SPEC_PATH = PROJECT_ROOT / "conf" / "rag-answer-spec.json"

ANSWER_STATUSES = ("succeeded", "parse_failed", "api_failed")

#: The contract constituents `RAG_GATE` counts, in the order it prints them. Every one of them
#: is a claim about the answer's own evidence -- never about whether the answer is any good.
CONTRACT_RULES = ("terminal", "refusal_empty", "claims_cite", "citations_retrieved",
                  "citations_in_scope")

#: The split RR-24 drew through the contract. A *mechanical* rule is one a correct pipeline
#: makes true regardless of what the model wrote -- a cited handle resolving to the retrieved
#: set, a stored month inside the declared window -- and `RAG_GATE` blocks on it. The rest is
#: *generator behaviour*: an uncited claim, a refusal carrying claims, a rejected output. It is
#: measured once against the unmoved 30/30 bar and reported, never fixed after the thirty are
#: seen (ADR-0006, CONTEXT.md *Mechanical fact / Generator behaviour*).
MECHANICAL_RULES = ("citations_retrieved", "citations_in_scope")
GENERATOR_RULES = ("terminal", "refusal_empty", "claims_cite")


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def word_count(s: str) -> int:
    return len(s.split())


# ------------------------------------------------------------------------- spec ----
@dataclass(frozen=True)
class Prompt:
    name: str
    version: str
    path: Path
    text: str


@dataclass(frozen=True)
class AnswerSpec:
    """`conf/rag-answer-spec.json` plus the prompt texts it names."""
    answer_spec_version: str
    status: str
    endpoint: str
    model_id: str
    api_mode: str
    inference: dict[str, Any]
    retrieval: dict[str, Any]
    limits: dict[str, Any]
    prompts: dict[str, Prompt]
    frozen: dict[str, Any] | None
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    @property
    def is_frozen(self) -> bool:
        return self.status == "frozen" and self.frozen is not None

    def require_frozen(self, *, purpose: str) -> dict[str, Any]:
        """The frozen prompt, or a refusal before the work starts rather than after it.

        The thirty evaluation questions run once, after the prompt freezes (ADR-0006). Running
        them under an unfrozen prompt would make the one run that is allowed a development run,
        and no later freeze could undo it.
        """
        if not self.is_frozen:
            raise ValueError(
                f"{purpose} needs a frozen prompt, but conf/rag-answer-spec.json is "
                f"status={self.status!r} with frozen_prompt="
                f"{'set' if self.frozen else 'absent'}; develop on the development question set "
                "and freeze first (ticket 12)")
        return self.frozen  # type: ignore[return-value]

    def retrieval_hash(self, *, generation: str) -> str:
        """Identity of the retriever the answers rest on, including the index generation.

        The generation is in here rather than beside it because a reindex changes what every
        question retrieved, and an answer set produced against a different generation is a
        different measurement wearing the same question ids.
        """
        return _sha256(json.dumps({"retrieval": self.retrieval, "generation": generation},
                                  sort_keys=True))

    def config_hash(self, prompt_name: str, schema: dict[str, Any], *,
                    generation: str, model_id: str | None = None,
                    limits: dict[str, Any] | None = None) -> str:
        """Identity of one answering configuration: settings, prompt, schema, retriever.

        `limits` defaults to this spec's own; a reopen passes the sealed commit's instead, to
        show that the sealed hash comes back when only the limits are put back (RR-24).
        """
        p = self.prompts[prompt_name]
        lim = self.limits if limits is None else limits
        payload = {"inference": self.inference, "system": p.text, "prompt_version": p.version,
                   "schema": schema, "model_id": model_id or self.model_id,
                   "limits": {k: v for k, v in lim.items() if k != "call_ceiling"},
                   "answer_spec_version": self.answer_spec_version,
                   "retrieval_hash": self.retrieval_hash(generation=generation)}
        return _sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def load_spec(path: Path = SPEC_PATH) -> AnswerSpec:
    doc = json.loads(path.read_text())
    if doc["host"] != "ollama":
        raise ValueError(f"only the local ollama host is provisioned (RR-19), got {doc['host']!r}")
    if doc["api_mode"] != "local":
        raise ValueError(f"api_mode must be 'local' under RR-19, got {doc['api_mode']!r}")
    if doc.get("status") not in ("provisional", "frozen"):
        raise ValueError(f"status must be provisional|frozen, got {doc.get('status')!r}")
    if float(doc["inference"]["temperature"]) != 0.0:
        raise ValueError("inference.temperature must be 0: the one evaluation run must be "
                         "reproducible from its recorded configuration")
    prompts = {}
    for name, p in doc["prompts"].items():
        f = PROJECT_ROOT / p["path"]
        text = f.read_text()
        if not text.strip():
            raise ValueError(f"prompt {name} at {p['path']} is empty")
        prompts[name] = Prompt(name=name, version=p["version"], path=f, text=text)
    frozen = doc.get("frozen_prompt")
    if frozen:
        if frozen["name"] not in prompts:
            raise ValueError(f"frozen_prompt names {frozen['name']!r}, not a declared prompt")
        if prompts[frozen["name"]].version != frozen["version"]:
            raise ValueError(f"frozen_prompt says {frozen['version']!r} but prompt "
                             f"{frozen['name']!r} is {prompts[frozen['name']].version!r}; the "
                             "freeze record has drifted")
    return AnswerSpec(answer_spec_version=str(doc["answer_spec_version"]), status=doc["status"],
                      endpoint=doc["endpoint"], model_id=doc["model_id"],
                      api_mode=doc["api_mode"], inference=doc["inference"],
                      retrieval=doc["retrieval"], limits=doc["limits"], prompts=prompts,
                      frozen=frozen, raw=doc)


# ------------------------------------------------------------- the decoding schema ----
def answer_schema(limits: dict[str, Any]) -> dict[str, Any]:
    """What Ollama constrains decoding to: the shape of an answer, and only the shape.

    `cite_id` and `window` are free strings on purpose -- see the module docstring. The array
    caps are structural and cannot deform a claim the way a length rule deforms a quote: they
    can only stop the model adding a seventh claim the prompt already asked it not to add.

    **`subject` and `subject_supported` come first because generation order is decision order.**
    Measured on the development set: `qwen3:8b` emitted `claims` before `refused`, so it wrote
    a list of complaints and only then decided whether it had refused -- and having written
    them, it never had. Asked in prose three times not to, it kept turning the refusal into a
    claim reading "complaints about flight delays are not present in the cited reviews". Naming
    the subject and judging its support *before* any claim exists is the fix, and it is a
    structural one: the schema decides what is written first, which no amount of prompt
    emphasis can. It constrains no content -- the model writes whichever subject and whichever
    verdict it likes.
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["subject", "subject_supported", "refused", "refusal_reason", "claims"],
        "properties": {
            "subject": {
                "type": "string",
                "description": "The subject the question asks about, in a few words. Write this "
                               "first, before reading for an answer."},
            "subject_supported": {
                "type": "boolean",
                "description": "True only if at least one supplied review complains about that "
                               "subject. Decide this before writing any claim."},
            "refused": {"type": "boolean"},
            "refusal_reason": {"type": ["string", "null"]},
            "claims": {
                "type": "array",
                "maxItems": int(limits["max_claims"]),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["claim", "citations"],
                    "properties": {
                        "claim": {
                            "type": "string",
                            "description": "One complaint visible in the cited reviews, in plain "
                                           "words. Never a count, a share, a direction, a claim "
                                           "of typicality, or a cause."},
                        "citations": {
                            "type": "array",
                            "maxItems": int(limits["max_citations_per_claim"]),
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["cite_id", "window"],
                                "properties": {
                                    "cite_id": {
                                        "type": "string",
                                        "description": "The id on a supplied review's id line "
                                                       "(R1, R2, …), copied exactly."},
                                    "window": {
                                        "type": "string",
                                        "description": "That review's window label, exactly as "
                                                       "it was supplied."}}}}}}},
        },
    }


# ------------------------------------------------------------------ the prompt body ----
def cite_handle(n: int) -> str:
    """The per-question citation handle for the nth retrieved review, 1-based."""
    return f"R{n}"


def with_handles(retrieved: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Stamp `cite_id` on each retrieved row in the order the prompt will show them.

    Done once, by the runner, so the handle the model was shown and the handle the gate checks
    are the same object rather than two derivations that could drift.
    """
    return [{**r, "cite_id": cite_handle(i)} for i, r in enumerate(retrieved, start=1)]


def render_review(row: dict[str, Any]) -> str:
    title = (row.get("title") or "").strip()
    body = (row.get("text") or "").strip()
    return (f"id: {row['cite_id']}  window: {row['window']}\n"
            f"    title: {title or '(none)'}\n"
            f"    text: {body}")


def render_question(question: dict[str, Any], retrieved: list[dict[str, Any]]) -> str:
    """The user turn: the question, its declared windows, and the retrieved reviews.

    The answer key is not in scope here and never will be -- not the supporting review ids, not
    the acceptable themes, not the expected refusal reason. Neither is the product's decline
    rank or the theme-shift direction, which ADR-0006 hides from generator and judge alike.
    """
    windows = question["scope"]["windows"]
    lines = [f"QUESTION: {question['question']}", "", "WINDOWS:"]
    for name in sorted(windows):
        w = windows[name]
        lines.append(f"  {name}: {w['start']} to {w['end']} (inclusive months)")
    lines.append("")
    if retrieved:
        lines.append(f"REVIEWS ({len(retrieved)}):")
        lines += [render_review(r) for r in retrieved]
    else:
        lines.append("REVIEWS (0): no review was retrieved for this question.")
    return "\n".join(lines)


# -------------------------------------------------------------------- validation ----
def subject_words(parsed: dict[str, Any]) -> int:
    """How long the model's `subject` line was -- recorded beside the answer, not enforced."""
    return word_count(str(parsed.get("subject") or ""))


def validate_answer(obj: Any, *, limits: dict[str, Any]) -> list[str]:
    """Shape only. Named failures, never repairs; see the module docstring for the split."""
    fails: list[str] = []
    required = {"subject", "subject_supported", "refused", "refusal_reason", "claims"}
    if not isinstance(obj, dict) or set(obj) != required:
        got = sorted(obj) if isinstance(obj, dict) else type(obj).__name__
        return [f"top level must be an object with exactly {sorted(required)}, got {got}"]
    if not isinstance(obj["refused"], bool):
        return ["refused must be a boolean"]
    if not isinstance(obj["subject_supported"], bool):
        return ["subject_supported must be a boolean"]
    # The subject's length is reported (`subject_words`), never a rejection reason: a 15-word
    # cap on this field rejected two of the thirty evaluation answers unread, on a field no
    # downstream consumer reads (RR-24). The caps that stay bound what the judge reads.
    if not isinstance(obj["subject"], str) or not obj["subject"].strip():
        fails.append("subject must be a non-empty string")
    # Internal consistency, not grounding: an answer that judges its own subject unsupported and
    # then answers anyway is contradicting itself on the same page, and the retry exists for
    # exactly that. Whether the judgement was *right* is RAG_QUALITY's abstention metric.
    if obj["refused"] == obj["subject_supported"]:
        fails.append(f"refused={obj['refused']} contradicts "
                     f"subject_supported={obj['subject_supported']}: refuse when the subject is "
                     "unsupported, answer when it is supported")
    claims = obj["claims"]
    if not isinstance(claims, list):
        return ["claims must be a list"]
    if len(claims) > int(limits["max_claims"]):
        fails.append(f"claims has {len(claims)} items, limit {limits['max_claims']}")
    for i, c in enumerate(claims):
        if not isinstance(c, dict) or set(c) != {"claim", "citations"}:
            fails.append(f"claims[{i}] must have exactly 'claim' and 'citations'")
            continue
        text, cites = c["claim"], c["citations"]
        if not isinstance(text, str) or not text.strip():
            fails.append(f"claims[{i}].claim must be a non-empty string")
        elif word_count(text) > int(limits["claim_max_words"]):
            fails.append(f"claims[{i}].claim has {word_count(text)} words, limit "
                         f"{limits['claim_max_words']}")
        if not isinstance(cites, list):
            fails.append(f"claims[{i}].citations must be a list")
            continue
        if len(cites) > int(limits["max_citations_per_claim"]):
            fails.append(f"claims[{i}].citations has {len(cites)} items, limit "
                         f"{limits['max_citations_per_claim']}")
        for j, cite in enumerate(cites):
            if not isinstance(cite, dict) or set(cite) != {"cite_id", "window"}:
                fails.append(f"claims[{i}].citations[{j}] must have exactly 'cite_id' and "
                             "'window'")
                continue
            if not isinstance(cite["cite_id"], str) or not cite["cite_id"].strip():
                fails.append(f"claims[{i}].citations[{j}].cite_id must be a non-empty string")
            if not isinstance(cite["window"], str) or not cite["window"].strip():
                fails.append(f"claims[{i}].citations[{j}].window must be a non-empty string")

    reason = obj["refusal_reason"]
    if obj["refused"]:
        # A refusal is a deliberate statement that the corpus cannot answer, so it must be
        # *empty*. A hedge carrying claims under a refusal flag is the failure mode ADR-0006
        # names by hand, and it is caught here as well as at the gate.
        if claims:
            fails.append("a refusal must carry no claims")
        if not isinstance(reason, str) or not reason.strip():
            fails.append("a refusal must carry a written reason")
        elif word_count(reason) > int(limits["refusal_reason_max_words"]):
            fails.append(f"refusal_reason has {word_count(reason)} words, limit "
                         f"{limits['refusal_reason_max_words']}")
    elif reason is not None:
        fails.append("refusal_reason must be null when refused is false")
    return fails


# ---------------------------------------------------------------- idempotency ----
def idempotency_key(*, question_id: str, model_id: str, answer_spec_version: str,
                    prompt_version: str, inference_config_hash: str,
                    retrieved_digest: str) -> str:
    """SHA-256 over the length-prefixed identity tuple (ADR-0006).

    `retrieved_digest` carries the ordered retrieved ids *and* their content, so the same
    question answered over a re-ranked or re-indexed set is a different logical inference
    rather than a cache hit wearing the old answer.
    """
    parts = (question_id, model_id, answer_spec_version, prompt_version, inference_config_hash,
             retrieved_digest)
    return _sha256("".join(f"{len(p.encode())}:{p}" for p in parts))


def retrieved_digest(retrieved: list[dict[str, Any]]) -> str:
    """The ordered retrieved set as one hash: ids in rank order, each with its content hash."""
    payload = [{"review_id": r["review_id"], "cite_id": r.get("cite_id"),
                "window": r["window"], "rank": r["rank"],
                "content": _sha256(f"{r.get('title') or ''}\n{r.get('text') or ''}")}
               for r in retrieved]
    return _sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False))


# ------------------------------------------------------------------- the contract ----
def month_in_window(month: str | None, window: dict[str, str]) -> bool:
    """Is `YYYY-MM` inside `[start, end]`? Lexicographic, which is correct for `YYYY-MM`."""
    if not month:
        return False
    return str(window["start"]) <= str(month)[:7] <= str(window["end"])


def contract_violations(answer: dict[str, Any], question: dict[str, Any],
                        retrieved: list[dict[str, Any]]) -> list[str]:
    """Every way this one answer breaks ADR-0006's citation and scope contract.

    The five rules, in the order `CONTRACT_RULES` names them:

    1. **terminal** -- the answer was produced at all. A `parse_failed` or `api_failed` row is
       a violation: a question with no answer cites nothing, and counting it as neutral would
       let a broken generator print 30/30 by never answering.
    2. **refusal_empty** -- a refusal carries no claims, no citations, and a written reason.
    3. **claims_cite** -- a non-refusal carries at least one claim, and every claim carries at
       least one citation. An empty answer that is not a refusal is a non-answer.
    4. **citations_retrieved** -- every cited handle is one this question's own retrieved set
       supplied. A handle the question never showed resolves to no review at all, which is the
       model answering from memory rather than from what it was given.
    5. **citations_in_scope** -- every citation's declared window is one the question declared,
       and that review's stored `review_month` really lies inside it.

    Returns the violations as sentences, so the gate can print the first few verbatim rather
    than reporting a count nobody can act on.
    """
    v: list[str] = []
    qid = question["question_id"]
    status = answer.get("status")
    if status != "succeeded":
        return [f"{qid}: status={status or 'none'}, so the question has no answer to check"]

    parsed = answer.get("parsed") or {}
    refused = bool(parsed.get("refused"))
    claims = parsed.get("claims") or []
    windows = question["scope"]["windows"]
    by_handle = {r.get("cite_id"): r for r in retrieved if r.get("cite_id")}

    if refused:
        if claims:
            v.append(f"{qid}: refusal carries {len(claims)} claim(s)")
        cited = [c for cl in claims for c in (cl.get("citations") or [])]
        if cited:
            v.append(f"{qid}: refusal carries {len(cited)} citation(s)")
        if not str(parsed.get("refusal_reason") or "").strip():
            v.append(f"{qid}: refusal carries no written reason")
        return v

    if not claims:
        v.append(f"{qid}: answer is not a refusal and carries no claims")
    for i, cl in enumerate(claims):
        cites = cl.get("citations") or []
        if not cites:
            v.append(f"{qid}: claim {i + 1} carries no citation")
        for c in cites:
            handle, win = c.get("cite_id"), c.get("window")
            row = by_handle.get(handle)
            if row is None:
                v.append(f"{qid}: claim {i + 1} cites {str(handle)[:16]!r}, which this "
                         "question's retrieved set does not supply")
                continue
            if win not in windows:
                v.append(f"{qid}: claim {i + 1} cites window {win!r}, which the question does "
                         f"not declare ({', '.join(sorted(windows))})")
                continue
            if win != row["window"]:
                v.append(f"{qid}: claim {i + 1} cites {handle} ({row['review_id'][:12]}…) as "
                         f"{win!r}, but it was retrieved for {row['window']!r}")
                continue
            if not month_in_window(row.get("review_month"), windows[win]):
                v.append(f"{qid}: claim {i + 1} cites {handle} ({row['review_id'][:12]}…, "
                         f"{row.get('review_month') or 'no month'}) as {win!r}, outside "
                         f"{windows[win]['start']}..{windows[win]['end']}")
    return v


def rule_of(violation: str) -> str:
    """Which `CONTRACT_RULES` constituent a violation sentence belongs to.

    Kept beside the sentences that produce it so the gate can report per-rule counts without
    the contract check having to return a parallel structure nobody else needs.
    """
    if "has no answer to check" in violation:
        return "terminal"
    if "refusal carries" in violation:
        return "refusal_empty"
    if "carries no claims" in violation or "carries no citation" in violation:
        return "claims_cite"
    if "retrieved set does not supply" in violation:
        return "citations_retrieved"
    return "citations_in_scope"

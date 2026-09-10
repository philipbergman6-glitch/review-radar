"""The frozen labelling spec, its hashes and its idempotency key (ADR-0003, amended by RR-19).

One *logical inference* is one (review, label source, model, spec version, prompt version,
inference config) tuple. Its idempotency key is a local SHA-256 over exactly that tuple,
computed here and never sent anywhere. Two runs of the same work therefore collide on the
key and the second is a cache hit, not a second row.

The inference config hash covers only *identity* settings -- temperature, output length,
thinking, seed, system prompt, output schema and the validation limits (a looser word limit
accepts responses a stricter one rejected, so it is part of what the label means). Execution
settings (host URL, timeouts, how
many reviews a run processes) are deliberately outside it, exactly as the embedding spec
separates identity from execution (ADR-0005).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

SPEC_PATH = PROJECT_ROOT / "conf" / "theme-label-spec.json"
# `agent_reference` is ground truth written by the in-session agent (RR-21). It is kept
# distinct from `local_llm` (the system under test) and from `human` (reserved for Philip)
# so no query can mistake agent labels for hand labels.
LABEL_SOURCES = ("local_llm", "hosted_llm", "agent_reference", "human", "classifier")
LABEL_STATUSES = ("succeeded", "model_abstained", "parse_failed", "api_failed")
BUDGET_LINES = ("discovery", "development", "training_pool", "audit", "inference")


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True)
class Prompt:
    name: str
    version: str
    path: Path
    text: str
    hash: str


@dataclass(frozen=True)
class FrozenPrompt:
    """The one prompt ADR-0003 lets downstream work depend on, recorded by ticket 06's freeze."""

    name: str
    version: str
    freeze_commit: str
    decided_in: str


@dataclass(frozen=True)
class LabelSpec:
    label_spec_version: str
    host: str
    endpoint: str
    model_id: str
    comparison_model_id: str
    api_mode: str
    inference: dict[str, Any]
    limits: dict[str, Any]
    prompts: dict[str, Prompt]
    frozen: FrozenPrompt | None
    raw: dict[str, Any]

    def require_frozen(self, *, purpose: str, prompt_name: str | None = None) -> FrozenPrompt:
        """The frozen prompt, hard-failing if there is none -- or if `prompt_name` is not it.

        The classifier's pass rule (ADR-0002) compares it against the labeller's own numbers,
        which were measured under one prompt. A teacher labelled by any other prompt makes that
        comparison meaningless, and it would do so silently -- the rows land in the same table
        and differ only by a hash -- so the refusal happens before the run, not in review.

        `prompt_name=None` asks only that a freeze exists, for a caller that reads the frozen
        prompt out of the spec rather than being handed one.
        """
        if self.frozen is None:
            raise ValueError(f"{purpose} needs the frozen prompt, but conf/theme-label-spec.json "
                             "carries no frozen_prompt block; run the freeze first (ticket 06)")
        if prompt_name is not None and prompt_name != self.frozen.name:
            raise ValueError(f"{purpose} must use the frozen prompt {self.frozen.name!r} "
                             f"({self.frozen.version}, frozen in {self.frozen.decided_in}), "
                             f"got {prompt_name!r}")
        return self.frozen

    def config_hash(self, prompt_name: str, schema: dict[str, Any], *,
                    extra: dict[str, Any] | None = None, model_id: str | None = None) -> str:
        """Identity of one inference configuration: settings + system prompt + output schema.

        `extra` carries identity material that lives outside this file -- the theme taxonomy's
        file hash, which is half of what a theme label *means* and is rendered into the system
        prompt at run time rather than copied into the prompt file. ADR-0003 requires it in the
        hash. `model_id` overrides the primary model, so the `llama3.2:3b` comparison row
        cannot collide with `qwen3:8b` on an idempotency key.
        """
        p = self.prompts[prompt_name]
        payload = {"inference": self.inference, "system": p.text, "prompt_version": p.version,
                   "schema": schema, "model_id": model_id or self.model_id, "limits": self.limits,
                   "label_spec_version": self.label_spec_version}
        if extra:
            payload["extra"] = extra
        return _sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False))


def load_spec(path: Path = SPEC_PATH) -> LabelSpec:
    doc = json.loads(path.read_text())
    if doc["host"] != "ollama":
        raise ValueError(f"only the local ollama host is provisioned (RR-19), got {doc['host']!r}")
    if doc["api_mode"] != "local":
        raise ValueError(f"api_mode must be 'local' under RR-19, got {doc['api_mode']!r}")
    if float(doc["inference"]["temperature"]) != 0.0:
        raise ValueError("inference.temperature must be 0: frozen labels must be reproducible")
    prompts = {}
    for name, p in doc["prompts"].items():
        f = PROJECT_ROOT / p["path"]
        text = f.read_text()
        if not text.strip():
            raise ValueError(f"prompt {name} at {p['path']} is empty")
        prompts[name] = Prompt(name=name, version=p["version"], path=f, text=text, hash=_sha256(text))
    frozen = None
    if (fp := doc.get("frozen_prompt")):
        if fp["name"] not in prompts:
            raise ValueError(f"frozen_prompt names {fp['name']!r}, which is not a declared prompt")
        if prompts[fp["name"]].version != fp["version"]:
            raise ValueError(f"frozen_prompt says {fp['version']!r} but prompt {fp['name']!r} is "
                             f"{prompts[fp['name']].version!r}; the freeze record has drifted")
        frozen = FrozenPrompt(name=fp["name"], version=fp["version"],
                              freeze_commit=fp["freeze_commit"], decided_in=fp["decided_in"])
    return LabelSpec(
        label_spec_version=str(doc["label_spec_version"]), host=doc["host"], endpoint=doc["endpoint"],
        model_id=doc["model_id"], comparison_model_id=doc["comparison_model_id"],
        api_mode=doc["api_mode"], inference=doc["inference"], limits=doc["limits"],
        prompts=prompts, frozen=frozen, raw=doc)


def idempotency_key(*, source_review_id: str, label_source: str, model_id: str,
                    label_spec_version: str, prompt_version: str, inference_config_hash: str) -> str:
    """SHA-256 over the length-prefixed tuple; length prefixes stop field-boundary collisions."""
    if label_source not in LABEL_SOURCES:
        raise ValueError(f"label_source must be one of {LABEL_SOURCES}, got {label_source!r}")
    parts = (source_review_id, label_source, model_id, label_spec_version, prompt_version,
             inference_config_hash)
    payload = "".join(f"{len(p.encode())}:{p}" for p in parts)
    return _sha256(payload)


# --------------------------------------------------------------- validation ----
def normalise(text: str | None) -> str:
    """Whitespace-collapsed, case-folded text for evidence checks; the stored quote is raw."""
    return " ".join((text or "").split()).casefold()


def quote_is_evidence(quote: str, title: str | None, text: str | None) -> bool:
    """The quote must appear verbatim (up to whitespace and case) in the title or the text."""
    q = normalise(quote)
    if not q:
        return False
    return q in normalise(title) or q in normalise(text)


def word_count(s: str) -> int:
    return len(s.split())


def validate_discovery(obj: Any, *, title: str | None, text: str | None, limits: dict[str, Any],
                       ) -> list[str]:
    """Semantic checks on a discovery response. Returns named failures; never repairs."""
    fails: list[str] = []
    if not isinstance(obj, dict) or set(obj) != {"complaints"}:
        return [f"top level must be an object with exactly 'complaints', got {sorted(obj) if isinstance(obj, dict) else type(obj).__name__}"]
    items = obj["complaints"]
    if not isinstance(items, list):
        return ["complaints must be a list"]
    if len(items) > int(limits["discovery_max_items"]):
        fails.append(f"complaints has {len(items)} items, limit {limits['discovery_max_items']}")
    seen: set[str] = set()
    for i, it in enumerate(items):
        if not isinstance(it, dict) or set(it) != {"quote", "aspect"}:
            fails.append(f"complaints[{i}] must have exactly 'quote' and 'aspect'")
            continue
        q, a = it["quote"], it["aspect"]
        if not isinstance(q, str) or not isinstance(a, str):
            fails.append(f"complaints[{i}] quote and aspect must be strings")
            continue
        if word_count(q) > int(limits["discovery_quote_max_words"]) or not q.strip():
            fails.append(f"complaints[{i}] quote has {word_count(q)} words, limit "
                         f"{limits['discovery_quote_max_words']}")
        if word_count(a) > int(limits["discovery_aspect_max_words"]) or not a.strip():
            fails.append(f"complaints[{i}] aspect has {word_count(a)} words, limit "
                         f"{limits['discovery_aspect_max_words']}")
        if not quote_is_evidence(q, title, text):
            fails.append(f"complaints[{i}] quote is not present in the review: {q[:60]!r}")
        key = normalise(q)
        if key in seen:
            fails.append(f"complaints[{i}] repeats an earlier quote")
        seen.add(key)
    return fails


DISCOVERY_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["complaints"],
    "properties": {
        "complaints": {
            "type": "array",
            "maxItems": 5,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["quote", "aspect"],
                "properties": {"quote": {"type": "string"}, "aspect": {"type": "string"}},
            },
        }
    },
}


# ------------------------------------------------------- the frozen label contract ----
LABEL_SCHEMA_PATH = PROJECT_ROOT / "conf" / "complaint-theme-label.schema.json"
TAXONOMY_PATH = PROJECT_ROOT / "conf" / "theme-taxonomy.json"
SENTIMENTS = ("positive", "negative", "mixed", "none")
CONFIDENCES = ("high", "medium", "low")


@dataclass(frozen=True)
class Taxonomy:
    version: str
    themes: list[dict[str, Any]]
    file_hash: str

    @property
    def ids(self) -> list[str]:
        return [t["id"] for t in self.themes]


def load_taxonomy(path: Path = TAXONOMY_PATH) -> Taxonomy:
    raw = path.read_bytes()
    doc = json.loads(raw)
    ids = [t["id"] for t in doc["themes"]]
    if len(set(ids)) != len(ids):
        raise ValueError("the frozen taxonomy repeats a theme id")
    return Taxonomy(version=str(doc["taxonomy_version"]), themes=doc["themes"],
                    file_hash=_sha256(raw.decode()))


def render_taxonomy(tax: Taxonomy) -> str:
    """The taxonomy as prompt text, rendered deterministically from the frozen file.

    The definitions live in one place. Copying them into a prompt file would let the two
    drift, and a labeller reading a stale definition is a silent measurement error, so the
    prompt is assembled at run time and the taxonomy's file hash goes into the inference
    config hash beside the system prompt and the output schema.
    """
    out = []
    for t in tax.themes:
        out.append(f"### {t['id']}  ({t['name']})")
        out.append(t["definition"])
        out.append("Counts as this theme:")
        out.extend(f"  - {x}" for x in t["includes"])
        out.append("Does NOT count as this theme:")
        out.extend(f"  - {x}" for x in t["excludes"])
        if t.get("boundary_note"):
            out.append(f"Boundary: {t['boundary_note']}")
        out.append("")
    return "\n".join(out).rstrip()


def decoding_schema(theme_ids: list[str], *, max_themes: int | None = None) -> dict[str, Any]:
    """What Ollama constrains decoding to. Deliberately weaker than the validation contract.

    Two things in `conf/complaint-theme-label.schema.json` are dropped here on purpose:

    * **the quote-length `pattern`.** Measured on the discovery run: enforcing an 8-word
      limit as a decoding pattern made `qwen3:8b` paraphrase the review to fit the pattern,
      and the evidence check then correctly rejected the paraphrase as invented. A length
      rule belongs to validation, where a violation is a recorded failure, not to decoding,
      where it silently deforms the answer.
    * **the `if`/`then` conditionals** (the abstention rules and `other.phrase` required iff
      `other.present`). Constrained decoding over conditional subschemas is not something the
      server promises; the validator enforces them exactly, and a breach is a named failure.

    The theme-id enum is kept: membership in the frozen taxonomy is what the labels *mean*,
    and constraining it cannot deform an answer the way a length rule can. The `description`
    strings are guidance the server passes through to the model, not constraints: nothing is
    rejected or truncated by them, so they cannot deform an answer either.

    `max_themes` caps the array length. Unlike a length rule on a quote it cannot deform what
    the model writes -- it can only stop it adding a fourth theme, which the prompt already
    asks it not to do. Default None leaves the cap at the taxonomy size, so every prompt
    version that ran before the cap existed keeps exactly the config hash it ran under.
    """
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["themes", "other", "abstain", "overall_sentiment", "label_confidence"],
        "properties": {
            "themes": {
                "type": "array", "maxItems": max_themes or len(theme_ids), "uniqueItems": True,
                "items": {"type": "object", "additionalProperties": False,
                          "required": ["theme_id", "evidence_quote"],
                          "properties": {
                              "theme_id": {
                                  "enum": list(theme_ids),
                                  "description": "A theme id from the frozen taxonomy. Use each "
                                                 "id at most once in this array."},
                              "evidence_quote": {
                                  "type": "string",
                                  "description": "A short span copied character-for-character "
                                                 "from the review title or text. 4 to 8 words "
                                                 "is right; more than 15 words is rejected. "
                                                 "Copy the fragment that carries the complaint, "
                                                 "not the whole sentence."}}},
            },
            "other": {"type": "object", "additionalProperties": False,
                      "required": ["present", "phrase"],
                      "properties": {"present": {"type": "boolean"},
                                     "phrase": {"type": ["string", "null"]}}},
            "abstain": {"type": "boolean"},
            "overall_sentiment": {"enum": list(SENTIMENTS)},
            "label_confidence": {"enum": list(CONFIDENCES)},
        },
    }


def validate_label(obj: Any, *, title: str | None, text: str | None, theme_ids: list[str],
                   limits: dict[str, Any]) -> list[str]:
    """Every semantic rule of ADR-0003's output contract. Returns named failures; never repairs."""
    fails: list[str] = []
    required = {"themes", "other", "abstain", "overall_sentiment", "label_confidence"}
    if not isinstance(obj, dict) or set(obj) != required:
        got = sorted(obj) if isinstance(obj, dict) else type(obj).__name__
        return [f"top level must be an object with exactly {sorted(required)}, got {got}"]
    if not isinstance(obj["abstain"], bool):
        fails.append("abstain must be a boolean")
    if obj["overall_sentiment"] not in SENTIMENTS:
        fails.append(f"overall_sentiment {obj['overall_sentiment']!r} is not one of {list(SENTIMENTS)}")
    if obj["label_confidence"] not in CONFIDENCES:
        fails.append(f"label_confidence {obj['label_confidence']!r} is not one of {list(CONFIDENCES)}")

    items = obj["themes"]
    if not isinstance(items, list):
        fails.append("themes must be a list")
        items = []
    seen: set[str] = set()
    for i, it in enumerate(items):
        if not isinstance(it, dict) or set(it) != {"theme_id", "evidence_quote"}:
            fails.append(f"themes[{i}] must have exactly 'theme_id' and 'evidence_quote'")
            continue
        tid, quote = it["theme_id"], it["evidence_quote"]
        if tid not in theme_ids:
            fails.append(f"themes[{i}] theme_id {tid!r} is not in the frozen taxonomy")
        if tid in seen:
            fails.append(f"themes[{i}] repeats theme_id {tid!r}")
        seen.add(tid)
        if not isinstance(quote, str) or not quote.strip():
            fails.append(f"themes[{i}] evidence_quote must be a non-empty string")
            continue
        limit = int(limits["label_quote_max_words"])
        if word_count(quote) > limit:
            fails.append(f"themes[{i}] evidence_quote has {word_count(quote)} words, limit {limit}")
        if not quote_is_evidence(quote, title, text):
            fails.append(f"themes[{i}] evidence_quote is not present in the review: {quote[:60]!r}")

    other = obj["other"]
    if not isinstance(other, dict) or set(other) != {"present", "phrase"}:
        fails.append("other must be an object with exactly 'present' and 'phrase'")
    elif not isinstance(other["present"], bool):
        fails.append("other.present must be a boolean")
    elif other["present"]:
        phrase = other["phrase"]
        limit = int(limits["other_phrase_max_words"])
        if not isinstance(phrase, str) or not phrase.strip():
            fails.append("other.phrase is required when other.present is true")
        elif word_count(phrase) > limit:
            fails.append(f"other.phrase has {word_count(phrase)} words, limit {limit}")
    elif other["phrase"] is not None:
        fails.append("other.phrase must be null when other.present is false")

    if obj["abstain"] is True:
        # An abstention is a deliberate refusal to label, not a failure: it must be *empty*,
        # or a partial answer would be counted as one.
        if items:
            fails.append("an abstention must carry no themes")
        if isinstance(other, dict) and (other.get("present") or other.get("phrase") is not None):
            fails.append("an abstention must have other={present: false, phrase: null}")
        if obj["label_confidence"] != "low":
            fails.append("an abstention must have label_confidence 'low'")
    return fails

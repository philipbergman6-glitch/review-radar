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
    raw: dict[str, Any]

    def config_hash(self, prompt_name: str, schema: dict[str, Any]) -> str:
        """Identity of one inference configuration: settings + system prompt + output schema."""
        p = self.prompts[prompt_name]
        payload = {"inference": self.inference, "system": p.text, "prompt_version": p.version,
                   "schema": schema, "model_id": self.model_id, "limits": self.limits,
                   "label_spec_version": self.label_spec_version}
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
    return LabelSpec(
        label_spec_version=str(doc["label_spec_version"]), host=doc["host"], endpoint=doc["endpoint"],
        model_id=doc["model_id"], comparison_model_id=doc["comparison_model_id"],
        api_mode=doc["api_mode"], inference=doc["inference"], limits=doc["limits"],
        prompts=prompts, raw=doc)


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

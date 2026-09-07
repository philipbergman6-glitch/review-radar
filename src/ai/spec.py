"""The embedding identity (ADR-0005): conf/embedding-spec.json, hashed over `identity` only.

The hash names the vectors: same hash, same vector for the same review. Execution
settings (batch size, device, threads) live under `execution_defaults`, are recorded in
the run manifest and are deliberately outside the hash.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common import config as C

SPEC_PATH = C.PROJECT_ROOT / "conf" / "embedding-spec.json"
IDENTITY_KEYS = ("model", "revision", "max_seq_length", "normalize", "dims", "similarity",
                 "text_prep_version", "cohort_min_words")


@dataclass(frozen=True)
class EmbeddingSpec:
    version: str
    identity: dict[str, Any]
    execution: dict[str, Any]
    path: Path

    @property
    def hash(self) -> str:
        canonical = json.dumps(self.identity, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    @property
    def model(self) -> str:
        return self.identity["model"]

    @property
    def dims(self) -> int:
        return int(self.identity["dims"])

    @property
    def min_words(self) -> int:
        return int(self.identity["cohort_min_words"])

    def rel_path(self) -> str:
        return str(self.path.relative_to(C.PROJECT_ROOT))


def load_spec(path: Path = SPEC_PATH) -> EmbeddingSpec:
    raw = json.loads(path.read_text())
    for key in ("spec_version", "identity", "execution_defaults"):
        if key not in raw:
            raise ValueError(f"{path}: missing top-level key {key!r}")
    ident = raw["identity"]
    missing = [k for k in IDENTITY_KEYS if k not in ident]
    if missing:
        raise ValueError(f"{path}: identity missing {missing}")
    if ident["similarity"] != "cosine" or ident["normalize"] is not True:
        raise ValueError(f"{path}: the ES field is cosine over unit vectors; normalize must be true")
    if int(ident["dims"]) != 384:
        raise ValueError(f"{path}: dims {ident['dims']} != 384 declared in conf/es/reviews.contract.json")
    return EmbeddingSpec(str(raw["spec_version"]), ident, raw["execution_defaults"], path)


def prepare_text(title: str | None, text: str) -> str:
    """text_prep_version 1: title + '. ' + text when the title is non-blank; whitespace collapsed."""
    t = " ".join((title or "").split())
    body = " ".join(text.split())
    return f"{t}. {body}" if t else body

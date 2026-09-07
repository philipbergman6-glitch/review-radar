"""Mapping contracts for the Elasticsearch serving projections (ADR-0004).

A contract file under conf/es/ carries the index settings (analyzers included), a
`"dynamic": "strict"` mapping, the list of fields every document must carry, and the
analyzer test cases. Elasticsearch enforces the mapping; it does not enforce presence, so
`validate_doc` is the indexer-side half of the contract and every document passes through
it before it is sent. `contract_hash` names the exact contract a run was built under.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common import config as C

CONTRACT_DIR = C.PROJECT_ROOT / "conf" / "es"


class ContractViolation(ValueError):
    pass


@dataclass(frozen=True)
class Contract:
    name: str
    path: Path
    version: str
    alias: str
    id_field: str
    required: tuple[str, ...]
    settings: dict[str, Any]
    mappings: dict[str, Any]
    analyzer_tests: tuple[dict[str, Any], ...]
    raw: dict[str, Any]

    @property
    def fields(self) -> frozenset[str]:
        return frozenset(self.mappings["properties"])

    @property
    def hash(self) -> str:
        canonical = json.dumps(self.raw, sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(canonical).hexdigest()

    @property
    def analyzers(self) -> tuple[str, ...]:
        return tuple(self.settings.get("analysis", {}).get("analyzer", {}))

    def body(self) -> dict[str, Any]:
        return {"settings": self.settings, "mappings": self.mappings}

    def rel_path(self) -> str:
        return str(self.path.relative_to(C.PROJECT_ROOT))


def load_contract(name: str) -> Contract:
    path = CONTRACT_DIR / f"{name}.contract.json"
    raw = json.loads(path.read_text())
    for key in ("contract_version", "alias", "id_field", "required", "settings", "mappings"):
        if key not in raw:
            raise ContractViolation(f"{path}: missing top-level key {key!r}")
    if raw["mappings"].get("dynamic") != "strict":
        raise ContractViolation(f"{path}: mappings.dynamic must be 'strict'")
    props = raw["mappings"]["properties"]
    unknown_required = [f for f in raw["required"] if f not in props]
    if unknown_required:
        raise ContractViolation(f"{path}: required fields not in mapping: {unknown_required}")
    if raw["id_field"] not in props:
        raise ContractViolation(f"{path}: id_field {raw['id_field']!r} not in mapping")
    return Contract(name=name, path=path, version=str(raw["contract_version"]), alias=raw["alias"],
                    id_field=raw["id_field"], required=tuple(raw["required"]),
                    settings=raw["settings"], mappings=raw["mappings"],
                    analyzer_tests=tuple(raw.get("analyzer_tests", [])), raw=raw)


def validate_doc(contract: Contract, doc: dict[str, Any]) -> None:
    """Hard-fail on a missing required field or a field the mapping does not know."""
    missing = [f for f in contract.required if doc.get(f) is None]
    if missing:
        raise ContractViolation(f"{contract.name}: document {doc.get(contract.id_field)!r} "
                                f"missing required fields {missing}")
    unknown = [k for k in doc if k not in contract.fields]
    if unknown:
        raise ContractViolation(f"{contract.name}: document {doc.get(contract.id_field)!r} "
                                f"carries fields outside the contract {unknown}")


def run_analyzer_tests(es, index: str, contract: Contract) -> list[dict[str, Any]]:
    """Execute every analyzer case against a live index; returns one result per case."""
    results = []
    for case in contract.analyzer_tests:
        resp = es.indices.analyze(index=index, analyzer=case["analyzer"], text=case["text"])
        got = [t["token"] for t in resp["tokens"]]
        results.append({**case, "got": got, "ok": got == case["tokens"]})
    return results

"""The evaluation artefact contract, and the table assembled from it (ADR-0011, RR-17).

Every capability gate writes one artefact to `eval/<capability>/gate.json`, validated
against `conf/eval-artifact.schema.json`. `conf/lineage_chain.toml` declares which
capabilities the submission claims. `make eval-table` joins the two and prints one row per
capability: value, threshold, verdict, scope, run id.

The rules the join enforces, and why each exists:

* **A missing artefact for a declared capability exits non-zero.** A capability is either a
  number or a written reason; silence would let an unfinished phase look like an absent one.
* **`status = "cut"` renders `NOT_RUN` with its mandatory `cut_reason` and passes.**
  "Not reached by submission date" is a legitimate reason, written as such.
* **`scope = "sample"` renders `NOT_RUN`, never `PASS`.** A phase is complete only at full
  scope; a sample run is a smoke test wearing a verdict.
* **Reproducibility and quality render in separate sections.** Only reproducibility blocks
  a phase; a missed quality bar is a result, not a broken pipeline. Mixing them is what
  makes a disappointing number look like a reason to reopen a frozen protocol.

The functions here are pure over already-loaded artefacts -- no Spark, no Elasticsearch, no
Postgres, no filesystem beyond the two config files. `scripts/eval_table.py` does the I/O.
"""
from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema

from src.common.config import PROJECT_ROOT

SCHEMA_PATH = PROJECT_ROOT / "conf" / "eval-artifact.schema.json"
CHAIN_PATH = PROJECT_ROOT / "conf" / "lineage_chain.toml"
EVAL_ROOT = PROJECT_ROOT / "eval"

ARTIFACT_VERSION = "1"
VERDICTS = ("PASS", "FAIL", "REPORTED", "NOT_RUN")
KINDS = ("reproducibility", "quality")
CHAIN_STATUSES = ("declared", "cut")

#: Sentinel for "this key is absent", so a caller can build an artefact dict without one.
OMIT = object()

#: Rendered in place of a verdict when the artefact that should carry one is not there.
MISSING = "MISSING"


# ------------------------------------------------------------------- contract ----
def load_schema() -> dict[str, Any]:
    """A fresh copy of the frozen contract, so no caller can mutate it for the next."""
    return json.loads(SCHEMA_PATH.read_text())


def validate_artifact(doc: Any) -> list[str]:
    """Named failures against `conf/eval-artifact.schema.json`. Never repairs."""
    validator = jsonschema.Draft202012Validator(load_schema())
    fails = []
    for err in sorted(validator.iter_errors(doc), key=lambda e: list(e.absolute_path)):
        where = ".".join(str(p) for p in err.absolute_path) or "<root>"
        fails.append(f"{where}: {err.message}")
    return fails


# ---------------------------------------------------------------------- chain ----
@dataclass(frozen=True)
class Capability:
    id: str
    phase: str
    gate_name: str
    kind: str
    status: str
    cut_reason: str | None
    jobs: tuple[str, ...]

    @property
    def artifact_path(self) -> Path:
        """Relative to the project root, so the same string prints and resolves."""
        return Path("eval") / self.id / "gate.json"


def load_chain(path: Path = CHAIN_PATH) -> tuple[Capability, ...]:
    """The declared chain. Hard-fails on a malformed declaration rather than skipping it."""
    with path.open("rb") as f:
        doc = tomllib.load(f)
    entries = doc.get("capability")
    if not entries:
        raise ValueError(f"{path} declares no capabilities")
    out: list[Capability] = []
    seen: set[str] = set()
    for i, e in enumerate(entries):
        for key in ("id", "phase", "gate_name", "kind"):
            if not str(e.get(key, "")).strip():
                raise ValueError(f"capability[{i}] is missing {key}")
        cap_id = e["id"]
        if cap_id in seen:
            raise ValueError(f"capability id {cap_id!r} is declared twice")
        seen.add(cap_id)
        if e["kind"] not in KINDS:
            raise ValueError(f"capability {cap_id}: kind must be one of {KINDS}, got {e['kind']!r}")
        status = e.get("status", "declared")
        if status not in CHAIN_STATUSES:
            raise ValueError(f"capability {cap_id}: status must be one of {CHAIN_STATUSES}, "
                             f"got {status!r}")
        reason = e.get("cut_reason")
        if status == "cut" and not str(reason or "").strip():
            raise ValueError(f"capability {cap_id}: status is cut, so cut_reason is mandatory")
        if status == "declared" and reason:
            raise ValueError(f"capability {cap_id}: cut_reason belongs to a cut capability only")
        out.append(Capability(id=cap_id, phase=e["phase"], gate_name=e["gate_name"],
                              kind=e["kind"], status=status, cut_reason=reason,
                              jobs=tuple(e.get("jobs", ()))))
    return tuple(out)


# ----------------------------------------------------------------------- rows ----
@dataclass(frozen=True)
class Row:
    capability: str
    phase: str
    gate_name: str
    kind: str
    metric: str
    value: str
    threshold: str
    verdict: str
    scope: str
    run_id: str
    note: str


def fmt_number(v: float) -> str:
    """Whole numbers print whole; fractions keep at least two decimals beside a threshold."""
    f = float(v)
    if f.is_integer():
        return str(int(f))
    s = f"{f:.4f}".rstrip("0")
    whole, _, frac = s.partition(".")
    return f"{whole}.{frac.ljust(2, '0')}"


def _blank(cap: Capability, verdict: str, note: str, *, scope: str = "-") -> Row:
    return Row(capability=cap.id, phase=cap.phase, gate_name=cap.gate_name, kind=cap.kind,
               metric="-", value="-", threshold="-", verdict=verdict, scope=scope,
               run_id="-", note=note)


def build_rows(chain, artifacts: dict[str, dict | None]) -> tuple[list[Row], list[str]]:
    """One row per declared capability, plus every reason the table is not trustworthy.

    `artifacts` maps capability id to the loaded artefact, or None when its file is absent.
    """
    rows: list[Row] = []
    errors: list[str] = []
    for cap in chain:
        doc = artifacts.get(cap.id)
        if cap.status == "cut":
            if doc is not None:
                errors.append(f"{cap.id}: declared cut but {cap.artifact_path} exists -- "
                              f"a cut capability has no result to publish")
            rows.append(_blank(cap, "NOT_RUN", cap.cut_reason or ""))
            continue
        if doc is None:
            errors.append(f"{cap.id}: {cap.artifact_path} is missing and the capability is not "
                          f"declared cut in {CHAIN_PATH.name}")
            rows.append(_blank(cap, MISSING, "no artefact, no cut_reason"))
            continue
        fails = validate_artifact(doc)
        if doc.get("capability") != cap.id:
            fails.append(f"capability: artefact says {doc.get('capability')!r}, filed under "
                         f"{cap.id!r}")
        if doc.get("kind") != cap.kind:
            fails.append(f"kind: artefact says {doc.get('kind')!r}, chain declares {cap.kind!r}")
        if doc.get("gate_name") != cap.gate_name:
            fails.append(f"gate_name: artefact says {doc.get('gate_name')!r}, chain declares "
                         f"{cap.gate_name!r}")
        if fails:
            errors.extend(f"{cap.id}: {f}" for f in fails)
            rows.append(_blank(cap, MISSING, "artefact does not validate"))
            continue
        rows.append(_row(cap, doc))
    return rows, errors


def _row(cap: Capability, doc: dict) -> Row:
    metric = doc.get("metric") or {}
    scope = doc["scope"]
    verdict = doc["status"]
    note = doc.get("cut_reason") or ""
    if scope == "sample" and verdict != "NOT_RUN":
        # A phase is complete only at full scope (ADR-0011). Never PASS on a smoke test.
        note = f"scope=sample, so {verdict} is not a completed phase; rerun at scope=full"
        verdict = "NOT_RUN"
    interval = metric.get("interval")
    if interval:
        note = (note + "; " if note else "") + \
            f"ci=[{fmt_number(interval[0])}, {fmt_number(interval[1])}]"
    return Row(
        capability=cap.id, phase=cap.phase, gate_name=cap.gate_name, kind=cap.kind,
        metric=metric.get("name", "-"),
        value=fmt_number(metric["value"]) if "value" in metric else "-",
        threshold=fmt_number(metric["threshold"]) if metric.get("threshold") is not None else "-",
        verdict=verdict, scope=scope, run_id=doc.get("pipeline_run_id") or "-", note=note)


# -------------------------------------------------------------------- render ----
HEADERS = ("phase", "capability", "gate", "metric", "value", "bar", "verdict", "scope",
           "run_id", "note")


def _table(rows: list[Row]) -> list[str]:
    if not rows:
        return ["  (none declared)"]
    cells = [list(HEADERS)] + [
        [r.phase, r.capability, r.gate_name, r.metric, r.value, r.threshold, r.verdict,
         r.scope, r.run_id[:8], r.note] for r in rows]
    widths = [max(len(c[i]) for c in cells) for i in range(len(HEADERS))]
    # The note is last and free-form: pad every column before it, never it.
    lines = []
    for c in cells:
        head = "  " + "  ".join(c[i].ljust(widths[i]) for i in range(len(HEADERS) - 1))
        lines.append(f"{head}  {c[-1]}" if c[-1] else head.rstrip())
    return lines


def render(rows: list[Row], errors: list[str]) -> str:
    """The whole table: blocking claims, then reported ones, then a terminal line."""
    out: list[str] = []
    out.append("Reproducibility -- these block their phase")
    out += _table([r for r in rows if r.kind == "reproducibility"])
    out.append("")
    out.append("Quality -- reported beside their bars, never blocking")
    out += _table([r for r in rows if r.kind == "quality"])
    out.append("")
    for e in errors:
        out.append(f"EVAL_TABLE_ERROR {e}")
    counted = [r for r in rows if r.verdict != MISSING]
    out.append(f"EVAL_TABLE={'INCOMPLETE' if errors else 'OK'} "
               f"capabilities={len(rows)} rendered={len(counted)} errors={len(errors)}")
    return "\n".join(out)

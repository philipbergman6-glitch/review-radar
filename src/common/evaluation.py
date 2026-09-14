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
* **A prior run's artefact renders as a prior row, never as the capability's verdict.** A
  sanctioned reopen (RR-24) archives the run it supersedes as `eval/<capability>/gate.<n>.json`;
  the table shows it beside the current row, marked superseded, so the first result on
  record is never erased -- and it never stands in when the current artefact is missing.

The functions here are pure over already-loaded artefacts -- no Spark, no Elasticsearch, no
Postgres, no filesystem beyond the two config files. `scripts/eval_table.py` does the I/O.

The writer side lives here too. `Verdict` is what a gate decided plus the exact lines it
prints saying so; `src/gates/*.py` holds one pure verdict function per capability, taking
already-loaded facts and returning that. `build_artifact` turns a verdict into a document
and `write_artifact` validates it before it reaches disk -- an artefact that does not match
the contract is a crash at the gate, never a bad row in the table.
"""
from __future__ import annotations

import json
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
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

#: Which run of the upstream job an edge may name: the chain's pin, or the one recorded.
UPSTREAM_PINS = ("chain", "recorded")

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


# ------------------------------------------------------------------ verdicts ----
@dataclass(frozen=True)
class Verdict:
    """What a gate decided, and the exact lines it prints saying so.

    Built by a pure function over already-loaded facts -- no services, no clock, no
    filesystem. `constituents` are the gate's named lines verbatim and `terminal` is its
    single `<NAME>_GATE=PASS|FAIL` line, so `emit()` reproduces the gate's output byte for
    byte. That is what makes the split a prefactor rather than a rewrite, and what lets a
    unit test assert the printed shape with nothing running.

    `checks` is the named constituent verdicts the terminal line summarises. A gate with no
    checks cannot return a verdict at all: a gate that cannot fail is not a gate (audit F3).
    """
    gate_name: str
    status: str
    constituents: tuple[str, ...]
    terminal: str
    metric: dict[str, Any]
    checks: tuple[tuple[str, bool], ...] = ()
    cut_reason: str | None = None

    def __post_init__(self) -> None:
        if self.status not in VERDICTS:
            raise ValueError(f"{self.gate_name}: status must be one of {VERDICTS}, "
                             f"got {self.status!r}")
        if not self.terminal.strip():
            raise ValueError(f"{self.gate_name}: a verdict must print a terminal line")
        if self.status == "NOT_RUN" and not str(self.cut_reason or "").strip():
            raise ValueError(f"{self.gate_name}: NOT_RUN must carry a written reason")

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    @property
    def failed(self) -> tuple[str, ...]:
        """The names of the constituents that did not hold, in declaration order."""
        return tuple(name for name, ok in self.checks if not ok)

    @property
    def lines(self) -> tuple[str, ...]:
        return (*self.constituents, self.terminal)

    def emit(self) -> None:
        for line in self.lines:
            print(line)


def constituent_metric(checks: Sequence[tuple[str, bool]]) -> dict[str, Any]:
    """A reproducibility gate's number is its constituent count (ADR-0011).

    Hard-fails on an empty constituent list rather than reporting a vacuous 0/0: the
    exactly-once gate once printed PASS for runs that tested nothing, and that is the
    failure mode this refuses to reproduce.
    """
    if not checks:
        raise ValueError("a gate with no constituents cannot print a verdict")
    return {"name": "constituents_ok", "value": sum(1 for _, ok in checks if ok),
            "threshold": len(checks), "direction": "eq"}


def repro_verdict(gate_name: str, checks: Sequence[tuple[str, bool]], terminal: str,
                  *, constituents: Sequence[str] = ()) -> Verdict:
    """The common reproducibility shape: every named constituent must hold, or the gate fails."""
    checks = tuple(checks)
    metric = constituent_metric(checks)
    return Verdict(gate_name=gate_name,
                   status="PASS" if metric["value"] == metric["threshold"] else "FAIL",
                   constituents=tuple(constituents), terminal=terminal, metric=metric,
                   checks=checks)


# ----------------------------------------------------------------- artefacts ----
def build_artifact(v: Verdict, *, capability: str, phase: str, kind: str, protocol_hash: str,
                   population: dict[str, Any], pipeline_run_id: str, scope: str,
                   model: str | None = None, prompt: str | None = None,
                   notes: Sequence[str] = (), created_at: str | None = None) -> dict[str, Any]:
    """One capability's result as the contract describes it. Does no I/O and no validation."""
    doc: dict[str, Any] = {
        "artifact_version": ARTIFACT_VERSION,
        "capability": capability,
        "phase": phase,
        "kind": kind,
        "gate_name": v.gate_name,
        "protocol_hash": protocol_hash,
        "model": model,
        "prompt": prompt,
        "population": population,
        "pipeline_run_id": pipeline_run_id,
        "scope": scope,
        "status": v.status,
        "constituents": list(v.lines),
        "created_at": created_at or datetime.now(UTC).isoformat(timespec="seconds"),
    }
    # A capability that did not run publishes its reason instead of a number (ADR-0011).
    if v.status == "NOT_RUN":
        doc["cut_reason"] = v.cut_reason
    else:
        doc["metric"] = v.metric
    if notes:
        doc["notes"] = list(notes)
    return doc


def write_artifact(doc: dict[str, Any], *, root: Path = EVAL_ROOT) -> Path:
    """Validate, then write `eval/<capability>/gate.json`. Never writes an invalid artefact."""
    fails = validate_artifact(doc)
    if fails:
        raise ValueError(f"{doc.get('capability')}: evaluation artefact does not match "
                         f"{SCHEMA_PATH.name}: " + "; ".join(fails))
    path = root / doc["capability"] / "gate.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    return path


def record(v: Verdict, *, root: Path = EVAL_ROOT, **kw: Any) -> Path:
    """Build and write in one step -- what every gate script calls after it prints."""
    return write_artifact(build_artifact(v, **kw), root=root)


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


@dataclass(frozen=True)
class Edge:
    """One declared join: the downstream job's named input is the upstream job's output.

    `spec_version` narrows the edge to one contract version of the downstream job -- reviews
    gained its embeddings input at v2 -- and is None when every version must show it.

    `upstream_pin` says which run of the upstream job this edge is allowed to name. The
    default, `chain`, is the strict one: the run the chain pins for that job, which is how a
    downstream built on a stale branch is caught. `recorded` says the downstream chooses --
    a job that draws one frame per run has several live outputs at once, and "whichever ran
    last" names the wrong one -- and then `pin_reason` is mandatory, because the weaker check
    has to be justified in writing rather than configured quietly (ticket 10a).
    """
    downstream: str
    input: str
    upstream: str
    status: str
    cut_reason: str | None
    spec_version: str | None
    upstream_pin: str = "chain"
    pin_reason: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.downstream, self.input)

    @property
    def name(self) -> str:
        return f"{self.downstream}<-{self.upstream}.{self.input}"

    def applies_to(self, spec_version: str) -> bool:
        return self.spec_version is None or self.spec_version == spec_version


@dataclass(frozen=True)
class Attribution:
    """How one table says which run wrote a row, when the snapshot summary cannot.

    ADR-0008 asks every Iceberg snapshot to carry its `run_id` in the snapshot summary, and
    every writer that uses `writeTo` stamps it. A table written by `MERGE INTO` cannot: the
    SQL path has no equivalent of the `snapshot-property.run_id` write option. Declaring the
    column here does not lower the bar -- it names the mechanism the lineage gate must check
    instead, so the link stays checked rather than skipped (ticket 10a).
    """
    table: str
    column: str
    reason: str


def load_attributions(path: Path = CHAIN_PATH) -> dict[str, Attribution]:
    """Tables attributed by column rather than by snapshot summary, keyed by table name."""
    out: dict[str, Attribution] = {}
    for i, a in enumerate(_read_chain(path).get("attribution", [])):
        for key in ("table", "mechanism", "column", "reason"):
            if not str(a.get(key, "")).strip():
                raise ValueError(f"attribution[{i}] is missing {key}")
        if a["mechanism"] != "column":
            raise ValueError(f"attribution[{i}]: the only mechanism the gate knows is 'column', "
                             f"got {a['mechanism']!r}")
        if a["table"] in out:
            raise ValueError(f"attribution for {a['table']} is declared twice")
        out[a["table"]] = Attribution(table=a["table"], column=a["column"], reason=a["reason"])
    return out


def _read_chain(path: Path) -> dict[str, Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


def load_edges(path: Path = CHAIN_PATH) -> tuple[Edge, ...]:
    """The declared edges. Hard-fails on a malformed or duplicated declaration."""
    out: list[Edge] = []
    seen: set[tuple[str, str, str | None]] = set()
    for i, e in enumerate(_read_chain(path).get("edge", [])):
        for key in ("downstream", "input", "upstream"):
            if not str(e.get(key, "")).strip():
                raise ValueError(f"edge[{i}] is missing {key}")
        status = e.get("status", "declared")
        if status not in CHAIN_STATUSES:
            raise ValueError(f"edge[{i}]: status must be one of {CHAIN_STATUSES}, got {status!r}")
        reason = e.get("cut_reason")
        if status == "cut" and not str(reason or "").strip():
            raise ValueError(f"edge[{i}]: status is cut, so cut_reason is mandatory")
        if status == "declared" and reason:
            raise ValueError(f"edge[{i}]: cut_reason belongs to a cut edge only")
        pin = e.get("upstream_pin", "chain")
        if pin not in UPSTREAM_PINS:
            raise ValueError(f"edge[{i}]: upstream_pin must be one of {UPSTREAM_PINS}, "
                             f"got {pin!r}")
        pin_reason = e.get("pin_reason")
        if pin == "recorded" and not str(pin_reason or "").strip():
            raise ValueError(f"edge[{i}]: upstream_pin is recorded, so pin_reason is mandatory")
        if pin == "chain" and pin_reason:
            raise ValueError(f"edge[{i}]: pin_reason belongs to a recorded pin only")
        spec = e.get("spec_version")
        ident = (e["downstream"], e["input"], spec)
        if ident in seen:
            raise ValueError(f"edge {e['downstream']}.{e['input']} is declared twice")
        seen.add(ident)
        out.append(Edge(downstream=e["downstream"], input=e["input"], upstream=e["upstream"],
                        status=status, cut_reason=reason,
                        spec_version=None if spec is None else str(spec),
                        upstream_pin=pin, pin_reason=pin_reason))
    return tuple(out)


def edge_for(edges: Sequence[Edge], downstream: str, input_name: str,
             spec_version: str) -> Edge | None:
    """The declared edge governing one recorded input, or None when none is declared."""
    for e in edges:
        if e.key == (downstream, input_name) and e.applies_to(spec_version):
            return e
    return None


def load_chain(path: Path = CHAIN_PATH) -> tuple[Capability, ...]:
    """The declared chain. Hard-fails on a malformed declaration rather than skipping it."""
    doc = _read_chain(path)
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
    prior: bool = False


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


def build_rows(chain, artifacts: dict[str, dict | None],
               priors: dict[str, list[dict]] | None = None) -> tuple[list[Row], list[str]]:
    """One row per declared capability, plus every reason the table is not trustworthy.

    `artifacts` maps capability id to the loaded artefact, or None when its file is absent.
    `priors` maps capability id to the archived artefacts of runs a reopen superseded, oldest
    first; each renders as a prior row directly under the current one.
    """
    rows: list[Row] = []
    errors: list[str] = []
    for cap in chain:
        doc = artifacts.get(cap.id)
        prior_docs = (priors or {}).get(cap.id, [])
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
        current = _row(cap, doc)
        rows.append(current)
        for i, prior in enumerate(prior_docs, start=1):
            prior_fails = validate_artifact(prior)
            if prior.get("capability") != cap.id:
                prior_fails.append(f"artefact says {prior.get('capability')!r}")
            if prior_fails:
                errors.extend(f"{cap.id}: prior artefact {i}: {f}" for f in prior_fails)
                continue
            rows.append(_prior_row(cap, prior, superseded_by=current.run_id))
    return rows, errors


def _prior_row(cap: Capability, doc: dict, *, superseded_by: str) -> Row:
    row = _row(cap, doc)
    note = (f"prior run, superseded by {superseded_by[:8]} through a sanctioned reopen "
            f"(RR-24); kept as the first result on record")
    return replace(row, note=f"{note}; {row.note}" if row.note else note, prior=True)


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
        [r.phase, f"{r.capability} (prior)" if r.prior else r.capability, r.gate_name,
         r.metric, r.value, r.threshold, r.verdict, r.scope, r.run_id[:8], r.note]
        for r in rows]
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
    current = [r for r in rows if not r.prior]
    counted = [r for r in current if r.verdict != MISSING]
    out.append(f"EVAL_TABLE={'INCOMPLETE' if errors else 'OK'} "
               f"capabilities={len(current)} rendered={len(counted)} "
               f"prior_rows={len(rows) - len(current)} errors={len(errors)}")
    return "\n".join(out)

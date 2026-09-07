"""The run ledger: one `pipeline_runs` row per execution attempt of every job (ADR-0008).

A job is an independently rerunnable command. Its driver calls `start()` *before* doing
anything, gets a UUID `run_id`, stamps that id on every output it writes (Iceberg snapshot
summary, ES document, eval artefact), and then calls `success()` or `failed()`. The row
is the lineage: it names its inputs and outputs by concrete identity (table + snapshot id,
catalogue load id, source sha256), and the outputs name the row back.

Contracts. Each `(job_name, spec_version)` declares the input, output and count keys it
must record and the count identity its numbers must satisfy. Validation returns *named*
failures ("identity: records_in 101 != records_out 80 + ...") and hard-fails by default;
a run whose numbers do not add up is `failed`, never quietly `success`.

Contracts registered here: silver, catalogue_load, bronze_drain, produce. Bronze and
produce are declared so the CHECK list and the registry agree from day one; their drivers
are instrumented in their own sessions (ADR-0008 §3), not in silver's.
"""
from __future__ import annotations

import json
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from src.common import config as C
from src.common.pg import connect

PROJECT_ROOT = C.PROJECT_ROOT

JOB_NAMES: tuple[str, ...] = (
    "produce", "catalogue_load", "bronze_drain", "silver", "gold",
    "search_index_reviews", "search_index_product_month", "embeddings",
    "theme_labels_llm", "theme_classifier_train", "theme_classifier_score", "rag_answers",
)

SILVER_SPEC_VERSION = "1"
CATALOGUE_LOAD_SPEC_VERSION = "1"
BRONZE_SPEC_VERSION = "1"
PRODUCE_SPEC_VERSION = "1"

# Files whose state decides `worktree_dirty`. data/ and notebooks/ are excluded on
# purpose: they are inputs and presentation, not the code that produced the run.
CLEANLINESS_POLICY_VERSION = "1"
CLEANLINESS_PATHS = ("src", "scripts", "conf", "tests", "pyproject.toml", "uv.lock",
                     "Makefile", "run.sh", "docker-compose.yml")


class ContractViolation(RuntimeError):
    def __init__(self, failures: list[str]):
        super().__init__("; ".join(failures))
        self.failures = failures


@dataclass(frozen=True)
class Contract:
    job_name: str
    spec_version: str
    inputs: dict[str, tuple[str, ...]]          # input name -> required keys
    outputs: dict[str, tuple[str, ...]]         # output name -> required keys
    counts: tuple[str, ...]
    identity: Callable[[dict[str, int | None], dict[str, Any]], list[str]] = \
        field(default=lambda records, counts: [])


def _n(d: dict[str, Any], key: str) -> int | None:
    v = d.get(key)
    return None if v is None else int(v)


def _silver_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    ri, ro, rr = records.get("records_in"), records.get("records_out"), records.get("records_rejected")
    removed = _n(counts, "collision_rows_removed")
    if None in (ri, ro, rr, removed):
        fails.append("identity: records_in/records_out/records_rejected/collision_rows_removed "
                     "must all be set on success")
        return fails
    if ri != ro + rr + removed:
        fails.append(f"identity: records_in {ri} != records_out {ro} + records_rejected {rr} "
                     f"+ collision_rows_removed {removed}")
    groups, unres, trows = (_n(counts, "collision_groups"), _n(counts, "unresolvable_groups"),
                            _n(counts, "collision_table_rows"))
    if None not in (groups, unres, trows) and removed != trows - groups + unres:
        fails.append(f"collision_rows_removed {removed} != collision_table_rows {trows} "
                     f"- collision_groups {groups} + unresolvable_groups {unres}")
    ex, conf = _n(counts, "exact_groups"), _n(counts, "conflicting_groups")
    if None not in (ex, conf, unres, groups) and ex + conf + unres != groups:
        fails.append(f"collision classes {ex}+{conf}+{unres} != collision_groups {groups}")
    distinct = _n(counts, "review_id_distinct")
    if distinct is not None and distinct != ro:
        fails.append(f"review_id_distinct {distinct} != records_out {ro}")
    reasons = counts.get("reject_reasons") or {}
    if sum(int(v) for v in reasons.values()) != rr:
        fails.append(f"reject_reasons sum {sum(int(v) for v in reasons.values())} != records_rejected {rr}")
    return fails


def _catalogue_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    src, loaded, final = _n(counts, "source_rows"), _n(counts, "rows_loaded"), _n(counts, "final_count")
    if records.get("records_in") != src:
        fails.append(f"records_in {records.get('records_in')} != source_rows {src}")
    if not (records.get("records_out") == loaded == final):
        fails.append(f"records_out {records.get('records_out')} / rows_loaded {loaded} / "
                     f"final_count {final} disagree")
    prices = sum(_n(counts, k) or 0 for k in ("parsed_prices", "missing_prices", "invalid_prices"))
    if prices != src:
        fails.append(f"parsed+missing+invalid prices {prices} != source_rows {src}")
    return fails


def _bronze_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    replayed = _n(counts, "records_replayed")
    if replayed is None:
        return ["counts.records_replayed missing"]
    ri, ro = records.get("records_in"), records.get("records_out")
    if ri != (ro or 0) + replayed:
        return [f"identity: records_in {ri} != records_out {ro} + records_replayed {replayed}"]
    return []


def _produce_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    attempted, acked = _n(counts, "records_attempted"), _n(counts, "records_acked")
    if records.get("records_out") != acked:
        return [f"records_out {records.get('records_out')} != records_acked {acked}"]
    if attempted is not None and acked is not None and acked > attempted:
        return [f"records_acked {acked} > records_attempted {attempted}"]
    return []


REGISTRY: dict[tuple[str, str], Contract] = {}


def _register(c: Contract) -> None:
    REGISTRY[(c.job_name, c.spec_version)] = c


_register(Contract(
    "silver", SILVER_SPEC_VERSION,
    inputs={"bronze": ("table", "snapshot_id"), "catalogue": ("table", "catalogue_load_id")},
    outputs={"silver.reviews": ("table", "snapshot_id"), "silver.rejects": ("table", "snapshot_id"),
             "silver.review_collisions": ("table", "snapshot_id")},
    counts=("collision_groups", "exact_groups", "conflicting_groups", "unresolvable_groups",
            "collision_table_rows", "collision_rows_removed", "unmatched_review_rows",
            "unmatched_parent_asins", "review_id_distinct", "reject_reasons", "catalogue_rows_read"),
    identity=_silver_identity))

_register(Contract(
    "catalogue_load", CATALOGUE_LOAD_SPEC_VERSION,
    inputs={"source": ("path", "sha256")},
    outputs={"products": ("table", "catalogue_load_id")},
    counts=("source_rows", "parsed_prices", "missing_prices", "invalid_prices", "rows_loaded",
            "final_count"),
    identity=_catalogue_identity))

_register(Contract(
    "bronze_drain", BRONZE_SPEC_VERSION,
    inputs={"kafka": ("topic",)},
    outputs={"reviews_raw": ("table", "snapshot_ids")},
    counts=("micro_batches", "records_replayed"),
    identity=_bronze_identity))

_register(Contract(
    "produce", PRODUCE_SPEC_VERSION,
    inputs={"source": ("path", "sha256", "bytes")},
    outputs={"kafka": ("topic",)},
    counts=("records_attempted", "records_acked"),
    identity=_produce_identity))

# Jobs whose contracts are registered by their own phase (ADR-0008 §2). They exist in the
# vocabulary now so the CHECK constraint and this registry stay in step.
for _later in ("gold", "search_index_reviews", "search_index_product_month", "embeddings",
               "theme_labels_llm", "theme_classifier_train", "theme_classifier_score",
               "rag_answers"):
    _register(Contract(_later, "0", inputs={}, outputs={}, counts=()))


def contract_for(job_name: str, spec_version: str | None = None) -> Contract | None:
    if spec_version is not None:
        return REGISTRY.get((job_name, spec_version))
    matches = [c for (j, _), c in REGISTRY.items() if j == job_name]
    return matches[-1] if matches else None


def _check_keys(section: str, required: dict[str, tuple[str, ...]], given: dict[str, Any]) -> list[str]:
    fails = []
    for name, keys in required.items():
        entry = given.get(name)
        if entry is None:
            fails.append(f"{section}.{name} missing")
            continue
        for k in keys:
            if not isinstance(entry, dict) or entry.get(k) is None:
                fails.append(f"{section}.{name}.{k} missing")
    return fails


def _finish(failures: list[str], raise_: bool) -> list[str]:
    if failures and raise_:
        raise ContractViolation(failures)
    return failures


def validate_start(job_name: str, spec_version: str, *, inputs: dict[str, Any],
                   params: dict[str, Any], raise_: bool = True) -> list[str]:
    c = REGISTRY.get((job_name, spec_version))
    if c is None:
        return _finish([f"no contract registered for ({job_name}, {spec_version})"], raise_)
    fails = _check_keys("inputs", c.inputs, inputs)
    if not isinstance(params, dict):
        fails.append("params must be a mapping")
    return _finish(fails, raise_)


def validate_finish(job_name: str, spec_version: str, *, records: dict[str, int | None],
                    outputs: dict[str, Any], counts: dict[str, Any], raise_: bool = True) -> list[str]:
    c = REGISTRY.get((job_name, spec_version))
    if c is None:
        return _finish([f"no contract registered for ({job_name}, {spec_version})"], raise_)
    fails = _check_keys("outputs", c.outputs, outputs)
    fails += [f"counts.{k} missing" for k in c.counts if k not in counts]
    fails += c.identity(records, counts)
    return _finish(fails, raise_)


# ------------------------------------------------------------------ git state ----
def git_state() -> tuple[str | None, bool]:
    """(commit sha, dirty) over the cleanliness paths; sha None outside a checkout."""
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, check=True,
                             capture_output=True, text=True).stdout.strip()
        status = subprocess.run(["git", "status", "--porcelain", "--", *CLEANLINESS_PATHS],
                                cwd=PROJECT_ROOT, check=True, capture_output=True, text=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None, True
    return sha, bool(status.strip())


# --------------------------------------------------------------------- ledger ----
def _json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


@dataclass
class Run:
    run_id: str
    job_name: str
    spec_version: str
    started_at: datetime

    @property
    def short(self) -> str:
        return f"{self.job_name} · {self.started_at:%Y-%m-%d %H:%M} · {self.run_id[:4]}…"


def start(job_name: str, spec_version: str, *, category: str, data_scope: str,
          inputs: dict[str, Any], params: dict[str, Any]) -> Run:
    """Insert the `running` row and return the id to stamp on every output."""
    if data_scope not in ("sample", "full"):
        raise ValueError(f"data_scope must be 'sample' or 'full', got {data_scope!r}")
    validate_start(job_name, spec_version, inputs=inputs, params=params)
    run_id = str(uuid.uuid4())
    sha, dirty = git_state()
    started = datetime.now(UTC)
    with connect() as conn:
        conn.execute(
            """INSERT INTO pipeline_runs (run_id, job_name, spec_version, status, category,
                   data_scope, started_at, git_commit_sha, worktree_dirty,
                   cleanliness_policy_version, inputs, outputs, counts, params)
               VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s, %s, %s::jsonb, '{}'::jsonb,
                       '{}'::jsonb, %s::jsonb)""",
            (run_id, job_name, spec_version, category, data_scope, started, sha, dirty,
             CLEANLINESS_POLICY_VERSION, _json(inputs), _json(params)))
    print(f"[ledger] {job_name} run {run_id} started (commit {sha or 'n/a'}"
          f"{', DIRTY' if dirty else ''})", flush=True)
    return Run(run_id, job_name, spec_version, started)


def success(run: Run, *, records_in: int, records_out: int, records_rejected: int,
            outputs: dict[str, Any], counts: dict[str, Any]) -> None:
    """Finalize to `success` only if the contract validates; otherwise mark `failed`."""
    records = {"records_in": records_in, "records_out": records_out,
               "records_rejected": records_rejected}
    fails = validate_finish(run.job_name, run.spec_version, records=records,
                            outputs=outputs, counts=counts, raise_=False)
    if fails:
        failed(run, notes="contract: " + "; ".join(fails), outputs=outputs, counts=counts,
               records=records)
        raise ContractViolation(fails)
    with connect() as conn:
        conn.execute(
            """UPDATE pipeline_runs SET status='success', finished_at=%s, records_in=%s,
                   records_out=%s, records_rejected=%s, outputs=%s::jsonb, counts=%s::jsonb
               WHERE run_id=%s AND status='running'""",
            (datetime.now(UTC), records_in, records_out, records_rejected,
             _json(outputs), _json(counts), run.run_id))
    print(f"[ledger] {run.job_name} run {run.run_id} success", flush=True)


def failed(run: Run, *, notes: str, outputs: dict[str, Any] | None = None,
           counts: dict[str, Any] | None = None,
           records: dict[str, int | None] | None = None) -> None:
    """Mark `failed`, keeping whatever partial outputs exist (never rolled back)."""
    records = records or {}
    with connect() as conn:
        conn.execute(
            """UPDATE pipeline_runs SET status='failed', finished_at=%s, notes=%s,
                   outputs=%s::jsonb, counts=%s::jsonb, records_in=%s, records_out=%s,
                   records_rejected=%s
               WHERE run_id=%s AND status='running'""",
            (datetime.now(UTC), notes[:4000], _json(outputs or {}), _json(counts or {}),
             records.get("records_in"), records.get("records_out"),
             records.get("records_rejected"), run.run_id))
    print(f"[ledger] {run.job_name} run {run.run_id} FAILED: {notes}", flush=True)


def latest_success(job_name: str, *, category: str, data_scope: str) -> dict[str, Any] | None:
    with connect() as conn:
        row = conn.execute(
            """SELECT run_id, spec_version, started_at, finished_at, records_in, records_out,
                      records_rejected, inputs, outputs, counts, params, git_commit_sha,
                      worktree_dirty
               FROM pipeline_runs
               WHERE job_name=%s AND status='success' AND category=%s AND data_scope=%s
               ORDER BY started_at DESC LIMIT 1""",
            (job_name, category, data_scope)).fetchone()
    if row is None:
        return None
    keys = ("run_id", "spec_version", "started_at", "finished_at", "records_in", "records_out",
            "records_rejected", "inputs", "outputs", "counts", "params", "git_commit_sha",
            "worktree_dirty")
    return {k: (str(v) if k == "run_id" else v) for k, v in zip(keys, row)}

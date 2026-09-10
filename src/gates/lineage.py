"""`LINEAGE_GATE`: the declared chain walked as a join, never an assertion (ADR-0008, RR-16).

The run ledger's claim is that every artefact, Iceberg snapshot and Elasticsearch generation
the submission publishes can be traced back to the `pipeline_runs` row that produced it. This
module decides whether that claim holds, over facts `scripts/gate_lineage.py` has already
loaded. Nothing here touches Postgres, Spark or Elasticsearch.

**A link is one checkable join**, and the gate prints how many it checked. Five kinds:

* `artefact` -- an `eval/<capability>/gate.json` names a `pipeline_run_id`; that run must
  exist in the ledger, have succeeded, run the job the chain declares for the capability, and
  agree with the artefact about scope. This is the trip case the ticket names: a run id with
  no ledger row is a broken chain, not a missing nicety.
* `output` -- a pinned run's recorded output must resolve physically: the Iceberg snapshot is
  in the table's history *and* its summary carries the run id; the Elasticsearch generation
  exists with the alias on it and every document stamped `source_run_id`; the catalogue rows
  carry the loader's id. `current=false` is not a broken link -- the identity still resolves --
  but it withholds `publication_ready`.
* `input` -- an input identity that no edge joins (bronze, whose drain writes no row yet) is
  still checked for existence, so a cut edge does not become an unchecked one.
* `edge` -- a run's recorded input names an upstream run id; that run's recorded outputs must
  contain the exact identity claimed. This is the join. It also fails when the upstream run is
  not the one the chain pinned for that job, which is how a stale branch is caught.
* `retention` -- a `failed` run's partial outputs are still present. Nothing is ever rolled
  back (ADR-0008 §3), so a failure stays diagnosable; a vanished partial output is a lie about
  what happened.

**A chain of nothing is not a clean chain.** `chain_links_checked = 0` is `FAIL`, always. The
failure this refuses to reproduce is the exactly-once gate that printed PASS over runs it had
never examined (audit F3).

**Two flags, because two questions.** `chain_clean` asks whether everything checked holds --
it is the gate's verdict. `publication_ready` asks the stricter question: is the whole declared
chain pinned, current and finished. A phase that has not run yet contributes no links and
leaves `publication_ready=false` while `chain_clean` stays true, which is exactly the shape a
partial submission should print. `worktree_dirty` is reported as evidence on every run and is
deliberately *not* folded into either flag: it is a property of how a run was produced, and
the vocabulary here is closed to `PASS`/`FAIL` -- there is no `PASS(dirty)` (RR-16).
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import Any

from src.common.evaluation import Verdict, constituent_metric, repro_verdict

GATE_NAME = "LINEAGE_GATE"
MODES = ("development", "publication")

#: The order the blocks print in: provenance, then what it rests on, then the joins.
KIND_ORDER = ("artefact", "pin", "output", "input", "edge", "retention")

#: One aggregate constituent per link kind, named as the gate reports it.
KIND_CHECKS = {"artefact": "artefacts_resolve", "pin": "jobs_pinned",
               "output": "outputs_resolve", "input": "inputs_resolve", "edge": "edges_join",
               "retention": "partial_outputs_kept"}


def _b(v: Any) -> str:
    return str(bool(v)).lower()


def short_id(run_id: str | None) -> str:
    """Run ids print at eight characters, as every other gate in this project prints them."""
    return (run_id or "none")[:8]


@dataclass(frozen=True)
class Link:
    """One checked join: its kind, the line the gate prints for it, and whether it held."""
    kind: str
    line: str
    ok: bool

    def __post_init__(self) -> None:
        if self.kind not in KIND_CHECKS:
            raise ValueError(f"unknown link kind {self.kind!r}")


# ------------------------------------------------------------------- the links ----
def artefact_link(*, capability: str, run_id: str | None, job: str | None, resolved: bool,
                  status: str | None, job_declared: bool, scope_matches: bool,
                  pinned: bool = True) -> Link:
    """`eval/<capability>/gate.json` → the ledger row that produced what it measured.

    `job_declared` is the claim that the artefact is filed where the chain says it belongs. A
    capability that declares no jobs -- the lineage and deliverables tracks, whose gates read
    the chain rather than transform data -- is not job-attributed, so its caller passes True:
    the artefact still has to name a run that resolves, which is the check that matters.
    """
    ok = bool(resolved and status == "success" and job_declared and scope_matches and pinned)
    line = (f"LINEAGE_ARTEFACT capability={capability} run={short_id(run_id)} "
            f"resolved={_b(resolved)} run_status={status or 'none'} job={job or 'none'} "
            f"declared_job={_b(job_declared)} scope_matches={_b(scope_matches)} "
            f"pinned={_b(pinned)} ok={_b(ok)}")
    return Link("artefact", line, ok)


def pin_link(*, capability: str, job: str, run_id: str | None, source: str,
             status: str | None) -> Link:
    """A job the capability declares → the ledger run the chain walks for it.

    `source=artifact` is the run the capability's own artefact names; `source=latest_success`
    is a job the capability claims beside it (search's product-month projection beside its
    reviews index). A declared job with no successful run is a claim with nothing behind it.
    """
    ok = bool(run_id and status == "success")
    line = (f"LINEAGE_PIN capability={capability} job={job} run={short_id(run_id)} "
            f"source={source} status={status or 'none'} ok={_b(ok)}")
    return Link("pin", line, ok)


def output_link(*, job: str, run_id: str, output: str, store: str, identity: str,
                exists: bool, stamped: bool, current: bool, detail: str = "") -> Link:
    """A recorded output → the physical artefact, stamped with the run that wrote it."""
    ok = bool(exists and stamped)
    line = (f"LINEAGE_OUTPUT run={short_id(run_id)} job={job} output={output} store={store} "
            f"identity={identity} exists={_b(exists)} stamped={_b(stamped)} "
            f"current={_b(current)}{' ' + detail if detail else ''} ok={_b(ok)}")
    return Link("output", line, ok)


def input_link(*, job: str, run_id: str, input_name: str, identity: str, exists: bool,
               reason: str) -> Link:
    """An input identity no edge joins -- checked for existence, and told why it is unjoined."""
    line = (f"LINEAGE_INPUT run={short_id(run_id)} job={job} input={input_name} "
            f"identity={identity} exists={_b(exists)} joined=false reason={reason} "
            f"ok={_b(exists)}")
    return Link("input", line, bool(exists))


def edge_link(*, downstream_job: str, downstream_run: str, input_name: str, upstream_job: str,
              upstream_run: str | None, identity: str, declared: bool, resolves: bool,
              pinned: bool) -> Link:
    """The join itself: downstream's recorded input is upstream's recorded output."""
    ok = bool(declared and resolves and pinned)
    line = (f"LINEAGE_EDGE downstream={downstream_job}/{short_id(downstream_run)} "
            f"input={input_name} upstream={upstream_job}/{short_id(upstream_run)} "
            f"identity={identity} declared={_b(declared)} resolves={_b(resolves)} "
            f"pinned={_b(pinned)} ok={_b(ok)}")
    return Link("edge", line, ok)


def retention_link(*, job: str, run_id: str, outputs: int, present: int, notes: str) -> Link:
    """A failed run's partial outputs are kept: nothing is rolled back (ADR-0008 §3)."""
    ok = outputs == present
    line = (f"LINEAGE_RETENTION run={short_id(run_id)} job={job} status=failed outputs={outputs} "
            f"present={present} reason={notes or 'none'} ok={_b(ok)}")
    return Link("retention", line, ok)


# ----------------------------------------------------- what is not a link ----
def cut_line(*, what: str, name: str, reason: str) -> str:
    """A link the submission does not claim. Printed with its reason; counted nowhere."""
    return f"LINEAGE_CUT {what}={name} reason={reason}"


def self_line(*, capability: str) -> str:
    """This gate does not walk its own artefact -- it writes it (see the module docstring)."""
    return (f"LINEAGE_SELF capability={capability} reason=this gate writes this artefact at the "
            f"end of the walk, so checking it would only ever check the previous walk")


def pending_line(*, capability: str, reason: str) -> str:
    """A declared capability with nothing to pin yet. Withholds publication, blocks nothing."""
    return f"LINEAGE_PENDING capability={capability} reason={reason}"


def ledger_line(*, runs_pinned: int, running: int, failed: int, dirty: int) -> str:
    """The ledger's own shape behind the chain, including the rows the chain did not pin."""
    return (f"LINEAGE_LEDGER runs_pinned={runs_pinned} orphan_running={running} "
            f"failed_runs={failed} dirty_runs={dirty}")


def terminal_line(*, mode: str, chain_clean: bool, publication_ready: bool, links: int,
                  passed: bool) -> str:
    return (f"LINEAGE_GATE gate_mode={mode} chain_clean={_b(chain_clean)} "
            f"publication_ready={_b(publication_ready)} chain_links_checked={links} "
            f"LINEAGE_GATE={'PASS' if passed else 'FAIL'}")


# ---------------------------------------------------------------- the verdict ----
def checks_for(links: Sequence[Link]) -> list[tuple[str, bool]]:
    """`chain_not_empty` always, then one aggregate per kind that actually has links.

    A kind with no links is left out rather than passed vacuously: a constituent that cannot
    fail inflates the count without testing anything (audit F3).
    """
    checks = [("chain_not_empty", len(links) > 0)]
    for kind in KIND_ORDER:
        of_kind = [x for x in links if x.kind == kind]
        if of_kind:
            checks.append((KIND_CHECKS[kind], all(x.ok for x in of_kind)))
    return checks


def verdict(links: Sequence[Link], *, mode: str, pending: int, stale_outputs: int,
            orphan_running: int, extra_lines: Sequence[str] = ()) -> Verdict:
    """`LINEAGE_GATE` over the links the walk checked.

    `publication_ready` needs more than a clean chain: every declared capability pinned
    (`pending == 0`), every output still the current one, and no run left `running` by a
    killed driver. In publication mode it becomes a constituent, so the gate that prints
    `publication_ready=false` also exits non-zero; in development it is reported beside a
    PASS, which is the honest shape while phases are still landing.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    links = tuple(links)
    checks = checks_for(links)
    chain_clean = all(ok for _, ok in checks)
    publication_ready = bool(chain_clean and pending == 0 and stale_outputs == 0
                             and orphan_running == 0)
    if mode == "publication":
        checks.append(("publication_ready", publication_ready))
    passed = all(ok for _, ok in checks)
    lines = [x.line for kind in KIND_ORDER for x in links if x.kind == kind]
    lines += list(extra_lines)
    return repro_verdict(GATE_NAME, checks,
                         terminal_line(mode=mode, chain_clean=chain_clean,
                                       publication_ready=publication_ready, links=len(links),
                                       passed=passed),
                         constituents=lines)


# ------------------------------------------------- the attestation other gates print ----
def contracts_line(*, gate_name: str, jobs: Sequence[str], missing: Sequence[str]) -> str:
    return (f"RUN_CONTRACTS gate={gate_name} jobs={','.join(jobs) or 'none'} "
            f"registered={len(jobs) - len(missing)}/{len(jobs)} "
            f"missing={','.join(missing) or 'none'} "
            f"run_contract_registered={_b(not missing)}")


def attest_contracts(v: Verdict, *, jobs: Sequence[str], missing: Sequence[str]) -> Verdict:
    """Add `run_contract_registered` to a phase gate's verdict, as a constituent that blocks.

    Every phase claims its jobs in `conf/lineage_chain.toml`; a job whose `(job_name,
    spec_version)` contract is not in the registry writes ledger rows nothing validates, and
    the lineage gate would then walk a chain of unchecked numbers. So the phase gate itself
    refuses to pass: the attestation is part of the phase's reproducibility claim, which is
    why it is added to reproducibility verdicts only -- a quality number is not made wrong by
    a missing contract, and its sibling repro gate already covers the same jobs.

    A capability that declares no jobs (the lineage and deliverables tracks) is returned
    untouched rather than given a constituent that cannot fail.
    """
    jobs = tuple(jobs)
    if not jobs:
        return v
    if v.metric.get("name") != "constituents_ok":
        raise ValueError(f"{v.gate_name}: contract attestation belongs to a reproducibility "
                         f"verdict, not to metric {v.metric.get('name')!r}")
    missing = tuple(missing)
    checks = v.checks + (("run_contract_registered", not missing),)
    terminal = v.terminal
    if missing:
        terminal = terminal.replace(f"{v.gate_name}=PASS", f"{v.gate_name}=FAIL")
    return replace(v, status="PASS" if all(ok for _, ok in checks) else "FAIL",
                   constituents=v.constituents + (contracts_line(gate_name=v.gate_name,
                                                                 jobs=jobs, missing=missing),),
                   terminal=terminal, metric=constituent_metric(checks), checks=checks)


def attest(v: Verdict, capability: str) -> Verdict:
    """`attest_contracts` with the jobs read from the chain and looked up in the registry.

    The one function here that reads anything: the chain file and the contract registry, both
    imported locally so this module stays importable with no database driver in the way. Every
    phase gate calls it on the way out, which is what makes the `jobs` list in
    `conf/lineage_chain.toml` load-bearing rather than documentation.
    """
    from src.common import runs
    from src.common.evaluation import load_chain

    jobs = next((c.jobs for c in load_chain() if c.id == capability), None)
    if jobs is None:
        raise ValueError(f"{capability}: not declared in the chain, so it has no jobs to attest")
    return attest_contracts(v, jobs=jobs,
                            missing=[j for j in jobs if runs.contract_for(j) is None])

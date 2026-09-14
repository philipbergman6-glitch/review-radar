"""`DEMO_GATE`: "it ran clean twice" turned from a memory into a number (RR-11, RR-13, ticket 18).

A **rehearsal** is not a claim someone makes; it is an *executed export* that can be read back
and disagreed with. `docs/demo/<date>/` holds one, and it counts only when all five of these
hold -- each of them derived from the export itself, never from a flag the runner wrote:

  executed        every code cell carries an `execution_count` and no cell output is an error
  timed           the cells' own recorded timestamps span <= 300 s, the live budget's ceiling
  stream_running  move 10's `DEMO_LIVE` line reports micro-batches over real rows, *and* a
                  `stream_produce` ledger run overlaps the window the cells timestamped
  exported        the notebook and its HTML rendering are both there

ADR-0009 asks the *recorded backup* for one more pair -- the Kibana dashboard PNG and the
exactly-once transcript -- and those are a separate blocking constituent, `backup_playable`,
rather than part of what makes a rehearsal a rehearsal. The ADR splits them that way for a
reason: whether the demo ran clean is a question about the cells, and neither of those two
files is produced by running them.

The two-sided stream check is the point of the exercise. The notebook's own counter says the
projection saw batches; the ledger says a replay process was feeding the topic while those
cells ran. Either alone is a notebook talking about itself; together they are two records that
would have to agree by accident.

`docs_present` is separate and blocking: the four written deliverables (ADR-0009, RR-01 log
item 11). A demo that rehearses beautifully with no design doc is not a deliverable.

Everything here is pure over already-loaded documents -- no filesystem, no Postgres, no clock.
`scripts/gate_demo.py` does the I/O.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.common.evaluation import Verdict, repro_verdict

GATE_NAME = "DEMO_GATE"

#: Two committed exports, and no fewer: "run clean twice" (RR-01 log item 11, RR-13).
REHEARSALS_REQUIRED = 2

#: The live budget's ceiling (ADR-0009). A rehearsal that overran did not rehearse the demo.
MAX_ELAPSED_S = 300

#: What a recorded backup directory carries (ADR-0009, RR-11 §4).
EXPORT_NOTEBOOK = "demo.executed.ipynb"
EXPORT_HTML = "demo.html"
EXPORT_KIBANA = "kibana-dashboard.png"
EXPORT_TRANSCRIPT = "exactly-once.txt"
EXPORT_FILES = (EXPORT_NOTEBOOK, EXPORT_HTML, EXPORT_KIBANA, EXPORT_TRANSCRIPT)

#: What running the notebook produces, and therefore what a rehearsal cannot be without.
REHEARSAL_FILES = (EXPORT_NOTEBOOK, EXPORT_HTML)

#: What the recorded backup carries beyond the rehearsal itself (ADR-0009). Neither comes out
#: of the cells -- Kibana's PNG export is not in this stack's licence and the exactly-once run
#: is its own job -- so they are copied in, and `backup_playable` is what checks they were.
BACKUP_FILES = (EXPORT_HTML, EXPORT_KIBANA, EXPORT_TRANSCRIPT)

#: The four written deliverables `docs_present` counts, by the path each one lives at. The
#: paths are fixed here rather than in ticket 19 so the gate can fail for their absence before
#: they are written -- which is the state the table should be showing today.
REQUIRED_DOCS: tuple[tuple[str, str], ...] = (
    ("design_doc", "docs/DESIGN.md"),
    ("slides", "docs/SLIDES.md"),
    ("readme", "README.md"),
    ("runbook", "docs/DEMO_RUNBOOK.md"),
)

#: The machine-readable line move 10 prints, from `src.serving.demo.stop_live_projection`.
LIVE_LINE = re.compile(r"^DEMO_LIVE (?P<body>.+)$", re.MULTILINE)

#: A full git commit sha, as `demo.context()` prints it at move 1.
SHA = re.compile(r"\b[0-9a-f]{40}\b")


def b(x: Any) -> str:
    return str(bool(x)).lower()


# ------------------------------------------------------------------ notebook reading ----
def cell_text(cell: Mapping[str, Any]) -> str:
    """Everything one executed cell put on screen, as one string."""
    parts: list[str] = []
    for out in cell.get("outputs") or []:
        kind = out.get("output_type")
        if kind == "stream":
            parts.append("".join(out.get("text") or []))
        elif kind in ("execute_result", "display_data"):
            parts.append("".join((out.get("data") or {}).get("text/plain") or []))
        elif kind == "error":
            parts.append("\n".join(out.get("traceback") or []))
    return "\n".join(parts)


def notebook_text(nb: Mapping[str, Any]) -> str:
    return "\n".join(cell_text(c) for c in nb.get("cells") or [])


def cell_errors(nb: Mapping[str, Any]) -> list[str]:
    """One entry per cell whose execution the notebook itself records as having failed."""
    out: list[str] = []
    for i, cell in enumerate(nb.get("cells") or []):
        if cell.get("cell_type") != "code":
            continue
        for o in cell.get("outputs") or []:
            if o.get("output_type") == "error":
                out.append(f"cell {i}: {o.get('ename') or 'error'}")
    return out


def unexecuted(nb: Mapping[str, Any]) -> list[int]:
    """Code cells with no execution count: the export was never executed, or stopped early."""
    return [i for i, c in enumerate(nb.get("cells") or [])
            if c.get("cell_type") == "code" and c.get("execution_count") is None]


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def window(nb: Mapping[str, Any]) -> tuple[datetime | None, datetime | None]:
    """When the cells say they started and finished, from nbclient's recorded timing.

    `record_timing` writes `metadata.execution` on every executed cell; an export without it
    is an export whose duration nobody can check, and the rehearsal does not count.
    """
    starts: list[datetime] = []
    ends: list[datetime] = []
    for cell in nb.get("cells") or []:
        ex = (cell.get("metadata") or {}).get("execution") or {}
        start = _stamp(ex.get("iopub.status.busy")) or _stamp(ex.get("iopub.execute_input"))
        end = _stamp(ex.get("shell.execute_reply")) or _stamp(ex.get("iopub.status.idle"))
        if start:
            starts.append(start)
        if end:
            ends.append(end)
    return (min(starts) if starts else None, max(ends) if ends else None)


def live_line(text: str) -> str | None:
    """Move 10's `DEMO_LIVE` line, verbatim, or None when the export does not carry one."""
    m = LIVE_LINE.search(text)
    return f"DEMO_LIVE {m.group('body').strip()}" if m else None


def live_counts(line: str | None) -> dict[str, int]:
    """`batches=` and `rows=` off the `DEMO_LIVE` line. Absent keys read as zero."""
    if not line:
        return {"batches": 0, "rows": 0}
    fields = dict(re.findall(r"(\w+)=(-?\d+)", line))
    return {"batches": int(fields.get("batches", 0)), "rows": int(fields.get("rows", 0))}


def commit_sha(text: str) -> str | None:
    """The git SHA move 1 printed, which ties the HTML export to this execution."""
    m = SHA.search(text)
    return m.group(0) if m else None


# ------------------------------------------------------------------------ rehearsal ----
@dataclass(frozen=True)
class Rehearsal:
    """One export directory, assessed. `faults` empty is the whole of "it counts"."""
    name: str
    code_cells: int
    unexecuted: tuple[int, ...]
    errors: tuple[str, ...]
    elapsed_s: float | None
    started_at: datetime | None
    finished_at: datetime | None
    micro_batches: int
    rows: int
    replay_run_id: str | None
    commit: str | None
    backup_playable: bool
    files_missing: tuple[str, ...]
    faults: tuple[str, ...]

    @property
    def counts(self) -> bool:
        return not self.faults

    @property
    def all_cells_ok(self) -> bool:
        return bool(self.code_cells) and not self.unexecuted and not self.errors

    @property
    def stream_running(self) -> bool:
        return self.micro_batches > 0 and self.rows > 0 and self.replay_run_id is not None


def overlapping_replay(runs: Iterable[Mapping[str, Any]], start: datetime | None,
                       end: datetime | None) -> str | None:
    """The id of the replay run *this* rehearsal started, by when it started.

    The rule is that the run began between the first and last cell timestamps, and not merely
    that it was alive across them. Move 2 starts the replay inside the notebook, so that is
    the true shape; the looser reading -- a run that began earlier and has not finished --
    would be satisfied forever by one killed producer left `running` in the ledger, which is
    a row this project has to reconcile by hand and therefore always has a few of.

    The last such run wins: a rehearsal rerun in the same minute should be attested by its own
    replay, not by the one before it.
    """
    if start is None or end is None:
        return None
    began_inside = [r for r in runs
                    if (t := _stamp(str(r.get("started_at")))) and start <= t <= end]
    return str(began_inside[-1]["run_id"]) if began_inside else None


def assess(name: str, nb: Mapping[str, Any], *, present_files: Sequence[str],
           html: str | None, replay_runs: Sequence[Mapping[str, Any]]) -> Rehearsal:
    """One export directory turned into a rehearsal, or into the named reasons it is not one."""
    text = notebook_text(nb)
    code_cells = sum(1 for c in nb.get("cells") or [] if c.get("cell_type") == "code")
    missing = tuple(f for f in EXPORT_FILES if f not in set(present_files))
    errors = tuple(cell_errors(nb))
    never_ran = tuple(unexecuted(nb))
    start, end = window(nb)
    elapsed = (end - start).total_seconds() if start and end else None
    line = live_line(text)
    counts = live_counts(line)
    sha = commit_sha(text)
    replay = overlapping_replay(replay_runs, start, end)
    # Derived from *this* notebook, not merely present: the rendering has to carry the same
    # git SHA and the same DEMO_LIVE line, so an HTML file left over from another run is not
    # a backup of this one.
    playable = (not [f for f in BACKUP_FILES if f in missing]
                and bool(html) and bool(line) and line in html and bool(sha) and sha in html)

    faults: list[str] = []
    if [f for f in REHEARSAL_FILES if f in missing]:
        faults.append(f"missing {','.join(f for f in REHEARSAL_FILES if f in missing)}")
    if not code_cells:
        faults.append("no code cells")
    if never_ran:
        faults.append(f"unexecuted cells {','.join(str(i) for i in never_ran)}")
    if errors:
        faults.append(f"failed cells {'; '.join(errors)}")
    if elapsed is None:
        faults.append("no recorded cell timing, so the duration cannot be checked")
    elif elapsed > MAX_ELAPSED_S:
        faults.append(f"elapsed {elapsed:.0f}s over the {MAX_ELAPSED_S}s ceiling")
    if not line:
        faults.append("no DEMO_LIVE line, so move 10 never reported the projection")
    elif counts["batches"] <= 0 or counts["rows"] <= 0:
        faults.append(f"the projection saw batches={counts['batches']} rows={counts['rows']}: "
                      "the stream was not running")
    elif replay is None:
        faults.append("no stream_produce run in the ledger overlaps these cells, so the "
                      "projection's batches are unattested")
    if not html or not line or line not in html or not sha or sha not in html:
        faults.append("the HTML export is absent or was not derived from this notebook")

    return Rehearsal(name=name, code_cells=code_cells, unexecuted=never_ran, errors=errors,
                     elapsed_s=elapsed, started_at=start, finished_at=end,
                     micro_batches=counts["batches"], rows=counts["rows"],
                     replay_run_id=replay, commit=sha, backup_playable=playable,
                     files_missing=missing, faults=tuple(faults))


# ----------------------------------------------------------------------------- docs ----
def missing_docs(present: Sequence[str]) -> tuple[tuple[str, str], ...]:
    have = set(present)
    return tuple((name, path) for name, path in REQUIRED_DOCS if path not in have)


# -------------------------------------------------------------------------- verdict ----
def rehearsal_line(r: Rehearsal) -> str:
    return (f"DEMO_REHEARSAL {r.name} cells={r.code_cells} all_cells_ok={b(r.all_cells_ok)} "
            f"elapsed_s={'none' if r.elapsed_s is None else f'{r.elapsed_s:.0f}'} "
            f"batches={r.micro_batches} rows={r.rows} "
            f"replay_run={(r.replay_run_id or 'none')[:8]} commit={(r.commit or 'none')[:8]} "
            f"backup_playable={b(r.backup_playable)} "
            f"backup_missing={','.join(f for f in BACKUP_FILES if f in r.files_missing) or 'none'} "
            f"counts={b(r.counts)}"
            + (f" faults={'; '.join(r.faults)}" if r.faults else ""))


def docs_line(present: Sequence[str]) -> str:
    missing = missing_docs(present)
    return (f"DEMO_DOCS present={len(REQUIRED_DOCS) - len(missing)}/{len(REQUIRED_DOCS)} "
            f"missing={','.join(p for _, p in missing) or 'none'}")


def terminal_line(rehearsals: Sequence[Rehearsal], present_docs: Sequence[str], *,
                  scope: str, passed: bool) -> str:
    counting = [r for r in rehearsals if r.counts]
    elapsed = [r.elapsed_s for r in rehearsals if r.elapsed_s is not None]
    missing = missing_docs(present_docs)
    return (f"DEMO_GATE rehearsals={len(counting)}/{REHEARSALS_REQUIRED} "
            f"max_elapsed_s={'none' if not elapsed else f'{max(elapsed):.0f}'} "
            f"all_cells_ok={b(rehearsals and all(r.all_cells_ok for r in rehearsals))} "
            f"stream_running={b(rehearsals and all(r.stream_running for r in rehearsals))} "
            f"backup_playable={b(rehearsals and all(r.backup_playable for r in rehearsals))} "
            f"docs_present={len(REQUIRED_DOCS) - len(missing)}/{len(REQUIRED_DOCS)} "
            f"gate_scope={scope} DEMO_GATE={'PASS' if passed else 'FAIL'}")


def verdict(rehearsals: Sequence[Rehearsal], present_docs: Sequence[str], *,
            scope: str) -> Verdict:
    """Five named constituents, every one of which can trip the gate.

    The three aggregate flags are taken over *every* export found, not over the ones that
    count: an export with a failed cell has to be able to turn `all_cells_ok` false, and a
    flag computed over the survivors would quietly be true of an empty set.
    """
    rehearsals = list(rehearsals)
    counting = [r for r in rehearsals if r.counts]
    checks = (
        ("rehearsals_at_threshold", len(counting) >= REHEARSALS_REQUIRED),
        ("all_cells_ok", bool(rehearsals) and all(r.all_cells_ok for r in rehearsals)),
        ("stream_running", bool(rehearsals) and all(r.stream_running for r in rehearsals)),
        ("backup_playable", bool(rehearsals) and all(r.backup_playable for r in rehearsals)),
        ("docs_present", not missing_docs(present_docs)),
    )
    passed = all(ok for _, ok in checks)
    constituents = [*(rehearsal_line(r) for r in rehearsals), docs_line(present_docs)]
    return repro_verdict(GATE_NAME, checks,
                         terminal_line(rehearsals, present_docs, scope=scope, passed=passed),
                         constituents=constituents)


def notes(rehearsals: Sequence[Rehearsal], present_docs: Sequence[str]) -> list[str]:
    out = [("a rehearsal is an executed export under docs/demo/<date>/, not a self-report: "
            "the cells' own recorded timestamps give the duration, move 10's DEMO_LIVE line "
            "gives the projection's batches, and a stream_produce ledger run overlapping "
            "those timestamps is the second, independent record that the stream was live")]
    rejected = [r for r in rehearsals if not r.counts]
    if rejected:
        out.append("exports that did not count: " + "; ".join(
            f"{r.name} ({', '.join(r.faults)})" for r in rejected))
    missing = missing_docs(present_docs)
    if missing:
        out.append("written deliverables still absent: " + ", ".join(
            f"{name} ({path})" for name, path in missing) + " -- ticket 19 writes them")
    return out

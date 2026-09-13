"""Lineage gate: walk the declared chain and prove every claim joins back to a ledger row.

The chain is declared in `conf/lineage_chain.toml` -- the same file `make eval-table` reads --
and this script hardcodes nothing about it. It walks:

    eval/<capability>/gate.json  ->  pipeline_runs row  ->  the outputs that row recorded
                                                        ->  the runs that row's inputs name

Every step is a **link**, and the terminal line prints how many were checked, so a verdict over
an empty chain is impossible. One run per job is pinned from the artefacts, plus -- for an edge
the chain declares `upstream_pin = "recorded"` -- the run the downstream actually named, which
is how a job that draws one frame per run is walked frame by frame rather than by whichever
frame was drawn last (ticket 10a). The decision is pure and lives in `src/gates/lineage.py`;
everything here is the I/O that feeds it: Postgres for the ledger and the catalogue, Spark for
Iceberg snapshot history, Elasticsearch for index generations, the filesystem for artefacts.

A capability the chain declares but that has not run yet contributes no links: it prints
`LINEAGE_PENDING` and withholds `publication_ready`, which is the honest shape of a submission
still being built. `--mode publication` makes that a failure; `--mode development` reports it
beside a PASS.

Exit 0 on LINEAGE_GATE=PASS, 1 otherwise.

Run:  ./run.sh python scripts/gate_lineage.py --mode development [--scope full] [--category X]
"""
from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from typing import Any

from src.common import config as C
from src.common import evaluation as E
from src.common.pg import connect
from src.gates import lineage as gate

#: Iceberg tables are addressed through the project's catalogue; anything else is another store.
ICEBERG_PREFIX = "lake."

#: The capability this gate publishes. It is skipped on the walk: see `gate.self_line`.
SELF = "lineage"

LEDGER_COLUMNS = ("run_id", "job_name", "spec_version", "status", "category", "data_scope",
                  "started_at", "finished_at", "git_commit_sha", "worktree_dirty",
                  "records_in", "records_out", "records_rejected",
                  "inputs", "outputs", "counts", "notes")


# ------------------------------------------------------------------- the ledger ----
def load_ledger() -> list[dict[str, Any]]:
    """Every row, newest last. The table is one row per job execution; it stays small."""
    with connect() as conn:
        rows = conn.execute(f"SELECT {', '.join(LEDGER_COLUMNS)} FROM pipeline_runs "
                            f"ORDER BY started_at").fetchall()
    return [{k: (str(v) if k == "run_id" else v) for k, v in zip(LEDGER_COLUMNS, r)} for r in rows]


def latest_success(ledger: Iterable[dict[str, Any]], job: str, *, category: str,
                   scope: str) -> dict[str, Any] | None:
    hits = [r for r in ledger if r["job_name"] == job and r["status"] == "success"
            and r["category"] == category and r["data_scope"] == scope]
    return hits[-1] if hits else None


# ------------------------------------------------------- identities inside a row ----
def identities(entry: dict[str, Any]) -> list[tuple[str, int]]:
    """Every (table, snapshot id) an inputs/outputs entry names.

    Most entries name one. `theme_samples` reads two gold tables in a single input, keyed
    `points_table`/`points_snapshot_id` and `episodes_table`/`episodes_snapshot_id`, so the
    pairing is derived from the key names rather than assumed to be singular.
    """
    out = []
    for key, value in entry.items():
        if not key.endswith("table") or not isinstance(value, str):
            continue
        prefix = key[:-len("table")]
        snap = entry.get(f"{prefix}snapshot_id")
        if snap is not None:
            out.append((value, int(snap)))
    return out


def run_reference(entry: dict[str, Any]) -> str | None:
    """The upstream run an input entry names, if it names one.

    `catalogue_load_id` *is* the loader's run id (ADR-0007), so the catalogue input is a join
    like any other even though it does not use the word.
    """
    ref = entry.get("run_id") or entry.get("catalogue_load_id")
    return None if ref is None else str(ref)


# ------------------------------------------------------------- physical resolvers ----
class Iceberg:
    """Snapshot history for the tables the chain touches, read once per table."""

    def __init__(self, spark: Any) -> None:
        self.spark = spark
        self._history: dict[str, dict[int, str | None]] = {}
        self._current: dict[str, int | None] = {}
        self._by_column: dict[tuple[str, str, int | None], dict[str, int]] = {}

    def _load(self, table: str) -> None:
        if table in self._history:
            return
        if self.spark is None:
            raise RuntimeError(f"{table} must be resolved but no Spark session was started; "
                               f"the walk decided no pinned run touched Iceberg")
        rows = self.spark.sql(f"SELECT snapshot_id, summary FROM {table}.snapshots").collect()
        self._history[table] = {int(r["snapshot_id"]): (r["summary"] or {}).get("run_id")
                                for r in rows}
        current = self.spark.sql(f"SELECT snapshot_id FROM {table}.refs "
                                 f"WHERE name = 'main'").collect()
        self._current[table] = int(current[0]["snapshot_id"]) if current else None

    def check(self, table: str, snapshot_id: int) -> tuple[bool, str | None, bool]:
        """(the snapshot is in this table's history, the run id stamped on it, it is current)."""
        self._load(table)
        history = self._history[table]
        return (snapshot_id in history, history.get(snapshot_id),
                self._current[table] == snapshot_id)

    def rows_by(self, table: str, column: str, snapshot_id: int | None) -> dict[str, int]:
        """Row counts per value of `column`, at one snapshot or at the table head.

        One grouped count per (table, column, snapshot) serves every run that claims it, so a
        table with three writers is read twice -- once at each snapshot, once at the head --
        rather than once per run.
        """
        key = (table, column, snapshot_id)
        if key not in self._by_column:
            if self.spark is None:
                raise RuntimeError(f"{table} is declared column-attributed but no Spark session "
                                   f"was started; the walk decided no pinned run touched Iceberg")
            version = "" if snapshot_id is None else f" VERSION AS OF {snapshot_id}"
            rows = self.spark.sql(f"SELECT {column} AS v, count(*) AS n FROM {table}{version} "
                                  f"GROUP BY {column}").collect()
            self._by_column[key] = {r["v"]: int(r["n"]) for r in rows if r["v"] is not None}
        return self._by_column[key]

    def attributed(self, table: str, column: str, snapshot_id: int,
                   run_id: str) -> tuple[int, int]:
        """(rows this run wrote at its snapshot, rows still carrying it at the table head)."""
        return (self.rows_by(table, column, snapshot_id).get(run_id, 0),
                self.rows_by(table, column, None).get(run_id, 0))


def es_generation(es: Any, index: str, alias: str) -> dict[str, Any]:
    """What Elasticsearch holds for one projection generation, and who stamped its documents."""
    from src.serving import projection as P
    if not es.indices.exists(index=index):
        return {"exists": False, "run_ids": [], "count": 0, "alias_targets": []}
    body = es.search(index=index, size=0,
                     aggs={"runs": {"terms": {"field": "source_run_id", "size": 5}}})
    return {"exists": True,
            "run_ids": [b["key"] for b in body["aggregations"]["runs"]["buckets"]],
            "count": int(es.count(index=index)["count"]),
            "alias_targets": P.alias_targets(es, alias)}


def catalogue_rows(load_id: str) -> int:
    with connect() as conn:
        return int(conn.execute("SELECT count(*) FROM products WHERE catalogue_load_id = %s",
                                (load_id,)).fetchone()[0])


def consumed_pins(pins: dict[str, dict[str, dict[str, Any]]], edges: Iterable[E.Edge],
                  by_id: dict[str, dict[str, Any]]) -> list[tuple[dict[str, Any], str]]:
    """The runs an already-pinned run recorded on an edge that lets it choose its upstream.

    `theme_samples` draws one frame per run -- discovery, development, audit, training_pool --
    so several of its outputs are live at once and `latest_success` names the wrong one for
    any particular labelling run. An edge declared `upstream_pin = "recorded"` says the
    downstream chooses; this walks those edges to closure, so the frame a run actually read is
    pinned and its outputs are checked beside the published one (ticket 10a).

    Returns `(upstream row, the job that consumed it)` pairs, in discovery order. A recorded
    upstream that did not succeed is *not* pinned: the edge link then reports it as unpinned,
    which is a chain resting on a run that never finished.
    """
    found: list[tuple[dict[str, Any], str]] = []
    seen = {(job, run_id) for job, of_job in pins.items() for run_id in of_job}
    frontier = [row for of_job in pins.values() for row in of_job.values()]
    edges = list(edges)
    while frontier:
        row = frontier.pop()
        for name, entry in sorted(row["inputs"].items()):
            declared = E.edge_for(edges, row["job_name"], name, row["spec_version"])
            ref = run_reference(entry)
            if declared is None or declared.upstream_pin != "recorded" or ref is None:
                continue
            upstream = by_id.get(ref)
            if upstream is None or upstream["status"] != "success":
                continue
            if (upstream["job_name"], ref) in seen:
                continue
            seen.add((upstream["job_name"], ref))
            found.append((upstream, row["job_name"]))
            frontier.append(upstream)
    return found


# ------------------------------------------------------------------ output links ----
def check_output(name: str, entry: dict[str, Any], row: dict[str, Any], *, iceberg: Iceberg,
                 es: Any, attributions: dict[str, E.Attribution] | None = None,
                 ) -> tuple[gate.Link, bool, bool]:
    """One recorded output resolved against the store that holds it.

    Returns the link plus the two facts the walk needs beside the verdict: whether the artefact
    is still there at all (what `retention` asks of a failed run) and whether it is still the
    current one (what `publication_ready` asks of a successful one).

    An output shape this gate cannot resolve is a crash, not a pass: a store nobody checks is
    exactly the hole the ledger exists to close.

    A table declared column-attributed is one several runs write into, and the two questions
    separate there. *Who wrote it* is still the snapshot summary when the writer could stamp
    one (`theme_samples` writes its frames with `writeTo`), and the column when it could not
    (`MERGE INTO` has no such write option). *Is it still current* can never be head-identity
    on a shared table -- the next writer moves the head within the hour -- so it is answered by
    the run's rows still being there, and still being that many (ticket 10a).
    """
    run_id, job = row["run_id"], row["job_name"]
    attributions = attributions or {}
    table = entry.get("table")
    if isinstance(table, str) and table.startswith(ICEBERG_PREFIX):
        found = identities(entry)
        if len(found) != 1:
            raise ValueError(f"{job}/{run_id}: output {name} = {json.dumps(entry)} does not name "
                             f"exactly one (table, snapshot id)")
        (t, snap), = found
        exists, stamped_run, current = iceberg.check(t, snap)
        declared = attributions.get(t)
        if declared is not None:
            written, retained = (iceberg.attributed(t, declared.column, snap, run_id)
                                 if exists else (0, 0))
            by_summary = stamped_run is not None
            return (gate.output_link(
                        job=job, run_id=run_id, output=name, store="iceberg",
                        identity=f"{t}@{snap}", exists=exists,
                        stamped=(stamped_run == run_id) if by_summary else written > 0,
                        current=retained == written and written > 0,
                        attribution=("snapshot_summary+" if by_summary else "")
                                    + f"column:{declared.column}",
                        detail=f"rows_written={written} rows_at_head={retained}"),
                    exists, retained == written and written > 0)
        return (gate.output_link(job=job, run_id=run_id, output=name, store="iceberg",
                                 identity=f"{t}@{snap}", exists=exists,
                                 stamped=stamped_run == run_id, current=current,
                                 detail=f"snapshot_run={gate.short_id(stamped_run)}"),
                exists, current)
    if "index" in entry and "alias" in entry:
        found = es_generation(es, entry["index"], entry["alias"])
        counted = found["count"] == int(entry.get("doc_count", -1))
        current = entry["index"] in found["alias_targets"]
        return (gate.output_link(job=job, run_id=run_id, output=name, store="elasticsearch",
                                 identity=entry["index"], exists=found["exists"],
                                 stamped=found["run_ids"] == [run_id] and counted, current=current,
                                 detail=(f"docs={found['count']} recorded={entry.get('doc_count')} "
                                         f"doc_run_ids={','.join(found['run_ids']) or 'none'}")),
                found["exists"], current)
    if entry.get("catalogue_load_id"):
        rows = catalogue_rows(entry["catalogue_load_id"])
        current = row.get("records_out") in (None, rows)
        return (gate.output_link(job=job, run_id=run_id, output=name, store="postgres",
                                 identity=f"{entry.get('table', 'products')}#"
                                          f"{gate.short_id(entry['catalogue_load_id'])}",
                                 exists=rows > 0, stamped=entry["catalogue_load_id"] == run_id,
                                 current=current, detail=f"rows={rows}"),
                rows > 0, current)
    if entry.get("path"):
        present = (C.PROJECT_ROOT / entry["path"]).exists()
        return (gate.output_link(job=job, run_id=run_id, output=name, store="file",
                                 identity=entry["path"], exists=present, stamped=present,
                                 current=present, detail="stamp=path_only"), present, present)
    raise ValueError(f"{job}/{run_id}: output {name} = {json.dumps(entry)} names no store this "
                     f"gate knows how to resolve; teach it the store or stop recording it")


# --------------------------------------------------------------------- the walk ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", default="development", choices=list(gate.MODES))
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    chain, edges = E.load_chain(), E.load_edges()
    attributions = E.load_attributions()
    ledger = load_ledger()
    by_id = {r["run_id"]: r for r in ledger}
    links: list[gate.Link] = []
    extra: list[str] = []

    # --- pin one run per declared job, rooted in the artefacts the phases published --------
    #: job -> {run id: ledger row}. A job usually has one pinned run; a job whose edges are
    #: declared `upstream_pin = "recorded"` has one per live output, all of them walked.
    pins: dict[str, dict[str, dict[str, Any]]] = {}

    def pin(row: dict[str, Any]) -> None:
        pins.setdefault(row["job_name"], {})[row["run_id"]] = row

    def is_pinned(job: str | None, run_id: str | None) -> bool:
        return run_id in pins.get(job or "", {})

    claimed: list[tuple[E.Capability, dict[str, Any]]] = []
    for cap in chain:
        if cap.id == SELF:
            extra.append(gate.self_line(capability=cap.id))
            continue
        if cap.status == "cut":
            extra.append(gate.cut_line(what="capability", name=cap.id,
                                       reason=cap.cut_reason or ""))
            continue
        path = C.PROJECT_ROOT / cap.artifact_path
        if not path.exists():
            extra.append(gate.pending_line(capability=cap.id,
                                           reason=f"{cap.artifact_path} not written yet"))
            continue
        doc = json.loads(path.read_text())
        if doc.get("status") == "NOT_RUN":
            extra.append(gate.pending_line(capability=cap.id,
                                           reason=doc.get("cut_reason", "artefact says NOT_RUN")))
            continue
        claimed.append((cap, doc))
        row = by_id.get(doc.get("pipeline_run_id"))
        if row is not None and row["status"] == "success":
            pin(row)

    for cap, doc in claimed:
        run_id = doc.get("pipeline_run_id")
        row = by_id.get(run_id)
        job = row["job_name"] if row else None
        links.append(gate.artefact_link(
            capability=cap.id, run_id=run_id, job=job, resolved=row is not None,
            status=row["status"] if row else None,
            job_declared=not cap.jobs or job in cap.jobs,
            scope_matches=bool(row and row["data_scope"] == doc.get("scope")),
            pinned=bool(row and is_pinned(job, run_id))))
        for job_name in cap.jobs:
            if job_name not in pins:
                found = latest_success(ledger, job_name, category=args.category, scope=args.scope)
                if found is not None:
                    pin(found)
            of_job = pins.get(job_name, {})
            primary = of_job.get(run_id) or next(iter(of_job.values()), None)
            links.append(gate.pin_link(
                capability=cap.id, job=job_name,
                run_id=primary["run_id"] if primary else None,
                source="artifact" if primary and primary["run_id"] == run_id else "latest_success",
                status=primary["status"] if primary else None))

    # --- a job with several live outputs: pin the run the downstream recorded, too ---------
    owner = {job: cap.id for cap, _ in claimed for job in cap.jobs}
    for upstream, consumer in consumed_pins(pins, edges, by_id):
        pin(upstream)
        links.append(gate.pin_link(capability=owner.get(consumer, consumer),
                                   job=upstream["job_name"], run_id=upstream["run_id"],
                                   source=f"consumed_by:{consumer}", status=upstream["status"]))
    for e in edges:
        if e.upstream_pin == "recorded" and e.status == "declared":
            extra.append(gate.recorded_pin_line(edge=e.name, reason=e.pin_reason or ""))

    # --- resolve every pinned run's outputs, then join its inputs to their upstream --------
    stale = 0
    es = None
    spark = None
    walked = [(job_name, row) for job_name, of_job in sorted(pins.items())
              for _, row in sorted(of_job.items())]
    try:
        if any(k in e for _, r in walked for e in r["outputs"].values()
               for k in ("index", "alias")):
            from src.serving.projection import client
            es = client()
        if any(str(e.get("table", "")).startswith(ICEBERG_PREFIX) for _, r in walked
               for e in list(r["outputs"].values()) + list(r["inputs"].values())):
            from src.common.spark import build
            spark = build("gate-lineage", cores="local[2]", driver_memory="2g")
        iceberg = Iceberg(spark)

        for job_name, row in walked:
            for name, entry in sorted(row["outputs"].items()):
                link, exists, current = check_output(name, entry, row, iceberg=iceberg, es=es,
                                                     attributions=attributions)
                links.append(link)
                stale += int(exists and not current)

        for job_name, row in walked:
            for name, entry in sorted(row["inputs"].items()):
                declared = E.edge_for(edges, job_name, name, row["spec_version"])
                ref = run_reference(entry)
                if ref is None:
                    reason = (declared.cut_reason if declared and declared.status == "cut"
                              else "the recorded input names no upstream run")
                    for table, snap in identities(entry):
                        exists, _, _ = iceberg.check(table, snap)
                        links.append(gate.input_link(job=job_name, run_id=row["run_id"],
                                                     input_name=name, identity=f"{table}@{snap}",
                                                     exists=exists, reason=reason))
                    continue
                upstream = by_id.get(ref)
                upstream_job = upstream["job_name"] if upstream else "unknown"
                wanted = identities(entry)
                have = [i for e in (upstream["outputs"].values() if upstream else [])
                        for i in identities(e)]
                resolves = bool(upstream) and all(i in have for i in wanted)
                if upstream and not wanted:      # the catalogue names an id, not a snapshot
                    resolves = any(e.get("catalogue_load_id") == ref
                                   for e in upstream["outputs"].values())
                links.append(gate.edge_link(
                    downstream_job=job_name, downstream_run=row["run_id"], input_name=name,
                    upstream_job=upstream_job, upstream_run=ref,
                    identity=",".join(f"{t}@{s}" for t, s in wanted) or gate.short_id(ref),
                    declared=bool(declared and declared.status == "declared"
                                  and declared.upstream == upstream_job),
                    resolves=resolves,
                    pinned=is_pinned(upstream_job, ref)))

        for edge in edges:
            if edge.status == "cut":
                extra.append(gate.cut_line(what="edge", name=edge.name, reason=edge.cut_reason or ""))
                continue
            for row in pins.get(edge.downstream, {}).values():
                if not edge.applies_to(row["spec_version"]) or edge.input in row["inputs"]:
                    continue
                links.append(gate.edge_link(
                    downstream_job=edge.downstream, downstream_run=row["run_id"],
                    input_name=edge.input, upstream_job=edge.upstream, upstream_run=None,
                    identity="none", declared=True, resolves=False, pinned=False))

        # --- a failed run keeps its partial outputs: nothing is ever rolled back ----------
        for row in [r for r in ledger if r["status"] == "failed" and r["outputs"]]:
            present = sum(int(check_output(name, entry, row, iceberg=iceberg, es=es,
                                           attributions=attributions)[1])
                          for name, entry in sorted(row["outputs"].items()))
            links.append(gate.retention_link(job=row["job_name"], run_id=row["run_id"],
                                             outputs=len(row["outputs"]), present=present,
                                             notes=(row["notes"] or "")[:60]))
    finally:
        if spark is not None:
            spark.stop()

    walked_ids = {row["run_id"] for _, row in walked}
    orphans = [r for r in ledger if r["status"] == "running" and r["run_id"] not in walked_ids]
    for r in orphans:
        extra.append(f"LINEAGE_ORPHAN run={r['run_id'][:8]} job={r['job_name']} "
                     f"started={r['started_at']:%Y-%m-%d %H:%M} status=running")
    failed = [r for r in ledger if r["status"] == "failed"]
    extra.append(gate.ledger_line(runs_pinned=len(walked), running=len(orphans),
                                  failed=len(failed),
                                  dirty=sum(1 for _, r in walked if r["worktree_dirty"])))

    pending = sum(1 for line in extra if line.startswith("LINEAGE_PENDING"))
    v = gate.verdict(links, mode=args.mode, pending=pending, stale_outputs=stale,
                     orphan_running=len(orphans), extra_lines=extra)
    v.emit()
    primary = next((r for j, r in walked if j == "silver"), None) or next(
        (r for _, r in walked), None)
    E.record(v, capability="lineage", phase="Lineage track", kind="reproducibility",
             protocol_hash=(primary or {}).get("git_commit_sha") or "uncommitted-worktree",
             population={"name": f"{args.category}/declared chain", "n": len(links),
                         "runs_pinned": len(walked), "capabilities_claimed": len(claimed),
                         "capabilities_pending": pending},
             pipeline_run_id=(primary or {}).get("run_id") or "no-run-pinned",
             scope=args.scope,
             notes=[f"mode={args.mode}", f"stale_outputs={stale}",
                    f"orphan_running={len(orphans)}"])
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

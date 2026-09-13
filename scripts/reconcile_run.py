"""Terminate a ledger run that a killed driver left `running` forever (ticket 10a).

A run row is opened `running` and closed by the job itself. When the driver dies -- an OOM
kill, a closed laptop, a `^C` between the write and the commit -- nobody closes it, and the
row stays `running` for as long as the ledger exists. `LINEAGE_GATE` prints it as
`LINEAGE_ORPHAN` and withholds `publication_ready`, which is correct: an open run is a claim
that something is still happening.

This is the one deliberate way to close one, and it is deliberately awkward: the run id and a
written reason are both mandatory, and a row that is not `running` is refused rather than
overwritten. Two things are recorded honestly:

* the status becomes `failed`, never `success` -- nobody watched this run finish, so its
  counts are unknown and stay null. ADR-0008 §3 keeps whatever partial outputs it wrote;
  nothing is rolled back, and the lineage gate's retention link still checks they are there.
* `finished_at` is the reconciliation time, not the run's end, and the note says so.

Run:  ./run.sh python scripts/reconcile_run.py --run <run id> --reason "what happened"
"""
from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from src.common.pg import connect

PREFIX = "reconciled"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", required=True, help="the run id left running")
    ap.add_argument("--reason", required=True,
                    help="what happened to the driver, in one sentence")
    args = ap.parse_args()
    if not args.reason.strip():
        raise SystemExit("--reason must say what happened; an empty reason closes nothing")

    now = datetime.now(UTC)
    note = (f"{PREFIX} {now:%Y-%m-%d %H:%M}Z: {args.reason.strip()} "
            f"finished_at is the reconciliation time, not the run's end; its counts were "
            f"never recorded and stay null")
    with connect() as conn:
        row = conn.execute("SELECT job_name, status, started_at FROM pipeline_runs "
                           "WHERE run_id = %s", (args.run,)).fetchone()
        if row is None:
            raise SystemExit(f"no ledger row for run {args.run}")
        job, status, started = row
        if status != "running":
            raise SystemExit(f"run {args.run} ({job}) is {status}, not running -- a finished "
                             f"run is never rewritten")
        conn.execute("UPDATE pipeline_runs SET status='failed', finished_at=%s, notes=%s "
                     "WHERE run_id=%s AND status='running'", (now, note, args.run))
    print(f"RUN_RECONCILED run={args.run[:8]} job={job} started={started:%Y-%m-%d %H:%M} "
          f"status=running->failed reason={args.reason.strip()}")
    sys.exit(0)


if __name__ == "__main__":
    main()

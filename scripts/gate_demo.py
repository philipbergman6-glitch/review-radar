"""Demo gate: count the rehearsals that can be read back, and check the four documents.

Reads every export directory under `docs/demo/<date>/`, the `stream_produce` rows in the run
ledger, and the four written deliverables. Prints one line per export, then the documents, then
`DEMO_GATE=PASS|FAIL`:

  DEMO_REHEARSAL  <date> cells= all_cells_ok= elapsed_s= batches= rows= replay_run= commit=
                  backup_playable= backup_missing= counts=  (and `faults=` when it does not)
  DEMO_DOCS       present=N/4 missing=

`src/gates/demo.py` decides; everything here is I/O. Nothing is taken on trust from a runner:
the cell count, the duration, the projection's batches and the git SHA all come out of the
exported notebook, and the replay that attests the stream comes out of the ledger.

On the way out the gate writes `eval/demo/gate.json` for `make eval-table`. It publishes
nothing when no export carries an attested replay run -- there would be no ledger row to
attribute the result to, and an artefact with a null run id would claim the capability did not
run, which is not the same as having rehearsed badly.

Exit 0 on PASS, 1 otherwise.  Run:  ./run.sh python scripts/gate_demo.py [--scope full]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from src.common import config as C
from src.common import evaluation as E
from src.common.pg import connect
from src.gates import demo as gate
from src.gates import lineage as L
from src.serving import demo as stage

DEMO_ROOT = C.PROJECT_ROOT / "docs" / "demo"

#: Every attempt, not only the successes: the rehearsal leaves its replay running when the
#: kernel exits, so the row that attests it is often `running` or later reconciled to `failed`.
#: Which of them attests which rehearsal is decided by start time, in `src/gates/demo.py`.
REPLAY_QUERY = """
    SELECT run_id, started_at, finished_at, status
      FROM pipeline_runs
     WHERE job_name = 'stream_produce' AND category = %s AND data_scope = %s
     ORDER BY started_at
"""


def replay_runs(scope: str, category: str) -> list[dict[str, Any]]:
    with connect() as conn:
        rows = conn.execute(REPLAY_QUERY, (category, scope)).fetchall()
    return [{"run_id": str(r[0]), "started_at": r[1], "finished_at": r[2], "status": r[3]}
            for r in rows]


def protocol_hash() -> str:
    """Identity of the frozen demo protocol: the running order plus the gate's own bars.

    Reordering the moves, adding one, or moving the rehearsal threshold changes this hash, so
    a published artefact cannot silently describe a different demo than the one rehearsed.
    """
    payload = json.dumps({
        "moves": [[m.n, m.title, m.surface, m.seconds] for m in stage.MOVES],
        "budget": stage.budget(),
        "rehearsals_required": gate.REHEARSALS_REQUIRED,
        "max_elapsed_s": gate.MAX_ELAPSED_S,
        "export_files": list(gate.EXPORT_FILES),
        "required_docs": [list(d) for d in gate.REQUIRED_DOCS],
    }, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


def read_export(d: Path, runs: list[dict[str, Any]]) -> gate.Rehearsal:
    """One directory assessed. A directory with no notebook is assessed, not skipped."""
    nb_path, html_path = d / gate.EXPORT_NOTEBOOK, d / gate.EXPORT_HTML
    try:
        nb = json.loads(nb_path.read_text()) if nb_path.exists() else {}
    except json.JSONDecodeError as exc:
        # stderr, so a corrupt export cannot interleave with the gate's own line protocol.
        # The empty notebook it falls back to has no code cells, so the export is refused.
        print(f"DEMO_EXPORT_UNREADABLE {d.name} {exc}", file=sys.stderr)
        nb = {}
    html = html_path.read_text(errors="replace") if html_path.exists() else None
    present = sorted(p.name for p in d.iterdir() if p.is_file())
    return gate.assess(d.name, nb, present_files=present, html=html, replay_runs=runs)


def present_docs() -> list[str]:
    return [path for _, path in gate.REQUIRED_DOCS if (C.PROJECT_ROOT / path).exists()]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    runs = replay_runs(args.scope, args.category)
    exports = sorted(d for d in DEMO_ROOT.iterdir() if d.is_dir()) if DEMO_ROOT.exists() else []
    rehearsals = [read_export(d, runs) for d in exports]
    docs = present_docs()

    v = L.attest(gate.verdict(rehearsals, docs, scope=args.scope), "demo")
    v.emit()

    attested = [r for r in rehearsals if r.counts and r.replay_run_id]
    if not attested:
        print(f"DEMO_GATE_NOT_PUBLISHED no attested rehearsal under "
              f"{DEMO_ROOT.relative_to(C.PROJECT_ROOT)}, so there is no run to publish against",
              file=sys.stderr)
        sys.exit(1)
    E.record(v, capability="demo", phase="Deliverables track", kind="reproducibility",
             protocol_hash=protocol_hash(),
             population={"name": f"committed exports under {DEMO_ROOT.name}/",
                         "n": len(rehearsals), "counted": len([r for r in rehearsals if r.counts]),
                         "required": gate.REHEARSALS_REQUIRED},
             pipeline_run_id=attested[-1].replay_run_id, scope=args.scope,
             notes=gate.notes(rehearsals, docs))
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

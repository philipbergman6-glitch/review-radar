"""The evaluation table: every capability the submission claims, with its verdict.

Reads `conf/lineage_chain.toml` and the per-capability artefacts at `eval/<capability>/gate.json`
(plus any `gate.<n>.json` a sanctioned reopen archived, rendered as prior rows), validates each
against `conf/eval-artifact.schema.json`, and prints one table -- blocking reproducibility
claims first, reported quality numbers second (ADR-0011).

Exit 1 when any declared capability is missing its artefact, when an artefact does not
validate, or when a cut capability nonetheless has one. A capability is either a number or a
written reason; the command refuses to print a table where it is neither.

Run:  ./run.sh python scripts/eval_table.py   (or `make eval-table`)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.common import evaluation as E


def _read(cap_id: str, path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise SystemExit(f"EVAL_TABLE_ERROR {cap_id}: {path.relative_to(E.PROJECT_ROOT)} "
                         f"is not JSON ({exc})")


def load_artifacts(chain) -> tuple[dict[str, dict | None], dict[str, list[dict]]]:
    """Loaded artefact per capability (None where the file is absent), and the archived
    artefacts of superseded runs (`gate.<n>.json`, RR-24) in archive order.

    Unreadable JSON hard-fails here rather than rendering as a missing artefact: the two
    have different fixes, and the table must not blur them.
    """
    docs: dict[str, dict | None] = {}
    priors: dict[str, list[dict]] = {}
    for cap in chain:
        path = E.PROJECT_ROOT / cap.artifact_path
        docs[cap.id] = _read(cap.id, path) if path.exists() else None
        archived = sorted(path.parent.glob("gate.[0-9]*.json"),
                          key=lambda p: int(p.suffixes[0][1:]))
        if archived:
            priors[cap.id] = [_read(cap.id, p) for p in archived]
    return docs, priors


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chain", default=str(E.CHAIN_PATH), help="declared chain to render")
    args = ap.parse_args()

    chain = E.load_chain(Path(args.chain))
    docs, priors = load_artifacts(chain)
    rows, errors = E.build_rows(chain, docs, priors)
    print(E.render(rows, errors))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

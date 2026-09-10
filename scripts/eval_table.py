"""The evaluation table: every capability the submission claims, with its verdict.

Reads `conf/lineage_chain.toml` and the per-capability artefacts at `eval/<capability>/gate.json`,
validates each against `conf/eval-artifact.schema.json`, and prints one table -- blocking
reproducibility claims first, reported quality numbers second (ADR-0011).

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


def load_artifacts(chain) -> dict[str, dict | None]:
    """Loaded artefact per capability, or None where the file is absent.

    Unreadable JSON hard-fails here rather than rendering as a missing artefact: the two
    have different fixes, and the table must not blur them.
    """
    docs: dict[str, dict | None] = {}
    for cap in chain:
        path = E.PROJECT_ROOT / cap.artifact_path
        if not path.exists():
            docs[cap.id] = None
            continue
        try:
            docs[cap.id] = json.loads(path.read_text())
        except json.JSONDecodeError as exc:
            raise SystemExit(f"EVAL_TABLE_ERROR {cap.id}: {cap.artifact_path} is not JSON ({exc})")
    return docs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chain", default=str(E.CHAIN_PATH), help="declared chain to render")
    args = ap.parse_args()

    chain = E.load_chain(Path(args.chain))
    rows, errors = E.build_rows(chain, load_artifacts(chain))
    print(E.render(rows, errors))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

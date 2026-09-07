"""Apply conf/postgres-migrations/NNNN_*.sql to the running PostgreSQL (ADR-0008).

Each migration file is applied together with its ledger row in one transaction and
its sha256 is recorded. Already-applied files are skipped; an applied file whose
checksum has changed is a hard failure (fix the history, do not edit it). A fresh
install created from 01_schema.sql carries a `baseline` row naming the last
migration it already contains; those are recorded as applied without executing.

Run:  ./run.sh python scripts/pg_migrate.py [--dry-run]
"""
from __future__ import annotations

import argparse
import hashlib
import sys

from src.common.config import PROJECT_ROOT
from src.common.pg import connect

MIGRATIONS = PROJECT_ROOT / "conf" / "postgres-migrations"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="report, apply nothing")
    args = ap.parse_args()

    files = sorted(MIGRATIONS.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not files:
        sys.exit(f"no migrations found under {MIGRATIONS}")

    with connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
                            name TEXT PRIMARY KEY, checksum TEXT,
                            applied_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
        applied = dict(conn.execute("SELECT name, checksum FROM schema_migrations").fetchall())
    baseline = applied.get("baseline")
    print(f"[migrate] {len(files)} file(s); baseline={baseline or 'none'}; "
          f"{len(applied) - (1 if baseline else 0)} already applied")

    for path in files:
        name = path.stem
        number = name.split("_", 1)[0]
        checksum = hashlib.sha256(path.read_bytes()).hexdigest()
        if name in applied:
            if applied[name] != checksum:
                sys.exit(f"[migrate] FAIL {name}: file changed after it was applied "
                         f"(recorded {applied[name][:12]}, now {checksum[:12]})")
            print(f"[migrate] skip  {name} (applied)")
            continue
        if baseline is not None and number <= baseline:
            print(f"[migrate] base  {name} (contained in baseline {baseline}, recorded only)")
            if not args.dry_run:
                with connect() as conn:
                    conn.execute("INSERT INTO schema_migrations (name, checksum) VALUES (%s, %s)",
                                 (name, checksum))
            continue
        print(f"[migrate] apply {name}")
        if args.dry_run:
            continue
        with connect() as conn:
            conn.execute(path.read_text())
            conn.execute("INSERT INTO schema_migrations (name, checksum) VALUES (%s, %s)",
                         (name, checksum))
    print("[migrate] done")


if __name__ == "__main__":
    main()

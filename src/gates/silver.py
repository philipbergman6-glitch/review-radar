"""P2 Silver's two verdicts: the identity gate, and the independent reproduction (ADR-0007 §7).

`SILVER_GATE` is printed twice by two callers over the same formatting:

* the **job** (`src/spark/silver.py`) prints it from what it just computed, over four
  constituents -- the row identity, `review_id` uniqueness, the join cardinality and, at full
  scope, the catalogue match;
* the **gate** (`scripts/gate_silver.py`) recomputes every one of those from the pinned
  Iceberg snapshots, and adds two the job cannot make about itself: that each output snapshot
  is stamped with the run id, and that the ledger row agrees with the tables.

Both terminal lines are byte-identical to what they printed before this seam existed, which
is what `tests/test_gate_verdicts.py` pins.

`SILVER_REPRO_GATE` is the second, independent claim: `scripts/reproduce_silver.py` re-derives
silver in pandas from the raw JSONL and compares. Its verdict is pure over the named checks
that comparison produced.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.common.evaluation import Verdict, repro_verdict

#: Reject precedence, in the order the silver spec applies it and the gate prints it.
REJECT_REASONS = ("unparsable_json", "missing_key_field", "invalid_rating",
                  "timestamp_out_of_range")


def reject_reason_lines(reason_counts: Mapping[str, int], counts: Mapping[str, Any]) -> list[str]:
    """One `SILVER_REJECT_REASON` line per reason, timestamps carrying their two diagnostics."""
    lines = []
    for r in REJECT_REASONS:
        extra = (f" below_1995={counts.get('timestamp_below_1995')} "
                 f"after_ingest={counts.get('timestamp_after_ingest')}"
                 if r == "timestamp_out_of_range" else "")
        lines.append(f"SILVER_REJECT_REASON reason={r} rows={reason_counts[r]}{extra}")
    return lines


def collisions_line(*, groups: int, exact: int, conflicting: int, unresolvable: int,
                    table_rows: int, removed: int) -> str:
    return (f"SILVER_COLLISIONS groups={groups} exact_groups={exact} "
            f"conflicting_groups={conflicting} "
            f"unresolvable_groups={unresolvable} "
            f"table_rows={table_rows} removed={removed}")


def ledger_line(*, run_id: str, snapshots_stamped: bool, ledger_matches_tables: bool,
                commit: str | None, dirty: bool) -> str:
    return (f"SILVER_LEDGER run_id={run_id} snapshots_stamped={str(snapshots_stamped).lower()} "
            f"ledger_matches_tables={str(ledger_matches_tables).lower()} commit={commit} "
            f"dirty={str(dirty).lower()}")


def rerun_line(*, previous_run: str | None, identical: bool | None) -> str:
    """`--verify-rerun`'s line. `identical=None` means there was no comparable earlier run."""
    if previous_run is None or identical is None:
        return "SILVER_RERUN previous_run=none identical=n/a"
    return f"SILVER_RERUN previous_run={previous_run} identical={str(identical).lower()}"


def gate_line(*, scope: str, bronze_snapshot: int, load_id: str, catalogue_rows: int,
              records_in: int, records_rejected: int, records_out: int,
              counts: Mapping[str, Any]) -> str:
    """The terminal `SILVER_GATE` line, unchanged since ADR-0007 §7 fixed its shape."""
    removed = counts["collision_rows_removed"]
    identity = records_in == records_rejected + records_out + removed
    unique = counts["review_id_distinct"] == records_out
    unmatched_ok = scope != "full" or (counts["unmatched_review_rows"] == 0
                                       and counts["unmatched_parent_asins"] == 0)
    ok = identity and unique and counts["join_cardinality_ok"] and unmatched_ok
    return (f"SILVER_GATE bronze_snapshot={bronze_snapshot} catalogue_load_id={load_id} "
            f"catalogue_rows_read={catalogue_rows} bronze_rows={records_in} reject_rows={records_rejected} "
            f"silver_rows={records_out} collision_rows_removed={removed} "
            f"unmatched_review_rows={counts['unmatched_review_rows']} "
            f"unmatched_parent_asins={counts['unmatched_parent_asins']} "
            f"join_cardinality_ok={str(counts['join_cardinality_ok']).lower()} "
            f"review_id_unique={str(unique).lower()} identity={'PASS' if identity else 'FAIL'} "
            f"gate_scope={scope} SILVER_GATE={'PASS' if ok else 'FAIL'}")


def gate_checks(*, scope: str, records_in: int, records_rejected: int, records_out: int,
                counts: Mapping[str, Any]) -> list[tuple[str, bool]]:
    """The four constituents the terminal line summarises, named in the order it prints them."""
    return [
        ("identity", records_in == records_rejected + records_out + counts["collision_rows_removed"]),
        ("review_id_unique", counts["review_id_distinct"] == records_out),
        ("join_cardinality", bool(counts["join_cardinality_ok"])),
        ("catalogue_matched", scope != "full" or (counts["unmatched_review_rows"] == 0
                                                  and counts["unmatched_parent_asins"] == 0)),
    ]


def verdict(*, scope: str, bronze_snapshot: int, load_id: str, catalogue_rows: int,
            records_in: int, records_rejected: int, records_out: int, counts: Mapping[str, Any],
            reason_counts: Mapping[str, int], collisions: Mapping[str, int],
            ledger: Mapping[str, Any] | None = None,
            rerun: Mapping[str, Any] | None = None) -> Verdict:
    """`SILVER_GATE` over already-recounted facts.

    `ledger` is the gate script's extra evidence -- snapshot stamping and ledger agreement --
    and is absent when the job prints its own gate, because a job cannot check itself against
    a ledger row it is still writing. When present it adds two constituents and can turn a
    PASS into a FAIL; the terminal line's own text never changes shape.

    `rerun` is `--verify-rerun`'s comparison against the previous run on the same bronze
    snapshot. It becomes a constituent only when such a run exists: an absent one prints
    `n/a` and is counted nowhere, because a constituent that cannot fail inflates the count
    without testing anything (audit F3).
    """
    lines = reject_reason_lines(reason_counts, counts)
    lines.append(collisions_line(**collisions))
    checks = gate_checks(scope=scope, records_in=records_in, records_rejected=records_rejected,
                         records_out=records_out, counts=counts)
    line = gate_line(scope=scope, bronze_snapshot=bronze_snapshot, load_id=load_id,
                     catalogue_rows=catalogue_rows, records_in=records_in,
                     records_rejected=records_rejected, records_out=records_out, counts=counts)
    if ledger is not None:
        lines.append(ledger_line(**ledger))
        checks += [("snapshots_stamped", bool(ledger["snapshots_stamped"])),
                   ("ledger_matches_tables", bool(ledger["ledger_matches_tables"]))]
    if rerun is not None:
        lines.append(rerun_line(**rerun))
        if rerun.get("identical") is not None:
            checks.append(("rerun_identical", bool(rerun["identical"])))
    if not all(ok for _, ok in checks):
        line = line.replace("SILVER_GATE=PASS", "SILVER_GATE=FAIL")
    return repro_verdict("SILVER_GATE", checks, line, constituents=lines)


def repro_gate_line(*, scope: str, checks: Sequence[tuple[str, bool]]) -> str:
    failed = [name for name, ok in checks if not ok]
    return (f"SILVER_REPRO_GATE scope={scope} checks={len(checks)} "
            f"failed={','.join(failed) or 'none'} "
            f"SILVER_REPRO_GATE={'PASS' if not failed else 'FAIL'}")


def repro(*, scope: str, checks: Sequence[tuple[str, bool]],
          constituents: Sequence[str] = ()) -> Verdict:
    """`SILVER_REPRO_GATE` over the named comparisons the pandas reproduction made.

    The constituent lines are passed in verbatim: the reproduction interleaves comparison and
    printing over a hundred lines of pandas, and prising those apart would risk the byte
    identity this seam exists to preserve. What is pure here is the decision.
    """
    return repro_verdict("SILVER_REPRO_GATE", checks,
                         repro_gate_line(scope=scope, checks=checks), constituents=constituents)

"""Silver gate: re-derive the identity from the ledger row and the pinned tables (ADR-0007 §7).

Reads the latest successful `silver` run, pins each of its three output tables to the
snapshot the row names, recounts, and prints the same lines the job printed -- from the
stored data, not from the job's memory. `--verify-rerun` also compares the two most recent
successful runs on the same bronze snapshot and catalogue load (counts and digests).

Exit 0 on SILVER_GATE=PASS, 1 otherwise.

Run:  ./run.sh python scripts/gate_silver.py [--scope full|sample] [--verify-rerun]
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from src.common import config as C
from src.common import runs
from src.common.pg import connect
from src.common.spark import build
from src.spark.silver import REJECT_REASONS, gate_line


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--verify-rerun", action="store_true")
    args = ap.parse_args()

    row = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    if row is None:
        sys.exit(f"no successful silver run for {args.category}/{args.scope}: SILVER_GATE=FAIL")
    inputs, outputs, counts = row["inputs"], row["outputs"], row["counts"]
    snapshot_id = int(inputs["bronze"]["snapshot_id"])
    load_id = inputs["catalogue"]["catalogue_load_id"]

    spark = build("gate-silver", cores="local[4]", driver_memory="2g")
    try:
        def pinned(name: str):
            o = outputs[name]
            return spark.read.option("snapshot-id", o["snapshot_id"]).table(o["table"])

        bronze_rows = spark.read.option("snapshot-id", snapshot_id).table(inputs["bronze"]["table"]).count()
        reviews, rejects, colls = pinned("silver.reviews"), pinned("silver.rejects"), pinned("silver.review_collisions")
        silver_rows = reviews.count()
        distinct_ids = reviews.select("review_id").distinct().count()
        reason_counts = {r: 0 for r in REJECT_REASONS}
        for r in rejects.groupBy("reject_reason").count().collect():
            reason_counts[r["reject_reason"]] = int(r["count"])
        reject_rows = sum(reason_counts.values())
        g = colls.groupBy("collision_group_id").agg(F.first("collision_class").alias("cls"), F.count("*").alias("n"))
        agg = g.agg(F.count("*").alias("groups"),
                    F.sum(F.when(F.col("cls") == "exact", 1).otherwise(0)).alias("exact"),
                    F.sum(F.when(F.col("cls") == "conflicting", 1).otherwise(0)).alias("conflicting"),
                    F.sum(F.when(F.col("cls") == "unresolvable", 1).otherwise(0)).alias("unresolvable"),
                    F.sum("n").alias("table_rows")).first()
        groups, exact, conflicting, unresolvable, table_rows = (int(agg[k] or 0) for k in
                                                               ("groups", "exact", "conflicting", "unresolvable", "table_rows"))
        removed = table_rows - groups + unresolvable
        unmatched = reviews.filter(F.col("product_title").isNull())
        recounted = {
            "collision_rows_removed": removed, "review_id_distinct": distinct_ids,
            "unmatched_review_rows": unmatched.count(),
            "unmatched_parent_asins": unmatched.select("parent_asin").distinct().count(),
            "join_cardinality_ok": bool(counts.get("join_cardinality_ok")),
        }
        stamped_ok = all(
            spark.sql(f"SELECT summary['run_id'] FROM {outputs[n]['table']}.snapshots "
                      f"WHERE snapshot_id = {outputs[n]['snapshot_id']}").first()[0] == row["run_id"]
            for n in outputs)
    finally:
        spark.stop()

    for r in REJECT_REASONS:
        extra = (f" below_1995={counts.get('timestamp_below_1995')} after_ingest={counts.get('timestamp_after_ingest')}"
                 if r == "timestamp_out_of_range" else "")
        print(f"SILVER_REJECT_REASON reason={r} rows={reason_counts[r]}{extra}")
    print(f"SILVER_COLLISIONS groups={groups} exact_groups={exact} conflicting_groups={conflicting} "
          f"unresolvable_groups={unresolvable} table_rows={table_rows} removed={removed}")
    ledger_agrees = (row["records_in"], row["records_out"], row["records_rejected"],
                     int(counts["collision_rows_removed"])) == (bronze_rows, silver_rows, reject_rows, removed)
    print(f"SILVER_LEDGER run_id={row['run_id']} snapshots_stamped={str(stamped_ok).lower()} "
          f"ledger_matches_tables={str(ledger_agrees).lower()} commit={row['git_commit_sha']} "
          f"dirty={str(row['worktree_dirty']).lower()}")
    line = gate_line(scope=args.scope, bronze_snapshot=snapshot_id, load_id=load_id,
                     catalogue_rows=int(counts["catalogue_rows_read"]), records_in=bronze_rows,
                     records_rejected=reject_rows, records_out=silver_rows, counts=recounted)
    if not (stamped_ok and ledger_agrees):
        line = line.replace("SILVER_GATE=PASS", "SILVER_GATE=FAIL")
    print(line)

    if args.verify_rerun:
        with connect() as conn:
            prev = conn.execute(
                """SELECT run_id, records_in, records_out, records_rejected, counts FROM pipeline_runs
                   WHERE job_name='silver' AND status='success' AND category=%s AND data_scope=%s
                     AND run_id <> %s AND (inputs->'bronze'->>'snapshot_id')::bigint = %s
                     AND inputs->'catalogue'->>'catalogue_load_id' = %s
                   ORDER BY started_at DESC LIMIT 1""",
                (args.category, args.scope, row["run_id"], snapshot_id, load_id)).fetchone()
        if prev is None:
            print("SILVER_RERUN previous_run=none identical=n/a")
        else:
            same = (prev[4].get("digests") == counts.get("digests")
                    and (prev[1], prev[2], prev[3]) == (row["records_in"], row["records_out"], row["records_rejected"]))
            print(f"SILVER_RERUN previous_run={prev[0]} identical={str(same).lower()}")
            if not same:
                sys.exit(1)
    sys.exit(0 if line.endswith("SILVER_GATE=PASS") else 1)


if __name__ == "__main__":
    main()

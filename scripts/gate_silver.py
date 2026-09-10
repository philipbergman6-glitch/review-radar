"""Silver gate: re-derive the identity from the ledger row and the pinned tables (ADR-0007 §7).

Reads the latest successful `silver` run, pins each of its three output tables to the
snapshot the row names, recounts, and prints the same lines the job printed -- from the
stored data, not from the job's memory. `--verify-rerun` also compares the two most recent
successful runs on the same bronze snapshot and catalogue load (counts and digests).

The decision is pure and lives in `src/gates/silver.py`; everything here is the I/O that
feeds it. On the way out it writes `eval/silver/gate.json` so `make eval-table` can render
P2 without anyone reading this output by hand.

Exit 0 on SILVER_GATE=PASS, 1 otherwise.

Run:  ./run.sh python scripts/gate_silver.py [--scope full|sample] [--verify-rerun]
"""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.pg import connect
from src.common.spark import build
from src.gates import lineage as L
from src.gates import silver as gate
from src.gates.silver import REJECT_REASONS


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

    rerun = None
    if args.verify_rerun:
        with connect() as conn:
            prev = conn.execute(
                """SELECT run_id, records_in, records_out, records_rejected, counts FROM pipeline_runs
                   WHERE job_name='silver' AND status='success' AND category=%s AND data_scope=%s
                     AND run_id <> %s AND (inputs->'bronze'->>'snapshot_id')::bigint = %s
                     AND inputs->'catalogue'->>'catalogue_load_id' = %s
                   ORDER BY started_at DESC LIMIT 1""",
                (args.category, args.scope, row["run_id"], snapshot_id, load_id)).fetchone()
        rerun = {"previous_run": None, "identical": None} if prev is None else {
            "previous_run": prev[0],
            "identical": (prev[4].get("digests") == counts.get("digests")
                          and (prev[1], prev[2], prev[3]) == (row["records_in"], row["records_out"],
                                                              row["records_rejected"]))}

    ledger_agrees = (row["records_in"], row["records_out"], row["records_rejected"],
                     int(counts["collision_rows_removed"])) == (bronze_rows, silver_rows, reject_rows, removed)
    v = gate.verdict(
        scope=args.scope, bronze_snapshot=snapshot_id, load_id=load_id,
        catalogue_rows=int(counts["catalogue_rows_read"]), records_in=bronze_rows,
        records_rejected=reject_rows, records_out=silver_rows, counts=recounted,
        reason_counts=reason_counts,
        collisions={"groups": groups, "exact": exact, "conflicting": conflicting,
                    "unresolvable": unresolvable, "table_rows": table_rows, "removed": removed},
        ledger={"run_id": row["run_id"], "snapshots_stamped": stamped_ok,
                "ledger_matches_tables": ledger_agrees, "commit": row["git_commit_sha"],
                "dirty": row["worktree_dirty"]},
        rerun=rerun)
    v = L.attest(v, "silver")
    v.emit()
    E.record(v, capability="silver", phase="P2 Silver", kind="reproducibility",
             protocol_hash=row["git_commit_sha"] or "uncommitted-worktree",
             population={"name": f"{args.category}/silver.reviews", "n": silver_rows,
                         "bronze_snapshot_id": snapshot_id, "catalogue_load_id": load_id,
                         "silver_snapshot_id": outputs["silver.reviews"]["snapshot_id"]},
             pipeline_run_id=row["run_id"], scope=args.scope)
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

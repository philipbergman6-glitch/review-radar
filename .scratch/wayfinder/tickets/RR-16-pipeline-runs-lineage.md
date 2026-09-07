---
id: RR-16
title: What pipeline_runs records, and what the demo shows from it
type: grilling
status: open
assignee: unassigned
blocked-by: []
blocks: [RR-13, RR-14]
---

## Question

`conf/postgres-init/01_schema.sql` defines a `pipeline_runs` table — `stage`, `category`,
`records_in`, `records_out`, `started_at`, `finished_at`, `notes` — with the comment "every
pipeline run records what it processed, so results are reproducible and we can show lineage
during the demo". Nothing writes to it `[observed: 0 rows, audit §2]`. Dead schema reads
worse on a CV than no schema, so either it gets a real job or it is dropped.

Decide:

1. **Keep or drop.** If kept, it is the project's lineage story: a grader asks "how do you
   know silver came from this bronze snapshot?" and the answer is a row. If dropped, the
   README stops mentioning lineage.
2. **What one row means.** Per stage, per run: which Iceberg snapshot id it read, which it
   wrote, counts in/out/rejected, wall-clock. Does the exactly-once gate write a row too?
3. **Who writes it.** Spark via JDBC at the end of each `foreachBatch` / job, or the driver
   script with `psycopg` after the job returns? The first is more honest about streaming;
   the second is simpler and cannot half-write.
4. **What the demo shows.** One query — `select stage, records_in, records_out, ... order by
   started_at` — as the "here is every run this data went through" move. Is that on the
   demo-moves list, and where?

The gate this feeds (`RR-13`): the silver reconciliation identity
`count(bronze) == count(silver) + count(rejects) + duplicates_removed` can be **read from
this table** rather than recomputed, which is what makes it a printable number.

Blocked on the silver decision, because silver is the first stage that has anything to
record.

## Input from RR-07 (closed 2026-09-06)

- The classifier run's `pipeline_runs` row carries model version, freeze commit, seed,
  chosen `regParam`, per-theme thresholds, `evaluation_status`, and for the full-corpus
  benchmark: rows scored, rows excluded (empty text), wall time, executor config.

## Input from RR-01 (closed 2026-09-06)

Every `product_month` document carries `source_gold_snapshot_id`, and the live alias points
at `product_month_<gold_snapshot_id>` (ADR-0004). The lineage gate should assert that the
alias's snapshot id equals the gold snapshot the latest `pipeline_runs` row for the
projection stage says it read — a second printable identity beside the silver reconciliation.

## Input from RR-02 (closed 2026-09-07) — unblocked

Silver's row is specified; decide the schema around it. Silver records: bronze input
snapshot id, silver output snapshot id, `catalogue_load_id`, `catalogue_rows_read`, every
gate count (`bronze_rows`, `reject_rows` per reason, `silver_rows`, collision groups by
class, `collision_rows_removed`, unmatched counts), status, `SILVER_SPEC_VERSION` (manually
bumped), `git_commit_sha`, `worktree_dirty`. The catalogue loader writes its **own** row
whose `run_id` *is* the `catalogue_load_id` stamped on every `products` row — so `products`
gains a `catalogue_load_id` column and `01_schema.sql` changes. Silver's JDBC read must see
exactly one distinct load id. Item 1 (keep or drop) is therefore effectively answered:
keep. Items 2–4 remain: exact columns (JSON `counts` blob vs typed columns), who writes
(driver-side psycopg after the job returns fits the batch model), and the demo query.

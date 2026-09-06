---
id: RR-16
title: What pipeline_runs records, and what the demo shows from it
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-02]
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

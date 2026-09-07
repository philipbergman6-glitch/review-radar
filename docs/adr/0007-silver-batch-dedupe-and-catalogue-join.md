---
status: accepted
date: 2026-09-07
---

# Silver is a snapshot-pinned batch with content-only dedupe and a JDBC catalogue join

Silver runs as a bounded Spark batch over one named bronze Iceberg snapshot and replaces its
three tables (`reviews`, `rejects`, `review_collisions`). Each output table commits
atomically; the three-table stage does not. A successful run records the complete output
set (amended 2026-09-07, ADR-0008). Rows are validated
first, under a fixed precedence of four reject reasons, and only valid rows are grouped by
`review_id = SHA-256(canonical(user_id, parent_asin, timestamp_ms))`. Groups are classified
exact / conflicting / unresolvable; a conflicting group's survivor is chosen by content
only (highest helpful votes, longest text, lowest canonical hash), an unresolvable group
(rating disagreement) has no survivor. The product catalogue is loaded once into PostgreSQL
by `COPY` under a `catalogue_load_id`, and every silver run reads it over Spark JDBC and
left-broadcast-joins it, denormalising four columns. The gate prints
`bronze_rows = reject_rows + silver_rows + collision_rows_removed` with
`collision_rows_removed = table_rows − groups + unresolvable_groups`.

## Considered options

- **Streaming silver as the authoritative path** — dedupe across 23 years needs the whole
  history in hand; a stateful stream would either hold it all or miss collisions. Rejected;
  a streaming path may sit beside batch for the P8 demo and reconcile against it.
- **Survivor by Kafka `(partition, offset)` or file line** — deterministic only if the
  producer preserves per-key order, and not reproducible from the raw JSONL that the
  independent pandas reproduction reads. Content-only ordering is stable by construction.
- **Prefer verified purchases as survivor** — would bias the verified-mix robustness check
  that grades decline candidates.
- **Always pick a survivor, never exclude** — for a rating-decline analysis a group that
  disagrees on rating has no known rating; excluding it is the honest choice. On the
  current file the class is empty (0 groups measured).
- **Snapshot the catalogue into Iceberg and broadcast from there** — faster on paper, but
  makes PostgreSQL a one-off source rather than part of every run; the JDBC read is a
  112,590-row broadcast, cheap enough that the honest architecture costs nothing.
- **Spark JDBC write for the loader** — awkward with `TEXT[]` columns; `COPY` via psycopg
  with a staging table and transactional swap is simpler and prints its own gate.
- **Inner join** — silently drops reviews and breaks the identity. Left join with printed
  unmatched counts (threshold zero at full scope) instead.

## Consequences

- The dedupe key is the review identity; every downstream key (search doc id, embedding
  key, theme label) is the same `review_id`, and it is computable from the source file
  alone.
- The canonical encoding (length-prefixed UTF-8, null ≠ empty, identity fields trimmed,
  title/text preserved exactly) is frozen and covered by golden-vector tests in two
  engines; that proves encoding parity only. Independent-implementation agreement is the
  pandas reproduction's job; rerun determinism is proven by running the same snapshot and
  load twice and comparing ids, counts, classes and an output digest.
- "6,139 duplicates" is retired as a phrase: 6,139 is the group count, the removed-row count
  is what silver prints (7,276 if every group has one survivor).
- `pipeline_runs` becomes load-bearing: the catalogue load id and the silver run row are
  how lineage is shown, with `SILVER_SPEC_VERSION`, commit SHA and a dirty-worktree flag.
  Downstream jobs pin reads to all three snapshots of one successful silver run, never to
  the tables' current snapshots; a partially committed run is `failed` with its partial
  outputs preserved and never rolled back (ADR-0008).
- No `details` keys are parsed; they stay in the raw metadata JSONL until a consumer exists.

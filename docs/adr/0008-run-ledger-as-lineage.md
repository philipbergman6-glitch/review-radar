---
status: accepted
date: 2026-09-07
---

# The run ledger is the lineage: one row per execution attempt, stamped on every output

`pipeline_runs` is the **run ledger**: one row per execution attempt of every independently
rerunnable job, inserted as `running` before the job starts and finalized to `success` or
`failed` after. The row's `run_id` (a UUID generated before execution) is stamped into every
Iceberg snapshot summary, Elasticsearch document and evaluation artefact the attempt
produces, so the ledger names its outputs and the outputs name their row. Each job's
contract — required input, output and count keys and a count identity — is registered in
code by `(job_name, spec_version)` and validated before `success`. Lineage is checked in
three separate ways: **provenance** (recorded identities resolve), **completeness** (a fully
drained producer run's acknowledged count matches the distinct Kafka records in a pinned
bronze snapshot) and **freshness** (the published chain uses the expected current outputs).

## Considered options

- **Drop the table** — dead schema reads worse than none; but ADR-0007 already made the
  catalogue load id and silver's gate counts depend on a row. Rejected.
- **Spark writes rows from `foreachBatch`** — honest about streaming but a second writer path
  from executors and abandons Iceberg's native streaming sink, whose ancestry-walk epoch
  dedupe is what makes replay accounting possible. Rejected; driver-side psycopg, with a
  `StreamingQueryListener` accumulating per-batch evidence for the whole execution
  (`recentProgress` is a bounded buffer and was rejected as the durable record).
- **One write after success** — a crashed job leaves nothing, and the id does not exist early
  enough to stamp on snapshots. Rejected for insert-then-finalize, accepting that a killed
  driver leaves `running`, which the freshness check catches.
- **Automatic rollback on partial multi-output failure** — rollback moves the current
  snapshot and could undo another writer's commit; it also adds recovery complexity.
  Rejected: a failed row keeps its partial outputs, and downstream jobs pin reads to the
  complete output set of one successful run, never to a table's current snapshot.
- **Fully typed wide table or one-row-per-metric** — the first rots as jobs are added, the
  second makes the demo query unreadable. Chosen: typed core counts plus `inputs`,
  `outputs`, `counts`, `params` JSONB, validated by the contract registry.
- **`BIGSERIAL` or a job-prefixed readable id** — serial ids are reused after a volume reset
  and would let a stamped snapshot point at the wrong row; a short hex suffix is not a
  durable cross-system identity; a job prefix sorts by job, not time. Chosen: UUID stored
  everywhere, shortened only for display, ordered by `started_at`.
- **`records_in == records_out` as bronze's identity** — false per attempt: after a crash
  between Iceberg commit and checkpoint, Spark re-presents the batch and the sink skips it.
  Chosen: `records_in == records_out + records_replayed`, replay matched by table, persistent
  query id and epoch on the current snapshot's ancestry.
- **`Σ producer.records_acked == bronze.records_in`** — fails across partial drains, multiple
  drains and replay. Replaced by the separate provenance and completeness checks.
- **Age-based staleness** — timestamps add nothing the snapshot-id comparison does not;
  freshness is id mismatch only.
- **`PASS(dirty)`** — would extend the verdict vocabulary frozen in ADR-0006. Chosen:
  `chain_clean` and `publication_ready` as separate fields; publication mode refuses a dirty
  chain and exits nonzero.

## Consequences

- Sample and full data never share a Kafka topic or a bronze table; the source-collision
  story stays uncontaminated, and time travel is demonstrated between an early micro-batch
  snapshot of the full drain and its completion. No `expire_snapshots` before submission.
- Every Kafka record carries a `producer_run_id` header; bronze stores the distinct producer
  ids and per-producer processed / committed / replayed counts.
- The expected chain and RR-12's cuts live in one configuration file
  (`conf/lineage_chain.toml`) shared by the lineage gate and the evaluation-table renderer;
  contracts live in code. A cut cannot orphan a required dependent; zero evaluated edges is
  a failure.
- Schema changes are migrations with a checksummed, transactional ledger; fresh installs
  record the baseline from the files; a test proves init and init+migrations agree. A
  Postgres volume reset erases ids that snapshots and ES documents reference and is never
  the routine mechanism.
- Cleanliness is captured at run start over all executable and dependency inputs, with a
  recorded policy version; a clean commit is necessary for a publication claim, not
  sufficient — input identities and effective configuration are recorded alongside.
- Bronze's count contract is frozen only with two tests: a replay fixture (deleted
  checkpoint commit for a non-empty batch → replayed count, zero new output, table
  unchanged) and rejection of missing attribution evidence.
- Durable streaming accounting may require additional evidence writes beyond insert and
  finalize; the lifecycle is insert plus finalization, not a guarantee of a `failed` row
  after every crash.

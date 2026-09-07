---
status: accepted
date: 2026-09-07
---

# The streaming phase is a reconciled projection beside batch, with injected lateness

The streaming phase (P8) replays the timestamp-sorted review file into its own Kafka topic
at a constant record rate, and one Structured Streaming job aggregates it per product and
calendar month under a 30-day event-time watermark, in append mode, into
`stream.product_month`; `foreachBatch` applies the frozen decline rule to closed months and
appends `stream.alerts`. Batch silver and gold remain the only source of truth; the streaming
tables are a demonstration projection that must **reconcile** with the pinned gold snapshot,
product-month by product-month, except where late rows were dropped. Lateness does not occur
naturally in this data, so it is **injected**: two deterministically selected held-back
slices, one released 7 days late in event time (must be accepted) and one 730 days late
(must be dropped), with expected counts known before the run. The gate prints both counts
beside Spark's own `numRowsDroppedByWatermark`, and a control run with no injection must
print zero drops.

## Considered options

- **Streaming silver that replaces or mirrors batch silver** — history-wide content-only
  dedupe cannot be reproduced by a stateful stream without holding all state (ADR-0007), and
  Philip's standing instruction is not to rebuild the pipeline to claim streaming. Rejected
  for an aggregation-only path whose `dropDuplicatesWithinWatermark(review_id)` is exact for
  key collisions because the id includes the timestamp.
- **Replaying the 10k sample into the full topic as the live streaming beat** — contradicts
  ADR-0008 (sample and full never share a topic or bronze table) and proves nothing about
  event time. Withdrawn; the stream has its own topic.
- **Event-time-linear compression** — spends 61% of the demo clock on the 5% of rows before
  2015. Rejected for a constant record rate over the whole sorted file, with an event-time
  clock printed so the compression is visible.
- **Letting partition interleaving produce the late events** — dishonest (the sort produced
  order, the consumer's skew is bounded by one micro-batch) and not reproducible. Rejected for
  held-back slices released at fixed event-time lags; the control run proves natural drops are
  zero.
- **Pre-running the replay before the demo** — safe but lifeless. Kept as the documented
  fallback if rehearsal shows contention; the primary is a background stream started in the
  notebook's Spark session at move 2 and read at a new closing move.
- **Product-months only, alerts by a later batch pass** — loses the "alert fires when
  persistence is satisfied" beat that ADR-0001 promises. Kept as the fallback if gold's rule
  is not a pure per-product function when P8 starts.

## Consequences

- P3 must factor the decline rule as a pure function over one product's calendar spine; the
  independent pandas reproduction needs the same factoring.
- A `sort_replay` job and the producer's held-back schedule are unconditional (about an
  hour); the watermark job, injection, gate and reconciliation are capped at two days and are
  RR-12's to cut.
- The live move list changes: move 2 starts the replay and query; a tenth move prints
  `STREAM_GATE` and the alerts table; the 701,528 → 711,528 checkpoint claim is retired to the
  recorded exactly-once transcript.
- In append mode a month is evaluated only after it closes, so no evaluation point ever
  changes flag because of late data; the honest loss numbers are rows dropped beyond the
  watermark and alerts touched by them, both printed.
- The survivor rule in the stream is first-arrival, not content-only; documented as a known
  difference that cannot change a rating aggregate given the measured collision classes.

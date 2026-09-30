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

## Amended 2026-09-14 (ticket 15), by building the control run

Three things this ADR assumed turned out not to hold, and the decisions that replaced them.
None of them moves a number or a threshold; all three were settled before the first control
run, and the run that followed printed `STREAM_GATE=PASS` with zero drops and zero differing
product-months over 193,939 compared. (Those were against the pre-freeze gold run; re-run on
2026-09-14 against the frozen gold `0cbdcc0d`, `eval/stream_control/gate.json` reads 129,330
compared, 0 differing — the spine shrank with the frozen `min_reviews`, the verdict did not.)

**Append-mode aggregates per calendar month are not expressible in Spark.** `window()` refuses
any interval carrying months, and append mode keys its state on an event-time attribute in the
grouping, which `date_trunc('month', event_ts)` is not. What runs instead is a `foreachBatch`
that appends each micro-batch's partial aggregate to `stream.product_month_batches` and an
atomic sum of one run's contributions into `stream.product_month`. Every projected column is
additive, so the sum is the aggregate. `distinct_users` is the one column that is not additive;
the projection does not carry it and `STREAM_GATE` names it as unprojected rather than letting
it go missing quietly. The watermarked, stateful part of the query -- which is where the
correctness claim lives -- is unchanged: `dropDuplicatesWithinWatermark(review_id)` under the
30-day watermark, exactly as decided above.

**The stream topic has one partition, not six.** Kafka orders records within a partition and
nowhere else, so six partitions do not carry the sort job's output in event-time order; they
carry six interleavings of it, and a consumer reading a bounded offset range from each
reassembles them out of order by however far the partitions have drifted. In the sparse early
years a drift of a few hundred records is years of event time, so the 30-day watermark would
drop rows and the control run would report lateness the partitioning invented. This is the
same objection this ADR already made to letting partition interleaving produce the late
events, applied to the topic that carries the order.

**Retention is off on the stream topic, because the message timestamp is the event time.**
Kafka's log cleaner deletes segments by the record timestamp, and this replay stamps each
record with the review's own event time -- which is what makes a console consumer show 2003
going past. Under the broker's default 168 hours a replay of twenty-three years of history is
therefore deleted within minutes of being written: the first control run read 212,469 of
701,528 records because 489,059 had already aged out between the replay finishing and the
projection starting, and both jobs reported success over the fraction that survived. The topic
holds a fixed historical replay, so wall-clock retention has nothing to express about it;
`retention.ms` and `retention.bytes` are set to -1 at replay time. The replay also resets the
topic first, so it holds exactly one ordering of the file and the lineage gate can resolve the
producer's Kafka output by comparing the topic's record count with the run's acked count.

## Amended 2026-09-14 (ticket 16), by building the demo run

The injection this ADR promised is built, and four things about it were decided at the
implementation rather than here. The demo run printed `STREAM_GATE=PASS` over 18
constituents: 2,000 dropped against 2,000 expected, 1,013 differing product-months and 309
months present only in gold, all 1,322 explained by dropped rows, with zero months present
only in the stream. (Against the frozen gold `0cbdcc0d`, run `c95be84a` on 2026-09-14 reads
882 differing and 177 gold-only, all 1,059 explained, zero only in the stream —
`eval/stream_demo/gate.json`.)

**The slices are drawn, held back and *predicted* before a record is sent.**
`src/ingest/lateness.py` is pure: it draws each slice by `slice_rank(seed, salt, review_id)`
over the frozen config, computes the send order that holds each drawn row back until the
first natural row at or after its event time plus its lag, and then simulates the watermark
over that order. A plan whose simulation does not land exactly on the frozen counts -- 2,000
near accepted, 2,000 far dropped, no natural row touched -- is an `InjectionPlanError`
before Kafka is opened, not a gate failure hours later. The held-back rows are written to a
sidecar whose sha256 the ledger records, so the gate reads the same list the producer sent.
The gate then opens `conf/stream_replay.toml` itself and checks the run's recorded sizes
and lags against the frozen ones: a run checked only against its own counts would pass
having injected 1,999.

**Spark judges a batch's late rows by the watermark in force one batch earlier.** The
watermark computed from batch N is the one applied to batch N+2, not N+1
(`eventTimeWatermarkForLateEvents` in `IncrementalExecution`); verified against pyspark
3.5.3 in `tests/test_stream_spark.py`, where the model's drop set and
`numRowsDroppedByWatermark` agree on an injected sequence. Eligibility for the far slice
therefore requires a natural row two batches and two slack widths before the release point
to be past `T + watermark`, which is what makes the drop independent of where the batch
boundary happens to fall. Under that rule 528,600 of the file's rows are far-eligible and
58,065 are undecidable in the sparse early years; the frozen 2,000 is drawn from the former.

**The demo replays onto its own topic, `reviews.stream.demo`.** The lineage gate resolves a
producer's Kafka output by comparing the topic's record count with the run's acked count, so
two replays cannot share one topic. Adding the name changed the protocol hash, so the sort
and the control run were replayed under the new hash (`4c8bccff40bd`) and the gate's
`control_and_demo_share_protocol` check holds. Two `stream_produce` runs are now live at
once, so the chain's `stream_aggregate ← stream_produce` edge is declared
`upstream_pin = "recorded"`: each projection names the replay whose topic it read, and both
replays are walked.

**Near acceptance is derived, and says so.** The projection carries product-months, not
review ids, so no row in it can be pointed at as "the near slice". What is observed is the
drop count (2,000, equal to the far slice), that all 2,000 far ids are on the topic, and that
every differing and every gold-only product-month is explained by exactly the far
contribution. Near acceptance follows from those; `STREAM_INJECTION` prints
`near_accepted=derived` rather than claiming an observation it did not make.

The alerts table the move list above promises is still not built; move 10 prints
`STREAM_GATE` only.

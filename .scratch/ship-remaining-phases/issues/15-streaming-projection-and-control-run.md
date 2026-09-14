# 15 — The streaming projection and the control run

**What to build:** a Structured Streaming job producing product-month aggregates **beside**
batch, never replacing it, and a control run proving the two agree. "Beside batch" stops
being a claim and becomes a checked reconciliation.

The shape:

- one streaming job **sharing silver's validation function**, so a divergence between the
  two paths shows up as a reconciliation failure rather than as silent drift;
- a 30-day watermark with `dropDuplicatesWithinWatermark` — exact here because the review
  identity carries the timestamp;
- append-mode product-month aggregates with the frozen decline rule applied per batch.

**The control run must print zero natural drops and zero differing product-months.** That
is what stops a passing demo run from being an artefact of an unsorted file.

**Blocked by:** 02 (gates write artefacts) and 14 (the sorted topic and paced replay).

**Status:** done (2026-09-14)

- [x] The streaming job calls silver's validation function, not a copy of it
- [x] A test asserts the streaming path's validation is identical to silver's
- [x] 30-day watermark and `dropDuplicatesWithinWatermark` in place
- [x] Streaming product-month aggregates reconciled against the batch gold table
- [x] The control run prints zero natural drops and zero differing product-months
- [x] Trip case: an unsorted input must make natural drops non-zero
- [x] Trip case: a diverged validation function must break reconciliation
- [x] The run registered a run contract

**The control run.** `STREAM_GATE=PASS` at `eval/stream_control/gate.json`, 11/11
constituents, run `fe24b1d5`. 701,528 records read off `reviews.stream`, 7,276 duplicates
removed — the number `sort_replay` recorded as `key_collision_rows` before this job existed —
leaving 694,252, which is silver's row count exactly. Zero dropped by the watermark. 193,939
product-months compared against `gold.product_month@2967561192178189477`, zero differing, zero
in gold and not in the stream, over sixteen micro-batches.

A further 210,541 product-months are in the stream and not in gold, and they are reported
rather than failed: gold materialises only products that could ever fill a decline window, so
the two tables are answering different questions there. What is left in `only_in_stream` after
that split — a product gold did materialise, in a month gold recorded no reviews for — is the
constituent that blocks, and it is zero.

**Alerts are not here.** ADR-0010 pairs the projection with `stream.alerts`, and the decline
rule per micro-batch needs a product's whole calendar spine, which only exists once the months
have closed. The control run's claim is about the projection and needs no alert; ticket 16's
demo run is where the "alert fires when persistence is satisfied" beat is asked for. The
projection carries the columns the rule reads, so nothing here blocks it.

**Three things ADR-0010 assumed that did not hold**, all settled before the first control run
and written up as an amendment to it: calendar-month windows are not expressible in Spark's
`window()`, so the append-mode aggregate is append-only per-batch contributions plus a summed
projection; the stream topic has one partition, because Kafka orders within a partition and
the watermark rests on order; and retention is off on the stream topic, because the record
timestamp is the event time and the broker's log cleaner deletes by it — the first control run
read 212,469 of 701,528 records for that reason, and both jobs reported success over the
fraction that survived.

**What this hands ticket 16.** A passing control run to contrast the injected one against; the
`stream_aggregate` job, its contract and `STREAM_GATE`'s constituents to extend; a
`--reset`ting replay so the demo run's topic holds exactly its own ordering; and the watermark
frozen in `conf/stream_replay.toml`, validated on load to sit strictly between the near and far
lags so the injected counts are falsifiable.

---
id: RR-10
title: Replay pacing, time compression and the watermark
type: grilling
status: closed
assignee: philip
blocked-by: []
blocks: [RR-13, RR-14]
---

## Question

The streaming story is the audit's F4 and the answer to "isn't this just batch with extra
steps?". Today: five Iceberg commits between 13:46:27 and 13:46:31 — the whole dataset drains
in ~5 seconds — and the file is not chronological (6,388 timestamp inversions in the first
10,000 lines; only 20 consecutive same-`asin` pairs). The producer docstring's ordering claim
has already been corrected in the repo.

Decide:

1. **Compression ratio.** The data spans 2000-11-01 → 2023-09-09, with a 2020 peak of 126,753
   reviews. Mapping ~23 years onto a demo window is the choice: how many minutes, and is the
   mapping linear over event time or over record count? Linear over event time makes the
   early years empty and 2020 a flood — which may be exactly the point, or may be a dead
   two minutes on stage. State which, and why.
2. **Producer rate.** What `--rate` follows from the ratio, and does the demo replay the
   whole 701,528 or a window of it? The runbook's live sequence currently replays a 10,000-row
   sample so bronze goes 701,528 → 711,528 — decide whether that step survives.
3. **Watermark delay.** How late is too late, given the replay is sorted by timestamp and
   lateness is therefore *injected* rather than natural. Say where the late events come from:
   a deliberately held-back slice is honest and demonstrable; pretending the sort produced
   them is not.
4. **Streaming silver: replace or sit beside the batch silver?** Two code paths cost
   maintenance and confuse the "which one is real?" question; one path means the earlier
   phases' gate has to be re-provable in streaming form.
5. **Sort mechanics.** A one-off sort of the replay file by timestamp, or sorting at the
   producer? And what this does to the `parent_asin` keyed partitioning that the producer
   currently demonstrates — chronological order across six partitions is not global order on
   the consumer side, and that is a Q&A trap worth pre-answering.

**Gate.** "Late data visibly handled" is not a number. The resolution must name the printed
one: e.g. count of events accepted after their window closed vs count dropped beyond the
watermark, with the expected values for the demo run.

## Input from RR-09 (closed 2026-09-04)

- The replay is a **historical backtest of a frozen monitoring rule**, not a prospective
  experiment; say so on stage.
- The demo payoff is the alert firing at `alert_triggered_at` (when persistence P was
  satisfied), never at the retrospective `condition_started_at`. The streaming gold must
  therefore emit the per-evaluation-point relation incrementally.
- The holdout starts 2020-01-01; the pre-2020 stretch is where the rule was developed, so a
  compression that spends most of the demo window before 2020 wastes the payoff. 2020–2021
  carry 251,650 reviews `[observed, phase0-profile]`; 2023 is censored at September.
- Watermark interacts with "unevaluable": a late review that lands after its product-month
  was evaluated must be handled as a re-evaluation or a documented loss; the printed gate
  should include how many evaluation points changed flag because of late data.

## Input from RR-01 (closed 2026-09-06)

Stream is **P8, conditional**: the cut rule is `RR-12`'s, but Philip's standing input is
"do not rebuild the whole pipeline merely to claim streaming", which bears directly on item 4
(replace vs sit beside the batch silver). The phase's deliverable is a paced replay,
event-time watermark demonstration and late-event *metrics* — the gate must print those.

## Input from RR-02 (closed 2026-09-07)

- Item 4 is settled: **batch silver is authoritative** (bounded run pinned to a bronze
  snapshot, atomic replace). A streaming silver, if P8 survives, is a *separate* demo
  table/path that must reconcile with batch after the bounded replay — same `review_id`,
  same collision classes, same printed identity — and never replaces it.
- Dedupe is history-wide (content-only survivor over the whole snapshot); a streaming path
  cannot reproduce it without holding all state, so its gate is reconciliation, not dedupe.

## Resolution (closed 2026-09-07)

Grilled in eight questions; Philip took every recommendation. Detail in
[ADR-0010](../../../docs/adr/0010-streaming-sits-beside-batch-with-injected-lateness.md);
terms in `CONTEXT.md` *Streaming*. Two residual numbers (rate, trigger) were fixed by the
session as stated assumptions, not grilled.

**Mechanism that reshaped the question `[observed, Spark semantics]`.** A watermark exists only
on a stateful aggregation and is `max event time seen − delay`, advanced per micro-batch.
Bronze has none. Wall-clock pacing decides nothing about lateness; it decides the visual and
the micro-batch composition. So *compression* is a staging choice and *watermark delay* is an
event-time choice, decided independently below.

**Contradiction resolved.** ADR-0008 ("sample and full never share a topic or bronze table")
and RR-11 move 2 (sample replayed into the full topic, 701,528 → 711,528) could not both
stand. ADR-0008 wins: the sample step is **withdrawn** from the live list; move 2 is
redefined in item 2.

### 1. Shape of the streaming path `[decided]`

- Own topic `reviews.stream`; one Structured Streaming job (`stream_product_month`, a job with
  its own run-ledger contract per ADR-0008) reads Kafka directly: parse → the **same silver
  validation function** (four reject reasons, shared code) → `withWatermark(event_time, 30 days)`
  → `dropDuplicatesWithinWatermark(review_id)` → per product, per calendar-month aggregation
  `(n_reviews, sum_rating, n_verified)` in **append** mode → Iceberg `stream.product_month`.
- No streaming silver table, no bronze drain of the stream topic. Batch silver/gold stay
  authoritative (ADR-0007); the streaming path is a **demo projection that reconciles**, never a
  second source of truth.
- Why dedupe-within-watermark is exact here: `review_id` hashes `(user, product, timestamp)`,
  so every key-collision group shares an event time and always falls inside one watermark span.
  Rating conflicts measured 0 and the single conflicting group differs only in `helpful_vote`
  (RR-02), so rating aggregates match batch exactly. Survivor rule differs — first arrival, not
  content-only — and is documented, not hidden.

### 2. Where P8 shows on stage; move 2 redefined `[decided]`

- **Background stream (option a).** Move 2 becomes: start the paced replay into `reviews.stream`
  and start the streaming query **inside the notebook's one Spark session** (non-blocking
  `start()`, ADR-0009's single session). It runs under moves 3–8. A **new move 10** in the 20 s
  spare prints the gate line and the `stream.alerts` table as it fills in event-time order.
- Fallback **(b) pre-run** — replay completes before the demo, move 2 shows the printed gate and
  the alerts table — is the documented fallback, taken if either rehearsal exceeds 300 s or a
  move fails under contention. Rehearsal decides; nothing else does.
- The "checkpoint reads only new records" story lives in the recorded exactly-once transcript
  (ADR-0009); it is no longer a live claim.

### 3. Compression and window `[decided]`

- **Whole sorted file, constant record rate** (record-count-linear). Pre-2015 = 35,044 rows (5%)
  flashes by; 2020+ = 327,636 rows (47%) is 47% of the clock — the holdout payoff. Event-time-
  linear was rejected: 61% of the clock on 5% of the data.
- **Rate 3,500 rec/s `[assumed by session]`**: 701,528 rows ≈ 200 s, fitting under moves 3–8.
  **Trigger 5 s `[assumed]`**: ≈ 17,500 rows per micro-batch ≈ 1.7 months of 2020 event time,
  ≈ 6 months of 2015. Both live in `conf/stream_replay.toml` and are the first things a
  rehearsal may retune.
- The notebook prints an **event-time clock** ("stream now at 2020-06") so the compression is
  visible, not implied.

### 4. Sort mechanics and partitioning `[decided]`

- A one-off job `sort_replay` writes `data/replay/All_Beauty.sorted.jsonl`: stable sort by
  `timestamp`, ties by canonical hash; ledger row records the output SHA-256; the producer's
  ledger row records that digest as its input.
- Producer stays a line streamer keyed by `parent_asin`, 6 partitions, `--rate` as today plus
  the held-back-slice schedule (item 5).
- **Pre-answered Q&A trap**: a global sort at the producer is not global order at the consumer.
  Spark's watermark is the global max across partitions minus the delay, so cross-partition
  skew inside one micro-batch is bounded by that batch's event-time span (months), far below a
  2-year far lag and never a source of drops at a 30-day delay for the near slice. The
  **control run** (no injection) printing `natural_dropped=0` is the proof, not the argument.

### 5. Where late events come from `[decided]`

- Lateness is **injected**; said on stage: "Amazon reviews arrive in order, so a held-back slice
  is the honest way to show the mechanism."
- Two **held-back slices**, selected deterministically by `review_id` hash mod from **2015+
  reviews only** (so micro-batch spans are months, not years), sizes ≈ 0.5% each (≈ 3,300 rows),
  frozen in `conf/stream_replay.toml` with their digests printed by the producer at start.
- Release is in **event-time terms**: the producer emits a held-back row when the stream's
  max timestamp is `lag` past the row's timestamp. **Delay W = 30 days; `lag_near = 7 days`
  (must be accepted); `lag_far = 730 days` (must be dropped)**. Expected values are known
  before the run: `late_accepted = |near|`, `late_dropped = |far|`.

### 6. Alerts in the stream `[decided]`

- **Option A**: `foreachBatch` applies the **frozen decline rule** to products whose
  product-months just closed and appends to `stream.alerts` with
  `fired_at_event_time = watermark at that batch` and the run id. Requires gold's rule to be a
  **pure function over one product's calendar spine** — which the pandas independent
  reproduction (RR-09) needs anyway; P3 inherits that factoring as a requirement.
- If that function is not factored that way when P8 starts, fall back to **option B**
  (`stream.product_month` only, alerts by a batch pass afterwards) and say so in the design doc.
- Reconciliation: `stream.alerts` equals gold's holdout alerts except for products touched by
  dropped-late rows, which are listed.

### 7. The printed gate `[decided]`

Command `scripts/gate_stream.py --run <run_id>` (reads the stream job's ledger row, its
`StreamingQueryListener` evidence, `stream.product_month`, `stream.alerts`, and the pinned gold
snapshot). Printed:

```
STREAM_LATE   late_accepted=N late_dropped=N natural_dropped=N spark_dropped_by_watermark=N
STREAM_RECON  product_months_equal=N product_months_differ=N differ_explained_by_dropped=true|false
STREAM_ALERTS alerts_stream=N alerts_batch=N alerts_touched_by_dropped=N
STREAM_GATE=PASS|FAIL run_kind=control|demo
```

Pass (demo run): `late_accepted == |near|`; `late_dropped == |far| == spark_dropped_by_watermark`
(Spark's own `numRowsDroppedByWatermark`, summed over batches from the listener evidence);
every differing product-month explained exactly by its dropped rows; alerts equal outside
touched products. Pass (control run, no injection): `natural_dropped == 0`,
`product_months_differ == 0`, `alerts_stream == alerts_batch`. **Both runs are required**;
the control runs before the demo, the demo run is the live one.
Tripping cases: W = 0 drops the near slice; replaying the unsorted file makes
`natural_dropped > 0`; a diverged validation function breaks `STREAM_RECON`.
RR-09's "evaluation points that changed flag because of late data" is **0 by construction**
in append mode (a window is evaluated only after it closes); the honest loss numbers are
`late_dropped` and `alerts_touched_by_dropped`.

### 8. Size cap and what survives a cut `[decided]`

- `sort_replay` + `--rate` pacing ≈ 1 h; **not conditional** — they answer F4's ordering half
  and feed the recorded backup regardless of P8.
- Watermark job + injection + gate + reconciliation ≈ 1.5–2 days; **cap P8 at 2 days**.

### Handoffs

- **RR-12**: P8 = 2-day cap, conditional; sort + pacing (1 h) unconditional; P3 must factor
  the decline rule as a pure per-product function or P8 degrades to option B.
- **RR-13**: Stream row fixed above (`STREAM_GATE`, two run kinds). Demo gate must count the
  redefined move 2 and the new move 10; the 701,528 → 711,528 claim is gone.
- **RR-14**: RR-11 move table amended (move 2 text, move 10 added); runbook §3 and README
  streaming rows say "paced replay, injected lateness, reconciled projection"; audit F4 marked
  addressed-by-decision.
- **RR-18**: the watermark figure (near accepted / far dropped) is a slide; the sample-replay
  beat is gone.

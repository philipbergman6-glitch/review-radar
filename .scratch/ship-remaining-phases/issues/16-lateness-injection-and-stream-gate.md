# 16 — Lateness injection, the demo run, and `STREAM_GATE`

**What to build:** the run that measures the watermark rather than asserting it. Lateness
is injected in **known quantities** — a near slice that must be accepted and a far slice
that must be dropped — and the gate passes only if exactly the frozen counts land on each
side, with every differing product-month explained by dropped rows.

Two runs are required for P8 and both must be present: the control run from ticket 15, and
this demo run. A gate that only ever saw the demo run could not tell a working watermark
from an unsorted file.

In the demo, the stream starts early (move 2) and its gate is printed late (move 10), so it
runs in the background rather than costing a pause.

**Blocked by:** 15 — the projection and the passing control run.

**Status:** done 2026-09-14

- [x] Near and far lateness slices injected in counts frozen before the run
- [x] The near slice is accepted and the far slice dropped, in exactly those counts
- [x] Every product-month differing from batch is explained by dropped rows
- [x] `STREAM_GATE` prints its named constituents then a terminal verdict line
- [x] The gate requires both the control run and the demo run to be present
- [x] Trip case: a zero watermark must drop the near slice and fail the gate
- [x] The evaluation artefact validates against the contract at `scope=full`

## Done 2026-09-14

`src/ingest/lateness.py` draws both slices by `slice_rank(seed, salt, review_id)` over the
frozen `conf/stream_replay.toml`, builds the send order that holds each drawn row back until
the first natural row at or after its event time plus its lag, and simulates the watermark
over that order. A plan that is not predicted to land exactly on the frozen counts is an
`InjectionPlanError` before Kafka is opened. Spark judges a batch's late rows by the
watermark in force one batch earlier, which the model reproduces and
`tests/test_stream_spark.py` checks against `numRowsDroppedByWatermark`; the far-slice
eligibility rule follows from it. The demo replays onto `reviews.stream.demo` (the lineage
gate resolves a Kafka output by record count, so two replays cannot share a topic), which
changed the protocol hash to `4c8bccff40bd`, so the sort and the control run were replayed
under it.

Runs: sort `756fb1f2` (sha `3a532607ee9c`), control replay `b3413602`, demo replay
`d8737729` (701,528 records at 3,000 rec/s, near=2000 far=2000 released), control aggregate
`74dde108` (0 drops) → `STREAM_GATE run_kind=control … PASS` 11/11, demo aggregate
`50caffcf` (2,000 dropped, 692,252 unique) → `STREAM_GATE run_kind=demo … PASS` 18/18 at
`scope=full`: 193,630 product-months compared, 1,013 differing and 309 only in gold, all
explained by dropped rows, 0 only in the stream, `control_and_demo_share_protocol=true`.
Near acceptance is derived from the drop count and the explained differences, not observed
per row, and the constituent line says `near_accepted=derived`.

Trip case: a zero watermark is refused at `InjectionSpec` construction (near lag must sit
strictly inside it), drops the near slice in the model and in Spark
(`tests/test_lateness.py`, `tests/test_stream_spark.py`), and fails the gate in
`tests/test_stream_projection.py`.

Not built: the `stream.alerts` table ADR-0010's move list mentions — move 10 prints
`STREAM_GATE` only. ADR-0010 amended; `make stream-demo` / `make gate-stream-demo` added.

# 14 — The sorted topic and the paced replay

**What to build:** a replay of the review file into its own Kafka topic, sorted once by
event time and paced so the pipeline is watchable — the demo shows a stream, not a
five-second batch. The event-time clock is printed as it advances, so an audience can see
time moving independently of the wall clock.

Two decisions are load-bearing:

- **Sorted once, into its own topic.** Event order becomes a property of the data rather
  than of the replay. The sort job records its input digest in the run ledger.
- **Constant record rate**, not event-time-linear compression. Pacing and slice sizes are
  frozen in configuration **before** any run.

Sample and full never share a topic.

**Blocked by:** None — can start immediately.

**Status:** done 2026-09-14

- [x] A sort job produces an event-time-ordered file and records its input digest in the ledger
      — `src/ingest/sort_replay.py`; run `bc1c5bec` records the input sha256, the output
      sha256 and 701,528 rows over 2000-11-01 → 2023-09-09, in 4.8 s
- [x] The replay writes to its own topic, separate from the batch topic and from sample
      — `reviews.stream` / `reviews.stream.sample` from `conf/stream_replay.toml`; the loader
      refuses a config where the two names are equal
- [x] Pacing is a constant record rate, read from frozen configuration
      — 3,000 rec/s; measured 2,999, and the run contract fails a run more than 10% off
- [x] The event-time clock is printed as it advances during replay
      — a line every 5,000 records: event date, records sent, rec/s, wall seconds
- [x] Pacing and slice sizes are committed before the first run
      — `conf/stream_replay.toml` `status = "frozen"`, committed before any replay existed;
      both producers hard-fail while it is provisional
- [x] The replay registered a run contract
      — `sort_replay` and `stream_produce` in `src/common/runs.py` + migration 0005, each
      with a named tripping case in `tests/test_stream_replay.py`

**Not in this ticket, on purpose:** lateness injection (the slices are frozen here, ticket 16
releases them) and the streaming job itself (ticket 15). Every replay this ticket can run is
therefore a *control* run in ADR-0010's sense, and its ledger row says `run_kind=control`.

**Found on the way:** `(timestamp, review_id)` is not a total order on this file. review_id
hashes (user, product, timestamp) and the source has 6,139 key-collision groups over 13,415
rows, so the sort key carries the line's content digest as a third component. The counts the
sort now records — 694,252 distinct review ids, 7,276 collision rows, 7,275 of them
byte-identical — reproduce silver's dedupe arithmetic from raw JSONL with no shared code, and
are the number of rows ADR-0010's `dropDuplicatesWithinWatermark` will drop in ticket 15.

# 18 — `DEMO_GATE`: rehearsals counted, documents checked

**What to build:** "it ran clean twice" turned from a memory into a number. A rehearsal is
an **executed export** — every cell succeeded, cell timestamps spanning ≤ 300 s, with the
stream running in the background so the rehearsal reflects demo conditions. `DEMO_GATE`
counts them and the threshold is two.

Alongside it, `docs_present` is a blocking four-file check.

The second rehearsal also produces the recorded backup: an export from an executed
notebook, so a failing stack on the day costs nothing.

**Blocked by:** 17 — the notebook.

**Status:** done (2026-09-14)

- [x] A rehearsal is defined as an executed export and validated as one, not self-reported
      — `src/gates/demo.py:assess()` derives every fact from the exported `.ipynb`: the code
      cells' `execution_count`, their `error` outputs, nbclient's recorded `metadata.execution`
      timestamps, move 10's `DEMO_LIVE` line and the git SHA move 1 printed. Nothing a runner
      writes is read
- [x] Cell timestamps must span ≤ 300 s for a rehearsal to count
      — `MAX_ELAPSED_S`, measured from `min(iopub.status.busy)` to `max(shell.execute_reply)`;
      an export with no recorded timing does not count either, because its duration cannot be
      checked
- [x] A rehearsal only counts if the stream was running during it
      — two independent records have to agree: move 10's `DEMO_LIVE batches= rows=` (the
      projection's own `foreachBatch` counter) **and** a `stream_produce` ledger run that
      *began between* the first and last cell timestamps. The looser "alive across the window"
      reading was rejected: one killed producer left `running` in the ledger would satisfy it
      forever
- [x] `DEMO_GATE` counts rehearsals against a threshold of two, and blocks
      — `REHEARSALS_REQUIRED = 2`; five named constituents, `metric = constituents_ok 4/5`
      today. `make gate-demo`, artefact at `eval/demo/gate.json`
- [x] Trip case: one rehearsal must not reach the threshold
      — `tests/test_demo_gate.py::test_one_rehearsal_does_not_reach_the_threshold`, and
      `test_no_rehearsals_at_all_fails_rather_than_passing_vacuously`: the three aggregate
      flags run over *every* export found, so they cannot be vacuously true of an empty set
- [x] Trip case: an export with a failed cell must not count
      — `test_an_export_with_a_failed_cell_does_not_count`, plus the unexecuted-cell,
      over-ceiling, no-timing, dead-stream, unattested-replay and wrong-HTML cases
- [x] `docs_present` checks all four files and blocks
      — `REQUIRED_DOCS`: `docs/DESIGN.md`, `docs/SLIDES.md`, `README.md`,
      `docs/DEMO_RUNBOOK.md`; parametrised so each one alone trips the gate. It reads
      **2/4 today** and is the only thing failing `DEMO_GATE` — ticket 19 writes the two
- [x] A recorded backup is exported from an executed notebook and committed or linked
      — `scripts/rehearse_demo.py` / `make rehearse`: `nbconvert --execute` to
      `demo.executed.ipynb`, then `--to html` **from that file** (never a second execution),
      the Kibana dashboard screenshotted with headless Chrome, and `exactly-once.txt` copied
      in. `backup_playable` is a real check, not an existence test: the HTML must carry the
      same git SHA and the same `DEMO_LIVE` line as the notebook beside it

## What the gate caught while being built

Neither of these was known before an export could be read back, which is the argument for the
ticket:

1. **The rehearsal saw no stream.** Under `nbconvert` the nine moves before move 10 take 14 s,
   not the four minutes of talking they take on stage, so the projection was stopped before
   Kafka had delivered anything (`batches=0 rows=0`, and the gate refused it). Move 10 now
   holds the stage to `MOVE_TEN_STARTS_S` (255 s) before stopping the query — zero wait on the
   day, because the talk has already spent it.
2. **Move 2 killed its own query.** Handing the `--reset` to the producer deletes the topic
   underneath a query already subscribed to it, and Spark correctly calls that data loss
   (`OffsetOutOfRangeException`, `failOnDataLoss`). `demo.reset_replay_topic()` now empties and
   recreates the topic *in the kernel* before anything subscribes, and the producer only
   appends.

## Also done here, by the ticket-16 handoff

Moves 2 and 10 are wired onto the **P8 demo run** (`reviews.stream.demo`,
`stream_producer --inject`, `demo.show_gate("stream_demo")`), which clears both of ticket 17's
`Move.pending` markers on the stream side and keeps the control run re-derivable through a
rehearsal. `make rehearse` re-derives the demo run afterwards, because the rehearsal's own
replay is the newest thing on that topic.

## The two rehearsals on record

`docs/demo/2026-09-14-3` (259 s, 50 micro-batches, 692,252 rows, replay `4c8a2df3`) and
`docs/demo/2026-09-14-4` (258 s, 49 batches, 692,252 rows, replay `0e3528ac`), both at
`commit 4ccc4e4a`, both with the four backup files. Two earlier exports were **rejected by the
gate** for the two defects above and deleted rather than committed; their orphaned replay runs
were reconciled to `failed` with the reason written out.

# 17 — The demo notebook: ten moves, one kernel

**What to build:** every built phase made visible in a single notebook kernel, so a grader
sees the system rather than reads about it. Without this, four built phases are invisible.

The notebook is **grown alongside the remaining phases, not stacked at the end** — it
exists as soon as the four built phases can be shown, and gains moves as P6, P7 and P8
land. A partial submission still has a working demo.

The shape:

- **One kernel** holding one Spark session, one Elasticsearch client and one Postgres
  connection.
- **Cells call serving helpers only**, so the demo exercises the pipeline's own code path
  rather than a parallel app written for the stage.
- **Ten moves, 4:40 of a 5:00 budget.** The paced replay starts at move 2; the stream gate
  prints at move 10.
- **Kibana carries exactly one move.** The health check runs in a real terminal first, not
  in the notebook.
- One move queries the run ledger and time-travels the lake — a Spark SQL `VERSION AS OF`
  read of silver's pinned bronze snapshot — so provenance is shown rather than described.

**Blocked by:** 02 — the built phases must write artefacts the notebook can show.

**Status:** done (2026-09-14)

- [x] One notebook, one kernel, one Spark session / ES client / Postgres connection
      — `demo.open_stage()` returns all three; the notebook opens it once and closes it once,
      asserted in `tests/test_demo_moves.py`
- [x] Cells call serving helpers; no logic is reimplemented in the notebook
      — `src/serving/demo.py`, one function per move; a test rejects a cell that names
      `SparkSession`, `Elasticsearch(`, `psycopg`, `spark.read` or a raw `SELECT`
- [x] Ten moves laid out with their time budget, totalling 4:40
      — `demo.MOVES` + `demo.budget()`: 260 s of moves + 20 s of surface switches, 20 s of
      the 300 s ceiling left unspent; `make demo-run-sheet` prints it
- [x] The replay starts at move 2 and the stream gate prints at move 10
      — move 2 starts the paced producer and the live projection; move 10 stops the query and
      shows the recorded `STREAM_GATE`
- [x] The lineage move queries the ledger and does a `VERSION AS OF` read
      — move 4: `demo.ledger()` then `demo.time_travel()`, which reads
      `bronze.reviews_raw VERSION AS OF` the snapshot silver's row pinned and compares the
      count to that run's `records_in` (701,528, verified)
- [x] Kibana carries exactly one move
- [x] Moves for phases not yet built are placed and marked, not omitted
      — moves 2 and 10 carry `Move.pending`: the injected slices, the alerts table and the
      demo run's own `STREAM_GATE` are ticket 16's, and the run sheet prints the reason

**Verified by executing the notebook** (`jupyter nbconvert --execute`) with moves 2 and 10's
stream cells neutralised, because move 2 resets `reviews.stream` and would discard the topic
the control run's gate re-derives from: every other cell ran clean in 18 s in one kernel. The
live projection was exercised separately against the existing topic — six micro-batches, the
event-time clock advancing 2015-06 → 2019-04.

**Not done here, by ticket boundary:** `DEMO_GATE`, the rehearsal exports under
`docs/demo/<date>/` and the `docs_present` check are ticket 18; the P8 demo run is ticket 16.

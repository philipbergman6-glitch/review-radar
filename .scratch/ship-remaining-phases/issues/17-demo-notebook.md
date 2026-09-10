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

**Status:** ready-for-agent

- [ ] One notebook, one kernel, one Spark session / ES client / Postgres connection
- [ ] Cells call serving helpers; no logic is reimplemented in the notebook
- [ ] Ten moves laid out with their time budget, totalling 4:40
- [ ] The replay starts at move 2 and the stream gate prints at move 10
- [ ] The lineage move queries the ledger and does a `VERSION AS OF` read
- [ ] Kibana carries exactly one move
- [ ] Moves for phases not yet built are placed and marked, not omitted

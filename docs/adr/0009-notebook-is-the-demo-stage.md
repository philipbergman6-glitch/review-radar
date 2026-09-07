---
status: accepted
date: 2026-09-07
---

# The demo stage is one notebook, with Kibana for the decline view

The five-minute live demo runs from `notebooks/demo.ipynb`: one kernel holding one Spark
session, one Elasticsearch client and one PostgreSQL connection, executed top to bottom with
one cell group per demo move. Cells only call functions from `src/serving/`; no logic lives
in the notebook. Kibana carries exactly one move, the decline-candidate dashboard over the
`product_month` alias. The stack health check runs in a real terminal before the notebook.
The recorded backup is the executed notebook exported to HTML, committed with the Kibana
dashboard PNG and the exactly-once transcript; a rehearsal is one such export whose cells
all succeeded within 300 s, and two are required.

## Considered options

- **Terminal + Kibana** — cheapest to build, and every gate already prints to a terminal.
  Rejected: the lineage move needs a Spark session on stage (`VERSION AS OF` beside the
  ledger row), which costs a JVM start per script, and the search, theme, evaluation and
  RAG moves are tables a terminal squashes.
- **Streamlit + Kibana** — interactive, with a query box and a candidate picker. Rejected:
  no move on the list needs interactivity, it is a second build on a three-week budget,
  a host process beside the 4 GB driver and torch, and not a course technology.
- **Kibana for everything** — course-expected verbatim. Rejected: the evaluation table,
  citations and fusion decomposition are pages, not tiles, and Kibana reads only the
  serving projection.
- **Screen recording as backup** — rejected for the executed-notebook export, which is
  produced by the rehearsal itself, carries the run ids and git SHA the cells print, and
  doubles as the rehearsal record.

## Consequences

- `src/serving/` holds the notebook's helpers (`demo.py`); the Streamlit plan is withdrawn
  from the README status table and architecture diagram.
- The exactly-once proof leaves the live list (65 s of 300) and becomes a figure plus a
  recording; the incremental-bronze move carries the checkpoint story live.
- The local `llama3.2:3b` label is a Q&A reserve, never on the clock (ADR-0003 stands).
- Move order and timings are recorded in the RR-11 resolution; the demo gate's printed
  number is RR-13's, the slide order RR-18's.
- Executing the notebook in CI against the compose stack is the obvious integration test
  and is decided with the integration-test strategy, not here.

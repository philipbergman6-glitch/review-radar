---
id: RR-14
title: Reconcile the register, the README and the coverage doc
type: task
status: open
assignee: unassigned
blocked-by: [RR-01, RR-02, RR-06, RR-08, RR-09, RR-10, RR-11, RR-12, RR-13, RR-15, RR-16, RR-17, RR-18]
blocks: []
---

## Question

The closing act of the map. Nothing to decide — carry every resolution into the three
documents that must agree, so the handoff to implementation sessions reads from one story.

1. **The build-plan artifact** (`Review Radar Build Plan`,
   https://claude.ai/code/artifact/958e44ac-2a9b-439b-a376-8563bbcec149). Republish it with
   **zero `open · P…` rows** in the decision register — each becomes a `settled` row carrying
   the reason to give when asked. Add the new out-of-scope lines (Kafka Connect ES sink,
   HDFS) with their arguments. Update the phase cards to the numbering from `RR-01` and the
   gates from `RR-13`.
   *Mechanics: read the artifact first and build the republish from the file the read
   returns; publish to the same URL so the link survives.*
2. **`README.md`.** Its run section points at the `RR-15` entrypoint and CI badge; the
   AI rows name the evaluation table shape from `RR-17`; lineage per `RR-16`. Its status table is the honest built/planned split and stays until
   everything is in the "built" column. Nothing moves to **built** on this map — no code is
   written here — but the *planned* rows should name the decided shape rather than "nothing
   written yet", and the phase numbering should match.
3. **`docs/course-coverage.md`.** Its Decisions section says Kibana and the explicit ES
   mapping are "Committed to Phase 2", which `RR-01` will have changed. Fix the phase
   references. Add the MLlib resolution from `RR-07` — the doc currently argues for MLlib
   in Finding 5 and the argument is answered nowhere.

**Do not re-extract the course PDFs.** They are gitignored and the coverage doc exists so
they need not be reopened.

Then check the whole map: every ticket closed, `Not yet specified` either graduated or
honestly still foggy, and a short handoff note naming what the first implementation session
picks up and what it already has decided for it.

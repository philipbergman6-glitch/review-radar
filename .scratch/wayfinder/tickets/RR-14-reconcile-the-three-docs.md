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

## Input from RR-07 (closed 2026-09-06)

- `docs/course-coverage.md` Finding 5 consequence updated to point at ADR-0002. The README
  and the build-plan register must say the same: MLlib in scope as a baseline classifier,
  not a fourth capability.

## Input from RR-08 (closed 2026-09-06)

- README status row `AI: LLM aspect sentiment + validation` and the phrase "LLM aspect
  sentiment" (README lines 9, 26, 120) must become *complaint-theme labelling* — `CONTEXT.md`
  lists "aspect sentiment" under _Avoid_.
- New documents to list and keep consistent: ADR-0003, `docs/LLM_LABEL_RUNBOOK.md`,
  `conf/complaint-theme-label.schema.json`, `docs/theme-taxonomy/merge-table.csv` (created at
  discovery).
- `label_source` string: ADR-0002 says `"llm"`, ADR-0003 says `hosted_llm | local_llm | human`.
  One value must win, in code and both ADRs.
- The map's "LLM host" note is now decided, not a default to argue with.

## Input from RR-01 (closed 2026-09-06)

`docs/course-coverage.md` Decisions §1–2 "Committed to Phase 2" → **P4 Search**, by name.
The README status table gains rows for the review search index, the `product_month`
projection and Kibana (`make up-ui`), and the artifact's phase cards take the eight-line list
from the `RR-01` answer with Deliverables as a track. The RAG and Stream cards read
"conditional; rule in `RR-12`".

## Input from RR-06 (closed 2026-09-06)

README row "Elasticsearch index (BM25 + kNN)" becomes BM25 + kNN + **client-side** RRF (RRF
is Enterprise-only on 8.17, RR-04); "349,059 … the population worth embedding" must read as
the *raw* ceiling of the vector cohort, the silver count being printed by the gate. The
coverage doc's BM25-vs-kNN paragraph should state the controlled vs production distinction
and that hybrid ≥ BM25 is a tested hypothesis, not a claim.

## Input from RR-17 (closed 2026-09-06)

- README's AI status table must use the RR-17 verdict vocabulary (PASS / FAIL / REPORTED /
  NOT_RUN) and say "evaluation set", never "gold set". RAG row reads "conditional; 30 frozen
  questions; five integer thresholds" and links ADR-0006.
- The build-plan artifact's "20 questions, faithfulness spot-check" line is superseded:
  30 questions, one human judge, grounded/adequate/abstention/false-refusal targets.
- Coverage doc: the RAG paragraph must state that temporal RAG answers contrast cited
  examples only; the quantitative theme-shift claim lives in the gold table.

## Input from RR-02 (closed 2026-09-07)

- Retire "6,139 duplicates" (README:68, coverage doc Veracity line): 6,139 is the
  **collision-group** count; the removed-row count is what silver prints (7,276 if every
  group has one survivor, all `[observed]` on the raw file).
- The PostgreSQL sentence everywhere: *two roles — Iceberg catalogue (atomic metadata
  pointer swap) and the relational catalogue source every silver run reads over JDBC.* The
  README architecture diagram's `JDBC (plan)` arrow becomes the decided shape.
- README planned rows for silver and the catalogue name the shape: batch pinned to a
  bronze snapshot, three tables, four reject reasons, three collision classes, `COPY`
  loader with `catalogue_load_id`, ADR-0007.

## Input from RR-11 (closed 2026-09-07)

README status row and architecture box now say "Demo notebook" instead of "Streamlit app"
(edited in the RR-11 commit). `docs/DEMO_RUNBOOK.md` §3 "Live, in order" is superseded by
the nine-move table in the RR-11 resolution and must be replaced at execution time; §1–2
and the checklists stand, with `ollama serve` added as optional at T-30. Coverage doc
references to Streamlit are to be withdrawn.

## Input from RR-10 (closed 2026-09-07)

- RR-11's move table is amended by ADR-0010: move 2 = start paced replay into `reviews.stream`
  + streaming query in the notebook session; move 10 (20 s) = `STREAM_GATE` line and
  `stream.alerts`. The sample-replay beat and "701,528 → 711,528" are withdrawn.
- `docs/DEMO_RUNBOOK.md` and README streaming rows: "paced replay of the sorted file, injected
  lateness (near accepted / far dropped), reconciled projection beside batch". `make
  produce-sample` stays for local development only, on its own topic.
- `docs/AUDIT_REPORT_2026-09-01.md` F4 → addressed by decision (ADR-0010), implementation in P8.
- `docs/course-coverage.md` §4: cite `withWatermark` / `dropDuplicatesWithinWatermark` /
  "event-time windows" (deck 3, 2026 part 3, lines 447/450/870) as the taught API P8 uses.

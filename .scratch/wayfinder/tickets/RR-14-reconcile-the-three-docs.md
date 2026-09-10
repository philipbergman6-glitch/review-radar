---
id: RR-14
title: Reconcile the register, the README and the coverage doc
type: task
status: closed
assignee: philip (agent session 2026-09-10)
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

## Input from RR-18 (closed 2026-09-10) — last blocker cleared

This ticket is now on the frontier and is the map's only open ticket.

- README's `Design doc, slides` row (`README.md:33`) stays `*planned*` but must name the
  decided shape: **eight sections / two pages** for the doc, **eight slides plus a title**
  for the talk, 9:30 of a 10:00 ceiling (5:00 slides + 4:40 demo). The outlines are not
  copied anywhere — they live in `RR-18` §3 and §7 and are cited.
- The build-plan artifact's presentation rows take `RR-18` §7's slide table, and its
  out-of-scope lines must carry the wording constraints from `RR-18` §4: Kafka Connect ES
  sink says **dropped for time**; HDFS concedes MinIO is not equivalent (block replication
  and rack awareness vs a flat key space with no atomic rename); Oozie is owned as an
  opinion.
- `docs/course-coverage.md` Addition C's V's paragraph becomes design-doc §2 verbatim — so
  fix it here first: retire "6,139 duplicates" for the collision-group / removed-row split
  per `RR-02` (6,139 groups, 7,276 rows removed).
- The README status table and the doc's §4 phase table share one spine: every phase, its
  gate name, its verdict from ADR-0011's vocabulary (PASS / FAIL / REPORTED / NOT_RUN), and
  a `cut_reason` for anything `NOT_RUN`. `RR-14`'s handoff note should say that the design
  doc is written *against* `conf/lineage_chain.toml`, not against prose.
- Nothing from `RR-18` needs an ADR; do not add ADR-0012 for it.

**Build state to reconcile against** `[observed README.md:17-33, 2026-09-10]`: P2 Silver,
P3 Gold, P4 Search, P5 Embeddings are **built** with passing gates. P6 Themes is mid-flight
(`5f8f907`). P7 RAG, P8 Stream, the demo notebook, the design doc and the slides are
`*planned*`. Several map tickets were written when far less existed; the reconciliation
should state the split honestly rather than inherit stale "planned" language.

## Resolution (closed 2026-09-10) — the map's destination is reached

All three documents now tell one story. Nothing was decided here; twenty-two resolutions
were carried into the places a reader actually looks.

### 1. The build-plan artifact — republished as **v4**, same URL

<https://claude.ai/code/artifact/958e44ac-2a9b-439b-a376-8563bbcec149>

- **The decision register has zero `open · P…` rows.** All six became settled rows carrying
  the reason to give when asked, plus eleven more that surfaced while settling them. Each of
  the six is tagged `was open` so the v3 → v4 delta is legible.
- Four of the six answers are *not* the option v3 posed, and the register says so rather than
  quietly picking a side: the silver dedupe rule is neither first-by-offset nor most-helpful
  (identity-based grouping with three classes); embedding scope stopped being a trade-off
  once throughput was measured; fusion is client-side because the ES endpoint is licensed
  out; the taxonomy is complaint-only, not aspect sentiment.
- **Out of scope became its own section** with the `RR-18` §4 wording constraints binding:
  Kafka Connect ES sink says *dropped for time*; HDFS concedes MinIO is not equivalent
  (block replication and rack awareness vs a flat key space with no atomic rename); Oozie is
  owned as an opinion; Sqoop/Pig keep the course's own citation.
- **Phase cards renumbered to `RR-01`** (P2 Silver → P3 Gold → P4 Search → P5 Embeddings →
  P6 Themes → P7 RAG → P8 Stream + a Deliverables *track*), each carrying its `RR-13` gate
  with the split ADR-0011 fixed: what blocks, what merely reports, and a tripping case.
- **A new presentation section** carries `RR-18` §7's slide table (9:30 of 10:00), the
  eight-section doc outline, the three trade-offs and slide 2's two variants.
- The v3 RAG line "20 questions, faithfulness spot-check" is replaced by the 30-question
  protocol. The stale "not built: silver, gold, ES index, all AI" masthead chips are gone.
- **The grade table was kept and explicitly labelled historical.** It is the auditor's
  2026-09-01 estimate; no fresh score has been measured, so re-scoring it would have been
  invention. The reasoning in its last column is why it stays.
- The six audit repairs are marked landed, each with the file and line that proves it —
  verified in code this session, not taken from memory (`prove_exactly_once.py:112,116`
  guards; `bronze.py:168-173` fanout comment; producer docstring; tracked sample).

### 2. `README.md`

- **The status table is now the phase spine `RR-18` requires**: every phase, its gate name,
  its verdict in ADR-0011's vocabulary, and the evidence. Sub-rows carry the quality lines
  that report without blocking. P6's three sub-rows, P7, P8, lineage and the three
  Deliverables rows previously had no representation at all.
- "LLM aspect sentiment" is gone from every position (lines 9, 26, 120 of the old file);
  the capability is **complaint-theme labelling**, and the AI paragraph says why the
  distinction matters — the taxonomy is complaint-only and the output is a theme set, not a
  polarity.
- `349,059` now reads as the **raw ceiling** of the vector cohort against the 345,418
  actually embedded; `6,139` reads as **collision groups over 13,415 rows → 7,276 removed**.
- **PostgreSQL's three roles are stated** (Iceberg catalogue · relational enrichment source ·
  run ledger) and the architecture diagram redrawn to show the stream topic, the AI tier and
  Kibana. The old `JDBC (plan)` framing is gone — the join is built.
- `ANTHROPIC_API_KEY` removed from the setup step; the local Ollama path is stated as the
  constraint it is. A **Decision record** section indexes the eleven ADRs and names the two
  rules running through them (thresholds set before measurement and not moved; a quality
  miss never reopens a phase).
- The opening question is quoted at the top — a reader now learns what the project answers
  before reading how it is built.

### 3. `docs/course-coverage.md`

- **Both "Committed to Phase 2" references corrected to P4 Search**, with a note explaining
  why the numbering moved, and both items marked **built** with what shipped.
- **Finding 3 gained the three qualifications** that stop the BM25-vs-kNN story from being an
  overclaim: two evaluation tables never merged; hybrid-beats-BM25 is a tested hypothesis and
  **H-E1 did not hold**; fusion is client-side on the basic licence. Labels are model-judged.
- **Finding 4 cites the taught streaming API** verbatim from deck 3 lines 447/450/870
  (`withWatermark`, `dropDuplicatesWithinWatermark`, `event-time windows`) — checked against
  `docs/course/` this session, not recalled.
- **Findings 7 and 8 are no longer "unresolved."** HDFS is declined including the single-node
  fallback (Kibana took the last memory headroom); Oozie is declined and owned as an opinion,
  with the run ledger given as the mechanism that actually filled the role.
- **The V's paragraph is fixed at the source**, since it becomes design-doc §2 verbatim, with
  an explicit warning never to write "6,139 duplicates."
- The Connect decision now separates its strong half (declining a *source*, argued from the
  deck) from its weak half (the *sink*, dropped for time) and forbids using the first to
  cover the second.
- A new closing section states what the AI layer does **not** earn: no course-technology
  credit, since `dense_vector` and `kNN` are 0 mentions. It also fixes the RAG boundary —
  temporal answers contrast cited examples only; the prevalence claim lives in the gold
  table, because a retriever returns what it ranks highest, not a representative sample.

### 4. `label_source` — propagated, not decided

`RR-13` item 8 owns the enum: `llm` · `agent_reference` · `classifier`. ADR-0003's
`hosted_llm` and ADR-0002's `llm` collapse to `llm`. This ticket carried the string into the
README and the register and decided nothing about it.

### 5. Map check

Twenty-two tickets closed, no blocked tickets, no open decisions. Two `Not yet specified`
patches were ruled **out of scope** rather than left as false fog — structured logging and
integration-test strategy became specifiable once silver existed, but both sit past the
destination and are inherited by implementation sessions under `RR-15`'s standing bar. Two
patches remain honestly foggy and are *deliberately* not map decisions: the decline-rule
thresholds and which gold population `product_month` projects, both of which belong to the
protocol freeze and must not be set before it.

### 6. Handoff to implementation

**What the first session picks up:** P6 Themes, mid-flight at `5f8f907` — label-v4 scored on
development and missing the bar; the diagnosis is in that commit. Then P7 RAG, then P8
Stream, in `RR-01` order, every day until 2026-09-21, shipping whatever is reached.

**What it does not have to decide** — all of it, essentially. The taxonomy is frozen, the
labelling host and its budget are set, the sampling frames are drawn, both baselines are
defined with their fitting protocol, and every gate's printed line is specified. The first
new code is P7's: `conf/eval-artifact.schema.json`, then `gate_rag.py`, `gate_stream.py`,
`gate_demo.py`, `gate_lineage.py`, `reproduce_gold.py` and `conf/lineage_chain.toml`.

**Three things that bind every session from here:**

1. **The design doc is written against `conf/lineage_chain.toml`, not against prose.** Its
   phase table *is* the evaluation table's spine, so an unbuilt phase appears with `NOT_RUN`
   and a `cut_reason` rather than being omitted.
2. **Thresholds do not move.** Not for a weaker labeller, not for a missed bar. A miss is
   `built, evaluated, below target` and is reported as such.
3. **A quality miss never reopens a phase** — that is what tuning against a holdout looks
   like. Only reproducibility failures reopen.

The three documents now agree, so the destination is reached and the map is closed.

---
id: RR-11
title: Demo surface — Streamlit, or a notebook plus Kibana
type: prototype
status: closed
assignee: philip
closed: 2026-09-07
blocked-by: [RR-01, RR-06, RR-08]
blocks: [RR-13, RR-14, RR-18]
---

## Question

What does the grader actually watch? Presentation & demo is 10% and stands at 1/10; the audit
notes the demo currently "has nothing to watch (5 s)".

The options, from the artifact and `docs/course-coverage.md`:

- **Streamlit** — interactive, but `src/serving/` is an empty package, it runs on the host
  alongside a 4 GB Spark driver and torch, and it is not a course technology.
- **A notebook plus Kibana** — cheaper, and Kibana is course-expected verbatim
  (`We will mostly use Kibana throughout this course`, 52 mentions, `Beats  Kafka  Logstash
  Elasticsearch  Kibana` as the taught ELK stack). `notebooks/` is currently empty.
- Some split: Kibana dashboard for the aggregate view, notebook for the search and RAG
  comparisons.

This is a **prototype** ticket, not a discussion: build the cheapest possible mock of the
demo surface — a static wireframe or a stub dashboard — and react to it. The question "how
should it look" is the one that decides this, and it is answered faster by looking than by
arguing.

Inputs the resolution needs, which is why this is blocked:

- the final phase numbering, so Kibana's home is fixed (`RR-01`);
- what the search comparison actually shows on screen (`RR-06`);
- what the aspect evaluation produces, since a precision/recall table wants a page, not a
  dashboard tile (`RR-08`).

The resolution must produce **the list of demo moves in order**, with the surface each one
runs on and its rough time on the clock — the artifact's Phase 8 says the surface should be
chosen "after the four demo moves are known", so naming them is part of this ticket.
Constraint: total live demo of five minutes, following a runbook that has run clean twice,
with a recorded backup. Streamlit only if a demo move genuinely needs interactivity.

Fold in `docs/DEMO_RUNBOOK.md`'s existing live sequence (healthcheck → producer → bronze →
Iceberg snapshots → exactly-once proof, ~65 s for the last) and say which of those survive
once the new phases exist. Also decide whether the deferred-but-cheap Iceberg time-travel
comparison earns a slot.

## Input from RR-08 (closed 2026-09-06)

- The demo **never makes a hosted call**. Every label shown is a cached
  `gold.review_theme_labels` row (raw response retained, auditable on stage).
- One optional live move: `llama3.2:3b` labelling a single review (~4 s, RR-03) under the
  frozen prompt, with a cached fallback. Requires `ollama serve` in the pre-demo checklist.
- The evaluation output is a page (per-theme table, coverage counts, disagreement examples),
  not a dashboard tile.

## Input from RR-01 (closed 2026-09-06)

Kibana's home is fixed: container under compose profile `ui` (`make up-ui`), dashboard a
repo-stored saved-object export over the `product_month` alias only; both are P4 Search
deliverables and exist before this ticket runs. Whether later panels inspect individual
reviews from the review search index is this ticket's call. Deliverables is a *track*, not
"Phase 8": this ticket owns the ordered demo moves and timing; the track owns artefacts,
rehearsal records and the playable backup (acceptance in `RR-01` log item 11).

## Input from RR-06 (closed 2026-09-06)

Candidate demo move: the **hybrid decomposition** — one descriptive query, three columns
(BM25 rank, kNN rank, fused score `Σ 1/(60+rank)`), showing a review that neither list
ranks first winning the fusion. Every review the demo can retrieve semantically is in the
≥ 20-word vector cohort; short reviews are reachable by BM25 only (production hybrid).

## Input from RR-17 (closed 2026-09-06)

- The evaluation summary table (`make eval-table`: three capability rows, verdicts
  PASS/FAIL/REPORTED/NOT_RUN) is a demo surface candidate in its own right; RAG's per-claim
  citation view (claim → cited review ids with verified window) is the RAG demo move.
- No local-model RAG row exists in the evaluation. A `llama3.2:3b` RAG answer, if wanted, is a
  demo move only and must be labelled unevaluated.
- Temporal RAG answers may only contrast cited examples; a live demo must not narrate
  "complaints about X increased" from a RAG answer — that sentence belongs to the
  theme-shift table.

## Input from RR-16 (closed 2026-09-07)

One move to place: **"every run this data went through"** — the ledger query over
`pipeline_runs` ordered by `started_at`, then Spark SQL `SELECT count(*) FROM
bronze.reviews_raw VERSION AS OF <silver row's bronze snapshot>` matching `records_in`, then
an early micro-batch snapshot vs the completed drain. 30–45 s; needs a Spark session on the
surface (psql alone shows only the ledger). Suggested position: right after the silver gate.

## Prototype (2026-09-07)

- File: `src/serving/PROTOTYPE_rr11_demo_surface.html` (throwaway; goes to a `prototype/rr-11` branch on resolution).
- Published: https://claude.ai/code/artifact/151ee31a-1b45-43b5-a378-34ac417b8b5d — `?variant=A|B|C`, ← → keys.
- Same eleven candidate moves on each variant; nine on by default (4:40 of 5:00 on B incl. surface switches); exactly-once (65 s) and the live `llama3.2:3b` label are optional and off.
- A = terminal + Kibana; B = one notebook with a live Spark session + Kibana; C = Streamlit tabs + terminal for pipeline moves + Kibana.
- Proposed default order: health → replay + incremental bronze → silver gate → lineage (ledger + `VERSION AS OF`) → Kibana decline candidates → hybrid decomposition → theme shift (cached) → eval table → RAG citations.
- Open for Philip: which surface; does exactly-once keep a slot at 65 s or move to the recorded backup; does the lineage move stay right after silver.

## Resolution (closed 2026-09-07)

Philip reviewed the three variants and took the recommendation: **variant B, a notebook
plus Kibana**. Detail in [ADR-0009](../../../docs/adr/0009-notebook-is-the-demo-stage.md);
terms in `CONTEXT.md` *Demo*. Prototype kept on branch `prototype/rr-11`
(`src/serving/PROTOTYPE_rr11_demo_surface.html`) and at the artifact link above.

### 1. The stage `[decided]`

- **`notebooks/demo.ipynb`** is the stage: one kernel holding one Spark session, one ES
  client and one psycopg connection for the whole demo, run top to bottom, one move per
  cell group. Every cell calls a function from the package — helpers live in
  `src/serving/` (the empty package gets a purpose: `demo.py` with `hybrid_decomposition`,
  `theme_shift`, `lineage_chain`, `show_citations`) — so the notebook is a thin runbook, not
  where logic lives. Committed with outputs stripped.
- **Kibana** (`make up-ui`, RR-01) carries exactly one move, the decline view, as one
  tab-switch. No review-level panels: individual reviews are shown from the notebook.
- **A real terminal** carries the first move only: `make health` reads as proof the stack
  is real in a way a `!` cell does not.
- **Streamlit is out.** No move on the list needs a query box or a picker; it would add a
  build, a host process beside the 4 GB driver and torch, and a non-course technology.
  README status row and architecture box change from "Streamlit app" to "Demo notebook".

### 2. The demo moves, in order `[decided]`

| # | move | surface | what is said out loud | s |
|---|---|---|---|---|
| 1 | Stack is real — `make health` | terminal | four services proved usable from Python | 10 |
| 2 | Replay sample + incremental bronze — `make produce-sample`, `make bronze` | notebook | the checkpoint means only the 10,000 new records are read; 701,528 → 711,528 | 35 |
| 3 | Silver gate — `make silver` → `SILVER_GATE=PASS` | notebook | every bronze row is accounted for: reject, survive, removed (RR-02 identity) | 15 |
| 4 | Every run this data went through — ledger query, `VERSION AS OF` silver's bronze snapshot = `records_in`, first micro-batch snapshot | notebook | the silver row names the snapshot it read; time travel reads it back (RR-16) | 35 |
| 5 | Decline candidates — dashboard over `product_month` | Kibana | the opening question, answered; grade reported, never used to hide a candidate (RR-09) | 45 |
| 6 | Hybrid decomposition — one descriptive query, BM25 / kNN / fused | notebook | a review neither list ranks first wins the fusion (RR-06) | 35 |
| 7 | Theme shift for candidate #1 — cached `gold.review_theme_labels` rows | notebook | themes are hypotheses, never a diagnosed cause (RR-08) | 35 |
| 8 | Evaluation table — `make eval-table` | notebook | one row per capability, threshold set before measurement (RR-17) | 20 |
| 9 | RAG with per-claim citations — one frozen question | notebook | every claim cites a review inside the asked window; it contrasts, never says "increased" (RR-17) | 30 |

Moves 260 s + ~20 s for two surface switches = **4:40**, 20 s spare. Moves 5–9 are
conditional on their phases existing; if P7 is cut (RR-12) move 9 goes and its 30 s is
spare, nothing shifts.

### 3. What is not on the live list `[decided]`

- **Exactly-once proof** (65 s, a fifth of the clock) moves to the **recorded backup** and
  the design doc: its `EOS_GATE` line is a figure, the recording is played only if asked.
  Move 2 carries the checkpoint story live.
- **Live `llama3.2:3b` label** is a **Q&A reserve move**, not on the clock: `ollama serve`
  stays on the T-30 checklist as optional, with the cached response as fallback (RR-08).
- **Iceberg time-travel comparison** earns no slot of its own; it lives inside move 4.
- Of the runbook's old live sequence: healthcheck → move 1; producer + bronze → move 2;
  `verify_iceberg` snapshots + time travel → folded into move 4; exactly-once → backup.

### 4. Recorded backup and rehearsal record `[decided]` — closes the fog patch

- The **recorded backup** is the executed notebook exported to HTML
  (`jupyter nbconvert --execute --to html`), plus a PNG export of the Kibana dashboard and
  the exactly-once run's terminal transcript, under `docs/demo/<date>/` with the git SHA and
  the run ids the cells printed. No screen recording.
- A **rehearsal** is one such export whose cells all ran without error and whose cell
  timestamps span ≤ 300 s. "Run clean twice" = two committed exports. The Deliverables
  track's acceptance (RR-01 log item 11) is satisfied by those two files; the printed
  number and threshold are RR-13's to fix.

### 5. Handoffs

- RR-13: the demo gate — command, printed value (rehearsal count, elapsed seconds),
  threshold.
- RR-18: slide order takes this move order; exactly-once becomes a figure on the
  architecture or trade-offs slide, not a demo beat.
- RR-14: README row 28 and the architecture box say "Demo notebook"; the runbook's live
  section is replaced by the table above at execution time.
- Fog (integration tests): CI executing `demo.ipynb` against the compose stack is the
  natural notebook test; decided there, not here.

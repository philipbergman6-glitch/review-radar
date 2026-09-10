---
id: RR-18
title: Design doc sections and slide narrative order
type: grilling
status: closed
assignee: philip
blocked-by: [RR-09, RR-11, RR-13]
blocks: [RR-14]
---

## Question

Graduated from the fog on 2026-09-04: the two deliverables worth 10% directly and framing
the other 90%. The brief (§7, §8) fixes the outer shape — a 1–2 page design document with an
architecture diagram, data flow, technologies and the AI capability; a 5–10 minute talk
covering problem and dataset, architecture, the AI capability and why, results and insights,
challenges and trade-offs; up to 3 minutes of questions.

Decide:

1. **Design doc sections, in order, with the number each one carries.** The coverage doc
   (`docs/course-coverage.md` §3) says the doc must name which of the V's the project
   exercises, each backed by a measured figure. Which sections, which figures — from the
   profile, the exactly-once gate, the phase gates (`RR-13`), and the evaluation tables
   (`RR-17`).
2. **The declined-out-loud paragraph.** Kafka Connect ES sink, HDFS, GraphFrames, Oozie —
   each with its one-line reason, per the map's Out of scope. Where it sits in the doc.
3. **Slide order.** Which of the demo moves opens (`RR-09` decides the question; `RR-11`
   decides the surface), where the exactly-once proof sits, where the evaluation table
   sits, where lineage (`RR-16`) sits if kept. Roughly 8 slides for 8 minutes.
4. **Trade-offs slide content.** Iceberg not a course technology (coverage doc Finding 1),
   Spark on the host not in a container, local LLM vs hosted, public-but-sensitive review
   text. Which three go on the slide.
5. **Q&A prep.** The audit's §6 red-teamed a prepared Q&A; which five questions get a
   written answer with a number in it.

**Resolution:** a section outline for the doc and a slide-by-slide outline, each entry
naming its figure. Not the doc, not the slides — those are execution.

## Input from RR-09 (closed 2026-09-04)

First three beats are fixed: (1) the opening question on screen, category-manager persona;
(2) the one line — *"N of M eligible products triggered at least one sustained-decline alert
during 2020–2023, using a rule developed on pre-2020 data and then applied unchanged"*;
(3) the hero product: two windows, adjusted theme shift, representative reviews. Then zoom
out: eligibility and yearly coverage, robustness grades, sensitivity variants, placebo
calibration and power. Three distinctions must appear in both doc and talk: alerts are not
causal proof; post-scan bootstrap intervals are descriptive unless the scan is calibrated;
the taxonomy is held out, or the aspect explanation is exploratory. The provenance result
(key collisions) is a Q&A answer, not a slide of its own. If the aspect half is unbuilt the
claim narrows to *"The completed result identifies sustained rating declines; aspect-level
explanation is preliminary."*

## Input from RR-01 (closed 2026-09-06)

Deliverables is a continuous **track**, not P8/P9: design doc, slides, README, runbook and
recorded backup are developed alongside the phases, with the acceptance criteria in `RR-01`
log item 11. The design doc's phase narrative uses the eight-line list from the `RR-01`
answer and must say RAG and Stream were conditional and how the `RR-12` rule resolved them.

## Input from RR-11 (closed 2026-09-07) — one blocker cleared

Surface and move order fixed (ADR-0009): health (terminal) → replay + incremental bronze →
silver gate → lineage → Kibana decline candidates → hybrid decomposition → theme shift →
evaluation table → RAG citations; 4:40 with 20 s spare. Slide order should follow it. The
exactly-once proof is a **figure** on the architecture or trade-offs slide, not a demo
beat; the live local label is a Q&A reserve. Only RR-13 still blocks.

## Input from RR-13 (closed 2026-09-10) — last blocker cleared

ADR-0011 splits every gate: `PASS|FAIL` carries **only reproducibility** claims and a FAIL
reopens the phase; every quality target prints its own `verdict=` on its own line and never
blocks, setting status `built, evaluated, below target`. Unreached and cut-on-merits share
`NOT_RUN` plus a mandatory `cut_reason` in `conf/lineage_chain.toml`, and `make eval-table`
hard-fails on an artefact missing *and* not declared cut. Verdict vocabulary: PASS / FAIL /
REPORTED / NOT_RUN. A phase is complete only at `scope=full`. This is what the design doc's
phase table and the evaluation slide are built on.

## Resolution (closed 2026-09-10, Philip took the recommendations across two grilling rounds)

**No ADR.** This ticket decides narrative order for two deliverables, not architecture; the
outlines below are the artifact, and `RR-14` carries them into the README and the build-plan
register. Everything here cites an existing decision rather than making a new one.

### 0. Two facts that reframed the ticket

- **The build is far ahead of the map's ticket index** `[observed README.md:17-33]`: P2
  Silver, P3 Gold, P4 Search, P5 Embeddings are **built** with passing gates (`GOLD_GATE`,
  `SEARCH_GATE`, `EMBED_GATE`; ANN recall@10 0.96). P6 Themes is mid-flight (`5f8f907`:
  label-v4 scored on development and missing). P7 RAG, P8 Stream, the demo notebook, the
  design doc and the slides are `*planned*`.
- **The talk's clock was already 94% spent.** RR-11 fixed a 4:40 live demo; the ticket's own
  "roughly 8 slides for 8 minutes" could not coexist with it. Settling the split was the
  prerequisite for every other slide decision.

### 1. The talk's time budget (Q1)

**10:00 total = 5:00 of slides + 4:40 of demo + 20 s slack**, then up to 3 minutes of
questions. The brief's ceiling is 10 minutes and there is no reward for finishing early;
RR-11 already engineered the demo to 4:40 behind a rehearsal gate (`DEMO_GATE …
max_elapsed_s`, threshold `rehearsals ≥ 2`). 5:00 of slides at ~35 s/slide = **8 slides plus
a title**. Cutting demo moves to buy slide time was rejected: the demo is the only part of
the talk that proves the pipeline runs.

### 2. Design-doc length posture (Q2)

**Two pages hard, with links — no appendix, no separate figure page.** The repo carries
eleven ADRs and a README status table; a design doc that restates them is worse than one
that points at them. Each claim carries a number and a link to `docs/adr/*` or `README.md`.
An appendix is where "we didn't want to cut" hides.

### 3. Design-doc sections, in order, with the figure each carries (Q3)

| # | section | the figure it carries |
|---|---|---|
| 1 | The question and who asks it | RR-09's question verbatim; category-manager persona; 701,528 reviews / 112,590 products |
| 2 | The data, and which V's it exercises | Volume 311 MB → 96.5 MB Parquet (3.2×); Velocity ~232k rec/s; Variety free-form `details` + Postgres catalogue; Veracity 6,139 collision groups, 720 empty-text |
| 3 | Architecture and data flow | **the diagram** — Kafka → Spark → Iceberg/MinIO, Postgres catalogue, ES + Kibana; the exactly-once proof sits here as a figure (RR-11), run ledger labelled |
| 4 | The pipeline, phase by phase | the eight-line P2…P8 list from `RR-01`; each line ends in its gate name and verdict |
| 5 | The AI capability, and why this one | deck 5 verbatim — `BM25 does not consider the semantic meaning` — then embeddings + client-side RRF; ANN recall@10 0.96, H-E1 not held / H-E2, H-E3 held |
| 6 | Results and insights | *"N of M eligible products triggered a sustained-decline alert during 2020–2023, using a rule developed on pre-2020 data and applied unchanged"*; hero product's two windows + theme shift |
| 7 | How we avoided fooling ourselves | ADR-0001 holdout + protocol freeze; ADR-0011's gate split; `RR-21`'s published agreement number |
| 8 | Trade-offs, and what was declined out loud | §5's three trade-offs + §4's four declines |

This is the brief's own §8 order with **two insertions**. §2 exists because Addition C is the
instructor's definitional slide (`Project that involves collection and analyze data with at
least on of the 4 V's`) and matching it costs a paragraph. §7 exists because it is the single
thing separating this from a student project and the brief's outline makes no room for it.

### 4. The declined-out-loud paragraph (Q4)

**All four declines, one line of reason each, as a block inside §8** — not its own section,
not scattered to where each technology would have appeared. Concentrated it reads as
judgement; scattered it reads as apology. Binding on the wording:

- **Kafka Connect ES sink** must say **dropped for time**, not hide behind the deck's
  `Except for a trivial "file" connector` line — that quote covers declining a Connect
  *source*, not a *sink* (map, Out of scope).
- **Single-node HDFS** must concede MinIO is not equivalent: block replication and rack
  awareness vs a flat key space with no atomic rename.
- **Sqoop/Pig** get the course's own citation, `Apache Sqoop moved into the Attic in June
  2021`. **Oozie** is owned as an opinion (Spark subsumes multistage orchestration), because
  the coverage doc Finding 8 says it is one.
- **GraphFrames/GraphX** — declined as a large detour; the 25% understanding criterion
  punishes breadth.

### 5. The three trade-offs (Q5) — they lead §8 and carry slide 8

1. **Iceberg is not a course technology** — one mention in 860 slides (coverage Finding 1),
   conceded; it scores under pipeline design (25%), not course technologies (20%).
2. **Local `qwen3:8b` instead of hosted Haiku** (`RR-19`: `ANTHROPIC_API_KEY` empty), with
   the 0.70 macro-F1 bar deliberately **not moved**.
3. **Public-but-sensitive review text** — beauty reviews carry acne/eczema/alopecia detail
   and `user_id`s; title and text only cross the wire, never ids (audit §6).

**Spark-on-the-host-not-in-a-container is dropped** from the slide: a local-resource
footnote, not a design trade-off, and it invites a question with no interesting answer.

### 6. How unbuilt phases are represented (Q6)

**Every phase appears in doc §4 and on the evaluation table with its gate verdict**;
unreached ones print `NOT_RUN` plus the `cut_reason` from `conf/lineage_chain.toml`, where
`"not reached by submission date"` is a legitimate, already-modelled reason (ADR-0011).
The doc's phase table *is* the eval table's spine. Omission was rejected — the grader finds
the gap anyway, and a `NOT_RUN` with a written reason reads as a system that knows its own
state. Writing unbuilt phases as built is forbidden by the map's no-overclaiming rule.

### 7. Slide-by-slide outline (Q7)

The demo is **one continuous block**, not interleaved — ADR-0009 puts every move in a single
kernel with one Spark session, and RR-10 starts the background stream at move 2, so the
session cannot be paused for slides. Structure: **setup (5 slides) → demo → payoff (3
slides)**, so everything the demo shows was claimed on a slide 30 seconds earlier and each
move lands as proof rather than as a tour.

| # | slide | s | the figure it carries |
|---|---|---|---|
| 0 | Title | 10 | project, dataset, one line |
| 1 | The question, and who asks it | 30 | RR-09 verbatim; category manager; 701,528 reviews / 112,590 products / 2000–2023 |
| 2 | The answer, in one line | 40 | the N-of-M line + hero product's two windows and theme shift; ends on "and here is why that number is defensible" |
| 3 | The data, and which V's | 30 | 311 MB → 96.5 MB (3.2×) · ~232k rec/s · free-form `details` + Postgres catalogue · 6,139 collision groups |
| 4 | Architecture and data flow | 45 | the diagram; exactly-once proof as a figure; run ledger labelled |
| 5 | The AI capability, and why this one | 40 | `BM25 does not consider the semantic meaning`; embeddings + client-side RRF; ANN recall@10 0.96 |
| — | **LIVE DEMO** | **280** | RR-11's nine moves + RR-10's move 10 (`STREAM_GATE`, `stream.alerts`) |
| 6 | How we avoided fooling ourselves | 45 | ADR-0001 holdout + protocol freeze; ADR-0011's split; `RR-21`'s agreement number |
| 7 | Results, zoomed out | 40 | eligibility and yearly coverage, robustness grades, sensitivity variants, placebo calibration + power |
| 8 | Trade-offs, and what we declined | 40 | the three trade-offs + the four declines |

Total **9:30** of the 10:00 ceiling. Slides 1–2 deliberately spend RR-09's three fixed beats
up front and the middle of the talk earns them back. RR-09's three distinctions — alerts are
not causal proof; post-scan bootstrap intervals are descriptive unless the scan is
calibrated; the taxonomy is held out or the aspect explanation is exploratory — appear on
slides 6 and 7, and in doc §6/§7.

**Demo-last was considered and rejected.** It is safer if the stack misbehaves, but RR-11
already bought that insurance with the recorded backup under `docs/demo/<date>/` and two
rehearsals; buying it twice costs the narrative.

### 8. Slide 2's two variants and when the fork is called (Q8)

**Both variants are written now; the fork is called at the second rehearsal**, which
`DEMO_GATE` already timestamps. The trigger is not "did P6 finish" but RR-09's own condition
— **fewer than 3 robust text-characterisable candidates** → the narrow variant, *"The
completed result identifies sustained rating declines; aspect-level explanation is
preliminary."* That count is printed by the gold analytical line (`text_characterisable`,
`fallback_triggered`), so the fork reads a number rather than a judgement. Shipping the
narrow variant unconditionally was rejected: it throws away a result that may exist by the
21st.

### 9. The five prepared Q&A answers (Q10 / ticket item 5)

Each pre-empts a question the audit §6 red-team predicted, and each is a number, not a
position.

1. **"Why Iceberg, when we didn't teach it?"** — one mention in 860 slides, conceded up
   front. 3.2× compaction, snapshot ids on every ledger row, `VERSION AS OF` shown live at
   demo move 4. Scores under pipeline design, not course technologies.
2. **"Show me a query where BM25 loses."** — five prepared queries with side-by-side BM25 /
   kNN / hybrid results; H-E1 not held, H-E2 and H-E3 held; ANN recall@10 0.96 vs exact.
3. **"One broker, one ES node — what does CAP even mean here?"** — operationally nothing;
   name the config that *would* decide it (`acks=all`, min ISR, replication factor 1 today)
   rather than claiming a partition story. This is audit §6's exact predicted follow-up.
4. **"Why a local 8B model, and doesn't that invalidate your 0.70 bar?"** —
   `llama3.2:3b` emitted all 8 themes on 5 of 8 reviews at 4.3 s/review; `qwen3:8b`
   discriminates at 6.7 s/review. The bar did not move; a miss is reported FAIL, and
   `RR-21`'s kappa on the stratified 50 prices the correlated-error risk.
5. **"Your 6,139 collisions — duplicates, or genuine repeat reviews?"** — 6,139 groups over
   13,415 rows: 6,138 exact, 1 conflicting on `helpful_vote`, **0 rating conflicts**, 7,276
   rows removed. Zero rating disagreements is the evidence they are the same review, not two
   opinions.

**Reserves, not written up:** *"why stream a static file"* (nothing downstream of Kafka
changes — that is the point of the broker; demo move 2 answers it visually) and *"schema
evolution — show me an example."* **Deliberately given no slot:** HDFS and Kafka Connect —
slide 8 declines both out loud, so a prepared answer would spend the argument twice.

### 10. What this hands to RR-14

- README's `Design doc, slides` row (`README.md:33`) stays `*planned*` but should name the
  decided shape: eight sections, two pages, eight slides plus a title, 9:30 of a 10:00 talk.
- The build-plan register's presentation rows take the slide table above.
- `docs/course-coverage.md` Addition C's V's paragraph becomes doc §2 verbatim — retire
  "6,139 duplicates" for the group/removed-row split per `RR-02`.
- Nothing in this ticket is an ADR; the outlines live here and are cited, not copied.

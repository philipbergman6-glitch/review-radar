---
id: RR-12
title: Cut order and drop-dead dates under the three-week budget
type: grilling
status: closed
assignee: philip
blocked-by: [RR-07]  # closed
blocks: [RR-13, RR-14]
---

## Question

Under three weeks from 2026-09-01 at 4–6 focused hours a day — call it ~80 hours, with the
submission date **assumed to be on or before 2026-09-21 and needing confirmation**. Six build
phases, three of them AI, plus deliverables that are themselves worth 10%.

Decide the order in which things get cut when the schedule slips, **before** it slips:

1. **Confirm the real deadline.** Everything below is arithmetic on it.
2. **A drop-dead date per phase** — the date after which starting that phase costs more than
   skipping it. The aspect phase carries a hand-labelling task (150–200 rows) that cannot be
   compressed by working faster, and RAG is cheap only if the embedding phase has landed.
3. **The cut order itself.** Candidate sequence to argue with: RAG first (it reuses the ES
   phase entirely, so cutting it loses one prompt and one eval), then the second insight
   thread, then the streaming re-implementation of silver, then the aspect subset size.
   Which of these is actually last to go?
4. **The floor.** What must exist for the project to be submittable at all? The audit's
   ranking by cost-to-grade is: transformation + results load (25%), any AI capability plus
   its evaluation (25%), product table + JDBC join, ES index populated, insight thread with
   numbers, then the deliverables. Anything below that line is optional by definition.
5. **The professional-bar reservation.** `RR-15` (CI, secrets, packaging) is a fixed ~2 h
   that must land before the next push; `RR-17`'s gold-set labelling is hours that cannot
   be compressed. Both get a line in the schedule.
6. **The deliverables reservation.** Design doc, slides, runbook, recorded backup, README run
   section, and a written one-paragraph explanation per file in `src/` — how many hours are
   ring-fenced for these, and from which date? They are 10% of the grade and the classic
   thing that gets eaten by the build.

Blocked on the MLlib scope decision, because adding a fourth AI item changes the arithmetic
before it starts.

**The resolution is a dated schedule, not a preference order** — each phase with a start-by
date and the cut it triggers if that date passes.

## Input from RR-07 (closed 2026-09-06)

- MLlib theme classifier baseline: **cap 1 focused day** once LLM labels exist. Cut before
  RAG **only if the LLM labels are late** (it cannot exist without them); otherwise it goes
  ahead of RAG polish. The timed full-corpus benchmark must fit inside the same day.

## Input from RR-08 (closed 2026-09-06)

Calendar constraints, not just hours:

- **Discovery cannot start until the pre-2020 decline rule has run** (it samples episode +
  control windows). Protocol freeze → gold pre-2020 run → discovery → taxonomy → prompt dev.
- **Hand labelling ≈ 7 h** (200 dev + 200 audit at ~1 min each), one person, plus a **40-row
  relabel ≥ 5 calendar days after the audit labels** — so audit labelling must finish ≥ 5 days
  before the report is written.
- **Batch API latency** up to 24 h per bulk line (discovery, training pool, holdout, audit) —
  four serialised waits unless overlapped; prompt development uses the standard API.
- ≤ 5 prompt versions; MLlib (1-day cap) only after the training pool is labelled.
- Console key + $20 workspace limit must be provisioned before the first hosted call.

## Input from RR-01 (closed 2026-09-06)

Phase list is fixed (`RR-01` answer). RAG (P7) and Stream (P8) are marked **conditional**;
this ticket decides the cut rule. Philip's protected core, verbatim intent: *Silver → Gold →
Search/Kibana → one evaluated AI capability → numerical insights → polished deliverables.*
Under schedule pressure, simplify Gold's analytical machinery rather than pull Search ahead
of Gold. The map's "do not cut yet" still holds; grill this late.

## Input from RR-17 (closed 2026-09-06)

- Hand-labelling range: **≈ 17–21 h** of Philip, un-parallelisable, 17 the optimistic bound
  (400 theme labels 6.7 h, 40 relabels 1 h, ~600 relevance judgements 3.5 h, RAG authoring +
  keys 3.3 h, RAG judging 1.5 h, plus taxonomy merge, relevance rules, answerability
  validation, two adjudication passes). Theme audit set opened last.
- Cut order inherited: conditional RAG (P7) is the first coherent cut; MLlib sub-row before
  RAG only if LLM labels are late (RR-07). If P6 must shrink, preserve the 80-row
  representative audit stratum and redesign taxonomy/support as a unit — never delete audit
  strata ad hoc.
- P7 has its own ledger (200 calls / $2); ADR-0003's 12,800 calls are fully allocated.
- Note: the Question above still says the submission date is *assumed*; the map's Notes record
  it as **confirmed 2026-09-21** `[observed 2026-09-04, Philip]`. Treat the date as fixed.

## Input from RR-10 (closed 2026-09-07)

- **P8 Stream is capped at 2 focused days** (watermark job, held-back injection,
  `gate_stream.py`, reconciliation) and stays conditional — yours to cut.
- **Unconditional, ≈ 1 h**: the `sort_replay` job and the producer's `--rate` pacing; they
  answer audit F4's ordering half and feed the recorded backup whether or not P8 exists.
- **Dependency on P3**: the decline rule must be a pure function over one product's calendar
  spine for the stream's `foreachBatch` alerts (ADR-0010). The pandas reproduction needs the
  same factoring, so this costs P3 nothing extra — but if P3 ships without it, P8 degrades
  to product-months only and loses the "alert fires live" beat.
- The 10k sample replay is withdrawn from the live demo (ADR-0008 conflict); no hours saved,
  none added.

## Working draft (2026-09-07 — SUPERSEDED by the resolution below; kept as the arithmetic record)

**Supply.** Today 2026-09-07; submission 2026-09-21 `[observed]`. Calendar `[inferred, verify]`:
Rosh Hashanah 5787 = sunset Fri 09-11 → nightfall Sun 09-13; Yom Kippur = sunset Sun 09-20 →
nightfall Mon 09-21, i.e. the submission date is Yom Kippur. Working days if holidays are
off: 09-07…09-11 (5, Fri short), 09-14…09-19 (6, Sat?), 09-20 half → ≈ 11.5 days ≈ 46–69 h
at 4–6 h/day. Without holidays off: 14 days ≈ 56–84 h.

**Demand `[assumed, my estimates]`.** Map tickets RR-13/18/14 ≈ 4 h · P2 Silver 8 · P3 Gold
14 (spine, pure-function rule, freeze, placebo, injected declines, holdout, controls, pandas
gate) · P4 Search 10 · P5 Embeddings 5 · P6 Themes 20 (incl. Philip 7.7 h labels, MLlib 1 day)
· P7 RAG 8 (incl. Philip ≈ 5 h) · P8 Stream 10 + 1 unconditional · Deliverables 16 (notebook
helpers 6, design doc 4, slides 3, 2 rehearsals 2, backup 1). **Total ≈ 96 h** vs 46–84 h.

**Hard calendar chain.** 40-row relabel ≥ 5 days after audit labels (ADR-0003) and before the
report → audit labels by 09-14 → taxonomy frozen by 09-14 morning → discovery Batch submitted
by 09-11 → gold pre-2020 run by 09-11 → silver green by 09-09. Batch waits (≤ 24 h) overlap RH.

**Draft dated schedule (5 h/day).**
| date | build | Philip-only hours | drop-dead / trigger |
|---|---|---|---|
| 09-07 | RR-12, RR-13 | — | — |
| 09-08–09 | P2 Silver + catalogue loader, `SILVER_GATE` | — | silver not green by 09-10 EOD → Gold simplification (fixed thresholds, no injected-decline power check, documented deviation) |
| 09-10–11 | P3 Gold pre-2020, protocol freeze, discovery sample → Batch | — | discovery not submitted by 09-11 → P6 becomes exploratory-only, P5 hybrid search is the evaluated capability |
| 09-12–13 | RH; Batch returns | — | — |
| 09-14 | P3 holdout + pandas gate | taxonomy merge 1.5 h, **audit labels 3.3 h** | audit labels not done by 09-15 → κ row NOT_RUN (relabel dropped), audit itself kept |
| 09-15 | P4 Search build | dev labels 3.3 h | — |
| 09-16 | P4 gate, P5 Embeddings | — | `EMBED_GATE` not green by 09-17 EOD → **P7 RAG cut** |
| 09-17 | P6 prompt dev (≤5), pool + holdout Batch; design-doc skeleton | relevance judgements 3.5 h | — |
| 09-18 | P6 theme shift, eval table; MLlib only if pool labels back | — | labels not back by 09-18 morning → MLlib cut |
| 09-19 | P6 audit scoring; P7 RAG **only if** eval table PASS by 09-18 EOD | RAG authoring/judging 5 h | — |
| 09-19–20 | **Deliverables ring-fence ≥ 10 h**: notebook, design doc, slides, 2 rehearsals, backup | relabel 40 rows 1 h (09-20) | submit before sunset 09-20 |

**Cut order (draft).** P8 Stream cut now (sort + pacing 1 h kept) → P7 RAG (drop-dead 09-17)
→ MLlib sub-row (09-18) → intra-annotator κ (09-15) → Gold calibration machinery (09-10).
**Floor** = P2 + P3 (computational gate) + P4 + P5 with its 20-query eval table + deliverables.

## Resolution (closed 2026-09-07, Philip)

**No contingency schedule.** Philip, verbatim intent: *"Forget about if the schedule slips
… Doesn't matter what happens. I work every day. So stop procrastinating and let's get to
work and build this thing."* `[observed 2026-09-07]`

- **Drop-dead dates:** none. **Pre-committed cut order:** none. The draft table above is
  the arithmetic record only (≈ 96 h demand vs 46–84 h supply, holiday dates unverified) and
  binds nothing.
- **The only rule is order.** Build P2 Silver → P3 Gold → P4 Search → P5 Embeddings →
  P6 Themes → P7 RAG → P8 Stream, in the RR-01 sequence, every day until 2026-09-21.
  Whatever is reached by the date ships; whatever is not is stated in the design doc as
  not built. "Conditional" (P7, P8, MLlib sub-row) now means exactly *built if reached*.
- **Kept regardless of position:** the 1 h `sort_replay` + `--rate` pacing (RR-10); the
  Deliverables track runs alongside every phase (each phase ships its `src/serving/` demo
  cell and its gate), so the doc, slides and recorded backup are assembled from what exists
  rather than ring-fenced by date.
- **Labelling chain** (audit labels → ≥ 5-day relabel, discovery Batch → taxonomy) is
  scheduled by P6 itself when it starts, not by this ticket.
- **Floor:** not defined. There is no floor because there is no cut decision; the protected
  core order (RR-01) already puts the highest-grade items first.

Unblocks `RR-13`. Planning after this point is limited to the three remaining map tickets;
implementation sessions begin now with P2 Silver (spec: RR-02 / ADR-0007, RR-16 / ADR-0008).

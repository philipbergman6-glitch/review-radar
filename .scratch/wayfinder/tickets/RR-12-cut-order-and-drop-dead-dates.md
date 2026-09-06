---
id: RR-12
title: Cut order and drop-dead dates under the three-week budget
type: grilling
status: open
assignee: unassigned
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

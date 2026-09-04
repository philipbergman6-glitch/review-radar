---
id: RR-12
title: Cut order and drop-dead dates under the three-week budget
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-07]
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

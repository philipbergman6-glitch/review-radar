---
id: RR-07
title: Does MLlib belong in the AI scope
type: grilling
status: open
assignee: unassigned
blocked-by: []
blocks: [RR-08, RR-12]
---

## Question

`docs/course-coverage.md` Finding 5 makes an argument the build-plan artifact never answers:

> An MLlib model scores under BOTH "course technologies" (20%) and "AI capability" (25%).
> A hosted-LLM call scores only under the latter. If the AI scope must be cut, cut toward
> MLlib, not away from it.

MLlib is taught in deck 3 (18 mentions, `SPARK MLLIB` section header). The artifact's settled
AI scope — embeddings, LLM aspect sentiment, RAG — contains no MLlib at all, and its decision
register never records why. Meanwhile the course-technologies row of the audit's grade table
projects 18/20 with "no HDFS" as the stated shortfall.

Decide:

1. Does MLlib enter the scope at all, and if so as what? The obvious candidate is the
   statistic the plan already wants for insights — per-product monthly rating z-score, or
   burst detection over trailing volume — implemented in MLlib rather than as hand-written
   Spark SQL. The audit is explicit that option (f) is worth having "only as a simple,
   explainable statistic that feeds an insight — not a model for its own sake".
2. If yes: does it *replace* any of the three AI capabilities, *add* to them, or *reframe*
   work that was going to happen anyway (the gold-layer drift calculation) so it also counts
   as a course technology?
3. If no: what is the one-sentence answer to "the course taught MLlib — why is there no
   model in your project?" It has to be better than silence, per the standing rule in
   `docs/course-coverage.md` ("a technology consciously declined with a stated reason
   demonstrates more command of the material than one included without understanding").

Weigh honestly against the 25% understanding criterion, which the audit says "punishes
breadth". Adding a fourth thing to a three-week solo build is a real cost; so is leaving a
double-scoring technology on the table.

The answer changes what the aspect-sentiment phase is for, so it blocks that ticket.

## Input from RR-09 (closed 2026-09-04)

Burst detection is **removed** from scope; do not propose it as the MLlib statistic. The
statistic that now exists is the decline alert rule (adjacent trailing windows on a calendar
spine, seeded clustered bootstrap, placebo calibration, injected-decline power check). If
MLlib enters, the honest framings are: the bootstrap/summary statistics via MLlib
`Statistics`, or an MLlib baseline classifier scored against the frozen LLM aspect labels
(RR-08/RR-17). Reframing the drift SQL as "MLlib" without a model would not survive Q&A.

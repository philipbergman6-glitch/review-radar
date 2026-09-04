---
id: RR-17
title: An evaluation table for every AI capability
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-06, RR-08]
blocks: [RR-13, RR-14]
---

## Question

AI capability is 25% and the rubric's words are "correctness, depth, and your understanding
of it". The brief (§10) adds "validate AI-generated content; LLMs can produce confident but
wrong answers". An AI feature without a measured evaluation is a demo; with one it is
engineering, and it is the difference between a CV line and a class project. `RR-08` already
fixes the aspect-sentiment validation. This ticket does the same for the other two, and makes
the three tables one shape.

For **each** of embeddings/semantic search (`RR-06`), aspect sentiment (`RR-08`) and RAG,
decide:

1. **The gold set.** How many items, how sampled (stratified by rating and by product), and
   who labels. Options for labelling: by hand; hosted Haiku as the labelling oracle with a
   hand-checked subset; weak labels from stars. Hand-labelling is the one task that cannot
   be sped up, so the count is a schedule decision as well (`RR-12`).
2. **The metric, per class, never accuracy.** 60.0% of reviews are 5★ — a constant
   "positive" scores 60%. Candidates: precision/recall/F1 per aspect-sentiment class;
   recall@k and MRR for semantic search against a set of (query → relevant reviews) pairs;
   for RAG, citation rate (every answer cites retrieved reviews) plus faithfulness judged
   against the cited text, plus a "no answer" rate on questions the corpus cannot answer.
3. **The threshold**, set now. Below it the capability ships as "built, evaluated, below
   target" with the number shown — never quietly.
4. **The disagreements.** What is reported about the rows the model got wrong. The audit's
   red-team question is "what do the disagreements look like?" — some will be the model
   being right about a misleading star rating, and saying so is the understanding the
   rubric grades.
5. **One table shape** for all three, so the slide and the design doc show them side by
   side: capability · gold set size · metric · value · threshold · pass/fail.
6. **Local vs hosted comparison, if both exist.** If `RR-08` picks Ollama for the demo, one
   row that scores the local model against Haiku on the same gold set is a cheap, sharp
   result: "the 3B model reaches X% of the hosted model's F1 at zero API cost".

**Resolution:** three filled-in specification rows (everything but the measured value), the
labelling plan with its hour estimate, and the command that will print each table.

## Input from RR-09 (closed 2026-09-04)

- The aspect gold set splits into a **pre-2020 validation set** (used to tune) and an
  **untouched post-2020 audit set** (opened only after the labelling spec freezes) so
  generalisation across the holdout boundary is a reported number.
- Add one table beyond the three capability rows: the **theme-shift table** per candidate —
  candidate and control counts, baseline and recent shares, matched-control-adjusted
  difference (per-control changes averaged, then subtracted), pointwise descriptive
  interval, label validation quality. Ranked by adjusted change; no significance claims;
  intervals stated as pointwise, not simultaneous.
- Injected-decline power results validate the rating alert only, not the theme analysis;
  the aspect table is the only evidence for the second half of the opening question.

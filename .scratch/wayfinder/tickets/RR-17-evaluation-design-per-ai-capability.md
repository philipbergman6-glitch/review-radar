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

## Input from RR-07 (closed 2026-09-06)

- Complaint-theme labelling gets a **three-system sub-row**: LLM theme labeller · text
  MLlib classifier · star-only theme baseline, all scored on identical audit rows. Pass
  rule pre-registered in RR-07 item 17 (beats star-only; ≥ 70% of LLM macro-F1).
- Support floors predeclared: ≈30 training positives, ≥10 audit positives, mechanical, same
  for all three systems. `other` excluded from headline macro, reported separately.
- Uncertainty: seeded paired bootstrap **clustered by product** on the two macro-F1
  differences, pointwise, context only.
- Classifier output is a *score*, not confidence; no comparability to `label_confidence`.

## Input from RR-08 (closed 2026-09-06)

- **LLM theme labeller pass rule, pre-registered** (ADR-0003): audit macro-F1 over supported
  named themes ≥ 0.70 **and** no supported theme with recall < 0.50. Supported = ≥ 10 audit
  positives. Report per-theme P/R/F1/support, development macro-F1 beside audit (the drop is a
  number), `other`, abstention, `parse_failed`/`api_failed` coverage, seeded product-clustered
  bootstrap interval as context.
- **Audit set = 200 rows**: 120 enriched from frozen discovery theme terms + 80
  prevalence-representative from post-2020 candidate/control windows; the 80 are reported
  separately as the unbiased number. Development set = 200 enriched pre-2020 rows.
- **Label-noise ceiling**: 40 audit rows relabelled by the same annotator ≥ 5 days later;
  per-theme intra-annotator κ reported. No second annotator exists.
- **Local vs hosted row**: `llama3.2:3b` scored on the identical development and audit rows
  under the identical frozen prompt; reported as "% of hosted macro-F1 at $0". Never primary.
- **Disagreement enum** (fixed): `model_missed` · `model_invented` · `definition_boundary` ·
  `human_error` · `star_misleading`. One row each, adjudicated once after scoring, labels and
  spec unchanged.
- **Sentiment weak-label check** is a sanity table only: 3★ excluded, one row per star bucket
  + overall, four predicted-sentiment counts, agreement rate with Wilson 95% interval.
- Contract in `conf/complaint-theme-label.schema.json`; wire body in `docs/LLM_LABEL_RUNBOOK.md`.

## Input from RR-01 (closed 2026-09-06)

The 20-query relevance judgement set is built in P4 Search (frozen queries, blind pooled
judging across analyzers, P@5 plus MRR@10 or nDCG@10) and reused for kNN and hybrid. It is
**not** the RAG evaluation set: RAG needs its own answerability, citation and faithfulness
judgements. Capability rows now map to phases P5 Embeddings, P6 Themes, P7 RAG.

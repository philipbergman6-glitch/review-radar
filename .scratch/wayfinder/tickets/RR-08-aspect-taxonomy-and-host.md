---
id: RR-08
title: Aspect taxonomy and the model that produces it
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-03, RR-07]
blocks: [RR-11, RR-13, RR-14, RR-17]
---

## Question

Three coupled decisions for the LLM aspect-sentiment phase.

1. **Taxonomy.** A fixed aspect list per category (scent, longevity, packaging, irritation,
   value, …) — easy to score, cheap to validate, but blind to whatever the reviews actually
   complain about. Or open extraction — richer, and much harder to build a precision/recall
   table for, because the label space is unbounded. If fixed: where does the list come from?
   Deriving it from a sample of reviews (or from the phase-3 decline products) is defensible;
   inventing it is not.
2. **Host.** Hosted Haiku vs a local model via Ollama, decided on the facts from
   `RR-03 LLM access — what is actually provisioned`. The 2026-09-04 review's default to
   argue with is **hybrid**: the local model does the enrichment that runs in the demo (no
   network, no key, no rate limit on stage); hosted Haiku is the labelling oracle for the
   gold set, or the comparison row in the evaluation table (`RR-17`). Reject that only with
   a number from `RR-03`. The brief permits hosted APIs and
   forbids sending private or sensitive data. The exposure the audit names: beauty reviews
   carry health details (acne, eczema, alopecia) alongside `user_id`s. The rule is review
   text only, never ids, results cached in Iceberg so the demo makes no live call. Whichever
   host wins, the runbook must state exactly what is sent — and the answer to "is public the
   same as not sensitive?" is a concession, not a denial.
3. **Subset size against budget.** The artifact proposes ~5,000 stratified reviews. Decide
   the number and the stratification (across ratings and across products), using the cost
   estimate from `RR-03`.

**And the validation design, which is the part that earns the marks.** The gate is a
precision/recall table, so this ticket must fix:

- how many rows are hand-labelled (the artifact says 150–200) and who labels them;
- the weak-label check: 1–2★ negative, 4–5★ positive, 3★ excluded, agreement reported;
- that results are **per class**, never accuracy — 60.0% of reviews are 5★, so a model that
  always answers "positive" scores 60% and is worthless (README already notes this);
- what gets reported about the disagreements, since the audit's red-team follow-up is "what
  do the disagreements look like?" and some of them will be the model being right about a
  misleading star rating.

If `RR-07` put MLlib in scope, say here how the two interact — an MLlib baseline against the
LLM labels would be a genuine evaluation rather than a bolted-on model.

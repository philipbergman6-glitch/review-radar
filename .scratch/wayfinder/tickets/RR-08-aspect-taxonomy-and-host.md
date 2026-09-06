---
id: RR-08
title: Aspect taxonomy and the model that produces it
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-03, RR-07]  # both closed
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

## Input from RR-09 (closed 2026-09-04)

- **Taxonomy is fixed and held out.** Derive from pre-2020 decline episodes **and their
  matched controls** (never candidates alone); definitions, inclusion/exclusion examples,
  prompts, `other/unknown`, abstention, multi-label; tune and validate on pre-2020 hand
  labels; freeze the complete labelling spec; apply unchanged to post-2020 candidates and
  controls. Post-2020 human labels never revise the spec. New post-2020 themes → "uncovered
  holdout content", exploratory appendix.
- **Budget has four allocations**: pre-2020 discovery; pre-2020 prompt dev/validation;
  post-2020 candidate + control inference; untouched post-2020 human audit. With 8–12
  themes use rare-theme enrichment for validation plus a smaller prevalence-representative
  audit sample, or shrink the taxonomy.
- **Concentration target**: 3 candidates × (1 + 5 controls) × 2 windows = 36
  product-windows for the holdout inference. Text eligibility counts non-empty reviews,
  not only 20+-word ones.
- **Representative example rule** (per window, per theme): carries the theme → high label
  confidence → rating near that theme's median → nearest embedding to theme/window
  centroid if available → helpful votes and stable id as tie-breakers. Label confidence is
  therefore a required output of the labeller.
- Bootstrap over reviews captures sampling variation only, not systematic label error; the
  aspect evaluation table (RR-17) is what covers the latter.

## Input from RR-07 (closed 2026-09-06)

- **MLlib is in scope as a theme classifier baseline** (ADR-0002), trained on LLM labels.
  Say here how the labelling spec serves it: the LLM output must label `other` explicitly
  and emit `label_confidence`; the classifier learns named themes only.
- **Split `other/unknown`**: *other* = out-of-taxonomy complaint (LLM-only output, audit
  category); *abstention* = the labeller declines. Distinct fields, distinct counts.
- **Fifth budget line**: an **LLM-labelled training pool** of ~3,000 pre-2020 reviews
  (2,000 representative core + ≤1,000 targeted from frozen discovery-stage theme terms),
  disjoint from the discovery sample, the development set and the audit set. Fix the exact
  count against the RR-03 cost here.
- The 1–2★/4–5★ weak-label agreement stays as a *sentiment* sanity check only; it is not
  the per-theme comparator (that is the star-only theme baseline, RR-17).
- Canonical terms: *development set* (pre-2020 hand labels) and *audit set* (post-2020,
  opened once). Avoid "gold set".

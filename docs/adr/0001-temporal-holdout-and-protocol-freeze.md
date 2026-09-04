---
status: accepted
date: 2026-09-04
---

# Decline alerts are backtested on a 2020-01-01 temporal holdout under a frozen rule

The presentation's opening question is *which products experienced a sustained decline in
customer ratings, and which complaint themes increased during that decline?* Answering it by
scanning every month of 2000–2023 for the largest drops, with thresholds tuned on the same
data, would produce candidates that are artefacts of the search. We therefore develop the
alert rule (window sizes, δ, persistence, episode closure, placebo ceiling, injected-decline
power targets) and the complaint-theme taxonomy on evaluation points **before 2020-01-01
only**, commit the complete configuration in one protocol-freeze commit, and then apply it
unchanged to 2020–2023. Results are reported as monitoring alerts from a historical backtest,
not as significant findings; the freeze commit is evidence that analyst discretion was
limited, not proof of pre-registration.

## Considered options

- **Two-stage freeze without a holdout** — cheaper, but aggregate decline distributions can
  still be used to steer thresholds toward a wanted candidate count.
- **Product-hash holdout** — tests generalisation across products but not whether a rule
  fixed in the past behaves sensibly on later events, which is the category-manager use case.
- **Decide the question after gold exists** (the build plan's original wording) — defers
  the aspect taxonomy, slide order and design doc to the last week of a three-week build.

## Consequences

- Primary candidates come only from 2020–2023 evaluation points; pre-2020 episodes are
  exploratory. 2023 is censored at September and must not be read as a full year.
- Post-2020 identities, outcome distributions and human labels stay untouched until the
  freeze; a post-2020 hand-labelled audit sample is a final test set.
- The chronological replay (streaming phase) becomes the demonstration of the backtest:
  the alert fires at the point persistence was satisfied, never at the retrospective start.
- Burst detection, near-duplicate discovery and any "trust" framing are removed from scope
  so the single insight thread gets the full budget.
- Once holdout candidates have been seen, the rule cannot honestly be re-tuned; changing it
  means declaring a new, exploratory analysis.

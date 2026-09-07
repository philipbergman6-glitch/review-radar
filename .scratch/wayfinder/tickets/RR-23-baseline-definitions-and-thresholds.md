---
id: RR-23
title: What the star-only baseline predicts, and when the two baselines' thresholds freeze
type: grilling
status: closed
assignee: agent (decided 2026-09-07, under ADR-0002 and ADR-0003)
blocked-by: [RR-07, RR-22]  # both closed
blocks: []
---

## Question

ADR-0002 puts the MLlib classifier "beside a star-only per-theme baseline" and requires
"taxonomy, prompt, classifier pipeline and per-theme thresholds freeze together". Two things
that leaves open, and both change a published number:

1. **What does a star-only baseline actually predict?** A star rating is one integer and a
   theme label is a set of ten booleans; "predict themes from stars" is not a rule until
   someone writes one. Written badly it is either trivially empty (never predict anything) or
   trivially maximal (predict every theme on every 1-star review), and either way it stops
   being the honest floor the comparison needs.
2. **Where do the thresholds come from?** ADR-0002 says they freeze with the prompt. It does
   not say what data fits them. Fitting on the audit set would make the baselines look better
   than the labeller for a reason that has nothing to do with the data.

## Answer (2026-09-07)

### The star-only baseline is a per-theme star threshold, fitted on development

For each theme `t`, the rule is `predict t iff rating <= k_t`, with `k_t` chosen from
{1, 2, 3, 4, 5} to maximise that theme's F1 **on the 200-review development set** against the
`agent_reference` labels. A theme whose best F1 is 0 at every threshold gets `k_t = 0`, which
predicts nothing, and that is reported rather than smoothed.

This is the strongest rule a star alone can express for this label shape, which is what makes
it a fair floor: it answers "how much of each complaint theme is just the review being
low-rated?" A single global threshold would understate it, and anything richer than the star
is no longer a star-only baseline.

It has no training beyond the five candidate thresholds, so it is fitted, frozen and reported
in the same commit as the classifier's thresholds.

### Both baselines' thresholds are fitted on development, frozen, then applied unchanged

The MLlib classifier's per-theme decision threshold is swept on the development set over the
same grid discipline (probability cut maximising development F1), frozen into
`conf/theme-classifier.json` together with the pipeline hyperparameters, and applied unchanged
to the audit set. The audit set is opened once, for all three systems, exactly as ADR-0002
says.

The classifier trains on the 3,000-review `training_pool` labelled by the **frozen** prompt,
`label_source="local_llm"` at the frozen `inference_config_hash`. Pool rows that ended
`parse_failed` or `api_failed` are dropped from *training* — a row with no label is not a row
with no themes, and training on it would teach the classifier that long, complaint-dense
reviews are empty. The count dropped is reported. This is a training-set decision only: on the
**evaluation** sets a failure still scores as an empty prediction for every system
(`src/ai/theme_scoring.py`), because that is what a consumer of these labels experiences.

Classifier predictions land in `gold.review_theme_labels` with `label_source="classifier"` and
an `inference_config_hash` over the classifier spec, so they can never be mistaken for the
primary labels and can never be aggregated into them.

## Consequences

- The star-only baseline is allowed to win on a theme. `arrived_damaged` and
  `does_not_work` are heavily concentrated in 1-star reviews, so a star threshold may beat a
  weak labeller there; that is information about the theme, not a defect in the comparison,
  and it is reported per theme rather than only in the macro average.
- Fitting `k_t` and the classifier cut on development means both baselines get exactly the
  same courtesy the labeller got (five prompt versions on development), so the audit
  comparison is between three systems that each saw the development set and none of which saw
  the audit set.
- ADR-0002's pass rule for the classifier — macro-F1 above the star-only baseline and at
  least 70% of the labeller's — is evaluated on the audit set only.

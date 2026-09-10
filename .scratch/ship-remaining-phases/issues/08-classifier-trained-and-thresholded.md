# 08 — MLlib classifier trained, thresholds fitted on development

**What to build:** the supervised theme classifier trained on the frozen-prompt pool and
scored the same way the star-only baseline is — per-theme probability cuts fitted on the
development set and applied **unchanged** to audit. Existing targets `classifier-train`,
`classifier-thresholds` and `classifier-score` carry the mechanics.

Fitting thresholds on development and freezing them before audit is what makes the
three-way comparison fair: the LLM, the classifier and the star-only baseline each get one
fitting pass on development and one scoring pass on audit, and none gets two.

**Blocked by:** 07 — the training pool must be labelled by the frozen prompt.

**Status:** ready-for-agent

- [ ] The classifier is trained on the frozen-prompt pool only
- [ ] Per-theme probability cuts are fitted on development and frozen
- [ ] Frozen cuts are applied to audit unchanged, with no refitting
- [ ] Classifier output carries `label_source=classifier` and never enters primary labels
- [ ] Development scores are committed before audit is opened
- [ ] The run registered a run contract

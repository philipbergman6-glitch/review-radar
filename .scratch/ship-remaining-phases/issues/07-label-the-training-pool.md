# 07 — Label the training pool with the frozen prompt

**What to build:** the training pool labelled by the frozen prompt, so the MLlib classifier
has a stable teacher to learn from. ADR-0002's pass rule compares the classifier against
this teacher, so the teacher must be the frozen one and nothing else.

Parse failures are **asymmetric by design**: on the evaluation sets a parse failure scores
as an empty prediction, but in classifier *training* the row is dropped — a row with no
label must never be confused with a row with no themes. The dropped count is reported.

`label_source` is a closed enum — `llm` · `agent_reference` · `classifier`. Pool rows are
`llm`. Classifier rows can never be aggregated into primary theme labels.

**Blocked by:** 06 — the prompt must be frozen first.

**Status:** ready-for-agent

- [ ] The training pool is labelled by the frozen prompt with `label_source=llm`
- [ ] Rows that failed to parse are dropped from training, and the dropped count is reported
- [ ] The same failure on an evaluation set scores as an empty prediction, not a drop
- [ ] The asymmetry has a unit test
- [ ] The run registered a run contract and rows carry the run id

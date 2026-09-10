# 09a — The blind audit reference labels

**What to build:** the `agent_reference` ground truth for the 200 audit reviews, written
blind, and imported so `THEMES_REFERENCE` prints `rows=200 distinct=200`.

**Why this exists as its own ticket:** ticket 09 named two blockers, 06 and 08, and this is
neither. It is the ground truth all three systems are scored *against*, so no macro-F1 exists
without it — and today `THEMES_REFERENCE` prints `rows=0 expected=200`. The blind export
`eval/themes/blind-audit.jsonl` was written on 2026-09-07 and has sat unlabelled since;
`eval/themes/reference-development.jsonl` exists and `reference-audit.jsonl` does not. Found
2026-09-11 while landing 09's mechanism, by running the gate.

It is the long pole of P6's close-out, not the classifier.

**This must be a fresh session, and that is protocol, not preference.** RR-21 condition 1:
"the agent labelling job sees only the review `title`, `text`, and the frozen taxonomy. It
must never see `qwen3:8b`'s output… any labelling run that had access to the model under test
is void." A session that has read the frozen prompt, the scoring code, or the labeller's
development numbers cannot write these labels — the correlated-error risk RR-21 priced is the
whole reason the agreement number is published, and a compromised ground truth makes both the
audit macro-F1 and that agreement number meaningless.

So: a session opened on the blind export and the taxonomy, and nothing else. Not this one, and
not one that has read ticket 06, 07, 08 or 09.

**Blocked by:** nothing. It can run in parallel with 08's classifier chain — different inputs,
different tool, no shared state but the labels table, which is partitioned by `label_source`.

**Blocks:** 09.

**Status:** ready-for-agent

- [ ] All 200 audit reviews labelled blind against the frozen taxonomy v1
- [ ] Rows imported with `label_source=agent_reference` and its own `model_id`, never `human`
      (RR-21 condition 2 — `human` is reserved for Philip's adjudication set)
- [ ] `make import-reference SAMPLE_NAME=audit` run, and `THEMES_REFERENCE` prints
      `rows=200 distinct=200 human_rows=0 ok=true`
- [ ] The labelling session is recorded as having had no exposure to the model under test
- [ ] The run registered a run contract (`theme_labels_reference`) and rows carry the run id

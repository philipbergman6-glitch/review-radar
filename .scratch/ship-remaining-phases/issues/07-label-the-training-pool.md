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

**Status:** in progress — code landed 2026-09-10; the labelling run is still inferring

- [ ] The training pool is labelled by the frozen prompt with `label_source=llm`
      — **run in flight**, `20e03c76`, `label-v5` / `qwen3:8b` / config `b84a2ce22193`
- [x] Rows that failed to parse are dropped from training, and the dropped count is reported
- [x] The same failure on an evaluation set scores as an empty prediction, not a drop
- [x] The asymmetry has a unit test
- [x] The run registered a run contract and rows carry the run id

## What was built

`src/ai/label_usage.py` is the asymmetry, stated once. `for_training` returns `None` for a
`parse_failed` or `api_failed` row — a drop — and `for_evaluation` returns `set()` for the same
row. A drop is `None` and never an empty set, because `set()` is a real answer ("the model read
this and found no complaint") and a caller writing `if labels:` would silently merge the two
readings back together. `tests/test_label_usage.py` asserts **both** directions on the same row,
which is the only form of the test that can fail if someone unifies them.

Both consumers now go through it rather than reimplementing it: `theme_classifier.train` filters
on `TRAINABLE_STATUSES` and reports `training_census` (rows kept, rows dropped, by named status,
drop rate) into the run contract, the `CLASSIFIER_TRAIN` line and `conf/theme-classifier.json`,
and hard-fails if its Spark filter and the census disagree. `scripts/score_themes.py` builds its
system predictions with `for_evaluation`, so the empty-prediction reading is a call to a tested
function instead of an incidental `or []`.

**The teacher can no longer be the wrong prompt.** `conf/theme-label-spec.json`'s `frozen_prompt`
block is loaded as structure (`LabelSpec.frozen`), the loader rejects a freeze record that names a
prompt the spec does not have or a version it no longer carries, and `--sample training_pool`
hard-fails through `require_frozen` unless the prompt is the frozen one — before the hours are
spent, not in review. `--prompt` now defaults to the frozen prompt everywhere, the Makefile's
`PROMPT` default moved `label_v4` → `label_v5`, and a test pins that line to the freeze so the two
cannot drift.

`scripts/pool_census.py` (`make pool-census`) is the record of what the teacher actually is:
assigned vs labelled, the drop by status, per-theme positives over the kept rows only, the parse
causes beside them, and the run ids on the rows. It refuses a partially labelled frame outright —
an assigned review with no row is an unfinished run, not a drop with a reason, and calling the
remainder a teacher would overstate it silently.

## Note on the ticket's enum

The ticket calls `label_source` a closed enum of `llm · agent_reference · classifier`. The frozen
enum in `src/ai/labels.py` is wider — `local_llm`, `hosted_llm`, `agent_reference`, `human`,
`classifier` — and predates this ticket (ADR-0003, RR-21). Pool rows are `local_llm`. Nothing was
changed to match the ticket's shorthand; narrowing a frozen enum after rows exist under it would
be a migration, not a tidy-up.

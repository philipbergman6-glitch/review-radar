# 08 — MLlib classifier trained, thresholds fitted on development

**What to build:** the supervised theme classifier trained on the frozen-prompt pool and
scored the same way the star-only baseline is — per-theme probability cuts fitted on the
development set and applied **unchanged** to audit. Existing targets `classifier-train`,
`classifier-thresholds` and `classifier-score` carry the mechanics.

Fitting thresholds on development and freezing them before audit is what makes the
three-way comparison fair: the LLM, the classifier and the star-only baseline each get one
fitting pass on development and one scoring pass on audit, and none gets two.

**Blocked by:** 07 — the training pool must be labelled by the frozen prompt.

**Status:** done — the four runs landed 2026-09-11 05:10–05:11, unattended, in the prescribed order

Every box was a claim about a **run**, and the runs have now happened — see **The runs**
below for the numbers each box is ticked against.

- [x] The classifier is trained on the frozen-prompt pool only
      — `CLASSIFIER_TRAIN a67d8221` spec `c0adf0923dc5`, pool 3000, trained on 2783, vocab 2514.
      Mechanism: `--source-config-hash` now defaults to the frozen prompt's derived hash and
      refuses any other value (`require_frozen_labeller`); training hard-fails below 100
      usable rows
- [x] Per-theme score cuts are fitted on development and frozen — ten cuts in `conf/theme-classifier.json`.
      Mechanism: `--fit-thresholds` refuses to overwrite a fitted spec without `--force`
- [x] Frozen cuts are applied to audit unchanged, with no refitting — untested until ticket 09
      — mechanism: `require_frozen_cuts` refuses cuts that are absent, fitted on any frame
      but development, or fitted against another taxonomy
- [x] Classifier output carries `label_source=classifier` and never enters primary labels
      — the first half is real (`classifier_identity`); the second has no enforcement point
      yet, see **Left open**
- [x] Development scores are committed before audit is opened — this commit
      — mechanism: `require_development_first`, on both commands that can produce an audit
      number, keyed to *this* model's spec hash
- [x] The run registered a run contract
      — `theme_classifier_train` / `theme_classifier_score` already call `runs.start`, and
      `conf/lineage_chain.toml` now claims both jobs so a missing run fails the chain

## What was built

`src/ai/classifier_spec.py` is how a *system* is picked out of the shared labels table. Three
systems answer the same question about the same reviews and their rows sit in one table; what
separates them is an identity — `(label_source, model_id, prompt_version,
inference_config_hash)`. The labeller's is a prompt and a model under one inference config; the
classifier's is the spec hash of the fitted model. `scripts/score_themes.py` accepted
`--source classifier` before this ticket but still built the config hash from the *labeller's*
prompt, so it would have filtered to zero classifier rows and reported a table over nothing.
`tests/test_classifier_scoring.py` asserts the two identities cannot collide, on the same review,
down to the idempotency key.

The artefact filename comes from the same module. `scripts/gate_themes.py` finds the audit score
by rebuilding its stem, so a drift between writer and reader prints `artefact=missing` and FAIL
over a score that exists; `llm_artefact_stem` is now the one place that shape is written, and a
test pins it to what the gate rebuilds.

The classifier's stem carries its spec hash — `classifier-v1-<hash12>`. Without it a retrained
classifier would write over the previous one's score file, and the audit guard below would then
be satisfied by a number measured on a model that no longer exists.

**Three freezes, as refusals rather than conventions.** `require_frozen_labeller` derives the
frozen prompt's config hash instead of taking it by hand, and refuses any other value passed to
`--source-config-hash`: the pool's rows differ from a superseded prompt's only by that column, so
a stale paste would train on the wrong labels silently. `require_frozen_cuts` rejects scoring with
no cuts, cuts fitted on anything but development, and cuts fitted against another taxonomy.
`require_development_first` refuses an audit pass until *this* model's development score artefact
is on disk — and it guards both commands that can produce an audit number, the scoring job and
`make classifier-table`, because a rule restated at two call sites is a rule that will one day be
stated at one. All three are pure functions with tests; the Spark job and the scorer call them
rather than restating them.

`conf/lineage_chain.toml` now claims `theme_classifier_train` and `theme_classifier_score` under
`themes_quality`, which is the capability whose published number is the three-way comparison. The
file's own comment already said these two runs "appear in `jobs` above and are pinned as runs" —
they did not; a `cut` capability's jobs are skipped by the lineage gate, so they had to go on a
declared one. `make classifier-table` is the classifier's per-theme table.

## What is left, and the order it runs in

When `20e03c76` reports success and `make pool-census` shows a complete frame:

    make classifier-train          # derives the pool's hash (b84a2ce22193…) from the freeze
    make classifier-thresholds
    make classifier-score SAMPLE_NAME=development
    make classifier-table SAMPLE_NAME=development       # commit this number before ticket 09

`CONFIG_HASH=` is optional and full-length only: it is checked for equality against the derived
hash, so its only use is to state the expectation out loud.

Nothing about the audit set moves here. Ticket 09 opens it once, for all three systems.

## Left open

ADR-0002 says "the primary job requires `label_source = "llm"` and a test enforces it". There is
no such job yet — nothing in the repo aggregates theme labels into a theme-shift table, so there
is nothing to put the filter on, and a guard with no caller would be an abstraction waiting for a
need. The enforcement point belongs to whichever ticket first aggregates labels (10, the P6
close-out artefact, is the earliest candidate). What holds today is weaker and worth stating
plainly: classifier rows are partitioned by `label_source`, carry a different
`inference_config_hash` and a different idempotency key, and `tests/test_classifier_scoring.py`
pins that they cannot collide with the labeller's rows — but nothing yet *refuses* to read them.

## The runs (2026-09-11 05:10–05:11)

`scripts/after_pool_train_classifier.sh` ran the four targets unattended when 07's pool run
reported success. `logs/classifier-chain.log` is the record.

    CLASSIFIER_TRAIN      run a67d8221  spec c0adf0923dc5  pool=3000 trained_on=2783
                          dropped_failed=217 drop_rate=0.0723 vocab=2514
    CLASSIFIER_THRESHOLDS ten per-theme cuts -> conf/theme-classifier.json
    CLASSIFIER_SCORE      run 4ed4235c  sample=development reviews=200 no_predicted_theme=4
    THEME_SCORE           sample=development source=classifier macro_f1=0.4053
                          bootstrap95=[0.3525,0.4539] supported=10 min_supported_recall=0.4118
                          failure_rate=0.0 other_agreement=0.92

**Development macro-F1 is 0.4053**, under the 0.70 bar and under the labeller's development
0.4633. Nothing is refitted on that news — the cuts are frozen as fitted and go to audit
unchanged, which is the whole point of fitting them before the holdout was opened.

Three properties of the number worth carrying into ticket 10, because each one shapes how the
three-way comparison should be read:

- **The classifier's failure rate is 0.0** where the labeller's is not: it answers for every
  review by construction. Its weakness is precision, not coverage — `not_as_described`
  predicts 155 of 200 reviews at precision 0.116 while recalling 0.947, which is a cut sitting
  near the floor on a poorly separated theme rather than a model that knows something.
- **Six of ten cuts landed at or near a grid edge** (0.1 ×2, 0.95 ×2, 0.15, 0.2 ×2). A cut at
  the edge of its sweep means F1 was still improving where the grid stopped; it is a statement
  about weak probability separation, not about the grid.
- **It is a student of a teacher that scores 0.4633.** The pool labels are `label-v5`'s output,
  so the classifier's ceiling is the labeller's accuracy, and 7.2% of the pool was dropped as
  unparseable before training ever began.

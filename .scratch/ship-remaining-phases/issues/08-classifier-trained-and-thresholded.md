# 08 — MLlib classifier trained, thresholds fitted on development

**What to build:** the supervised theme classifier trained on the frozen-prompt pool and
scored the same way the star-only baseline is — per-theme probability cuts fitted on the
development set and applied **unchanged** to audit. Existing targets `classifier-train`,
`classifier-thresholds` and `classifier-score` carry the mechanics.

Fitting thresholds on development and freezing them before audit is what makes the
three-way comparison fair: the LLM, the classifier and the star-only baseline each get one
fitting pass on development and one scoring pass on audit, and none gets two.

**Blocked by:** 07 — the training pool must be labelled by the frozen prompt.

**Status:** in progress — code landed 2026-09-10; the three runs wait on 07's labelling run

Every box is a claim about a **run**, and no run has happened: what landed is the mechanism
each box needs, and the refusal that will make it true. Nothing is ticked until the numbers
exist.

- [ ] The classifier is trained on the frozen-prompt pool only
      — **blocked**: 07's pool run `20e03c76` was at 225/3000 reviews at 17:50, ETA ~5.7h.
      Mechanism: `--source-config-hash` now defaults to the frozen prompt's derived hash and
      refuses any other value (`require_frozen_labeller`); training hard-fails below 100
      usable rows
- [ ] Per-theme score cuts are fitted on development and frozen — blocked on the train.
      Mechanism: `--fit-thresholds` refuses to overwrite a fitted spec without `--force`
- [ ] Frozen cuts are applied to audit unchanged, with no refitting
      — mechanism: `require_frozen_cuts` refuses cuts that are absent, fitted on any frame
      but development, or fitted against another taxonomy
- [ ] Classifier output carries `label_source=classifier` and never enters primary labels
      — the first half is real (`classifier_identity`); the second has no enforcement point
      yet, see **Left open**
- [ ] Development scores are committed before audit is opened
      — mechanism: `require_development_first`, on both commands that can produce an audit
      number, keyed to *this* model's spec hash
- [ ] The run registered a run contract
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

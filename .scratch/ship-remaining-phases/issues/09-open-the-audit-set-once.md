# 09 — Open the audit set once, for all three systems

**What to build:** the single scoring pass over the held-out audit set, run for the LLM
labeller, the MLlib classifier and the star-only baseline **together**, so no system gets
a second look at held-out data.

This is the measurement the whole P6 protocol exists to protect. Everything that could be
tuned has been frozen: the prompt (ticket 06), the classifier thresholds (ticket 08), the
star-only thresholds (already frozen under RR-23). After this pass, none of them moves.

A macro-F1 below the 0.70 bar is a **reported FAIL, not a blocker**. The bar was set before
any number existed and does not move. P6's status becomes `built, evaluated, below target`
and P7 is not held up. Reopening P6 on a quality result is exactly what tuning against a
holdout looks like.

**Blocked by:** 06 (frozen prompt) and 08 (frozen classifier thresholds).

**Status:** in progress — the pass and its refusals landed 2026-09-11; no audit row has been
read, and three preconditions are outstanding (below)

Every box is a claim about a **run**, and no run has happened. What landed is the one command
that may open the set, and the refusals that make "once" true rather than intended.

- [ ] All three systems scored on the audit set in one pass
      — **blocked**, see the three preconditions. Mechanism: `make audit-once`
      (`scripts/open_audit.py`) scores all three or none, and the three single-system scorers
      now refuse `--sample audit` outright (`require_measurement_goes_through_the_pass`)
- [ ] Per-theme and macro-F1 with bootstrap intervals committed for each system
      — mechanism: one `system_report` builds the artefact shape for all three, so the gate
      cannot be handed a differently-built dict by a system that scored badly
- [ ] No threshold, prompt or cut changed after the audit numbers were seen
      — mechanism: `THEMES_SEAL` re-derives the freeze fingerprint from today's config files
      and prints every difference by name
- [ ] The audit set is not scored again for any reason
      — mechanism: `require_audit_unopened` refuses a second pass and quotes the first one's
      numbers back
- [ ] The run registered a run contract and results carry the run id
      — the artefacts carry `run_ids` from the rows they scored. The star floor carries none,
      stated as such: it ran no inference and an invented run id would claim it did

## What was built

`src/ai/audit_seal.py` is the once-ness, as refusals rather than as intent.

**The boundary that makes "once" workable: inference is not opening the set.** Labelling the
audit reviews, or running the classifier over them, produces predictions — that can crash,
resume, and be re-run, and it is idempotent by key. What may happen only once is *measurement*:
turning predictions into a number against the reference labels. Without that line, "open the
audit set once" either forbids re-running a crashed inference job or forbids nothing at all.
So `scripts/score_themes.py` and `scripts/baseline_star_only.py --score` now refuse
`--sample audit` and name `make audit-once`; scoring one system at a time is how the set gets
opened three times, one disappointing number at a time.

**The fingerprint covers the pass rule, not only the systems.** `collect_freezes` gathers the
frozen prompt and its derived config hash, the classifier's spec hash and per-theme cuts, the
star thresholds, the taxonomy — and ADR-0003's 0.70/0.50 bars. Lowering the bar after a
disappointing audit is the same act as refitting a cut: it makes the published claim untrue.
A fingerprint over the systems but not the rule would let it through silently, so
`MACRO_F1_BAR` and `MIN_RECALL_BAR` moved out of `scripts/gate_themes.py` into `audit_seal`
and the gate imports them — a rule the seal cannot see is a rule the seal cannot protect.
`tests/test_audit_seal.py` moves each of ten frozen things alone and asserts the fingerprint
changes and `freezes_moved` names it; the two bars are two of the ten.

The seal stores the whole freeze material beside the hash, because a hash can only ever
*detect*. `THEMES_SEAL` prints `classifier.thresholds.delivery 0.41 -> 0.37`, not "the
fingerprint differs". It also records each score artefact's sha256: the fingerprint proves the
inputs did not move, the sha256s prove the outputs did not.

The verdict is written **into** the seal at the moment of opening, however it reads, and the
seal is written on a FAIL exactly as on a PASS. A missing seal after a bad number is an
invitation to re-run; and deciding what a disappointing number means after seeing it is the
failure the whole file guards.

**Two things were made derivable without Spark**, because the seal and the gate must name the
same rows the pass scored, with no cluster in sight. `PROMPT_MAX_THEMES` moved from the Spark
labelling job into `src/ai/labels.py` behind `prompt_schema(prompt_version, theme_ids)`: eight
call sites were writing `decoding_schema(ids, max_themes=PROMPT_MAX_THEMES.get(v))` by hand,
and a config hash assembled with the cap forgotten silently selects *no rows at all*, which
reads as "this system was never run" rather than as a mistake. `frozen_llm_identity` derives
the labeller's identity from the freeze rather than taking a prompt name from an operator.

`src/ai/theme_scoring.system_report` is the artefact shape, written once. Both existing writers
were building it by hand; the development artefacts they produce are **byte-identical** before
and after the change (checked against the committed files, both systems).

## The three preconditions, and the order they run in

Ticket 08's chain (`scripts/after_pool_train_classifier.sh`) is in flight — the pool re-labelling
resumed at 00:57 and is ~2,500 reviews from done. Beyond it, opening the audit set needs:

1. **The classifier**, from 08's chain: `classifier-train` → `classifier-thresholds` →
   `classifier-score SAMPLE_NAME=development`, then `classifier-score SAMPLE_NAME=audit`
   (inference over the audit reviews, which the seal does not restrict).
2. **The audit labelling run**: `make label-themes SAMPLE_NAME=audit` under the frozen prompt.
   The audit frame has never been labelled — the ledger's only `theme_labels_llm` runs are
   discovery, development and the training pool. ~200 reviews at ~5s each.
3. **The blind audit reference labels — not produced, and not producible by this session.**
   `eval/themes/blind-audit.jsonl` was exported on 2026-09-07, but no agent has labelled it:
   `reference-development.jsonl` exists and `reference-audit.jsonl` does not, and
   `THEMES_REFERENCE` today prints `rows=0 expected=200`. This is the ground truth all three
   systems are scored against, so nothing about ticket 09 can proceed without it.

   **It needs a fresh session.** RR-21 condition 1 is binding: "the agent labelling job sees
   only the review title, text, and the frozen taxonomy… any labelling run that had access to
   the model under test is void." This session has read the frozen prompt, the scoring code and
   `qwen3:8b`'s development numbers, so labels written here would be void under the protocol
   that the audit's validity rests on. Worth ticketing separately — it is a precondition of 09
   that 09 never named, and it is the long pole, not the classifier.

Then, and only then: `make audit-once`. It refuses until every freeze is in place, until each
system's development score is on disk, and until both inferring systems have a terminal row for
all 200 audit reviews — a system scored on the reviews it happened to answer for measures
something else.

## Verified today

    make audit-once                          -> AUDIT_REFUSED theme-classifier.json is missing
    score_themes.py --sample audit           -> refused, names `make audit-once`
    baseline_star_only.py --score --sample audit -> refused, names `make audit-once`
    gate-themes                              -> THEMES_SEAL seal=missing ok=false (1/7)
    425 tests pass; the two development artefacts are byte-identical after the refactor

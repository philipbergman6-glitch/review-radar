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

**Status:** done — the set was opened 2026-09-11 06:26 UTC, seal `f39ce4f57abd`, verdict FAIL.
It cannot be opened again.

- [x] All three systems scored on the audit set in one pass — one invocation, three artefacts.
      Mechanism: `make audit-once`
      (`scripts/open_audit.py`) scores all three or none, and the three single-system scorers
      now refuse `--sample audit` outright (`require_measurement_goes_through_the_pass`)
- [x] Per-theme and macro-F1 with bootstrap intervals committed for each system
      — mechanism: one `system_report` builds the artefact shape for all three, so the gate
      cannot be handed a differently-built dict by a system that scored badly
- [x] No threshold, prompt or cut changed after the audit numbers were seen
      — mechanism: `THEMES_SEAL` re-derives the freeze fingerprint from today's config files
      and prints every difference by name
- [x] The audit set is not scored again for any reason — `require_audit_unopened` is now live
      — mechanism: `require_audit_unopened` refuses a second pass and quotes the first one's
      numbers back
- [x] The run registered a run contract and results carry the run id
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

---

## The pass (2026-09-11 06:26 UTC)

Preconditions cleared in order: 07's pool run finished 05:10 and 08's chain trained and froze
the cuts by 05:11; 09a's 200 blind reference labels landed at 01:40; the audit labelling run
`e39584c3` finished 09:25 IDT (200 inferred, 173 ok, **27 parse failures**, 0 API failures);
`classifier-score SAMPLE_NAME=audit` (`147d43ae`) put the classifier's predictions in place.
Then one invocation of `make audit-once`, against a clean tree at `aafaad58`.

    AUDIT_FREEZES fingerprint=f39ce4f57abd prompt=label-v5 classifier=c0adf0923dc5
                  star=v1 taxonomy=0cc29c374779 bar=0.7/0.5

| system | macro-F1 | bootstrap 95% | min supported recall | failure rate | supported |
|---|---|---|---|---|---|
| LLM `label-v5` / `qwen3:8b` | **0.4583** | [0.3647, 0.5223] | 0.4667 | 0.135 | 9/10 |
| MLlib `classifier-v1` | 0.3899 | [0.3074, 0.4639] | 0.3333 | 0.0 | 9/10 |
| star-only floor | 0.2968 | [0.2612, 0.3493] | 0.7222 | 0.0 | 9/10 |

`AUDIT_ONCE published=llm macro_f1=0.4583 bar=0.7 min_supported_recall=0.4667 recall_bar=0.5
verdict=FAIL`. Both bars are missed, the headline by a wide margin. P6 is `built, evaluated,
below target`; nothing is refitted and P7 is not held up. The target exits non-zero on a FAIL,
which is correct — the artefacts and the seal are written first, precisely so a bad number
cannot be met with a silent re-run.

### The finding the development set could not give

On development the labeller's interval overlapped the star-only baseline's, and ticket 10 was
written to report that overlap as a result about weak local models on a J-shaped corpus.
**On the held-out set the intervals are disjoint** — [0.3647, 0.5223] against [0.2612, 0.3493].
The labeller is distinguishably better than a star threshold, and the development overlap was
the smaller sample, not the truth. Ticket 10 must report the audit comparison as the finding
and the development overlap as the weaker earlier evidence, not the other way round.

The classifier's interval [0.3074, 0.4639] still overlaps the floor's. It is **not**
distinguishable from a star threshold, and that is the honest reading of the MLlib arm.

### Three properties of the published number

- **Failure rate 0.135.** 27 of 200 audit reviews came back unparseable after a retry, against
  7.2% on the training pool. Each scores as an empty prediction, so they are 27 recall losses
  that precede any judgement about theme quality. The classifier's is 0.0 by construction.
- **The recall bar is missed too**, 0.4667 against 0.50, on the weakest supported theme.
- **The star floor has the highest recall of the three** (0.7222) and the lowest macro-F1. It
  is a high-recall, low-precision instrument, which is what predicting themes from a star
  rating should look like.

### A reporting defect, recorded and not fixed here

The artefacts carry a per-stratum split — 120 enriched, 80 prevalence-representative. In the
representative stratum **no theme reaches `min_support` 10**, so `macro_f1` is `None` for all
three systems, correctly. But `bootstrap_macro_f1` still returns an interval beside it, because
it averages over the resamples that happened to contain a supported theme: star-only reads
`[0.483, 0.800]` there, *above* its own overall score, which would be an absurd thing to publish.

It is not fixed by regenerating the artefacts. Their sha256s are in the seal, and rewriting a
scored artefact after the set is opened is the exact act the seal exists to refuse. Ticket 10
publishes the representative stratum as `NOT_RUN` with the reason "no theme reaches min_support
10 in 80 rows" and prints no interval. If the bootstrap is ever changed to return `(None, None)`
when the point estimate is `None`, that is a change to a *future* pass, not to this one.

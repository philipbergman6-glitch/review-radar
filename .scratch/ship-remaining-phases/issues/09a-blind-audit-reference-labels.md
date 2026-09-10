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

**Status:** done (2026-09-11, run `e1cee8de`)

- [x] All 200 audit reviews labelled blind against the frozen taxonomy v1
- [x] Rows imported with `label_source=agent_reference` and its own `model_id`, never `human`
      (RR-21 condition 2 — `human` is reserved for Philip's adjudication set)
- [x] `make import-reference SAMPLE_NAME=audit` run, and `THEMES_REFERENCE` prints
      `rows=200 distinct=200 human_rows=0 ok=true`
- [x] The labelling session is recorded as having had no exposure to the model under test
- [x] The run registered a run contract (`theme_labels_reference`) and rows carry the run id

---

## Resolution (2026-09-11)

Labelled by a fresh session opened on `eval/themes/blind-audit.jsonl` and
`conf/theme-taxonomy.json` alone — no map file, no prompt, no score/census/selection artefact,
no P6 ticket or commit. It reported the isolation held; the only disclosure was an `ls` of
`eval/themes/`, which printed filenames and no content. Annotator identity `claude-opus-5`,
protocol `reference-blind-v1`, same as the development set.

Policy applied is the development set's, verbatim (commit `e77fe29`): a theme is labelled when
the review complains about what the definition covers, an explicitly accepted shortcoming is
not a complaint, praise is never a label, and there is no cap on themes per review.

**One correction mid-run.** The first pass abstained on three Spanish reviews, on a "wrong
language" rule that came from the brief and not from the output contract, where `abstain` means
unreadable, empty of content, or not about a product. The scorer reads an abstained reference
row as ground truth of *no themes*, so a system that read the Spanish correctly would have been
charged a false positive. The three were relabelled normally: `aud-0036` carries
`does_not_work` on a Spanish evidence quote, `aud-0058` and `aud-0063` are praise with no
theme. Abstentions in the audit reference are now **0**.

**Import:** `REFERENCE_LABELS run_id=e1cee8de sample=audit scope=full selected=200 accepted=200
rejected=0 abstained=0 no_theme=97 other=12`. Every evidence quote passed the same verbatim
check the model's quotes pass. `THEMES_REFERENCE rows=200 distinct=200 expected=200
human_rows=0 ok=true`.

**Shape of the ground truth**, and it differs from development on purpose — the audit frame is
120 enriched plus 80 prevalence-representative post-2020 rows, where development is 200 fully
enriched:

| | development | audit |
|---|---|---|
| no theme at all | 65 | 97 |
| `other.present` | 16 | 12 |
| theme labels | — | 165 |

Per theme: `does_not_work` 28, `poor_build_quality` 21, `not_as_described` 18, `hard_to_use` 18,
`overpriced` 17, `irritation_or_harm` 15, `unpleasant_scent` 15, `unpleasant_texture` 14,
`arrived_damaged` 11, `wrong_size_or_fit` 8.

**A fact ticket 10 must carry:** `wrong_size_or_fit` has 8 reference positives, below ADR-0003's
`min_support` of 10 for the audit set. It is therefore **unsupported** and excluded from the
macro-F1 average, which will be taken over nine themes, not ten. That is the support rule doing
its job — a theme with 8 positives cannot swing the headline — but it is a stated property of
the audit number and belongs beside it in the evaluation table, not discovered later.

Twelve borderline calls were reported and are worth reading before any adjudication
disagreement is treated as an error: the `overpriced` rule (an explicit money/price/value word
is required, so "Not worth it" does not qualify), worked-then-failed routed to
`poor_build_quality` per the taxonomy's `excludes`, pain-causing stiffness routed to
`irritation_or_harm` only, and listing mismatches labelled even inside positive reviews but not
when the reviewer waves them off.

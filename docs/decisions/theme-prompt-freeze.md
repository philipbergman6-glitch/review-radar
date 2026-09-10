# Which theme-labelling prompt gets frozen

ADR-0003 line 27 allows at most five prompt versions, requires every version's development
per-theme table to be committed, and requires the last selected prompt to be frozen **before**
the audit set is opened. This file is that selection. It is written in two parts, and the parts
are committed in that order on purpose: **the rule below was committed before the scores it
selects on existed in the repo**, so the rule cannot have been fitted to the outcome.

## The rule, stated before the numbers

1. **Candidates are prompt versions only**: `label-v4` and `label-v5`. `label-v1` … `label-v3`
   were superseded during development and never scored at volume; the star-only baseline
   (RR-23) is a floor to measure against, not a candidate to freeze — it runs no prompt.
2. **The selection metric is development macro-F1 over supported themes**, on the identical 200
   `development` rows, against the same `agent_reference` ground truth, scored by
   `scripts/score_themes.py` under identical settings: `min_support=10`, `seed=20260907`,
   `bootstrap_draws=1000`. A configuration scored under different settings is re-scored, not
   compared.
3. **Parse failures stay inside the metric.** The scorer reads a `parse_failed` row as an empty
   prediction, so a prompt that cannot produce valid output is penalised for it. The
   diagnostic ceiling over answered rows is not the selection metric and never substitutes for
   it.
4. **The winner is the higher macro-F1 point estimate.** Not the higher interval bound, not the
   larger overlap-free margin — the bootstrap interval is context, per ADR-0003 line 32, and
   does not adjudicate.
5. **Ties break in this order**: (a) a gap below 0.01 absolute macro-F1 is a tie; (b) the tie
   goes to the lower parse-failure rate, because that is the cheaper system to operate;
   (c) if still tied, to the earlier version, because the later one bought nothing.
6. **The star-only floor is reported, not gated.** Whether the winner's bootstrap interval
   overlaps star-only's is written into the freeze record either way. An overlap does not veto
   the freeze — something has to be frozen before the audit opens — but it is a stated
   limitation that the P6 verdict and the theme-shift table inherit.
7. **The audit set is not consulted.** No `audit` row of any labelling configuration may be read,
   scored or looked at while this selection is made. The freeze record carries the count of
   `local_llm` audit labels present at freeze time as evidence; it must be zero.
8. **No sixth prompt.** The ADR-0003 budget is five versions and it is spent (ticket 05). If both
   candidates disappoint, the better of the two is frozen and the disappointment is reported.

## Success criteria for this selection

- Three score artefacts in `eval/themes/`, all over the same 200 development rows, each with
  `reference_rows=200` and full coverage accounted for.
- One selection artefact, `eval/themes/selection-development.json`, applying the rule above and
  naming the winner, the margin, the star-only overlap verdict and the audit-untouched count.
- A `frozen_prompt` block in `conf/theme-label-spec.json` naming the winner and its freeze
  commit, so `scripts/gate_themes.py:check_prompt` can re-derive the freeze rather than trust it.

## The selection

Produced by `make select-prompt FREEZE=1` at commit `67926ed`, the commit that carries the rule
above and nothing else. The artefact is `eval/themes/selection-development.json`; the numbers
here are copied from it and are re-derivable by re-running the target.

All three systems, 200 `development` rows, the same `agent_reference` ground truth,
`min_support=10`, `seed=20260907`, 1000 bootstrap draws, taxonomy v1 (`0cc29c374779`):

| System | macro-F1 | bootstrap 95% | min supported recall | parse-failure rate |
|---|---:|---|---:|---:|
| `label-v5` **(frozen)** | **0.4633** | [0.3830, 0.5145] | 0.2941 | 0.250 |
| `label-v4` | 0.4298 | [0.3537, 0.4843] | 0.1176 | 0.295 |
| `star-baseline-v1` (floor) | 0.3656 | [0.3392, 0.4075] | 0.4615 | 0.000 |

**Winner: `label-v5`**, by rule 4 — 0.4633 against 0.4298, a margin of 0.0336, more than three
times the 0.01 tie epsilon, so no tie-break was needed. It is also the cheaper system to
operate (50 parse failures against 59), and it lifts the worst supported theme's recall from
0.1176 to 0.2941; neither fact was allowed to influence the choice, and neither had to.

`label-v5` beats `label-v4` on seven of the ten themes. The two it loses are `overpriced`
(0.429 against 0.500) and `does_not_work` (0.405 against 0.416); the gains are concentrated in
`wrong_size_or_fit` (0.240 → 0.475) and `unpleasant_texture` (0.143 → 0.286), the two themes v4
was worst at. Per-theme tables for both are committed:
`eval/themes/score-development-label-v4-qwen3_8b.json` and
`eval/themes/score-development-label-v5-qwen3_8b.json`.

### The star-only overlap, stated plainly

`label-v5`'s interval [0.3830, 0.5145] **overlaps** the star-only floor's [0.3392, 0.4075]. The
point estimates are 0.0978 apart and the overlap band is narrow — 0.3830 to 0.4075 — but it is
an overlap, and on 200 development rows the labeller is therefore **not distinguishable from
predicting complaint themes off the star rating**. This does not veto the freeze (rule 6):
something must be frozen before the audit opens, and `label-v5` is the best of what exists. It
is a stated limitation that the P6 verdict and the theme-shift table inherit, and it is the
reason the audit result is worth running rather than assumed.

The floor beats both prompts on one axis: its minimum supported-theme recall is 0.4615, higher
than either LLM's. A rule that predicts every theme below a star threshold cannot miss much;
what it cannot do is say *which* complaint, which is what precision 0.32 on `overpriced` and
the whole taxonomy's separation costs.

### Plumbing cost, separated

Per rule 3 the scores above already carry the parse failures as empty predictions. Beside them,
from `eval/themes/parse-census-development-*.json`:

| Version | parse-failed | dominant cause | causes |
|---|---:|---|---|
| `label-v5` | 50 (0.250) | `quote_not_verbatim` 38 | `repeated_theme` 12, `quote_too_long` 2 |
| `label-v4` | 59 (0.295) | `quote_too_long` 27 | `quote_not_verbatim` 24, `repeated_theme` 19 |

v5's prompt was written against v4's census and it worked where it aimed — `quote_too_long`
collapsed 27 → 2 — while `quote_not_verbatim` rose 24 → 38: shorter quotes are easier to
paraphrase than to copy. A quarter of v5's rows are still lost to plumbing rather than to
disagreement, and its diagnostic ceiling over answered rows is 0.5969. That ceiling is not the
score and was not used to select; it is the size of the prize a sixth prompt would have been
chasing, and ADR-0003's five-version budget is spent.

### The freeze

`conf/theme-label-spec.json` now carries:

```json
"frozen_prompt": {"name": "label_v5", "version": "label-v5", "freeze_commit": "67926ed9…"}
```

`freeze_commit` is the commit the selection was made at; the commit that records the block is
its child, so both are ancestors of any later audit run — which is the ancestry
`scripts/gate_themes.py:check_prompt` re-derives before it will pass `THEMES_PROMPT`.

**The audit set was not touched.** At freeze time `gold.review_theme_labels` held zero `audit`
rows of any label source, and `eval/themes/` held no `score-audit-*.json`. `select_prompt.py`
checks both and exits non-zero rather than selecting if either is non-empty; the counts are in
the artefact's `audit_evidence` block.

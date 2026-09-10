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

_Filled in by `make select-prompt` after the rule above was committed. See the section appended
below._

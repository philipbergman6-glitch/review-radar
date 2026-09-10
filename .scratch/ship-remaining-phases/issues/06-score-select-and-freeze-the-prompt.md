# 06 — Score v4, v5 and star-only on development; select and freeze

**What to build:** the like-for-like comparison that selects the frozen prompt. All three
systems scored on the same 200 development rows, per-theme and macro, with bootstrap
intervals, and the winner frozen with its version and freeze commit recorded so the audit
set is opened against something immutable.

The numbers already on the board: `label-v4` macro-F1 0.4298 [0.354, 0.484] over 200 rows;
star-only 0.3656 [0.339, 0.407]. **Those intervals overlap** — the LLM is not currently
distinguishable from predicting themes off the star rating. Whether v5 changes that is the
question this ticket answers.

ADR-0003 line 27 requires every version and its development per-theme table committed. The
freeze happens **before** the audit set is opened, and the prompt does not change after it.

**Blocked by:** 05 — `label-v5` must have run on development.

**Status:** done — landed 2026-09-10

- [x] All three systems scored on the identical 200 development rows
- [x] Per-theme table plus macro-F1 with bootstrap intervals committed for every version
- [x] The winner is selected on development macro-F1 alone, by a rule stated before the numbers
- [x] The frozen prompt's version and freeze commit are recorded in the repo
- [x] The star-only interval overlap is stated explicitly, whichever way it lands
- [x] The parse-failure census appears beside the score so plumbing cost is separable
- [x] The audit set has not been touched

## What was decided

The rule went in first, in its own commit (`67926ed`, `docs/decisions/theme-prompt-freeze.md`),
with the artefacts it selects on still absent from the repo. Then `label-v5` was scored over the
real 200 rows for the first time — the committed artefact had been a 24-row scorer fixture — and
the star-only floor was re-scored so it declares the settings it was measured under, which
`src/ai/prompt_selection.require_comparable` now hard-fails on rather than assuming.

| System | macro-F1 | bootstrap 95% | min supported recall | parse-failure rate |
| --- | ---: | --- | ---: | ---: |
| `label-v5` **(frozen)** | **0.4633** | [0.3830, 0.5145] | 0.2941 | 0.250 |
| `label-v4` | 0.4298 | [0.3537, 0.4843] | 0.1176 | 0.295 |
| `star-baseline-v1` | 0.3656 | [0.3392, 0.4075] | 0.4615 | 0.000 |

**`label-v5` wins by rule 4** — a 0.0336 margin, over three times the 0.01 tie epsilon, so no
tie-break was needed. It is also cheaper to operate and lifts the worst supported theme's recall
from 0.1176 to 0.2941; neither was allowed to decide, and neither had to.

**The star-only overlap did not go away.** v5's [0.3830, 0.5145] overlaps the floor's
[0.3392, 0.4075] in a narrow band. The point estimates are 0.0978 apart, but on 200 development
rows the labeller is still not distinguishable from predicting complaint themes off the star
rating. That does not veto the freeze — something has to be frozen before the audit opens — and
it is now written into ADR-0003 amendment (d) and inherited by the P6 verdict.

**The audit set is untouched and the selector proves it**: `gold.review_theme_labels` held zero
`audit` rows of any label source and `eval/themes/` held no `score-audit-*.json`;
`scripts/select_prompt.py` exits non-zero rather than selecting if either is non-empty, and the
counts are recorded in the artefact's `audit_evidence`.

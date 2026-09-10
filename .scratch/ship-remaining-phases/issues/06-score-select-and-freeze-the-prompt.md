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

**Status:** ready-for-agent

- [ ] All three systems scored on the identical 200 development rows
- [ ] Per-theme table plus macro-F1 with bootstrap intervals committed for every version
- [ ] The winner is selected on development macro-F1 alone, by a rule stated before the numbers
- [ ] The frozen prompt's version and freeze commit are recorded in the repo
- [ ] The star-only interval overlap is stated explicitly, whichever way it lands
- [ ] The parse-failure census appears beside the score so plumbing cost is separable
- [ ] The audit set has not been touched

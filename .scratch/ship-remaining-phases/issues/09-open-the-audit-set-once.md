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

**Status:** ready-for-agent

- [ ] All three systems scored on the audit set in one pass
- [ ] Per-theme and macro-F1 with bootstrap intervals committed for each system
- [ ] No threshold, prompt or cut changed after the audit numbers were seen
- [ ] The audit set is not scored again for any reason
- [ ] The run registered a run contract and results carry the run id

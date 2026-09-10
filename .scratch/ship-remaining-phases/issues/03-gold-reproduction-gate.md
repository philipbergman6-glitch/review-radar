# 03 — Gold reproduction gate

**What to build:** an independent re-derivation of the gold tables that can be run against
the pinned snapshot and either reconciles or does not — the same claim `reproduce-silver`
already makes one layer down. Gold currently asserts its own correctness using the code
that produced it; this makes reproducibility a checked property.

The reproduction is deliberately *not* the Spark path. It re-derives the gold aggregates
by an independent route from the pinned silver snapshot and reconciles counts and key sets
against the published tables.

**Blocked by:** 02 — the gates must already write artefacts.

**Status:** done

- [x] `make reproduce-gold` re-derives gold from the pinned silver snapshot independently
- [x] Counts and key sets are reconciled against the published gold tables
- [x] The gate prints its named constituents then a terminal verdict line
- [x] It writes an evaluation artefact carrying the run id and the snapshot it reproduced
- [x] A trip case exists: a perturbed slice must make reconciliation fail
- [x] A vacuous run — zero rows compared — cannot print PASS

## What the full run showed

```
GOLD_REPRO run_id=6e1c0d3a silver_snapshot=8507728480475534879 rule_status=provisional
           rule_config_hash_mine=cbe5400be465 rule_config_hash_run=cbe5400be465 tolerance=1e-09
GOLD_REPRO products total_mine=112565 total_run=112565 materialised_mine=13122 materialised_run=13122
GOLD_REPRO compare=product_month      compared=473268 only_mine=0 only_theirs=0 fields=11 mismatched=none
GOLD_REPRO compare=evaluation_points  compared=217325 only_mine=0 only_theirs=0 fields=27 mismatched=none
GOLD_REPRO compare=decline_episodes   compared=964    only_mine=0 only_theirs=0 fields=12 mismatched=none
GOLD_REPRO MAX_DROP_SPAN episodes=964 differing=257 verdict=REPORTED
GOLD_REPRO_GATE scope=full checks=10 failed=none GOLD_REPRO_GATE=PASS
```

Every key set is identical and every one of 50 compared fields agrees, on 473k spine rows,
217k evaluation points and 964 episodes.

## How independent it actually is

`scripts/reproduce_gold.py` imports nothing from `src/spark/gold.py` or `src/gold/rule.py` —
a test asserts that of the source text. It re-derives in plain Python: its own monthly fold,
its own calendar spine, direct window sums where the reference uses prefix sums, and the
episodes as a second pass over the finished point list rather than the reference's
single-pass state machine. Spark reads the pinned snapshots and does nothing else.

The two implementations meet in exactly one place, `tests/test_reproduce_gold.py`, which runs
both over 300 randomised spines per rule status on every commit. That is what made the two
findings below visible without standing up the stack.

**The tolerance earns its keep.** The two summation orders disagree in the last bit of a
double (`recent_verified_mean` 3.148387096774194 vs …193), which is precisely what
`conf/decline_rule.toml`'s `[reproduction] metric_tolerance = 1e-9` was put there for.

## One finding, reported not buried

**`max_drop` is measured from the alert, not from the episode.** `src/gold/rule.py`'s
docstring says an episode is open "from the first point of that run"; the code initialises
`max_drop` at the alert point and only updates it afterwards. When a pre-alert supporting
point dropped further than anything after it, the two readings differ — **257 of 964
episodes, 27%**.

This is a divergence between gold's spec and gold's code, not a reproduction failure, and the
reproduction has no standing to call either one wrong. So `max_drop` is reconciled over the
published span (and agrees exactly), while the documented span is computed beside it and the
difference is counted on its own `MAX_DROP_SPAN … verdict=REPORTED` line. Nothing blocks;
nothing disappears. **P3 should decide which reading it means before the rule is frozen** —
after the freeze this becomes an edit to a frozen protocol.

## What is deliberately not compared

Two fields, each with the reason written in the script's docstring:

* **`persistence_run`** — the reference's own loop counter, not a property of an evaluation
  point. Reproducing it would mean reproducing the loop.
* **nothing else.** `evaluable_points`, `supported_points` and `last_supported_at` are
  undocumented but turned out to be exactly derivable from the episode's span, and they agree
  on every randomised spine and all 964 real episodes.

One latent divergence is recorded rather than fixed: `episode_key` is assigned to every point
in an episode's span. That coincides with the reference exactly while
`unevaluable_resets_persistence = true`, which the config freezes. Flip that switch and the
two readings of "which points belong to the episode" part company — and this gate is what
would say so.

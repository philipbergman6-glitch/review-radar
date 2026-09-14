---
status: accepted
date: 2026-09-14
---

# The decline rule is calibrated before it is frozen, and the placebo ceiling never moves

ADR-0001 fixed *when* the rule stops moving — one protocol-freeze commit, before the
2020–2023 holdout is opened. It did not fix what has to be true for that commit to be
allowed. Without that, "frozen" only means "written down": the 964 alerts `make gold` printed
came from placeholder thresholds measured on the data they were chosen against, and an
arbitrary rule firing an arbitrary number of times is not a finding.

So the freeze now has a precondition that prints two numbers, both measured on evaluation
points before `holdout_start` and both compared against bars committed to
`conf/decline_rule.toml` *before* the producer ran:

* **Placebo trigger rate.** Each product's monthly aggregates are cut into six-month blocks
  and circularly rotated and shuffled. Volume, seasonality and dependence shorter than a
  block survive; trend longer than a block does not, so every alert on a permuted series is a
  false one. The unit is the one the persona argument is stated in — alerts arriving on one
  desk in one calendar month — because a ceiling expressed per product-month cannot be
  compared to a workload. RR-09 round 5 fixes it at **1 placebo alert/month**: four
  investigations of capacity, at most a quarter of them noise.
* **Detection power.** Reviews inside a product's own history are relabelled downward from a
  drawn eligible month — timestamps, volume, verification and identity untouched — so the
  mean falls by a known amount, and the rule is re-run. The bar is **80% of an injected
  0.3-star step, detected within 6 evaluable points**.

`scripts/calibrate_gold.py` searches the committed grid rather than scoring the provisional
values alone: those were placeholders, and the freeze has to choose something. Searching is
legitimate exactly here and nowhere afterwards — every number comes from before
`holdout_start`, and the search ends at the freeze commit. The selection rule is committed
with the grid: admissible is under the ceiling; the winner has the highest power at the
primary effect, ties going to the lower median delay and then to the larger delta and longer
persistence, which is the conservative direction.

## The measured result

`GOLD_CALIBRATION` **FAILs**, and the shape of the failure is the finding. Of 144
configurations, 116 hold the placebo ceiling. The best of them,
`B6 R12 δ0.3 P3 min_reviews20`, runs at **0.80 placebo alerts/month** and reaches
**power 0.186 [0.168, 0.205]** at the 0.3-star step — against a target of 0.80.

The target is not merely missed, it is unreachable in this grid at any noise budget. The
frontier tops out at **power 0.46 at 3.99 alerts/month**, four times over the ceiling. There
is no configuration here that both keeps an honest noise rate and reliably catches a
0.3-star decline, on this corpus, with these window shapes.

Two results fell out of the measurement and are recorded because they are not obvious:

* A trailing-baseline rule absorbs a sharp step within `B` months, so a **gradual** decline
  can hold the condition for more consecutive points than a step of the same terminal size.
  At 0.5 stars the six-month ramp is caught more often than the step (0.303 vs 0.277). This
  costs nothing here — the bar is stated at the step and the ramp is reported beside it — but
  it means "the rule detects sharp declines best" would have been false.
* Calibration truncates each spine at `holdout_start`, so no window of any calibration point
  reaches a holdout rating. The gold job stops at the first holdout *point*, which still lets
  a 2019-12 point average 2020 ratings into its recent window. The difference is visible:
  the same provisional rule prints 964 alerts in gold and 836 here.

## Considered options

- **Weaken the ceiling until power clears 0.80.** Rejected, and pre-rejected: RR-09 round 5
  says in advance not to. A noise bar chosen after seeing that the rule cannot clear it is
  not a bar, and this is the exact move ADR-0001 exists to prevent.
- **Restate the bar at 0.5 stars**, where power is higher. Rejected for the same reason, and
  it is the move the superseded `[calibration]` block in `6178411` had already made — it
  injected 0.5 stars, cited an ADR that did not exist, and nothing read it.
- **Widen the grid** until something clears both. Not rejected in principle, but the frontier
  says the shortfall is about window shapes and per-product review volume, not about a corner
  of the grid that was not searched: the loosest rule available reaches 0.46.
- **Report power without a bar.** Rejected: a target set after the measurement is the thing
  the evaluation table's threshold column exists to make impossible.

## Consequences

- **The ceiling blocks the freeze; the power target does not.** `scripts/freeze_rule.py`
  refuses outright when no configuration held the ceiling, when the calibration is stale, or
  when its evaluator disagreed with `src/gold/rule.py`. A power shortfall is the one waivable
  refusal, because RR-09 round 5 pre-declared this contingency — *do not weaken the ceiling;
  report the trade-off* — and the waiver is an explicit flag that writes its own reason into
  the config, so the override is recorded rather than silent.
- **The published claim changes shape.** The holdout result is a monitoring backtest with a
  known and low detection rate: alerts it raises are unlikely to be noise, and the declines
  it misses are many. "N of M eligible products alerted" stays true and stays the opening
  line; "the rule finds declining products" does not follow from it and must not be said.
- **Investigate and watchlist** (RR-09 round 5) are the pre-declared response to exactly this
  outcome and become the way the alert list is presented.
- The rule that gets frozen is **not** the provisional one: `recent_months` 6 → 12,
  `persistence` 2 → 3, `min_reviews` 10 → 20. Every downstream count derived from gold moves
  with it, and anything computed under the provisional rule is stale until gold is re-run.
- `GOLD_CALIBRATION` is quality, so by ADR-0011 it does not block P3. It blocks the freeze
  instead, which is the one place on this map where a quality number is allowed to stop
  something.

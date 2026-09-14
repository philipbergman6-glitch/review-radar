---
status: accepted
date: 2026-09-14
---

# The placebo permutes the spine, and power injects a step into products the rule leaves alone

[ADR-0001](0001-temporal-holdout-and-protocol-freeze.md) requires a placebo ceiling and
injected-decline power targets in the protocol freeze, and
[ADR-0011](0011-phase-gates-print-a-number-and-only-reproducibility-blocks.md) makes
`GOLD_CALIBRATION` fail on "a rule whose placebo rate exceeds the ceiling". Neither says how
either number is constructed, and until `conf/decline_rule.toml` gained its `[calibration]`
table there was nothing to read it off. This decides both constructions, before the producer
exists and before either number has been seen.

**Placebo — permute month order within a product.** The decline rule
(`src/gold/rule.py:1-20`) looks for one thing: `recent_mean <= baseline_mean - delta`
sustained over `persistence` consecutive evaluable points on a calendar spine. The null it
must be calibrated against is therefore "this product's reviews, with no sustained temporal
decline in them". We build that by permuting the **payloads** across the spine's fixed month
labels — `review_count`, `rating_sum` and the rest of `SPINE_FIELDS` travel together, the
calendar itself does not move, because `evaluate` rejects a spine that is not a complete
consecutive sequence (`src/gold/rule.py:116-118`). Volume, rating distribution and sparsity
all survive; only the ordering the rule reads is destroyed. An alert on a permuted spine is a
false positive by construction, and the share of permuted spines that alert is the placebo
rate.

**Power — inject a step into products that do not alert.** A product whose real spine
produces no alert is given a step decline of `injected_delta` stars from a uniformly drawn
evaluable month onward, and the rule is re-run. The share detected is the power. Drawing the
subjects from the non-alerting products is what makes the number mean "would this rule catch
a decline that started here", rather than re-measuring products that already alert.

Both run on pre-2020 evaluation points only, seeded from `[calibration] seed`, over
`[calibration] draws` draws. Both are pure functions over spines, so they call
`evaluate(spine, rule)` directly and need neither Spark nor a second implementation.

## Considered options

- **Placebo by random cut point** — apply the rule at randomly drawn months on unpermuted
  spines. Rejected: a genuinely declining product still declines under it, so the "null" is
  contaminated by exactly the signal being measured, and the rate it prints is an unknown
  mixture of false and true positives.
- **Placebo against matched stable controls** — reuse the P6 matching
  (`src/gold/controls.py`). Rejected as circular: `require_condition_false` selects controls
  *by* the rule not firing on them, so the placebo rate it yields is near zero by definition
  and measures nothing.
- **Synthetic spines from a fitted distribution** — clean under the null, but its rate
  depends on how well the fit matches real sparsity, so a failure is uninterpretable: the
  rule and the generative model are confounded. Permutation needs no model.
- **Power against the real declines in the data** — there is no ground truth for which
  pre-2020 products "truly" declined; that label is what the rule is for.

## Consequences

- The placebo rate is a property of *this* category's spines, not a universal false-positive
  rate; it is reported with `draws` and the seed beside it.
- **The placebo rate is an upper bound on the false-positive rate, not a point estimate**,
  and is reported as such. Permutation removes the ordering but keeps each product's own
  marginal spread, so a product whose real history is volatile or genuinely declining still
  holds the low months a spurious alert needs — a random arrangement can cluster enough of
  them in one recent window. The bias runs upward, which is the safe direction for a ceiling:
  clearing a conservative test is still cleared.
- The evaluable-point count survives **in aggregate**, not point by point. Permutation moves
  which months land in which window, so individual points change evaluability while the total
  barely moves.
- Power is conditional on `injected_delta = 0.5` and is meaningless quoted without it. A
  step, not a ramp: the rule's condition compares two window means, and a ramp would confound
  the magnitude with the onset shape.
- `GOLD_CALIBRATION` can fail. If it does, the honest moves are to report it or to declare a
  new, exploratory analysis — not to retune `[rule]` until it clears, which
  [ADR-0001](0001-temporal-holdout-and-protocol-freeze.md) already forbids.
- Because this ADR and `[calibration]` both land before the producer, the git history is the
  evidence that neither construction nor bar was chosen after seeing a number.

## Validated before acceptance

Both constructions were exercised against `evaluate()` on **synthetic spines** — 500 products
per arm, seeded — to check that they behave as claimed. These are properties of the
constructions, not results about this category's data, and none of them is the calibration:

| arm | real spines alert | permuted alert |
|---|---|---|
| stable products (the null) | 0.000 | 0.002 |
| products given a real 0.5-star decline | 0.986 | 0.522 |

The first row is the construction working: under a true null, permuting changes nothing and
the rate sits at the floor. The second row is what produced the upper-bound consequence above
— permutation cuts a real signal from 0.986 to 0.522 rather than to the floor, because the
declining product's own low months survive the shuffle. Power was exercised the same way:
injecting 0.5 stars into the 500 products the rule left alone was detected in 0.984 of them,
so `power_target = 0.80` is a bar the construction can express rather than one it cannot
reach. Permuting payloads across fixed month labels also keeps the spine a legal complete
calendar, and the aggregate evaluable-point count moved by −0.49%.

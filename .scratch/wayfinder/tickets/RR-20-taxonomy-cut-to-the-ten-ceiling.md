---
id: RR-20
title: All eleven candidate themes clear the support rule but the ceiling is ten
type: grilling
status: closed
assignee: philipbergman (decided 2026-09-07)
blocked-by: [RR-08, RR-19]  # both closed
blocks: [RR-21]
---

## Question

ADR-0003 "Taxonomy and human labels" applies a support rule — keep a theme only when it
occurs in at least 3% of the low-rated discovery reviews and on at least two products —
and then caps the taxonomy: "Target eight themes; ten is a hard ceiling."

The 600-review discovery run (`a4a62ffd`, config `af22cf44df76`, 484 succeeded, 897 phrases,
499 distinct aspects) was merged by hand into **11 candidate themes**, and
`scripts/score_taxonomy.py` measured every one of them over the 313 low-rated discovery
reviews `[docs/theme-taxonomy/theme-support.csv, 2026-09-07]`:

| theme | low-rated reviews | share | products |
|---|---:|---:|---:|
| `does_not_work` | 71 | 22.7% | 59 |
| `poor_build_quality` | 45 | 14.4% | 43 |
| `overpriced` | 33 | 10.5% | 32 |
| `wrong_size_or_fit` | 32 | 10.2% | 31 |
| `unpleasant_texture` | 27 | 8.6% | 28 |
| `breaks_or_wears_out` | 25 | 8.0% | 24 |
| `unpleasant_scent` | 23 | 7.4% | 24 |
| `hard_to_use` | 23 | 7.4% | 24 |
| `not_as_described` | 22 | 7.0% | 24 |
| `irritation_or_harm` | 21 | 6.7% | 22 |
| `arrived_damaged` | 18 | 5.8% | 19 |

**All eleven pass.** The rule cannot break the tie, and the script refuses to trim
(`over_ceiling=yes`) because which theme goes is a scoping judgement, not a measurement.
Nothing downstream can start — the taxonomy is on screen during labelling and hashed into
the label prompt's config — so this blocks the rest of P6.

## Answer (2026-09-07)

**Merge `breaks_or_wears_out` into `poor_build_quality`.** Ten themes, nothing dropped.
Frozen as `conf/theme-taxonomy.json` v1.

Rationale, in Philip's words as chosen from three costed options: cheap build and early
failure are adjacent complaints about the same underlying fault, so folding them loses less
than deleting a category outright. The two alternatives were rejected on stated grounds:

- **Drop `arrived_damaged`** (lowest support, 5.8%). Rejected because it is the only
  *fulfilment* theme rather than a product theme, which makes it the one most likely to
  actually shift post-2020 — dropping it risks deleting the result the analysis is for.
- **Merge `unpleasant_texture` + `unpleasant_scent` into `unpleasant_sensory`.** Rejected
  because a product that smells wrong and one that feels wrong are different failures to a
  manufacturer; the merge would blur the finding rather than compress it.

Measured cost of the chosen merge: `poor_build_quality` goes from 45 to **67** low-rated
reviews (14.4% → 21.4%), 64 products, 13 merged aspects, and the wear-over-time signal
stops being separable from the cheap-build signal. That loss is recorded here so the
theme-shift table cannot later be read as if durability were tracked on its own.

Unchanged by this decision: coverage is **219 of 313 low-rated reviews (70.0%)** by at least
one theme; the remaining 30% are `other`. The 424-aspect long tail stays unmapped and is
reported, not quietly folded in to inflate a theme.

The support rule and the ceiling were both frozen in ADR-0003 before any of this was
measured, and neither moved. `scripts/score_taxonomy.py` re-runs the rule against the frozen
merge and prints `over_ceiling=no`.

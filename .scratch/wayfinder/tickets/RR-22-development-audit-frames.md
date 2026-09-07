---
id: RR-22
title: How the development, audit and training-pool frames are drawn
type: grilling
status: closed
assignee: agent (decided 2026-09-07, under ADR-0003 as amended)
blocked-by: [RR-20, RR-21]  # both closed
blocks: []
---

## Question

`src/spark/theme_samples.py` implements exactly one frame, `discovery`, and raises
`NotImplementedError` for the other four with the stated reason: "development, audit,
training_pool and inference frames all depend on the frozen theme terms (ADR-0003)".
The taxonomy froze on 2026-09-07 (`conf/theme-taxonomy.json` v1, `RR-20`), so the blocker
is gone — but ADR-0003's sampling clause is not directly executable as written. Three
things it names do not exist yet:

1. **"the frozen theme terms."** ADR-0003 says the audit's 120 enriched rows are "drawn by
   frozen theme terms". `conf/theme-taxonomy.json` carries definitions, `includes`,
   `excludes` and boundary notes — prose written for a labeller, not a term list a
   `rlike` can match. `docs/theme-taxonomy/merge-table.csv` carries 75 *aspect* labels
   (`product effectiveness`, `not effective`, `damaged packaging`), which are the model's
   own abstractions and mostly do not occur verbatim in review text. There is no artifact
   that says which strings enrich for which theme.
2. **The audit population.** ADR-0003 wants "80 prevalence-representative rows from
   candidate and control windows", post-2020. But post-2020 *windows* cannot exist:
   `conf/decline_rule.toml` is still `status = "provisional"`, so the gold job refuses to
   evaluate any point on or after `holdout_start` and the last gold run reports
   `holdout_eligible_products=0`, `holdout_alerts=0`. Drawing an audit frame from
   rule-derived post-2020 windows would require the protocol freeze, which ADR-0001 puts
   after this work, not before it.
3. **The development frame's composition.** ADR-0003 says "200 enriched pre-2020
   development reviews" and defines "enriched" only for the audit set.

Also open, and cheaper: whether the training pool and inference frames land in the same
change.

## Answer (2026-09-07)

### 1. Theme terms are a *derived, frozen artifact*, not a hand-written list

`conf/theme-terms.json` v1, written once by `scripts/freeze_theme_terms.py` and committed.
Per theme it holds whole-word unigrams and bigrams mined from the **evidence quotes** of the
discovery complaints whose aspect merged into that theme (`merge-table.csv` supplies
aspect → theme; `gold.discovery_phrases` at the frozen `inference_config_hash` supplies the
quotes). A term is kept when it occurs in at least two distinct discovery reviews of its
theme and its theme-vs-rest odds clear a fixed ratio; stopwords are dropped; a term claimed
by two themes goes to the theme with the higher odds and to no other.

Why derived rather than written by hand: a hand list is a second, unaudited taxonomy
artifact, and the one thing worse than a rough enrichment filter is one nobody can
reproduce. This one is a pure function of frozen pre-2020 inputs, so the frame is
reproducible from the repo alone, and the script prints the term count per theme.

What it is **not**: it is not a classifier, it never appears in a prompt, and it never
touches a label. It only decides which reviews are *offered* for labelling — a review that
matches `does_not_work`'s terms is not thereby labelled `does_not_work`, and the enriched
rows are expected to carry many true negatives. Using a qwen3-derived artifact to build the
frame is not a blindness breach under `RR-21`: the system under test is qwen3's *theme
labels on these reviews*, the discovery sample is disjoint from every other frame, and the
frame is fixed before any labelling starts.

### 2. The audit population is the pre-2020 candidate and control **products**, observed after the holdout boundary

The audit frame is every silver review with `review_month >= protocol.holdout_start`
(2020-01) on a `parent_asin` that appears in `gold.matched_controls` as either
`candidate_asin` or `control_asin`, carrying `role` from that table. No decline rule is
evaluated post-2020 and none needs to be: the *products* were selected pre-2020, and the
holdout side of their spine is exactly the untouched post-2020 material ADR-0003 wants.
"Window" in ADR-0003's phrase is therefore read as the product's holdout-side span, and
this reading is stated here rather than left to be inferred from the code.

The alternative — freeze the decline protocol first so post-2020 episodes exist — was
rejected: ADR-0001 orders the freeze after the pre-2020 development work, and ADR-0003
requires the audit set to be untouched, which a rule-selected post-2020 window is not.

### 3. Composition and quotas

| Frame | Size | Draw |
|---|---:|---|
| `development` | 200 | pre-2020, enriched: 20 per theme by term match |
| `audit` (enriched) | 120 | post-2020, enriched: 12 per theme by term match |
| `audit` (representative) | 80 | post-2020, prevalence draw, no term filter, reported separately |
| `training_pool` | 3000 | pre-2020, prevalence draw |
| `inference` | — | **still blocked** on the decline-rule protocol freeze |

Quotas are filled theme by theme in **ascending discovery support** — the rarest theme
picks first — so `arrived_damaged` (5.8% of low-rated discovery reviews) is not crowded out
by `does_not_work` (22.7%). Within a theme, candidates are ordered by the seeded
`draw_key`. A review is consumed by the first theme that takes it and is never offered
again. A theme that cannot fill its quota releases the shortfall to a final pass over the
remaining enriched pool by draw key, and the per-theme fill is printed and stored in the
run ledger, never silently topped up.

**No rating stratification outside `discovery`.** ADR-0003 asks for a 70% low-rated
oversample only for the discovery draw, whose job was to surface complaint vocabulary. A
development or audit frame skewed to 1–2 stars would inflate every theme's prevalence and
make the representative subset incomparable to it. Enrichment here is by text, not by star.

Disjointness is unchanged: the existing left-anti join against
`gold.theme_sample_assignments` stands, and pre-2020 frames are drawn in the order
`discovery` → `development` → `training_pool`. The audit frame is disjoint by construction
(it is the only post-2020 frame), and the left-anti join is kept there anyway.

`inference` continues to raise `NotImplementedError`, now with the correct reason: it needs
`conf/decline_rule.toml` frozen so post-2020 candidate and control windows exist. That is
the known blocker already recorded in the P6 handoff, and it is outside P6's critical path.

## Consequences

- The enrichment filter is only as good as the discovery quotes; a theme whose complaints
  are phrased in words the discovery sample never produced will be under-represented in the
  enriched rows. This is why the audit carries 80 prevalence rows that no term touched, and
  why they are scored and reported separately.
- Per-theme audit support is not guaranteed to reach ten positives. Enrichment offers 12
  reviews per theme; how many are true positives is a labelling outcome, not a draw
  guarantee. A theme below ten audit positives is *unsupported* and drops out of macro-F1
  by ADR-0003's own rule — reported, not back-filled.
- `conf/theme-terms.json` is frozen at the same moment as the frames. Regenerating it after
  a frame is drawn would silently change what "enriched" meant, so the script refuses to
  overwrite an existing file without `--force`, and the file's hash goes in the ledger.

---
id: RR-09
title: The question the presentation opens with
type: grilling
status: closed
assignee: philipbergman (grilled 2026-09-04)
blocked-by: []
blocks: [RR-13, RR-14, RR-18]
---

## Question

The audit puts results & insights at 2/15 today and warns of "60% are 5-star" findings; the
grade table's projected shortfall is "no sharp question yet". Two candidates are on the
table:

- **"Which products got worse, and why?"** — rating decline across the 1,902 products with
  ≥50 reviews, explained by an aspect shift. Uses gold's drift calculation and gives the
  aspect phase something to be *for*.
- **"Which reviews should you not trust?"** — review bursts and incentivised-review
  detection: 6,139 exact duplicate `(user, product, timestamp)` triples, unverified-purchase
  spikes, near-duplicate text found via embeddings. Uses the embedding phase for something
  other than search.

The artifact says "decide on the data, after gold exists" — but gold is execution and this
map ends before it. So the decision this ticket must actually make is one of:

- commit to one question now, on the profile evidence already measured; or
- commit to the **procedure** that picks it: the specific numbers to compute from gold, and
  the threshold at which each question is judged to have a real answer behind it.

Either way the resolution must name:

1. the opening question (or the decision rule and its inputs);
2. what a *good* answer looks like as a number, so the gate stops being "three insights
   written down";
3. whether the second candidate survives as a supporting thread or is dropped — under three
   weeks, two insight threads may be one too many;
4. what the answer is if the data does not support the chosen question — the fallback, named
   in advance rather than improvised.

Discipline to hold, from the artifact's own gate: "an outlier you cannot narrate is a bug,
not an insight." Each finding is reproduced on a slice in pandas and must match Spark.

## Grilling log

### Round 1 — 2026-09-04

Settled `[observed, Philip]`:

1. **Commit now** to the drift thread. Wording: *"Which products experienced a sustained
   decline in customer ratings, and which complaint themes increased during that
   decline?"* Not "got worse, and why" — that claims deterioration and causation.
   Eligibility for a decline candidate is multidimensional and pre-registered (volume in
   both periods, meaningful decrease, persistence, robustness, verified-mix check).
   Thresholds chosen from the monthly count distribution, before any product identity is
   seen. Fallback stays in the same family: *"Which complaint themes distinguish low-rated
   periods from high-rated periods among sufficiently reviewed products?"*
2. **Trust thread demoted to a provenance result.** Classify the 6,139 key collisions
   (exact / conflicting / unresolvable), deterministic survivor rule, auditable reject
   table, report both groups and rows removed. Never call it trust or incentivisation.
   Burst detection and near-duplicate discovery are **removed**, not deferred.
3. **Persona: category manager**, promised only "identify products whose customer
   experience appears to be deteriorating and inspect which complaint themes changed".
   Gold finds, aspects characterise, search retrieves, RAG summarises with citations.
4. **Reproduction: pandas over raw JSONL**, deterministic multi-product cohort (all
   eligible products or a fixed-hash sample incl. one candidate, one stable, one boundary
   case), reproducing parse/validity counts, dedupe, product-month counts, means, verified
   ratios, drift, and candidate selection. Exact match on counts and ids; tolerance on
   floats. No shared transformation code.
5. **Constraint handed to RR-08:** the aspect subset must not be candidates only —
   before/after windows for candidates, matched stable/improving controls, small random
   or edge sample for validation.

Terms written to `CONTEXT.md`.

### Round 2 — 2026-09-04

Settled `[observed, Philip]`:

5. **Decline measure = two adjacent trailing calendar windows** at each evaluation point:
   baseline = preceding B months, recent = following R months; candidate if recent mean ≤
   baseline − δ; persistence = condition true for P consecutive *evaluable* endpoints.
   Not "first B months" (launch effects, decade-old baselines). Historical scanning has a
   multiple-testing problem: fixed cutoff, temporal holdout, or label as exploratory.
6. **Calendar spine, never skip empty months.** Empty product-months kept with
   `review_count = 0`, null rating stats, no imputation. Eligibility = min total reviews
   and min active months *within each window*. A window with insufficient reviews is
   unevaluable; it neither extends nor confirms persistence.
7. **Mean rating is the primary selection statistic**; ≤2★ share is a secondary
   characterisation and robustness signal, not a universal hard gate (a 5★→3★ shift
   would be lost). Metadata `average_rating` is a coarse sanity check only.
8. **Robustness**: seeded bootstrap, resampled separately within baseline and recent,
   clustered by `user_id`; influence sensitivity as a proportion or max-observation
   influence, not fixed k; verified-only recompute recorded as a **robustness grade**
   (robust in all + verified-only / visible overall, inconclusive verified-only /
   explained or reversed by verification mix). Diagnostics live in a separate
   product-level candidate table, not in `gold.product_month`.
9. **Two-stage protocol freeze** (aggregate histograms → committed config → selection),
   described honestly as limiting analyst discretion, not proof of pre-registration.
   Preferably develop thresholds on an earlier period, lock, evaluate on a temporal holdout.
10. **Gate split in two.** *Computational gate* (pass/fail): Spark and pandas cohort ids
    agree, counts exact, metrics within tolerance, reasons/exclusions complete, repeated
    runs deterministic. *Analytical outcome* (reported, not pass/fail): n and % of eligible
    selected, n with sufficient aspect evidence, robustness grades, whether enough
    narratable cases exist. The "5 to 5%" band is rejected as narrative routing. All
    candidates in a machine-readable table; narrate only the top three by a predeclared
    ranking rule; example reviews chosen deterministically.
11. **No relaxation ladder.** One locked primary spec; predeclared sensitivity variants
    reported with labels primary / moderate evidence / exploratory; fallback triggers if
    fewer than **three** robust, narratable candidates.
12. **Theme shift = matched-control-adjusted theme shift**, not DiD unless pre-trends are
    tested. Controls: same calendar windows, similar baseline mean and volume, observed in
    both periods, stable under the locked rule, similar characteristics where available.
    Report (cand recent − cand baseline) − (ctrl recent − ctrl baseline) per theme with
    counts and uncertainty; ranked effect sizes with intervals or multiplicity correction,
    no binary "significant theme" claims. Taxonomy developed on candidates **and**
    controls, then frozen. If the aspect half is unbuilt the claim narrows to: *"The
    completed result identifies sustained rating declines; aspect-level explanation is
    preliminary."*

### Round 3 — 2026-09-04

Settled `[observed, Philip]`:

13. **Temporal holdout, cutoff 2020-01-01.** All threshold and sensitivity design uses
    pre-2020 data only; post-2020 identities and outcome distributions hidden until the
    config freezes. Post-cutoff recent windows may use pre-2020 baselines (context, not
    leakage). Report evaluable products per holdout year; 2023 is censored at September.
    Describe the replay as a **historical backtest of a frozen monitoring rule**. 2020
    regime change makes calendar-matched controls essential.
14. **Monthly scan primary, calibrated.** Persistence alone is not a multiplicity
    treatment. Calibrate the complete trigger on pre-2020: alerts per eligible
    product-year, plus a blocked time-permutation / placebo null for the false-alert
    rate; freeze an acceptable rate with (B, R, δ, P). Post-2020 results are **alerts**;
    bootstrap intervals describe effect uncertainty, not significance. Store every
    episode (after a recovery rule); the product narrative uses the first robust holdout
    onset. Fixed-cutoff variant answers a different question and stays a variant only.
15. **Text-characterisable** = separate counts per window: non-empty reviews (aspect
    classification), 20+-word reviews (embeddings / rich evidence), sampled per window,
    expected per theme where measurable. Not 20+ words alone. Fallback reworded:
    *"Across sufficiently observed products, which complaint themes are more prevalent in
    each product's low-rated periods than in its high-rated periods?"* — within-product
    differences first, then aggregate; minimum counts per period; labelled descriptive.
16. **Ranking** by bootstrap lower bound of (baseline − recent), after robustness
    classification, same bootstrap design for all, not presented as corrected evidence.
    **Representative example** per window and theme: carries the theme → high label
    confidence → rating near the median for that theme → nearest embedding to
    theme/window centroid if available → helpful votes and stable id as tie-breakers.
    Raw `helpful_vote` favours old reviews; "most helpful" may be shown separately, not as
    evidence.
17. **Controls**: match on baseline mean, log count, baseline negative share, verified
    ratio, active months, pre-period trend. `main_category` likely constant; `store` only
    if complete and meaningful (89.9% populated `[observed]`). Compute theme change **per
    control**, average the five (equal or capped weights), subtract from candidate. Report
    control reuse; fewer than five → actual count, wider uncertainty, *under-controlled*.
18. **Three relations**: product evaluation point (config id, month, window bounds,
    calendar/active months, review and long-text counts, mean, negative share,
    verified-only stats, flags, rejection reason); decline episode / candidate (grain =
    one row per episode, product narrative = first qualifying holdout episode);
    candidate–control membership with distance.
19. **Ranked adjusted theme-share changes with pointwise descriptive intervals**; always
    show counts, shares, adjusted difference, interval, label validation quality.
    Taxonomy leakage: either derive from pre-2020 examples + controls, freeze, validate,
    apply post-2020 — or declare aspect discovery exploratory. Bootstrap captures
    sampling variation, not LLM-label error.
20. **Opening line**: *"N of M eligible products triggered at least one sustained-decline
    alert during 2020–2023, using a rule developed on pre-2020 data and then applied
    unchanged."* Denominator = products eligible at least once in the holdout. Then hero
    product, windows, adjusted theme shift, representative reviews; then zoom out.

### Round 4 — 2026-09-04

Settled `[observed, Philip]`:

21. **Held-out taxonomy, strict.** Derive themes from pre-2020 episodes + controls →
    definitions, inclusion/exclusion examples, prompts, other/unknown, abstention,
    multi-label → tune and validate labeller on pre-2020 hand labels → freeze the complete
    labelling spec → apply unchanged post-2020. Post-2020 human labels never revise the
    spec; a post-2020 audit sample is a final test set opened only after the freeze. New
    post-2020 themes are reported as *uncovered holdout content* (exploratory appendix),
    never added retroactively. Budget has **four** allocations: pre-2020 discovery,
    pre-2020 prompt development/validation, post-2020 candidate+control inference,
    untouched post-2020 audit. Rare themes: shrink the taxonomy or stratify with
    rare-theme enrichment plus a smaller prevalence-representative audit sample.
22. **Episode state model with asymmetric missing-data handling.** Before trigger: an
    unevaluable month does not count toward P and preferably interrupts calendar
    persistence. After trigger: a short unevaluable gap leaves the episode open with
    status unknown, does not add evidenced duration; > G unevaluable months closes as
    *gap*; K consecutive evaluable false points (K = 2, confirmed on pre-2020) closes as
    *recovery*; else *end_of_data*. Store `condition_started_at`, `alert_triggered_at`,
    `last_supported_at`, `episode_closed_at`, `closed_by`. The streaming demo raises the
    alert at `alert_triggered_at`, never at the retrospective start.
23. **Placebo trigger rate, not "false-alert rate".** Freeze specifies: how placebo series
    preserve volume, seasonality and temporal dependence; unit = triggers per eligible
    product-year; the ceiling; detection power and delay on injected synthetic declines;
    (B, R, δ, P, G, K). Tune on one pre-2020 portion, calibrate on another (blocked
    rolling-origin folds or split periods), freeze, then touch 2020–2023. Ceiling chosen
    by translating into *expected alerts requiring investigation per month at the actual
    eligible-product count*, balanced against detection probability and delay for
    declines of practical size.

### Round 5 — 2026-09-04

Settled `[observed, Philip]`:

24. **Investigation capacity = 4 new investigations/month** (persona assumption, stated in
    the design doc). Placebo budget ≤ 25% of it ≈ **1 placebo trigger/month** ≈ 0.0063 per
    eligible product-year at 1,900 eligible. If no configuration reaches that with
    reasonable power, do not weaken the ceiling: report the trade-off and add two output
    tiers — **investigate** (≤ 4 highest-evidence new episodes/month) and **watchlist**
    (further qualifying signals retained, no manual review). Capacity controls
    prioritisation, not existence of findings.
25. **Injected declines**: row-level relabelling preserving timestamps, volume,
    verification, identity. Two mechanisms: *severe complaint shift* (5★ → 1–2★) and
    *moderate satisfaction erosion* (5★ → 3★, mean falls, ≤2★ share does not). Predeclared
    effect grid ≈ 0.2★ / **0.3★ primary** / 0.5★ mean decline. Step at a random eligible
    month; ramp linear over 6 calendar months to the same terminal size, then persists.
    Injection dates need enough history and follow-up; multiple seeds; stratify by volume.
    Success criteria frozen before final calibration: ≥ 80% detection of the 0.3★ step,
    within 6 evaluable points; ramp reported, not required to match; all three sizes
    reported. Effect sizes and power targets are set independently of detector
    performance. Limitation to state: injections validate the rating alert only, not the
    theme analysis.

## Answer

Closed 2026-09-04 after five grilling rounds (log above). Terms in `CONTEXT.md`; the
holdout/freeze decision in `docs/adr/0001-temporal-holdout-and-protocol-freeze.md`.

**1. The opening question.** *"Which products experienced a sustained decline in customer
ratings, and which complaint themes increased during that decline?"* Asked by a category
manager. Committed now, not by a gold-time contest. Opening line of the talk: *"N of M
eligible products triggered at least one sustained-decline alert during 2020–2023, using a
rule developed on pre-2020 data and then applied unchanged."* Then the hero product, then
zoom out.

**2. What a good answer is, as numbers.** Two gates, kept apart:

- *Computational gate* (pass/fail, printed by the gold job): pandas-over-raw-JSONL
  reproduction on a deterministic cohort matches Spark — candidate ids exact, counts exact,
  metrics within tolerance, rejection reasons complete, re-runs deterministic.
- *Analytical outcome* (printed, never pass/fail): eligible products, alerts and episodes
  in the holdout, share of eligible, robustness grades, text-characterisable count, and
  whether ≥ 3 robust text-characterisable candidates exist. Per candidate: drop with
  bootstrap interval; per theme: matched-control-adjusted share change with interval.

The rule: adjacent trailing calendar windows (B baseline, R recent) on a calendar spine,
alert when recent mean ≤ baseline − δ for P consecutive evaluable points; episodes closed by
K false points, a gap > G, or end of data. (B, R, δ, P, G, K, minimum counts, placebo
ceiling) are set from pre-2020 data only and committed in one **protocol freeze** before the
2020–2023 holdout is touched. Calibration = placebo trigger rate ≤ ~1/month at the eligible
count, and ≥ 80% detection of an injected 0.3★ step within 6 evaluable points.

**3. The second candidate.** Dropped as an insight. Survives only as a **provenance
result** in silver: key collisions classified (exact / conflicting / unresolvable),
deterministic survivor, auditable rejects, both group and row counts printed. Never called
trust, incentivisation or bursts. Burst detection and near-duplicate search are **removed**
from scope, not deferred.

**4. Fallback.** Triggered if fewer than three robust, text-characterisable candidates in
the holdout under the frozen rule — no relaxation ladder, sensitivity variants reported but
never promoted. Fallback question: *"Across sufficiently observed products, which complaint
themes are more prevalent in each product's low-rated periods than in its high-rated
periods?"* — within-product first, then aggregate, labelled descriptive. If the aspect half
is unbuilt, the claim narrows to *"The completed result identifies sustained rating
declines; aspect-level explanation is preliminary."*

**Handoffs written into:** RR-02 (collision classes, survivor, counts), RR-07 (the alert
statistic is the MLlib candidate; bursts gone), RR-08 (held-out taxonomy, four-way budget,
controls, representative-example rule), RR-10 (backtest framing, alert fires at
`alert_triggered_at`), RR-13 (gold gate shape), RR-17 (theme-shift table shape, post-2020
audit set), RR-18 (first three slides fixed).

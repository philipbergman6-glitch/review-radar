# Review Radar

A review-intelligence pipeline over Amazon `All_Beauty` reviews. This glossary holds the
project's language only; implementation decisions live in the wayfinder tickets and ADRs.

## Language

### The opening question

**Opening question**:
The single question the presentation is built to answer: *which products experienced a
sustained decline in customer ratings, and which complaint themes increased during that
decline?*
_Avoid_: "which products got worse, and why" (claims deterioration and causation the data
cannot support)

**Category manager**:
The persona who asks the opening question: someone monitoring a catalogue who wants to
know which products' customer experience appears to be deteriorating and what customers
now complain about.
_Avoid_: shopper, trust team, analyst

**Complaint theme**:
An aspect of a product that reviews mention negatively (e.g. scent, leakage, irritation,
delivery). A shift in complaint themes is a hypothesis for investigation, never a diagnosed
cause.
_Avoid_: root cause, reason, "why"

**Decline candidate**:
A product that satisfies every pre-registered eligibility condition for a sustained rating
decline: enough reviews in both comparison periods, a meaningful decrease, persistence over
several months, a robustness check, and no explanation from a changed verified mix.
_Avoid_: declining product (a candidate is a statistical finding, not a verdict), outlier

**Protocol freeze**:
The point at which the decline rule's thresholds are committed to version control, after
seeing only aggregate distributions and before any candidate is selected. It limits analyst
discretion; it does not prove nobody saw product identities.
_Avoid_: pre-registration (overclaims), tuning

**Evaluation point**:
A calendar month-end at which a product's baseline window (the preceding B months) and
recent window (the following R months) are compared. A point is *evaluable* only if both
windows meet the minimum reviews and active months; an unevaluable point neither extends
nor confirms persistence.

**Calendar spine**:
The complete sequence of calendar months from a product's first to last review, including
months with zero reviews. Empty months carry a zero count and null statistics; time is
never compressed by skipping them.

**Robustness grade**:
The recorded outcome of the verified-only recompute for a decline candidate: *robust*
(holds in all and in verified-only reviews), *inconclusive* (visible overall, underpowered
verified-only), or *mix-explained* (reversed or explained by the verification mix).
A grade is reported, never used to hide a candidate.

**Sensitivity variant**:
A predeclared alternative to the primary decline rule (looser δ, shorter P, fixed cutoff)
whose candidate count is reported beside the primary. Variants are labelled *moderate
evidence* or *exploratory* and never replace the primary definition.
_Avoid_: relaxation ladder, fallback thresholds

**Matched control**:
A product observed over the same calendar windows as a decline candidate, similar in
baseline rating and volume, and stable under the locked decline rule. Controls anchor the
theme-shift comparison and the taxonomy.

**Theme shift**:
The change in a complaint theme's share between a candidate's baseline and recent windows,
minus the same change in its matched controls. Descriptive, not causal; report with counts
and intervals.
_Avoid_: difference-in-differences (unless pre-trends are tested), effect of, caused

**Alert**:
An evaluation point at which the locked decline rule's condition holds with the required
persistence. An alert is a monitoring signal, not proof that a product deteriorated.
_Avoid_: detection, finding (until narrated), significant decline

**Decline episode**:
The span on one product's spine from the first qualifying condition point, through the
alert trigger, until closed by recovery (K consecutive evaluable false points), by a gap of
more than G unevaluable months, or by end of data. A product may have several; the
narrative uses its first robust episode in the holdout.

**Holdout**:
Evaluation points from 2020-01-01 onward. Thresholds and sensitivity variants are developed
on points before it; the holdout is a historical backtest of the frozen rule.
_Avoid_: test set, prospective experiment

**Placebo trigger rate**:
How often the complete trigger rule fires per eligible product-year on pre-2020 placebo
series that preserve volume, seasonality and temporal dependence. Frozen with the
thresholds alongside a detection-power check on injected declines; it is the multiplicity
treatment for the monthly scan.
_Avoid_: false-alert rate (no ground truth of false alerts exists), false-positive rate

**Uncovered holdout content**:
Complaint themes that appear in post-2020 reviews but are absent from the frozen taxonomy.
Reported as an exploratory appendix; never added to the primary analysis retroactively.

**Text-characterisable**:
A decline candidate whose baseline and recent windows each hold enough non-empty review
text for theme classification, with a sufficient 20+-word subset for retrieval. The
condition that makes a candidate narratable before the aspect phase runs.

**Representative example**:
The review shown as evidence for a theme in a window, chosen deterministically by theme
presence, label confidence and typicality, not by helpfulness.
_Avoid_: most helpful review (shown separately, if at all), hand-picked example

**Investigate tier / Watchlist tier**:
The at most four highest-evidence new episodes per month go to *investigate*, the
category manager's declared capacity; further qualifying episodes are kept on the
*watchlist* without manual review. Capacity prioritises findings; it never suppresses them.

**Injected decline**:
A synthetic deterioration used to measure the alert rule's power on pre-2020 data: existing
5★ reviews relabelled (to 1–2★ for a complaint shift, to 3★ for satisfaction erosion) with
timestamps, volume and verification unchanged, as a step or a six-month ramp. Validates the
rating alert only, never the theme analysis.

**Computational gate**:
The pass/fail check that Spark and the independent pandas reproduction agree on cohort ids,
counts, and metrics within tolerance, with complete rejection reasons and deterministic
re-runs. Separate from the analytical outcome.

**Analytical outcome**:
What the locked rule found: candidates as a count and share of eligible products, their
robustness grades, and whether at least three robust, narratable candidates exist. Reported,
never passed or failed.

**Independent reproduction**:
A second implementation of the finding logic, written from the same specification but
sharing no transformation code with the Spark job, run in pandas over raw JSONL on a
deterministic cohort. Counts and selected product ids must agree exactly; floating-point
results within a tolerance.
_Avoid_: spot check, sanity check

**Fallback question**:
The question the presentation answers if no decline candidate survives: *which complaint
themes distinguish low-rated periods from high-rated periods among sufficiently reviewed
products?* Same gold table, same aspect pipeline.

### Complaint-theme labelling

**Complaint-theme labelling**:
The shared task of assigning each review the frozen, multi-label set of complaint themes it
mentions negatively. Positive praise is not labelled. Two implementations exist; both must emit
the same named-theme targets.
_Avoid_: aspect sentiment (the rubric's phrase, not ours), classification (ambiguous between the two implementations)

**LLM theme labeller**:
The primary implementation of complaint-theme labelling: a prompted language model applying
the frozen labelling spec. Its labels are the only ones that feed the theme-shift analysis.
_Avoid_: the model, the classifier, teacher

**MLlib theme classifier baseline**:
A Spark ML text classifier trained on LLM-labelled pre-2020 reviews and scored against the
same human audit sets as the labeller. A baseline and a scale path, never a source of primary
labels.
_Avoid_: student model, distilled model (no soft targets are transferred), MLlib model

**LLM-labelled training pool**:
A pre-2020 sample of reviews labelled by the frozen LLM theme labeller solely to train the
classifier baseline. Disjoint from the hand-labelled development and audit sets.
_Avoid_: training data (unqualified), silver labels

**Star-only theme baseline**:
A per-theme classifier whose only features are rating buckets, trained on the same
LLM-labelled training pool as the text classifier. The comparator that makes "text adds
signal beyond stars" a testable claim.
_Avoid_: star weak label (that is the separate 1–2★/4–5★ sentiment sanity check), naive baseline

**Other / Abstention / No predicted theme**:
Three distinct labelling outcomes. *Other*: a complaint exists outside the named taxonomy;
an LLM-only output and human-audit category that the classifier never learns or infers.
*Abstention*: the LLM labeller declines to label confidently. *No predicted theme*: no
named-theme classifier score cleared its threshold. Reported as separate coverage counts;
never compared as equivalents.
_Avoid_: unknown (ambiguous between other and abstention), empty label, classifier "other"

**Classifier score**:
The per-theme logistic output of the MLlib theme classifier baseline, used only for ranking
and the frozen per-theme threshold. Not calibrated and not comparable to the labeller's
label confidence.
_Avoid_: confidence, probability, calibrated probability

**Label source**:
The field on every theme label naming which implementation produced it. Primary analysis
requires the LLM source; classifier-sourced labels reach only the versioned prediction
table, the scale benchmark and, if the classifier passes its gate, the exploratory
prevalence chart.

**Insufficient-support theme**:
A named theme with fewer than the predeclared minimum of positive training examples
(about 30). Fitted and reported as exploratory; never removed, and the taxonomy is never
changed after classifier performance has been seen.
_Avoid_: dropped theme, failed theme

**Development set / Audit set**:
The pre-2020 hand-labelled reviews used to tune the labeller and the classifier are the
*development set*; the untouched post-2020 hand-labelled reviews opened once after the
freeze are the *audit set*. Development numbers are validation, never final performance.
_Avoid_: test set (for the development set), gold set (ambiguous)

**Model abstention / Parse failure**:
An abstention is a valid model decision to decline a label. A parse failure is a technical
failure after strict validation and one identical retry. They are stored and reported separately.
_Avoid_: treating an invalid response as abstention, silently repaired label

### Data quality

**Key collision**:
Two or more source rows sharing the same `(user_id, parent_asin, timestamp)`. A collision
is a fact about the source file; it does not by itself prove a duplicate review.
_Avoid_: duplicate (until classified), dupe

**Exact duplicate**:
A key collision whose rows also agree on every meaningful review field (rating, title,
text, verified flag, helpful votes). One row survives; the rest are removed and counted.

**Conflicting collision**:
A key collision whose rows differ in content or rating. Resolved by a deterministic
survivor rule; the non-survivors are kept in an auditable table, not dropped.

**Provenance result**:
The data-quality finding that key collisions originate in the source file, not in the
pipeline. Exactly-once delivery means each Kafka event lands once; it says nothing about
repetition inside the source.
_Avoid_: trust finding, incentivised reviews, review bursts (all removed from scope)

**Unverified review**:
A review whose `verified_purchase` flag is false. Unverified means unverified; it does not
mean false, incentivised, or untrustworthy.

### Search serving

**Review search index**:
The Elasticsearch index holding every deduplicated, validated silver review with non-empty
text, supporting BM25 retrieval. It is a serving projection of silver, not a store.
_Avoid_: search index (too generic), the ES data, the corpus

**Serving projection**:
A deterministically rebuildable Elasticsearch representation derived from an authoritative
Iceberg table or snapshot; never the system of record. Embeddings stored in Iceberg are
derived data, not a projection; a vector-enabled review index is one.
_Avoid_: mirror, copy, cache, primary index

**Mapping contract**:
The versioned Elasticsearch settings, mappings and analyzer definitions together with the
indexer-side required-field validation and the executable tests that enforce them. The
mapping alone is not the contract, because Elasticsearch does not enforce field presence.
_Avoid_: schema, the mapping (alone), dynamic mapping

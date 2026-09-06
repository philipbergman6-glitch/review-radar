---
id: RR-07
title: Does MLlib belong in the AI scope
type: grilling
status: closed
assignee: philip
closed: 2026-09-06
blocked-by: []
blocks: [RR-08, RR-12]
---

## Question

`docs/course-coverage.md` Finding 5 makes an argument the build-plan artifact never answers:

> An MLlib model scores under BOTH "course technologies" (20%) and "AI capability" (25%).
> A hosted-LLM call scores only under the latter. If the AI scope must be cut, cut toward
> MLlib, not away from it.

MLlib is taught in deck 3 (18 mentions, `SPARK MLLIB` section header). The artifact's settled
AI scope — embeddings, LLM aspect sentiment, RAG — contains no MLlib at all, and its decision
register never records why. Meanwhile the course-technologies row of the audit's grade table
projects 18/20 with "no HDFS" as the stated shortfall.

Decide:

1. Does MLlib enter the scope at all, and if so as what? The obvious candidate is the
   statistic the plan already wants for insights — per-product monthly rating z-score, or
   burst detection over trailing volume — implemented in MLlib rather than as hand-written
   Spark SQL. The audit is explicit that option (f) is worth having "only as a simple,
   explainable statistic that feeds an insight — not a model for its own sake".
2. If yes: does it *replace* any of the three AI capabilities, *add* to them, or *reframe*
   work that was going to happen anyway (the gold-layer drift calculation) so it also counts
   as a course technology?
3. If no: what is the one-sentence answer to "the course taught MLlib — why is there no
   model in your project?" It has to be better than silence, per the standing rule in
   `docs/course-coverage.md` ("a technology consciously declined with a stated reason
   demonstrates more command of the material than one included without understanding").

Weigh honestly against the 25% understanding criterion, which the audit says "punishes
breadth". Adding a fourth thing to a three-week solo build is a real cost; so is leaving a
double-scoring technology on the table.

The answer changes what the aspect-sentiment phase is for, so it blocks that ticket.

## Input from RR-09 (closed 2026-09-04)

Burst detection is **removed** from scope; do not propose it as the MLlib statistic. The
statistic that now exists is the decline alert rule (adjacent trailing windows on a calendar
spine, seeded clustered bootstrap, placebo calibration, injected-decline power check). If
MLlib enters, the honest framings are: the bootstrap/summary statistics via MLlib
`Statistics`, or an MLlib baseline classifier scored against the frozen LLM aspect labels
(RR-08/RR-17). Reframing the drift SQL as "MLlib" without a model would not survive Q&A.

## Grilling log

### Round 1 — 2026-09-04

Fact checked first `[observed]`: PySpark 3.5.3 `pyspark.ml.stat` = ChiSquareTest,
Correlation, KolmogorovSmirnovTest, Summarizer, MultivariateGaussian. **No bootstrap.** The
"MLlib Statistics" framing from RR-09 is withdrawn: it would be `groupBy().agg()` in a
costume. Deck 3 slide 65 says `spark.mllib` is in maintenance and `spark.ml` is primary, so
whatever enters is `spark.ml`.

Settled `[observed, Philip]`:

1. **MLlib enters as a `spark.ml` baseline classifier for complaint themes**, trained on
   LLM labels, scored on the same per-class table as the labeller (RR-17). An evaluation
   row and a scale path, not a fourth capability; the LLM stays primary. Rejected: out
   (leaves a double-scoring technology and a second stated gap in the 18/20 row); `ml.stat`
   utilities (no model, and a second statistic beside the frozen rule); replacing the LLM
   (needs the LLM's labels anyway).
2. **Cap 1 focused day.** Cut before RAG only if the LLM labels are late, since it cannot
   exist without them; otherwise it goes ahead of RAG polish. RR-12 inherits this rule.
3. **Declination sentence for a predictive model** (design doc): *"Our question is
   retrospective monitoring under a frozen rule, not prediction; the only supervised task
   with labels we trust is theme classification, so that is where the MLlib model sits, as a
   baseline and scale path for the LLM labeller."* A decline predictor tuned on pre-2020
   declines would be the search artefact ADR-0001 forbids.

### Round 2 — 2026-09-05

Settled `[observed, Philip]`:

4. **Terms** (in `CONTEXT.md`): *complaint-theme labelling* is the task; *LLM theme
   labeller* the primary; *MLlib theme classifier baseline* the Spark model. "LLM-labelled
   training data", not "distillation" — no soft targets are transferred.
5. **Leakage-safe split.** Taxonomy from pre-2020 discovery → labeller tuned on the
   pre-2020 hand-labelled *development set* → a separate pre-2020 *LLM-labelled training
   pool* for the classifier → classifier thresholds/hyperparameters tuned on the
   development set only → taxonomy, prompt, classifier pipeline and per-theme thresholds
   frozen together → LLM, classifier and star weak-label baseline scored once on the
   untouched post-2020 *audit set*. Pre-2020 numbers are validation, never final.
6. **Scale story is conditional.** Before measurement: "the fitted classifier provides an
   in-Spark path to full-corpus labelling." Promoted to a result only after a timed
   full-corpus run inside the one-day cap, reported as rows, wall time, Spark config, and
   the RR-03 comparison. Never promise "minutes" unmeasured.

### Round 3 — 2026-09-05

Settled `[observed, Philip; Q1/Q2/Q4/Q6/Q7 accepted as recommended, inferred from round 4
building on them]`:

7. **Pipeline**: `Tokenizer → StopWordsRemover → CountVectorizer (≈20k vocab, minDF 5) →
   IDF → one binary L2 LogisticRegression per theme`, text only. CountVectorizer over
   HashingTF so top coefficients per theme are a slide. Star rating is not a feature.
8. **Per-theme threshold** maximising F1 on the development set, frozen with the pipeline.
9. **Training pool**: new, pre-2020, disjoint from discovery, development and audit sets
   (the discovery sample was read while building the taxonomy). A fifth line in RR-08's
   LLM budget.
10. **Full-corpus output** goes to gold as its own table, versioned by model and freeze
    commit, **never joined into the theme-shift table**. Exploratory prevalence chart only.
11. **Narrative placement**: sub-row under complaint-theme labelling; three capabilities
    stay three.
12. **ADR-0002** written: `docs/adr/0002-mllib-as-theme-classifier-baseline.md`.

### Round 4 — 2026-09-06

Settled `[observed, Philip]`:

13. **Star baseline repaired.** A raw star weak label cannot name a theme. The comparator
    is a **star-only theme baseline**: per-theme classifier on rating buckets only, same
    training pool. Table = LLM theme labeller · text MLlib classifier · star-only
    classifier, all against human labels. The 1–2★/4–5★ weak-label agreement stays in
    RR-08 as a sentiment sanity check, not a theme predictor.
14. **Output contract.** `other` is a trainable binary target if the frozen LLM output
    labels it explicitly; no `unknown`/`abstain` class is trained. Empty classifier output
    = *no predicted in-taxonomy theme*, not abstention. Four coverage counts reported:
    named theme, other, LLM abstention, classifier empty. **Handoff to RR-08**: split
    `other/unknown` into `other` (out-of-taxonomy complaint) and abstention.
15. **Pool composition**: 2,000 representative core + up to 1,000 targeted candidates
    selected only from pre-2020 discovery artefacts (frozen theme terms), never audit data;
    `sampling_stratum` and `selection_reason` on every row. Support floor ≈ 30 positives;
    below it a theme is *insufficient-support / exploratory*, never removed.
16. **Model selection**: tiny shared grid (≈3 `regParam` values, L2), fixed-seed folds on
    the LLM-labelled pool, CountVectorizer and IDF fitted inside each fold; human
    development set used only for per-theme thresholds. No per-theme feature engineering,
    no elastic-net search.
17. **Pass rule, pre-registered**: *The MLlib text classifier passes if its post-2020
    macro-F1 across supported named themes is strictly greater than the star-only
    baseline's macro-F1 and is at least 70% of the LLM theme labeller's macro-F1 on the
    identical audit rows.* Per-theme support and F1 beside the macro; `other` excluded
    from the headline macro and reported separately; a named theme excluded only if
    predeclared insufficient-support before the audit was opened. Point estimates decide
    pass/fail; a seeded paired bootstrap interval on the two macro-F1 differences is
    context, never a second gate.

### Round 5 — 2026-09-06

Settled `[observed, Philip]`:

18. **Audit-side support floor**: ≥ 10 positive audit rows for a theme to enter the headline
    macro-F1, predeclared in the freeze commit, applied mechanically to all three systems.
    Below it: reported per theme with support, flagged.
19. **Paired bootstrap clustered by product**, seeded, same convention as the decline rule.
    Interval stated as pointwise.
20. **Benchmark corpus** = all reviews with non-empty text (~349k). One Spark job timed end
    to end: load silver → frozen pipeline → versioned gold table. Report rows, wall time,
    executor config, RR-03 comparison. Empty-text rows counted, excluded.

### Round 6 — 2026-09-06

Settled `[observed, Philip]`:

21. **Named themes only.** `other` is a heterogeneous residual; a binary model for it would
    learn generic complaint language. `other` stays an LLM-only output and human-audit
    category. Empty classifier output = `no_predicted_theme`; it never infers `other`.
    Keeps `other` out of macro-F1 and out of coefficient slides.
22. **Score, not confidence.** Enrichment shifts training prevalence and pseudo-label
    training establishes no calibration. The logistic output is a *classifier score*, used
    for ranking and the frozen threshold only. LLM `label_confidence` is a separate field
    with no claimed comparability. Calibration out of scope under the cap.
23. **Failed classifier and the chart.** Timed benchmark and versioned prediction table
    always permitted, carrying `evaluation_status`. Exploratory prevalence chart only if
    the classifier passes. Primary theme-shift job requires `label_source = "llm"`,
    enforced in code and tested; a table name is not structural protection.

## Answer

Closed 2026-09-06 after six grilling rounds (log above). Terms in `CONTEXT.md` under
*Complaint-theme labelling*; the decision in
`docs/adr/0002-mllib-as-theme-classifier-baseline.md`.

**1. MLlib enters**, as a `spark.ml` **theme classifier baseline** for complaint-theme
labelling: `Tokenizer → StopWordsRemover → CountVectorizer (≈20k, minDF 5) → IDF → one L2
LogisticRegression per named theme`, text only. Trained on a pre-2020 **LLM-labelled
training pool** (~3,000: 2,000 representative core + ≤1,000 targeted from frozen discovery
terms, `sampling_stratum` and `selection_reason` on every row), tiny shared `regParam` grid
with vectoriser fitted inside each fold, per-theme thresholds from the human development
set, all frozen with the taxonomy and prompt. The RR-09 "MLlib Statistics" framing is
withdrawn: `pyspark.ml.stat` in 3.5.3 has no bootstrap `[observed]`.

**2. It adds a row, replaces nothing, reframes nothing.** Sub-row under complaint-theme
labelling: *LLM theme labeller · text MLlib classifier · star-only theme baseline*, all
against human labels, on the untouched post-2020 audit set. Pre-registered pass rule:
*post-2020 macro-F1 across supported named themes strictly greater than the star-only
baseline's and ≥ 70% of the LLM labeller's, on identical audit rows.* Support floors: ≈30
training positives and ≥10 audit positives, predeclared, mechanical. `other` is LLM-only;
the classifier emits `no_predicted_theme`. Outputs are scores, not confidences. Classifier
labels never feed primary analysis (`label_source = "llm"` enforced and tested); the
full-corpus timed run on all non-empty-text reviews is a conditional scale result, and its
exploratory prevalence chart appears only if the gate passes.

**3. The sentence for "where is your model?"**: *"Our question is retrospective monitoring
under a frozen rule, not prediction; the only supervised task with labels we trust is theme
classification, so that is where the MLlib model sits, as a baseline and scale path for the
LLM labeller."*

**Budget**: 1 focused day once LLM labels exist; cut before RAG only if labels are late.

**Handoffs written into:** RR-08 (fifth budget line, pool composition, `other` vs
abstention split, `label_confidence` stays), RR-12 (cap and cut rule), RR-13 (the pass rule
is the printed gate), RR-16 (`pipeline_runs` carries model version, freeze commit, seed,
`regParam`, `evaluation_status`), RR-17 (three-system row shape, audit floor, clustered
paired bootstrap), RR-14 / `docs/course-coverage.md` Finding 5.

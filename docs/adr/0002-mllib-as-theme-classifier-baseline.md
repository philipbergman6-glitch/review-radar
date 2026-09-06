---
status: accepted
date: 2026-09-06
---

# MLlib enters as a theme classifier baseline trained on LLM labels, not as a predictor

The course teaches Spark MLlib, and an MLlib model scores under both "course technologies"
and "AI capability", yet the project's question is retrospective monitoring under a frozen
rule (ADR-0001), which leaves no supervised target we trust except complaint-theme
labelling. We therefore add one `spark.ml` text classifier (CountVectorizer → IDF → one L2
logistic regression per theme) trained on a pre-2020 pool labelled by the frozen LLM theme
labeller, and score it on the same human development and audit sets as the labeller, beside
a star-only per-theme baseline. It is an evaluation row and an in-Spark scale path, not a
fourth AI capability, and the LLM labeller remains the primary system.

## Considered options

- **No MLlib, declined with a sentence** — leaves a double-scoring technology untouched and
  a second stated gap in the course-technology row beside HDFS.
- **`pyspark.ml.stat` utilities on the rating windows** — the installed 3.5.3 module has no
  bootstrap; a Summarizer or KS test beside the frozen mean-difference rule is a second
  statistic in a costume, and not a model.
- **MLlib as the labeller, replacing the LLM** — needs the LLM's labels to train anyway and
  removes the depth the AI criterion grades.
- **A decline predictor** — tuned on pre-2020 declines it is exactly the search artefact
  ADR-0001 exists to prevent, and there are no future labels.

## Consequences

- Classifier labels never feed the theme-shift table or any primary result; they live in
  their own versioned gold table with an evaluation status. The primary job requires
  `label_source = "llm"` and a test enforces it; the exploratory prevalence chart is
  permitted only if the classifier passes its frozen gate.
- The classifier learns named themes only. `other` stays an LLM-only output, and the
  classifier emits `no_predicted_theme` rather than inferring it. Its outputs are scores,
  not calibrated probabilities.
- The training pool is a fifth LLM budget line and must be disjoint from the discovery
  sample, the development set and the audit set.
- Taxonomy, prompt, classifier pipeline and per-theme thresholds freeze together; the
  post-2020 audit set is opened once, for all three systems.
- Capped at one focused day; cut before RAG only if the LLM labels are late.

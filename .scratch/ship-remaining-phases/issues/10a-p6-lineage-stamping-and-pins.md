# 10a — P6's lineage links: unstamped snapshots and mis-resolved pins

**What to build:** whatever makes `LINEAGE_GATE` walk P6 and come back clean — or a written,
declared reason why one of its links cannot be walked, in the same vocabulary ADR-0011 uses
everywhere else.

**Why this exists as its own ticket.** It was found by landing ticket 10, and it is not
ticket 10's. Before P6 published `eval/themes/gate.json`, the lineage gate printed
`LINEAGE_PENDING capability=themes` and walked 34 links for `LINEAGE_GATE=PASS`. With the
artefacts in place it walks 54 and prints `LINEAGE_GATE=FAIL`. **Nothing broke.** The gate was
passing by omission, which is the exact silence the evaluation spine exists to make
impossible; the FAIL is the first time P6's links have actually been checked.

Two distinct defects, both real:

**1. The theme-label snapshots carry no `run_id` in their snapshot summary.**

    LINEAGE_OUTPUT run=e39584c3 job=theme_labels_llm       output=gold.review_theme_labels ... stamped=false snapshot_run=none ok=false
    LINEAGE_OUTPUT run=e1cee8de job=theme_labels_reference output=gold.review_theme_labels ... stamped=false snapshot_run=none ok=false
    LINEAGE_OUTPUT run=147d43ae job=theme_classifier_score output=gold.review_theme_labels ... stamped=false snapshot_run=none ok=false

ADR-0008 requires the run id stamped into every Iceberg snapshot, and every other writer does
it the same way — `src/spark/silver.py:316`, `src/spark/theme_samples.py:351,394`, all
`.writeTo(...).option("snapshot-property.run_id", run_id)`. `src/ai/theme_labels.merge_chunk`
cannot: it writes with `spark.sql("MERGE INTO ...")`, and the `writeTo` builder option has no
equivalent on the SQL path.

**Provenance is not actually lost**, and that matters for how this gets fixed: every row
carries `run_id` as a *column* (`src/ai/theme_labels.py:48`), so any row in the table can still
be attributed. What is missing is the snapshot-summary mechanism the lineage gate happens to
check. So the fix is a decision between two honest options, not a bug to squash:

- teach the merge writer to stamp (an Iceberg snapshot property on the SQL path, if one
  exists on 1.6.1), or
- teach the lineage gate that a MERGE-written table is attributed by column rather than by
  snapshot summary, and check *that* — with the check named, not skipped.

Re-running the writers to produce stamped snapshots is a third option and the worst one: the
pool run alone is hours, and ticket 09's boundary already says inference may repeat but
measurement may not.

**2. Three edges resolve to a different upstream run than the chain pins.**

    LINEAGE_EDGE downstream=theme_labels_llm/e39584c3       input=samples upstream=theme_samples/3265c771 ... pinned=false
    LINEAGE_EDGE downstream=theme_labels_reference/e1cee8de input=samples upstream=theme_samples/3265c771 ... pinned=false
    LINEAGE_EDGE downstream=theme_samples/aa4cccbf          input=gold    upstream=gold/4cd1fde7          ... pinned=false

The chain pins `theme_samples` by `latest_success` and gets `aa4cccbf`, but the labelling runs
consumed `3265c771`. Both are legitimate `theme_samples` runs — the job runs once per frame —
so `latest_success` is the wrong resolution rule for a job with several live outputs, not a
sign that anything was built on stale data. Same shape on the `theme_samples → gold` edge.

**Also visible, and probably its own small fix:** `LINEAGE_ORPHAN run=1dd07a73 job=gold
started=2026-09-07 10:39 status=running` — a gold run that never terminated, still `running`
four days later, plus `dirty_runs=5`.

**Blocked by:** nothing. It needs no new measurement and touches nothing frozen.

**Blocks:** the publication-mode lineage gate, and therefore the design doc's claim that every
capability traces back to the run that produced it.

**Status:** ready-for-agent

- [ ] `gold.review_theme_labels` is attributable by the mechanism the lineage gate checks —
      stamped, or checked by column with the check named
- [ ] `theme_samples` resolves per frame rather than by `latest_success`
- [ ] The orphaned `running` gold run is terminated or explained
- [ ] `make gate-lineage` prints `chain_clean=true`, or names each unwalkable link with a
      declared `cut_reason` rather than a `pending`

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

**Status:** done (2026-09-13)

- [x] `gold.review_theme_labels` is attributable by the mechanism the lineage gate checks —
      stamped, or checked by column with the check named
- [x] `theme_samples` resolves per frame rather than by `latest_success`
- [x] The orphaned `running` gold run is terminated or explained
- [x] `make gate-lineage` prints `chain_clean=true`, or names each unwalkable link with a
      declared `cut_reason` rather than a `pending`

**How it closed.** Re-stamping was never on the table: a snapshot summary is immutable, so even
teaching `merge_chunk` to stamp would leave the three existing snapshots unstamped, and
re-running is excluded. So the gate learned the second option, declared in
`conf/lineage_chain.toml` as `[[attribution]]`: a table several runs write into is attributed
by its `run_id` column, and the output line prints `attribution=column:run_id rows_written=200
rows_at_head=200` rather than hiding which check answered.

The declaration turned out to own a second thing as well — `current` cannot mean head-identity
on a shared table, where the next writer moves the head within the hour — so
`theme_sample_assignments` and `matched_controls` are declared too. Where the writer *can*
stamp the snapshot summary it still must: the column answers `current`, never `stamped`.

The pin rule became `upstream_pin = "recorded"` on three edges, each with a mandatory
`pin_reason` the gate prints: `theme_samples` draws one frame per run, so the frame a labelling
run read is the one it recorded. The recorded run is pinned and walked *beside* the published
one — the weaker pin check buys more checking, not less. The default stays strict.

The orphan closed through `make reconcile-run`, which records `failed` (never `success`), keeps
its counts null, and writes a note saying `finished_at` is the reconciliation time.

`LINEAGE_GATE=PASS chain_clean=true chain_links_checked=64` (was FAIL over 54).

**Left open, and printed rather than papered over:** `stale_outputs=4` still withholds
`publication_ready` — gold run `4cd1fde7`'s three tables, superseded by the `6e1c0d3a` re-run
that P6's frames predate, and the audit frame's `matched_controls` rows, overwritten by the
training-pool draw (`rows_at_head=0`). Both are true statements about the lake. Ticket 19's
publication-mode claim needs a decision on them: redraw, or declare the supersession.

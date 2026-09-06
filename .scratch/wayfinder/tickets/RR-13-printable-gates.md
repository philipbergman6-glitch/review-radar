---
id: RR-13
title: A printed number for every phase gate
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-01, RR-02, RR-06, RR-08, RR-09, RR-10, RR-12, RR-16, RR-17, RR-11]
blocks: [RR-14, RR-18]
---

## Question

The map's standing rule is that every phase gate prints a number. Several currently do not:

| phase | gate as written | is it a number? |
|---|---|---|
| Silver | `count(bronze) == count(silver) + count(rejects) + duplicates_removed` | yes — keep |
| Gold | "three insights written down with numbers" | **no** — a count of prose |
| ES + embeddings | "index doc count equals silver cohort count" plus "the tables exist and were read aloud once" | half |
| Aspects | "the evaluation table exists with real numbers" | **no** — existence, not a threshold |
| RAG | "20/20 answers cite retrieved reviews" | yes — keep |
| Stream | "a late event visibly lands in the correct window" | **no** — visibly is not a number |
| Deliverables | "a dry run to someone who has not seen the project" | **no** |
| Lineage (`RR-16`) | none yet | must be a row count or a snapshot-id match |
| Repo baseline (`RR-15`) | none yet | CI green on the commit; `pytest` count |

For each phase in the numbering fixed by `RR-01`, produce:

1. the **command** that prints the gate (a script path and its flags);
2. the **printed value**, named exactly;
3. the **pass threshold** — the value at which the phase is done, decided now rather than
   after seeing the output, which is the whole point of a gate;
4. what happens on failure — does the phase reopen, or does a documented deviation ship?

Learn from the audit's F3: the existing exactly-once gate prints `PASS` for runs that tested
nothing, because it never asserts `0 < partial < records`. **A gate that cannot fail is not a
gate** — every threshold here needs a case that would trip it.

The AI rows take their thresholds from `RR-17`; the silver row may be read from
`pipeline_runs` per `RR-16`. Blocked on every phase-content decision, because a threshold cannot be set before the phase
knows what it produces.

## Input from RR-09 (closed 2026-09-04)

Gold row is now two lines, not "three insights":

| phase | printed | pass |
|---|---|---|
| Gold — computational | `cohort_ids_match`, `counts_match`, `max_abs_metric_diff`, `rejection_reasons_complete`, `rerun_identical` from the pandas reproduction over raw JSONL | all true, diff ≤ tolerance stated in the freeze config |
| Gold — analytical (reported, not pass/fail) | `eligible_products`, `holdout_alerts`, `episodes`, `share_of_eligible`, robustness grade counts, `text_characterisable`, `fallback_triggered` (`< 3`) | none — an outcome, printed alongside the frozen config hash |

Plus a calibration line printed once at the freeze: `placebo_triggers_per_eligible_product_year`,
`detection_rate_0.3_step`, `median_delay_evaluable_points`, against the frozen ceiling
(≈ 1 trigger/month at the eligible count) and ≥ 80% / ≤ 6 points.

## Input from RR-07 (closed 2026-09-06)

- The classifier gate is the pre-registered pass rule in RR-07 item 17: one command prints
  the three-system table (LLM labeller · text classifier · star-only baseline) with
  per-theme support and F1, macro-F1, and PASS/FAIL. A test asserts the primary theme-shift
  job requires `label_source = "llm"`.

## Input from RR-08 (closed 2026-09-06)

The complaint-theme gate must print, from cached `gold.review_theme_labels` rows only:

1. the three-system per-theme table (LLM labeller · text MLlib · star-only) with support,
   P/R/F1, macro-F1 and **PASS/FAIL** against ADR-0003 (macro-F1 ≥ 0.70, min recall ≥ 0.50)
   and ADR-0002 (classifier rule);
2. coverage counts: named theme · `other` · abstention · `parse_failed` · `api_failed`;
3. the budget ledger: logical hosted calls vs 12,800, estimated spend vs $20, per budget line;
4. the sentiment weak-label table with Wilson intervals;
5. the disagreement-cause counts.

A test asserts the primary theme-shift job requires `label_source = "hosted_llm"`
(ADR-0003 value; ADR-0002 wrote `"llm"` — reconcile to one string in RR-14).

## Input from RR-01 (closed 2026-09-06)

Phase numbering to use: P2 Silver, P3 Gold, P4 Search, P5 Embeddings, P6 Themes, P7 RAG,
P8 Stream, plus the Deliverables track. **Search gate** is decided in substance
(`scripts/gate_search.py`, constituents in `RR-01` log item 10, final line
`SEARCH_GATE=PASS|FAIL`); this ticket freezes the exact printed names. Analyzer scores are
outcomes, not thresholds. **Deliverables track** acceptance replaces "a dry run to someone":
two consecutive complete rehearsals within the time limit, every move succeeds or uses its
documented fallback, playable backup within the limit, README/design doc/slides/runbook
present — turn each into a printed check. Alias-to-gold-snapshot match is a lineage gate
constituent (`RR-16`).

## Input from RR-06 (closed 2026-09-06)

**Embeddings gate** decided in substance (`scripts/gate_embeddings.py`, constituents in the
RR-06 answer item 10, final line `EMBED_GATE=PASS|FAIL`); this ticket freezes the printed
names. Gate = ID-set identities (silver cohort = active-spec Iceberg rows = vector-bearing ES
docs), vector validity, 20/20 queries × 3 retrievers complete, judgements complete through
rank 10, no error/OOM/restart. Reported outcomes: P@5/MRR@10 by stratum, hypotheses held or
not, disagreement distribution, ANN recall@10 vs exact, peak memory beside the 2 GB cap,
latency (descriptive). Nothing about relevance quality is a pass line.

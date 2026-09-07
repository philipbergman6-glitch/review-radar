---
id: RR-13
title: A printed number for every phase gate
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-10, RR-11, RR-12]
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

## Input from RR-17 (closed 2026-09-06)

- `RAG_GATE=PASS|FAIL` = the citation/scope contract only (30/30: every non-refused answer
  cites ≥ 1 retrieved-set review in correct scope; refusals carry no claims or citations).
  The four quality targets (grounded ≥ 16/20, adequate ≥ 14/20, abstention ≥ 8/10, false
  refusal ≤ 2/20) print PASS/FAIL each but do not fail the phase; they set status `built,
  evaluated, below target`.
- Every capability gate script writes `data/eval/<capability>.json` against
  `conf/eval-artifact.schema.json` (protocol hash, model/prompt identity, population identity,
  `pipeline_run_id`, status, cut reason). `make eval-table` renders the one-shape table and
  hard-fails on a missing or invalid artefact unless the capability is declared cut.
- Verdict vocabulary for every printed gate line: PASS / FAIL / REPORTED / NOT_RUN.

## Input from RR-02 (closed 2026-09-07)

Silver row is decided. Command `make silver` (→ `src/spark/silver.py --bronze-snapshot <id>`);
printed: `SILVER_REJECT_REASON` × 4 (zeros too, `timestamp_out_of_range` split into
`below_1995` / `after_ingest`), `SILVER_COLLISIONS groups exact_groups conflicting_groups
unresolvable_groups table_rows removed`, and one `SILVER_GATE … SILVER_GATE=PASS|FAIL` line.
Pass (full scope): `bronze_rows == reject_rows + silver_rows + collision_rows_removed` with
`removed = table_rows − groups + unresolvable_groups`; `count(distinct review_id) ==
silver_rows`; join cardinality preserved; `unmatched_review_rows == 0` and
`unmatched_parent_asins == 0`. Sample scope prints `gate_scope=sample`, unmatched threshold
not applied. Two further printed checks: loader gate (`rows_loaded == final_count`, key
unique) and `--verify-rerun` determinism (same snapshot + load twice → identical ids,
counts, classes, output digest). Tripping cases exist for each: an unresolvable group,
a dropped catalogue row, a nondeterministic tie-break.

## Input from RR-16 (closed 2026-09-07) — one blocker cleared

Lineage row: `scripts/gate_lineage.py --mode publication`; printed `LINEAGE_GATE=PASS|FAIL
gate_mode chain_clean publication_ready chain_links_checked=N`; pass = `PASS` with
`publication_ready=true` and `N > 0`. Silver's reconciliation identity is read from its
ledger row, not recomputed. Bronze's gate that can fail = the replay fixture test
(`records_replayed > 0`, `records_out == 0`, table unchanged) plus rejection of missing
attribution evidence. Every phase's gate gains the line "run contract registered" for the
jobs it adds. Cuts and `NOT_RUN` come from `conf/lineage_chain.toml`, shared with RR-17.

## Input from RR-11 (closed 2026-09-07) — second blocker cleared

The Deliverables row now has something to count. Stage = `notebooks/demo.ipynb` + Kibana
(ADR-0009); a **rehearsal** = an executed export under `docs/demo/<date>/` with every cell
succeeded and cell timestamps spanning ≤ 300 s. Candidate gate: a script that reads the
committed exports and prints `DEMO_GATE=PASS|FAIL rehearsals=N max_elapsed_s=…` with
threshold `rehearsals ≥ 2`. The exactly-once gate is no longer a live move, so its printed
line is a design-doc figure only. Only RR-10 and RR-12 still block.

---
id: RR-13
title: A printed number for every phase gate
type: grilling
status: closed
assignee: philip
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

## Input from RR-10 (closed 2026-09-07) — last blocker cleared

**Stream row** decided. Command `scripts/gate_stream.py --run <run_id>`; printed
`STREAM_LATE late_accepted late_dropped natural_dropped spark_dropped_by_watermark`,
`STREAM_RECON product_months_equal product_months_differ differ_explained_by_dropped`,
`STREAM_ALERTS alerts_stream alerts_batch alerts_touched_by_dropped`, and
`STREAM_GATE=PASS|FAIL run_kind=control|demo`. Two runs required: control (no injection) passes
on `natural_dropped == 0`, `product_months_differ == 0`, equal alerts; demo passes on
`late_accepted == |near|`, `late_dropped == |far| == spark_dropped_by_watermark`, every
differing product-month explained by dropped rows, alerts equal outside touched products.
Tripping cases: watermark delay 0 drops the near slice; the unsorted file makes
`natural_dropped > 0`; a diverged validation function breaks reconciliation. Expected slice
sizes are frozen in `conf/stream_replay.toml` before any run.

**Deliverables row** changes: move 2 is now "start paced replay + streaming query", a new
move 10 prints `STREAM_GATE` and the alerts table; the 701,528 → 711,528 claim is gone. The
rehearsal gate's ≤ 300 s must hold with the stream running in the background; if not, the
documented fallback (pre-run) applies and the gate counts that variant.

Every blocker of this ticket is now closed; it is on the frontier once RR-12 closes.

## Resolution (closed 2026-09-10, Philip took the recommendations)

**ADR-0011** — `docs/adr/0011-phase-gates-print-a-number-and-only-reproducibility-blocks.md`
carries the canonical table. The decisions:

1. **Failure policy (the ticket's item 4, previously unanswered anywhere).** Two classes.
   A gate's `PASS|FAIL` contains **only reproducibility claims** — counts that reconcile, ID
   sets that are identical, reruns that agree, artefacts that exist and validate; a FAIL
   reopens the phase. **Every quality target prints its own `verdict=PASS|FAIL` on its own
   line and never blocks**; a miss sets the capability's status to `built, evaluated, below
   target`. Rationale: reopening a phase over a quality miss means tuning against a held-out
   set, which ADR-0001, ADR-0003 and RR-21 all forbid — the two failures need opposite
   responses, so they cannot share a verdict.
2. **Scope.** A phase is complete only at `scope=full`. Sample runs print `gate_scope=sample`
   and render `NOT_RUN`, never PASS. Full scope infeasible before 2026-09-21 → the row ships
   `REPORTED scope=sample` with the reason: a declared deviation.
3. **Not reached vs cut.** One verdict, `NOT_RUN`, plus a mandatory `cut_reason` in
   `conf/lineage_chain.toml` (`"not reached by submission date"` is a legitimate reason).
   `make eval-table` hard-fails on an artefact missing *and* not declared cut — a capability
   is either a number or a written reason, never silence.
4. **Five of nine rows were already built** `[observed at 5f8f907]`: `SILVER_GATE`
   (`src/spark/silver.py:465`), `GOLD_GATE` (`src/spark/gold.py:339`), `SEARCH_GATE`
   (`scripts/gate_search.py:166`), `EMBED_GATE` (`scripts/gate_embeddings.py:189`),
   `THEMES_GATE` (`scripts/gate_themes.py:206`). This ticket cites RR-02 / RR-01 / RR-06 /
   RR-10 / RR-16 for their printed names rather than re-deciding them, and freezes only what
   nothing had decided.
5. **Newly frozen lines.** `RAG_GATE=PASS|FAIL` (30/30 citation and scope contract only) with
   `RAG_QUALITY grounded= adequate= abstention= false_refusal=` non-blocking;
   `DEMO_GATE=PASS|FAIL rehearsals=N max_elapsed_s= all_cells_ok= stream_running=
   backup_playable= docs_present=`, threshold `rehearsals ≥ 2`, `docs_present` a blocking
   four-file check; `GOLD_REPRO_GATE` (independent pandas reproduction, mirroring
   `SILVER_REPRO_GATE`) and `GOLD_CALIBRATION … verdict=PASS|FAIL`, which **blocks the
   protocol freeze** — a rule that fails calibration must not be frozen. Its thresholds stay
   the freeze's business, not this ticket's.
6. **Two universal constituents.** Every phase gate prints
   `run_contract_registered=true|false` for the jobs it adds (ADR-0008), blocking. Every
   capability gate writes `eval/<capability>/gate.json`, which must exist and validate.
7. **P6 corrections** (RR-08's gate had drifted from RR-19/21/23): the hosted budget ledger
   is withdrawn (the 12,800-call / $20 ceiling lapsed with the move to local `qwen3:8b`) and
   replaced by `THEMES_BUDGET labelled_reviews= elapsed_s= model=qwen3:8b api_mode=local`;
   the repeat-kappa rows stay `NOT_RUN` and the published agreement number is
   `THEMES_AGREEMENT … verdict=REPORTED` (the Philip-50, per-theme and overall kappa with
   Wilson intervals); the 0.70 macro-F1 and 0.50 minimum-recall bars move out of
   `THEMES_GATE` into `THEMES_QUALITY` — **the bars do not move**, they stop blocking P7.
8. **`label_source` enum reconciled here, not in RR-14.** RR-14 reconciles documents; this
   ticket owns what prints. Canonical set, one enum in `src/ai/labels.py`: `llm` (primary
   labels, whatever the host) · `agent_reference` (blind ground truth) · `classifier` (MLlib
   baseline). ADR-0003's `"hosted_llm"` and ADR-0002's `"llm"` collapse to `llm`; the primary
   theme-shift job asserts it. RR-14 propagates the string, it no longer decides it.
9. **Evaluation-artefact path corrected.** RR-17 / ADR-0006 froze
   `data/eval/<capability>.json`; the built layout is `eval/<capability>/` `[observed]`. The
   artefact is `eval/<capability>/gate.json` against `conf/eval-artifact.schema.json`, which
   does not exist yet and is P7's first deliverable. ADR-0006's contract is otherwise
   unchanged.
10. **The repo-baseline row is dropped.** RR-15 landed and CI runs `ruff check` + `pytest -q`
    on every push `[observed: .github/workflows/ci.yml:28,37]`. `make check` green is a
    standing invariant, not a phase completion check; restating it as a gate adds a line
    nothing consumes. `run_contract_registered` is the per-phase invariant that replaces it.

**Every constituent carries a tripping case** (ADR-0011's table, right-hand column) — the
audit's F3 rule: a gate that cannot fail is not a gate.

**Not decided here, deliberately:** the decline-rule thresholds and the calibration ceiling
(protocol freeze, `Not yet specified`); writing `gate_rag.py`, `gate_stream.py`,
`gate_demo.py`, `gate_lineage.py`, `reproduce_gold.py`, `conf/eval-artifact.schema.json` and
`conf/lineage_chain.toml` — execution, inherited by the P7/P8 implementation sessions.

Unblocks `RR-18` and (with RR-18) `RR-14`.

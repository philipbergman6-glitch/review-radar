---
label: wayfinder:map
title: Review Radar phases 2–8 — lock every open decision
created: 2026-09-01
---

# Review Radar phases 2–8 — lock every open decision

## Destination

Every `open · P2…P8` row in the [Review Radar Build Plan](https://claude.ai/code/artifact/958e44ac-2a9b-439b-a376-8563bbcec149)
decision register is settled with a stated rationale; the phase numbering is fixed so
Kibana and the explicit Elasticsearch mapping each have exactly one home; every phase
carries a gate that **prints a number**; every AI capability has an evaluation table with a
threshold set before it is measured; the repo meets the professional bar below; and the
register, `README.md` and `docs/course-coverage.md` say the same thing.

Reaching that point ends this map. Building silver, gold, the ES index and the AI layer is
**execution**, handed off to separate implementation sessions — it is not on this map.

## Notes

**Domain.** BIU 8688697201 Big Data and AI, solo final project. Rubric weights: data &
pipeline 25, course technologies 20, AI capability 25, results & insights 15, presentation
& demo 10, understanding & Q&A 5. Audit of 2026-09-01 scores the repo at 25/100 today,
~91 with the v3 roadmap executed.

**Tracker.** Local markdown. This file is the map; `tickets/*.md` are its child tickets.
Frontier = tickets with `status: open`, `assignee: unassigned`, and every id in
`blocked-by` already closed.

**Standing preferences for this effort**

- **Every phase gate must print a number.** A gate that reads "three insights written" or
  "a dry run went well" is not a gate. Each resolution that touches a gate names the
  command, the printed value, and the pass threshold.
- **Do not read the course PDFs.** They are gitignored and huge. `grep` `docs/course/*.txt`
  for verbatim quotes; `docs/course-coverage.md` already holds most of what is needed and
  records the method.
- **Cite, don't recall.** Quote the audit, the profile, or a deck line by file and line.
  Tag facts `[observed]` / `[inferred]` / `[assumed]`, as `docs/AUDIT_REPORT_2026-09-01.md`
  and `docs/DEMO_RUNBOOK.md` do.
- **Skills.** `/grilling` and `/domain-modeling` on every grilling ticket; `/research` for
  research tickets; `/prototype` where the question is "how should it look".
- **Time budget.** 4–6 focused hours/day. **Submission date confirmed 2026-09-21**
  `[observed 2026-09-04, Philip]`. Philip's instruction the same day: do not cut scope
  yet — he intends to put in a lot of work first. `RR-12` closed 2026-09-07: no contingency
  schedule, no cuts — build in phase order every day until the date.

**Professional bar (added 2026-09-04).** This project goes on Philip's CV, so the
destination includes the repo reading as a professional's work, not a student's. The
2026-09-04 review graded what exists **B-** — senior-quality foundation, no project yet —
and named the gaps `[observed]`: no CI, credentials hardcoded as defaults, `sys.path`
hacks in every entrypoint, a `pipeline_runs` table nothing writes, `print` for logging,
tests covering two pure functions. **Execution override:** the map carries exactly one
execution ticket, `Repo professional baseline — CI, secrets, packaging` (`RR-15`), because
it must be true before the next push; everything else stays decisions. Every implementation
session after this map inherits the bar: a test per layer, a printed gate, a `pipeline_runs`
row, secrets from `.env` only.

**LLM host.** Decided in `RR-08` (ADR-0003): Haiku 4.5 is the primary labeller; `llama3.2:3b` (Ollama 0.33.2, installed, `ollama serve` not left running) is a development/audit comparison row and one optional live demo move. Facts in `LLM access — what is actually provisioned` (`RR-03`).

**Phases and tracks (fixed in `RR-01`, 2026-09-06).** A *phase* is an ordered
implementation milestone with one composite completion gate; a *track* is an ongoing
workstream progressing alongside phases, completed by its own acceptance criteria. Names are
the durable references; numbers are ordering labels. Authoritative list: **P2 Silver → P3
Gold → P4 Search → P5 Embeddings → P6 Themes → P7 RAG (conditional) → P8 Stream
(conditional)**, plus the **Deliverables track** running throughout. "Conditional" means the
cut rule is `RR-12`'s to decide; the protected core is Silver → Gold → Search/Kibana → one
evaluated AI capability → numerical insights → polished deliverables. One line per phase in
the `RR-01` answer.

**Key inputs.** `docs/AUDIT_REPORT_2026-09-01.md` (§7 revised plan, §9 phase plan),
`docs/course-coverage.md` (Decisions section), `docs/DEMO_RUNBOOK.md`, README status table,
`docs/phase0-profile.txt`, and the build-plan artifact above.

## Decisions so far

<!-- one line per closed ticket; the detail lives in the ticket, never here -->

- [Reciprocal rank fusion on the Elasticsearch basic licence](tickets/RR-04-hybrid-fusion-rrf-licence.md) — RRF is Enterprise-only in 8.17 (`RRFRankPlugin.java` licence constant); live `bd-es` on basic returns HTTP 403 `security_exception` for both the `retriever.rrf` and legacy `rank.rrf` forms, no fallback. kNN, `retriever.standard/knn`, and `dense_vector` (≤4096 dims, `num_candidates` ≤10000, `index:true`) all work on basic. Fallback for RR-06: two ES calls fused client-side with `Σ 1/(60+rank)`, which is also the more explainable option. Findings: `docs/research/RR-04-rrf-licence-es817.md`.
- [MiniLM embedding throughput on this host](tickets/RR-05-embedding-throughput.md) — all-MiniLM-L6-v2 on the M5 CPU does ~374 reviews/s at 256 tokens (batch 64, 4 threads), ~906 reviews/s truncated to 64 tokens with 10 threads; peak RSS 0.7–2.0 GB. Supabase running costs only 1.5%, so the runbook's 2× contention figure does not apply to host-side torch. Cohorts: 123,510 reviews (≥20 words, 1,902 products) = 5.5 min; all 349,059 = 15.6 min. Both far under the one-hour bound, so RR-06 turns on index size and memory, not embedding time. Script `scripts/bench_embed.py`; findings `docs/research/RR-05-minilm-embedding-throughput.md`.
- [LLM access — what is actually provisioned](tickets/RR-03-llm-access-provisioning.md) — Hosted: `ANTHROPIC_API_KEY` declared but empty in `.env`; no other LLM key anywhere; `anthropic` SDK already a dependency. Local: Ollama 0.33.2 now installed, `llama3.2:3b` = 2.0 GB on disk / 2.5 GB RSS, ~54 tok/s generation on Metal, 3.8 s per review, unchanged with the bronze job running. Cost for the 5,000-review subset: hosted Haiku 4.5 ≈ $6 ($3 via Batch API); local ≈ 5.3 h serial. 700k reviews ≈ $859 hosted / 735 h local, so the full corpus is out for either host. Call path: plain HTTP to `/api/generate` from a driver-side batch script. Findings `docs/research/RR-03-llm-access-provisioning.md`. Decides nothing; feeds RR-08.
- [The question the presentation opens with](tickets/RR-09-opening-question.md) — *"Which products experienced a sustained decline in customer ratings, and which complaint themes increased during that decline?"*, asked by a category manager; committed now, not after gold. Alert rule = adjacent trailing calendar windows on a calendar spine with persistence; thresholds, episode closure, placebo-trigger ceiling (≈ 1/month at the eligible count) and injected-decline power target (≥ 80% on a 0.3★ step within 6 evaluable points) all developed on pre-2020 data and committed in one protocol freeze, then applied unchanged to a 2020-01-01 temporal holdout (ADR-0001). Gate split: computational (pandas over raw JSONL matches Spark, pass/fail) vs analytical outcome (reported). Fallback if < 3 robust text-characterisable candidates: within-product low- vs high-rated period theme contrast; no relaxation ladder. Trust thread demoted to a silver provenance result (key collisions classified, counted); burst detection and near-duplicate discovery removed. Taxonomy held out too (pre-2020 candidates + controls, frozen). Terms in `CONTEXT.md`. Handoffs in RR-02/07/08/10/13/17/18.
- [Repo professional baseline — CI, secrets, packaging](tickets/RR-15-repo-professional-baseline.md) — landed 2026-09-04, CI green on the first push ([run 33883548702](https://github.com/philipbergman6-glitch/review-radar/actions/runs/33883548702), 1m5s, no JDK). `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `PG_PASSWORD` are `_req()` in config and `${VAR:?}` in compose; no known-credential default anywhere. Repo installs editable via hatchling, all 11 `sys.path.insert` sites gone. `Makefile`: `up down health produce produce-sample bronze verify eos test lint check`; README points at it. 31 ruff findings cleared incidentally. Every later implementation session inherits: `make check` green before push.
- [Does MLlib belong in the AI scope](tickets/RR-07-mllib-in-ai-scope.md) — Yes, as a `spark.ml` **theme classifier baseline** (CountVectorizer → IDF → per-theme L2 logistic regression, text only) trained on a ~3,000-review pre-2020 LLM-labelled pool, scored on the untouched post-2020 audit set beside the LLM labeller and a star-only theme baseline. Pre-registered pass rule: macro-F1 > star-only and ≥ 70% of the LLM's. A sub-row under complaint-theme labelling, never a fourth capability, never a predictor; classifier labels never feed primary analysis (`label_source = "llm"` enforced). `pyspark.ml.stat` has no bootstrap, so the "MLlib Statistics" framing is withdrawn. Cap 1 day; cut before RAG only if LLM labels are late. ADR-0002; terms in `CONTEXT.md`.
- [Where Kibana and the explicit ES mapping live](tickets/RR-01-kibana-es-mapping-home.md) — Both live in a new **P4 Search** phase, split from **P5 Embeddings**; final list P2 Silver / P3 Gold / P4 Search / P5 Embeddings / P6 Themes / P7 RAG (conditional) / P8 Stream (conditional) + a Deliverables *track*. Kibana = compose profile `ui` (`make up-ui`, 8.17.0 pinned, repo-stored saved-object export, idempotent import) reading only the `product_month` alias. Mapping + analyzer = a *mapping contract* (`dynamic: strict`, indexer required-field validation, contract and analyzer tests), the first Search deliverable, undroppable because the gate cannot pass without it. Review search index = every validated silver row with non-empty text, deterministic ids. `product_month` = serving projection per gold snapshot, alias swap, `source_gold_snapshot_id` on every doc (ADR-0004). Stemmed vs `text.unstemmed` measured on a frozen 20-query blind-pooled set, reused for kNN/hybrid, not for RAG. Gate `scripts/gate_search.py` → `SEARCH_GATE=PASS|FAIL`. Terms in `CONTEXT.md`.
- [Aspect taxonomy and the model that produces it](tickets/RR-08-aspect-taxonomy-and-host.md) — Complaint-only, pre-2020-discovered taxonomy: 600-review seeded discovery sample, target 8/cap 10 supported themes, merge table committed; 200 development + 200 audit hand labels, 40 repeated after ≥5 days. Haiku 4.5 is primary; local 3B is a development/audit comparison and one cached-input demo move. Only title + text cross the wire. Pass = audit macro-F1 ≥0.70 and no supported-theme recall <0.50; ≤5 prompt versions. Hard ceiling 12,800 hosted calls / $20, Batch for frozen bulk. Strict JSON contract, one identical retry, cached Iceberg attempts and SHA-256 idempotency are in ADR-0003 and `docs/LLM_LABEL_RUNBOOK.md`.


- [Embedding scope and how the hybrid retriever fuses](tickets/RR-06-embedding-scope-and-fusion.md) — Vector cohort = every deduplicated silver review with ≥20 whitespace words in `text` (profile predicate frozen verbatim; raw ceiling 349,059, gate compares ID sets not counts); embedded input title + text, 256 tokens; `gold.review_embeddings` keyed by `(review, embedding_spec_hash)` where the hash covers identity settings only, never batch/threads; driver-side batched embedding. Fusion = unweighted client-side RRF (`Σ 1/(60+rank)`, window 50, ties by review id), `int8_hnsw` cosine. Two evaluation tables never merged: controlled (all three on the cohort) and production (unfiltered BM25, production hybrid). 20 queries = 10 lexical + 10 descriptive, binary, incremental blind pooling, pre-registered P@5 hypotheses reported not gated. Gate `scripts/gate_embeddings.py` → `EMBED_GATE=PASS|FAIL` on identities and completeness only; ANN recall@10 vs exact, memory, latency reported. ADR-0005; terms in `CONTEXT.md`.

- [An evaluation table for every AI capability](tickets/RR-17-evaluation-design-per-ai-capability.md) — Three capability summary rows (P5 search · P6 theme labelling with two sub-rows · P7 RAG) plus a separate theme-shift outcome table; columns `capability · phase · evaluation set · metric · value [interval] · threshold · verdict · evidence path`, verdicts PASS/FAIL (any frozen threshold, code- or human-derived) / REPORTED / NOT_RUN. RAG: 30 frozen questions (20 answerable, 10 unanswerable in three strata), answerability by evidence scan + manual validation never by retriever output, slots from the RR-09 decline ranking, structured answer keys with forbidden prevalence/direction claims, per-window retrieval, claim-level citations, Philip the sole judge, fixed-denominator integer thresholds (contract 30/30 gate; grounded ≥16/20, adequate ≥14/20 with refusal = failure; abstention ≥8/10; false refusal ≤2/20), seven-step disagreement precedence, own 200-call/$2 ledger. `make eval-table` renders per-capability JSON artefacts. Labelling ≈17–21 h → RR-12. ADR-0006; terms in `CONTEXT.md` *Evaluation*.

- [Silver dedupe rule and catalogue-join strategy](tickets/RR-02-silver-dedupe-and-join.md) — Silver = bounded batch pinned to a bronze snapshot, atomic replace of `reviews`/`rejects`/`review_collisions`; validate first (four reject reasons, fixed precedence, empty text not a reject), then group valid rows by `review_id = SHA-256(canonical(user, product, timestamp_ms))`. Classes exact / conflicting (same rating; survivor = helpful_vote ↓, text length ↓, canonical hash ↑, content-only) / unresolvable (rating disagreement, no survivor). Measured: 6,139 groups, 13,415 rows, 6,138 exact, 1 conflicting (helpful_vote), 0 rating conflicts → 7,276 rows removed. Catalogue: orjson → psycopg `COPY` → staging → transactional swap under a `catalogue_load_id`; silver reads it over Spark JDBC each run, left broadcast join, four denormalised columns, unmatched must be 0 at full scope. PostgreSQL is in the pipeline twice: Iceberg catalogue and enrichment source. Gate `SILVER_GATE=PASS|FAIL` on `bronze = rejects + silver + removed`, `removed = table_rows − groups + unresolvable`, distinct `review_id`, join cardinality; determinism by `--verify-rerun`. No `details` keys parsed. ADR-0007; terms in `CONTEXT.md` *Data quality*.

- [What pipeline_runs records, and what the demo shows from it](tickets/RR-16-pipeline-runs-lineage.md) — Keep, as the **run ledger**: one row per execution attempt of every job (`produce · catalogue_load · bronze_drain · silver · gold · search_index_reviews · search_index_product_month · embeddings · theme_labels_llm · theme_classifier_train · theme_classifier_score · rag_answers`), typed core + `inputs/outputs/counts/params` JSONB, UUID `run_id` generated before execution and stamped into every Iceberg snapshot, ES doc and eval artefact; insert `running` → finalize `success|failed`, partial outputs kept, never rolled back; downstream pins to one successful run's complete output set. Contracts keyed `(job_name, spec_version)` in `src/common/runs.py`. Bronze per invocation, snapshot-attributed counts, `records_in == records_out + records_replayed` with two mandated tests; Kafka header `producer_run_id`. Gate `scripts/gate_lineage.py --mode development|publication` prints provenance / completeness / freshness blocks and `LINEAGE_GATE=… chain_clean=… publication_ready=… chain_links_checked=N` from `conf/lineage_chain.toml` (shared with `make eval-table`). Demo move = ledger query + Spark SQL `VERSION AS OF` on silver's bronze snapshot. Migrations with checksummed ledger; sample and full never share a topic. ADR-0008; ADR-0006/0007 amended; terms in `CONTEXT.md` *Lineage*.

- [Demo surface — Streamlit, or a notebook plus Kibana](tickets/RR-11-demo-surface.md) — **Notebook + Kibana** (prototype: three variants, Philip took the recommendation). Stage = `notebooks/demo.ipynb`, one kernel with one Spark session / ES client / psycopg connection, cells calling `src/serving/` helpers only; Kibana carries exactly one move (decline candidates); `make health` runs in a real terminal first. Streamlit withdrawn (README updated). Nine live moves, 4:40 of 5:00: health → replay + incremental bronze → silver gate → lineage (ledger + `VERSION AS OF`) → Kibana decline view → hybrid decomposition → theme shift (cached) → eval table → RAG citations. Exactly-once (65 s) leaves the live list for the recorded backup and a design-doc figure; live `llama3.2:3b` is a Q&A reserve. Recorded backup = executed notebook exported to HTML + Kibana PNG + EOS transcript under `docs/demo/<date>/`; rehearsal = one export, all cells clean, ≤ 300 s; two required. ADR-0009; terms in `CONTEXT.md` *Demo*. Prototype on branch `prototype/rr-11`.

- [Replay pacing, time compression and the watermark](tickets/RR-10-time-compression-and-watermark.md) — Stream = a **reconciled projection beside batch**, own topic `reviews.stream`, one Structured Streaming job (shared silver validation → `withWatermark` 30 days → `dropDuplicatesWithinWatermark(review_id)`, exact for key collisions because the id carries the timestamp → append-mode product-month aggregates → `foreachBatch` frozen decline rule → `stream.alerts`). Whole sorted file (`sort_replay` job, SHA-256 in the ledger) at a constant record rate (3,500 rec/s, 5 s trigger, both in `conf/stream_replay.toml`), event-time clock printed; event-time-linear rejected. Lateness **injected**: two held-back slices from 2015+ (~0.5% each), near lag 7 d must be accepted, far lag 730 d must be dropped, expected counts known before the run; a control run must print zero natural drops. Gate `scripts/gate_stream.py` → `STREAM_LATE / STREAM_RECON / STREAM_ALERTS / STREAM_GATE=PASS|FAIL run_kind=control|demo`. Stage: background stream started at move 2 in the notebook's session, new move 10 prints the gate; pre-run is the fallback. Sample replay withdrawn from the live list (ADR-0008 conflict). P8 capped at 2 days, sort + pacing unconditional. ADR-0010; terms in `CONTEXT.md` *Streaming*.

- [Cut order and drop-dead dates under the three-week budget](tickets/RR-12-cut-order-and-drop-dead-dates.md) — **No contingency schedule.** Philip's call 2026-09-07: no drop-dead dates, no pre-committed cut order, work every day; build P2→P8 in the RR-01 order and ship whatever is reached by 2026-09-21, stating the rest as not built. "Conditional" = built if reached. Sort + pacing (1 h) kept; deliverables assembled alongside each phase, not ring-fenced. Arithmetic record (≈ 96 h demand vs 46–84 h supply) kept in the ticket, binds nothing. Implementation sessions start now with P2 Silver.

- [P6 labeller host under no hosted access, and who hand-labels](tickets/RR-19-p6-labeller-host-and-ground-truth.md) — Hosted Haiku is not provisioned (`ANTHROPIC_API_KEY` empty), so P6's primary labeller is local **`qwen3:8b`** via Ollama, chosen on a measured smoke test: `llama3.2:3b` emitted all 8 candidate themes on 5 of 8 reviews (degenerate) at 4.3 s/review, `qwen3:8b` produced discriminating labels at 6.7 s/review; concurrency does not help (6.08 s/review at 3 threads), so P6 plans serially and `qwen3:14b` is out on 16 GB. `llama3.2:3b` stays the comparison row and demo call; hosted Haiku becomes a stated `NOT_RUN` row. **Philip hand-labels both 200-row sets** (+40 repeats for kappa) — agent labels are not admissible as P6 ground truth, unlike the P4/P5 `judge=claude` compromise; the labelling tool ships before the taxonomy freezes. The 0.70 macro-F1 bar **does not move** under the weaker labeller: a miss is reported FAIL and the theme-shift table carries the caveat. Hosted call ceiling and $20 cap lapse, replaced by a time budget (~6.7 s/review; discovery 1.1 h, prompt dev 1.9 h, training pool 5.6 h, holdout inference ≤15 h, audit 0.4 h); `api_mode="local"`, `model_id="qwen3:8b"`, table schema unchanged. ADR-0003 amended.
- [The taxonomy cut to the ten-theme ceiling](tickets/RR-20-taxonomy-cut-to-the-ten-ceiling.md) — All **11** candidate themes cleared ADR-0003's support rule (≥3% of the 313 low-rated discovery reviews, ≥2 products), but the ceiling is ten and the rule cannot break a tie. **`breaks_or_wears_out` merged into `poor_build_quality`** — ten themes, nothing dropped; the alternatives (drop `arrived_damaged`, the only fulfilment theme and the one most likely to shift post-2020; or blur texture into scent) were rejected on stated grounds. Measured cost: `poor_build_quality` 14.4% → 21.4% (67 low-rated reviews, 64 products, 13 aspects) and durability stops being separable from cheap build. Coverage 219/313 = 70.0%; the 424-aspect long tail stays `other`, reported not folded in. Frozen as `conf/theme-taxonomy.json` v1; `scripts/score_taxonomy.py` prints `over_ceiling=no`.
- [Ground-truth owner reversed — agent labels, Philip adjudicates](tickets/RR-21-ground-truth-owner-reversed.md) — Supersedes RR-19's ground-truth clause only; the host decision stands. **The agent labels all 400; Philip hand-labels a stratified 50 of the audit set and the agreement is published as a number** (per-theme + overall, Cohen's kappa, Wilson interval), so the correlated-error risk is priced rather than caveated. Binding: labelling is blind (title/text/taxonomy only, never `qwen3:8b`'s output, stars, or product); provenance is `label_source="agent_reference"`, never `human`; the 50 are drawn by the seeded `draw_key` before Philip sees anything and never overwrite or re-tune; the 0.70 bar does not move; the limitation sits in the evaluation table's verdict column, not a footnote. **The 40 repeat-kappa rows become `NOT_RUN`** — intra-annotator stability is undefined for a deterministic labeller, and is not replaced with a number that merely looks like one.
- [How the development, audit and training-pool frames are drawn](tickets/RR-22-development-audit-frames.md) — ADR-0003's sampling clause was not executable as written. **Theme terms become a derived frozen artifact** `conf/theme-terms.json` v1 (`scripts/freeze_theme_terms.py`): whole-word uni/bigrams mined from the discovery *evidence quotes* of each theme's merged aspects, kept on ≥2 distinct reviews and a fixed theme-vs-rest odds ratio, each term owned by one theme. It never enters a prompt and never touches a label — it only decides which reviews are *offered*, so it is not an `RR-21` blindness breach. **The audit population is the pre-2020 candidate and control products observed at/after `holdout_start`** — post-2020 *windows* cannot exist while `conf/decline_rule.toml` is provisional (`holdout_eligible_products=0`), and a rule-selected window would not be untouched. Quotas: development 200 = 20/theme; audit 120 = 12/theme + 80 prevalence rows scored separately; training pool 3,000 prevalence; filled rarest-theme-first by seeded `draw_key`, shortfalls released to a final pass and printed, never topped up. **No rating stratification outside `discovery`.** `inference` stays `NotImplementedError`, blocked on the protocol freeze.
- [What the star-only baseline predicts, and when the two baselines' thresholds freeze](tickets/RR-23-baseline-definitions-and-thresholds.md) — ADR-0002 names a "star-only per-theme baseline" without a rule, and "thresholds freeze together" without saying what fits them. **Star-only = per-theme star threshold** `predict t iff rating <= k_t`, `k_t` chosen from {1..5} to maximise that theme's F1 **on development** against `agent_reference`; a theme whose best F1 is 0 everywhere gets `k_t = 0` and predicts nothing, reported not smoothed. **Both baselines' thresholds are fitted on development, frozen, applied unchanged to the audit set**, which is opened once for all three systems. The classifier trains on the 3,000-row pool labelled by the *frozen* prompt; `parse_failed`/`api_failed` pool rows are dropped from **training** (a row with no label is not a row with no themes) and the count reported — but on the **evaluation** sets a failure still scores as an empty prediction for every system. Classifier rows carry `label_source="classifier"` so they can never be aggregated into the primary labels.

- [A printed number for every phase gate](tickets/RR-13-printable-gates.md) — **Only reproducibility blocks.** A gate's `PASS|FAIL` carries counts that reconcile, ID sets that match, reruns that agree, artefacts that validate; a FAIL reopens the phase. Every quality target prints its own `verdict=PASS|FAIL` on its own line and never blocks — a miss sets status `built, evaluated, below target`, because reopening on a quality miss means tuning against a held-out set (ADR-0001/0003/RR-21 forbid it). Phase complete only at `scope=full`; sample renders `NOT_RUN`. Never-reached and cut-on-merits share `NOT_RUN` plus a mandatory `cut_reason` in `conf/lineage_chain.toml`; `make eval-table` hard-fails on an artefact missing *and* not declared cut. Five rows were already built and are cited, not re-decided; newly frozen are `RAG_GATE` + `RAG_QUALITY`, `DEMO_GATE … rehearsals≥2 docs_present=`, `GOLD_REPRO_GATE`, and `GOLD_CALIBRATION` which **blocks the protocol freeze**. Universal constituents: `run_contract_registered=` on every phase gate, `eval/<capability>/gate.json` on every capability gate. P6 corrected — hosted budget ledger withdrawn for `THEMES_BUDGET`, repeat-kappa stays `NOT_RUN` with `THEMES_AGREEMENT` reported instead, and the 0.70/0.50 bars move out of `THEMES_GATE` into `THEMES_QUALITY` **without moving**. `label_source` enum settled here: `llm` · `agent_reference` · `classifier`. ADR-0006's artefact path corrected to `eval/<cap>/gate.json`. Repo-baseline row dropped — CI is not a phase gate. ADR-0011.

## Not yet specified

- *(Design-doc contents and slide order graduated 2026-09-04 into `Design doc sections and
  slide narrative order` (`RR-18`).)*
- *(Burst detection threshold removed 2026-09-04 — `gold.bursts` is out of scope, `RR-09`.)*
- **Decline-rule thresholds (B, R, δ, P, G, K, minimum counts, placebo ceiling).** Set at
  the protocol freeze from pre-2020 aggregates only, per `RR-09` / ADR-0001; deliberately
  *not* a map decision. **Now also includes the low-/high-rated fallback-period
  rule** (minimum counts, selection, tie-break), which ADR-0001 does not yet define and which
  RAG's fallback questions inherit unchanged (`RR-17`). **`RR-13` adds a constraint, not a
  number:** the freeze must print `GOLD_CALIBRATION … verdict=PASS|FAIL` against the ceiling
  and the ≥ 80% / ≤ 6-point targets, and a rule that fails calibration **must not be frozen**.
- **Which gold population `product_month` projects.** `RR-01` says: only what P3
  materialises, never all 112,590 products across their lifetimes. What P3 materialises is
  the protocol freeze's minimum-count business (`RR-09`), so this sharpens with it.
- *(`details` keys: closed 2026-09-07 by `RR-02` — silver parses none; they stay in the raw
  metadata JSONL until a consumer exists.)*
- *(RAG retrieval depth, sparse-vector fallback and the evaluation set graduated 2026-09-06
  into `RR-17` / ADR-0006 — closed.)*
- *(Iceberg time travel as a demo move: graduated 2026-09-07 into `RR-16`'s answer — folded
  into the lineage move; placed as move 4 by `RR-11`.)*
- *(Recorded-backup format and rehearsal logistics: closed 2026-09-07 by `RR-11` — executed
  notebook export, two rehearsals ≤ 300 s; the counting gate is `RR-13`'s.)*
- **Structured logging and observability.** `print` everywhere today. The metrics-row half
  is now answered by the run ledger (`RR-16`); whether the pipeline also gets a logger or
  Spark UI screenshots waits until silver exists.
- **Integration test strategy.** One test per layer that runs against the compose stack —
  which fixture, how CI gets a JDK and Docker. The CI skeleton exists (`RR-15`, no JDK, the
  workflow comment says what to add); sharpens once silver has something to test.

## Ticket index (updated 2026-09-10, RR-13 closed)

Closed: `RR-01`, `RR-02`, `RR-03`, `RR-04`, `RR-05`, `RR-06`, `RR-07`, `RR-08`, `RR-09`,
`RR-10`, `RR-11`, `RR-12`, `RR-13`, `RR-15`, `RR-16`, `RR-17`, `RR-19`, `RR-20`, `RR-21`,
`RR-22`, `RR-23`. Frontier — open, unblocked: `RR-18`.
Blocked: `RR-14` (on RR-18).
Philip's instruction 2026-09-07: implementation starts now (P2 Silver); the three remaining
map tickets are worked alongside, not before.

## Out of scope

- **Kafka Connect Elasticsearch sink** — a genuine 43-mention coverage hole
  (`docs/course-coverage.md` Addition A), declined on the three-week budget rather than on
  the merits. The deck's own line — `The Kafka project does not itself develop any actual
  connectors … Except for a trivial "file" connector` — covers declining a Connect
  *source*, not a *sink*, so the design doc must say out loud that the sink was dropped for
  time. An acknowledged gap reads as judgement; an unmentioned one reads as ignorance.
- **Single-node HDFS demo step** — taught hands-on across 41 mentions with live shell
  transcripts, and still declined: Colima has 8 GB shared with Kafka, Elasticsearch,
  PostgreSQL and MinIO, and Iceberg needs S3-compatible storage regardless. Concede
  explicitly that HDFS and MinIO are not equivalent (block replication and rack awareness
  vs a flat key space with no atomic rename) — `docs/course-coverage.md` §5.
- **The 23M-review `Beauty_and_Personal_Care` run** — days of laptop CPU for a scale story
  that per-product skew (median 2 reviews, max 1,962) already tells.
- **Streaming AI enrichment as its own build (option e)** — torch inside Spark Python
  workers on 16 GB. Satisfied incidentally if embeddings ever run in a `foreachBatch`.
- **Automated narrative generation (option g) and NL→ES DSL (option d)** — deferred in the
  artifact; under three weeks they are out.
- **GraphFrames / GraphX**, **Oozie / Sqoop / Pig** — declined with arguments already
  written in `docs/course-coverage.md` §6–7. Sqoop has the course's own citation
  (`Apache Sqoop moved into the Attic in June 2021`); Oozie's cut is an opinion and must be
  owned as one.
- **Review-burst detection, near-duplicate review discovery, and any "which reviews should
  you not trust" framing** — removed by `RR-09` (2026-09-04), not deferred. The data has no
  ground truth for incentivised reviews, the measurements were never made, and a second
  insight thread would split a three-week budget. Key collisions survive only as a silver
  provenance result.
- **Implementing phases 2–8** — execution, handed off once this map closes.

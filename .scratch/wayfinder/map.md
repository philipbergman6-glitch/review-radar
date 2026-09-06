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
  yet — he intends to put in a lot of work first. `RR-12` takes the date as its input but
  should be grilled late, not now.

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
- [Aspect taxonomy and the model that produces it](tickets/RR-08-aspect-taxonomy-and-host.md) — Complaint-only, pre-2020-discovered taxonomy: 600-review seeded discovery sample, target 8/cap 10 supported themes, merge table committed; 200 development + 200 audit hand labels, 40 repeated after ≥5 days. Haiku 4.5 is primary; local 3B is a development/audit comparison and one cached-input demo move. Only title + text cross the wire. Pass = audit macro-F1 ≥0.70 and no supported-theme recall <0.50; ≤5 prompt versions. Hard ceiling 12,800 hosted calls / $20, Batch for frozen bulk. Strict JSON contract, one identical retry, cached Iceberg attempts and SHA-256 idempotency are in ADR-0003 and `docs/LLM_LABEL_RUNBOOK.md`.

## Not yet specified

- *(Design-doc contents and slide order graduated 2026-09-04 into `Design doc sections and
  slide narrative order` (`RR-18`).)*
- *(Burst detection threshold removed 2026-09-04 — `gold.bursts` is out of scope, `RR-09`.)*
- **Decline-rule thresholds (B, R, δ, P, G, K, minimum counts, placebo ceiling).** Set at
  the protocol freeze from pre-2020 aggregates only, per `RR-09` / ADR-0001; deliberately
  *not* a map decision.
- **Which `details` keys silver parses on read.** May graduate out of the silver dedupe and
  join ticket, or may only sharpen once the products load has run.
- **RAG retrieval depth and question scope.** Product-scoped vs corpus-wide, and `k`.
  Hangs on what the ES index actually contains after the embedding-scope decision.
- **Iceberg time travel as a demo move.** Deferred-but-cheap in the artifact; belongs to
  the demo-moves list, which does not exist yet.
- **Recorded-backup format and rehearsal logistics.** Follows the demo surface.
- **Structured logging and observability.** `print` everywhere today. Whether the pipeline
  gets a logger, Spark UI screenshots, or a metrics row is not worth deciding until silver
  exists; noted so it is not forgotten at the professional bar.
- **Integration test strategy.** One test per layer that runs against the compose stack —
  which fixture, how CI gets a JDK and Docker. The CI skeleton exists (`RR-15`, no JDK, the
  workflow comment says what to add); sharpens once silver has something to test.

## Ticket index (updated 2026-09-06)

Closed: `RR-03`, `RR-04`, `RR-05`, `RR-07`, `RR-08`, `RR-09`, `RR-15`. Frontier — open,
unblocked: `RR-01`, `RR-02`, `RR-10`, `RR-12`. Blocked: `RR-06` (only on `RR-01`), `RR-11`,
`RR-13`, `RR-14`, `RR-16`, `RR-17`, `RR-18`.
Suggested order: grill `RR-01`, `RR-06`, `RR-17`, `RR-12`, `RR-10`, `RR-02`.

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

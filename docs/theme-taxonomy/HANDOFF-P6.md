# Handoff — resume P6 Themes at commit 6/6

Continue **P6 Themes** on Review Radar (BIU Big Data final project, submission 2026-09-21).
HEAD is `507b44c`; commits 1–5 of the `P6 Themes (n/6)` series are done. Work in the frozen
decisions below — they are settled, not open for redesign. Any *newly surfaced* decision is
resolved as a wayfinder ticket in `.scratch/wayfinder/` first (`RR-22` is the next free id),
with a one-line index entry on `.scratch/wayfinder/map.md`, before you build on it.

Read first: `docs/adr/0003-frozen-complaint-theme-labelling.md` including all three
amendments, `.scratch/wayfinder/tickets/RR-19`, `RR-20`, `RR-21`, and
`docs/theme-taxonomy/README.md`.

## What is already built

- `conf/theme-taxonomy.json` **v1, frozen** — ten themes with definitions, includes, excludes
  and boundary notes. Do not add, rename, or re-scope a theme.
- `conf/theme-label-spec.json` — `qwen3:8b` via Ollama, `api_mode=local`, temperature 0,
  seed 20260907, `think:false`. `llama3.2:3b` is the comparison model.
- `conf/theme_sampling.toml` — frozen sampling protocol; `[development]` 200,
  `[audit]` 200 (120 enriched + 80 representative), `[training_pool]` 3000,
  `[inference]` max 8000.
- `src/ai/labels.py` — spec loading, config hash, idempotency key, evidence validation.
  `LABEL_SOURCES` includes `agent_reference`.
- `src/ai/ollama.py` — schema-constrained generate, one identical retry, terminal statuses.
- `src/ai/discover_phrases.py` — the discovery job (done, run `a4a62ffd`).
- `src/spark/theme_samples.py` — draws samples; **only `--sample discovery` is implemented.**
  `run_theme_samples` raises `NotImplementedError` at line ~151 for the other four because
  they needed the frozen theme terms. The terms are now frozen, so **unblocking those frames
  is your first coding task.**
- `scripts/propose_taxonomy.py`, `scripts/score_taxonomy.py`, `scripts/discovery_failures.py`
  and their `make` targets. 179 tests pass; `make check` is lint + tests.

## What to build, in order

1. **Draw the development and audit frames** — implement the remaining branches of
   `src/spark/theme_samples.py`. The audit frame's 120 enriched rows use the frozen theme
   terms; the 80 representative rows are prevalence-drawn and **reported separately**.
   Disjointness against existing assignments is already enforced by a left-anti join — keep it.
2. **A blind labelling job** writing `gold.review_theme_labels` (schema in ADR-0003) with
   `label_source="agent_reference"`. See the blindness rules below — they are binding.
3. **Label the 200 development reviews**, then run **at most five** prompt versions against
   them. Commit every version and its per-theme development table. **Freeze the selected
   prompt before the audit set is opened**, in its own commit.
4. **Label the 200 audit reviews** with the frozen prompt and score: macro-F1 over supported
   themes (>= 10 audit positives), per-theme precision/recall/F1/support, development
   macro-F1, `other`, abstention, parse/API failure coverage, and a seeded product-clustered
   bootstrap interval as context.
5. **Adjudicate every disagreement once**, without changing labels or spec. The five allowed
   causes are `model_missed`, `model_invented`, `definition_boundary`, `human_error`,
   `star_misleading`. One row per disagreement, cause counts, five worked examples including
   at least one misleading-star case where the model was right.
6. **The comparison and baseline rows**: `llama3.2:3b` on the same audit set, an MLlib theme
   classifier trained on the 3,000-review pool (ADR-0002), and a star-only baseline.
7. **The sentiment weak-label check** exactly as ADR-0003 specifies (exclude 3-star; Wilson
   intervals; never revise text labels to agree with stars).
8. **`scripts/gate_themes.py`** printing `THEMES_GATE=PASS|FAIL` with the numbers, plus
   README status, the evaluation-table row, and a decisions doc.

## Binding rules — do not relax any of these

- **Blindness (RR-21).** The labelling job sees only review `title`, `text`, and the frozen
  taxonomy. Never `qwen3:8b`'s output, the star rating, the product, the window, or its own
  earlier labels for the same review. A run with access to the system under test is void and
  must be discarded, not patched.
- **Provenance.** Agent ground truth is `label_source="agent_reference"`. `human` is reserved
  for Philip. Never present agent labels as human labels in any table, chart, or sentence.
- **Philip's adjudication set.** 50 audit reviews, stratified over the ten themes plus
  `other`, drawn with the seeded `draw_key` **before** he sees any agent label. He labels
  blind. His labels never overwrite the agent's and never re-tune anything. Report per-theme
  and overall agreement, Cohen's kappa, and a Wilson 95% interval **in the evaluation table's
  verdict column, not a footnote**.
- **The pass rule does not move.** `audit macro-F1 >= 0.70` and `no supported-theme recall
  < 0.50`, frozen before any measurement. A miss is reported `FAIL` with the per-theme table
  and the theme-shift table inherits the caveat. Never re-tune a threshold after seeing a
  result.
- **The 40 repeat rows are `NOT_RUN`**, reason "intra-annotator stability is not defined for a
  deterministic labeller". Do not substitute a number that resembles the original.
- **Never** strip code fences, repair JSON, coerce a value, or manufacture a quote. An invalid
  response gets one identical retry, then `parse_failed` with both raw responses stored.
- **Every job writes a run-ledger row** (ADR-0008). Contracts live in `src/common/runs.py` and
  `job_name` is guarded by a Postgres CHECK — a new job name needs a migration in
  `conf/postgres-migrations/` **and** the same name folded into `conf/postgres-init/01_schema.sql`
  with the baseline bumped, or `tests/test_runs_contracts.py` fails.
- **Aggregations filter on `inference_config_hash`** so rows from a superseded prompt or limit
  never mix into a count.

## Measured facts worth not rediscovering

- Discovery ran 600 reviews: `ok=484 parse_failed=116 (19.3%) api_failed=0`, 897 phrases,
  499 aspects. All failures were semantic validation, none transport: `quote_too_long` 82,
  `quote_not_in_review` 26, `aspect_too_long` 20.
- **The one identical retry is a measured no-op for validation failures** — 114 of 116 retried
  to a byte-identical error, because temperature 0 with a fixed seed is deterministic. It is
  kept because it is the frozen protocol and still real for `api_failed`. Budget the doubled
  inference cost; do not "fix" it.
- **Throughput degrades badly over a long serial run**: 2.7 s/review for the first 500,
  ~30 s/review for the last 100 (18,538 s total). The RR-19 budget of 6.7 s/review came from
  an 8-review smoke test and under-plans everything. Run long jobs in the background with
  `nohup ... &` and a log file, and plan the training pool (3,000) and holdout inference
  (<= 8,000) against the tail rate, not the smoke rate.
- Quote word limit is **12**; an earlier 8-word limit enforced as a JSON-schema `pattern` made
  `qwen3:8b` paraphrase the review to fit, which the evidence check then correctly rejected as
  invented. Do not re-add a length pattern to the schema.
- Taxonomy coverage: at least one theme covers 219 of 313 low-rated discovery reviews (70.0%);
  the 424-aspect long tail stays `other` and is reported, not folded in.

## Environment

- `./run.sh <cmd>` sets JDK 17 + the venv; `make check` = lint + tests; `make help` lists targets.
- Colima/docker stack is up (`bd-postgres bd-kafka bd-minio bd-es bd-kibana`); `make up` if not.
- `ollama serve` must be running on `localhost:11434` with `qwen3:8b` and `llama3.2:3b` pulled.
- **Reading `.env`, keys or credentials through Bash is blocked by a guardrail hook. This is
  intentional — do not attempt it.** There is no hosted LLM access; `ANTHROPIC_API_KEY` is
  empty and hosted Haiku is a stated `NOT_RUN` row.
- macOS has no `timeout` binary; use the Bash tool's own timeout parameter.

## Known blocker, outside P6's critical path

`conf/decline_rule.toml` is still `status = "provisional"`. The **protocol freeze** — setting
B, R, δ, P, G, K and the minimum counts from pre-2020 aggregates only, after calibrating the
placebo trigger rate and injected-decline power — has not happened. It blocks the post-2020
holdout inference and all of P7, but **not** steps 1–8 above, which are pre-2020 work plus the
untouched post-2020 audit set. Run the calibration and bring Philip the numbers; the threshold
values are his call, and per ADR-0001 they are deliberately not a wayfinder map decision.

## Working style

Philip wants: gates that print a number; thresholds frozen before measurement and never
re-tuned; failures characterised by named cause rather than as one opaque rate; limitations
stated in the artifact rather than a footnote; no contingency planning, cut orders or hour
budgets — build in phase order every day. Be concise. Commit at each checkpoint; context can
compact without warning.

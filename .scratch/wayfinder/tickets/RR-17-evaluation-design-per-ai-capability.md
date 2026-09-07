---
id: RR-17
title: An evaluation table for every AI capability
type: grilling
status: closed
assignee: philip
closed: 2026-09-06
blocked-by: [RR-06, RR-08]
blocks: [RR-13, RR-14]
---

## Question

AI capability is 25% and the rubric's words are "correctness, depth, and your understanding
of it". The brief (§10) adds "validate AI-generated content; LLMs can produce confident but
wrong answers". An AI feature without a measured evaluation is a demo; with one it is
engineering, and it is the difference between a CV line and a class project. `RR-08` already
fixes the aspect-sentiment validation. This ticket does the same for the other two, and makes
the three tables one shape.

For **each** of embeddings/semantic search (`RR-06`), aspect sentiment (`RR-08`) and RAG,
decide:

1. **The gold set.** How many items, how sampled (stratified by rating and by product), and
   who labels. Options for labelling: by hand; hosted Haiku as the labelling oracle with a
   hand-checked subset; weak labels from stars. Hand-labelling is the one task that cannot
   be sped up, so the count is a schedule decision as well (`RR-12`).
2. **The metric, per class, never accuracy.** 60.0% of reviews are 5★ — a constant
   "positive" scores 60%. Candidates: precision/recall/F1 per aspect-sentiment class;
   recall@k and MRR for semantic search against a set of (query → relevant reviews) pairs;
   for RAG, citation rate (every answer cites retrieved reviews) plus faithfulness judged
   against the cited text, plus a "no answer" rate on questions the corpus cannot answer.
3. **The threshold**, set now. Below it the capability ships as "built, evaluated, below
   target" with the number shown — never quietly.
4. **The disagreements.** What is reported about the rows the model got wrong. The audit's
   red-team question is "what do the disagreements look like?" — some will be the model
   being right about a misleading star rating, and saying so is the understanding the
   rubric grades.
5. **One table shape** for all three, so the slide and the design doc show them side by
   side: capability · gold set size · metric · value · threshold · pass/fail.
6. **Local vs hosted comparison, if both exist.** If `RR-08` picks Ollama for the demo, one
   row that scores the local model against Haiku on the same gold set is a cheap, sharp
   result: "the 3B model reaches X% of the hosted model's F1 at zero API cost".

**Resolution:** three filled-in specification rows (everything but the measured value), the
labelling plan with its hour estimate, and the command that will print each table.

## Input from RR-09 (closed 2026-09-04)

- The aspect gold set splits into a **pre-2020 validation set** (used to tune) and an
  **untouched post-2020 audit set** (opened only after the labelling spec freezes) so
  generalisation across the holdout boundary is a reported number.
- Add one table beyond the three capability rows: the **theme-shift table** per candidate —
  candidate and control counts, baseline and recent shares, matched-control-adjusted
  difference (per-control changes averaged, then subtracted), pointwise descriptive
  interval, label validation quality. Ranked by adjusted change; no significance claims;
  intervals stated as pointwise, not simultaneous.
- Injected-decline power results validate the rating alert only, not the theme analysis;
  the aspect table is the only evidence for the second half of the opening question.

## Input from RR-07 (closed 2026-09-06)

- Complaint-theme labelling gets a **three-system sub-row**: LLM theme labeller · text
  MLlib classifier · star-only theme baseline, all scored on identical audit rows. Pass
  rule pre-registered in RR-07 item 17 (beats star-only; ≥ 70% of LLM macro-F1).
- Support floors predeclared: ≈30 training positives, ≥10 audit positives, mechanical, same
  for all three systems. `other` excluded from headline macro, reported separately.
- Uncertainty: seeded paired bootstrap **clustered by product** on the two macro-F1
  differences, pointwise, context only.
- Classifier output is a *score*, not confidence; no comparability to `label_confidence`.

## Input from RR-08 (closed 2026-09-06)

- **LLM theme labeller pass rule, pre-registered** (ADR-0003): audit macro-F1 over supported
  named themes ≥ 0.70 **and** no supported theme with recall < 0.50. Supported = ≥ 10 audit
  positives. Report per-theme P/R/F1/support, development macro-F1 beside audit (the drop is a
  number), `other`, abstention, `parse_failed`/`api_failed` coverage, seeded product-clustered
  bootstrap interval as context.
- **Audit set = 200 rows**: 120 enriched from frozen discovery theme terms + 80
  prevalence-representative from post-2020 candidate/control windows; the 80 are reported
  separately as the unbiased number. Development set = 200 enriched pre-2020 rows.
- **Label-noise ceiling**: 40 audit rows relabelled by the same annotator ≥ 5 days later;
  per-theme intra-annotator κ reported. No second annotator exists.
- **Local vs hosted row**: `llama3.2:3b` scored on the identical development and audit rows
  under the identical frozen prompt; reported as "% of hosted macro-F1 at $0". Never primary.
- **Disagreement enum** (fixed): `model_missed` · `model_invented` · `definition_boundary` ·
  `human_error` · `star_misleading`. One row each, adjudicated once after scoring, labels and
  spec unchanged.
- **Sentiment weak-label check** is a sanity table only: 3★ excluded, one row per star bucket
  + overall, four predicted-sentiment counts, agreement rate with Wilson 95% interval.
- Contract in `conf/complaint-theme-label.schema.json`; wire body in `docs/LLM_LABEL_RUNBOOK.md`.

## Input from RR-01 (closed 2026-09-06)

The 20-query relevance judgement set is built in P4 Search (frozen queries, blind pooled
judging across analyzers, P@5 plus MRR@10 or nDCG@10) and reused for kNN and hybrid. It is
**not** the RAG evaluation set: RAG needs its own answerability, citation and faithfulness
judgements. Capability rows now map to phases P5 Embeddings, P6 Themes, P7 RAG.

## Input from RR-06 (closed 2026-09-06)

The embeddings/semantic-search row is decided: gold set = the Search 20-query set (10
lexical + 10 descriptive), binary relevance, one annotator, incremental blind pooling of
kNN/hybrid top-10s; metric = macro-average P@5 (primary), MRR@10; **two tables** — a
controlled comparison on the vector cohort (BM25-filtered · kNN · hybrid-filtered) and the
production rows (unfiltered BM25 · production hybrid). Pre-registered hypotheses: BM25 ≥ kNN
lexical, kNN ≥ BM25 descriptive, hybrid ≥ BM25 overall — reported, no pass threshold on
relevance. Disagreements = per-query overlap distribution plus the per-query judged results.
Supplementary: ANN recall@10 vs exact cosine (no labels). The one-shape table's
"threshold/pass" cell for this row reads "gate: engineering identities; relevance:
hypothesis, reported".

## Grilling log

Four rounds, 2026-09-06. Round 1 settled the three capability rows, Philip as sole judge,
the one-shape table vocabulary and the artefact-plus-renderer design. Philip rejected: a
regex scan as proof of unanswerability; "top five by theme shift" as slot selection (it
conflicts with the frozen RR-09 rating-decline ranking and biases toward easy cases);
conditional-denominator metrics (refusing hard questions improves the rate); a flat
citation list; one unpartitioned top-10 for temporal questions; drawing RAG calls from the
ADR-0003 ledger (already fully allocated, 600 + 1,000 + 3,000 + 8,000 + 200 = 12,800); an
outcome/cause-mixed disagreement enum; and deriving a required direction from the
theme-shift aggregate when the answer is forbidden to make directional claims. Final
labelling range revised upward three times: 14 → 14–18 → 16–20 → 17–21 h.

## Answer

Closed 2026-09-06. Protocol in `docs/adr/0006-frozen-rag-evaluation-protocol.md`; terms in
`CONTEXT.md` under *Evaluation*.

### The three capability summary rows

"Three capability summary rows plus a separate theme-shift outcome table" is the accurate
formulation — not "four tables". P5 already carries controlled and production retrieval
tables; P6 carries per-theme and diagnostic tables. The summary table is the one place all
three meet. The sentiment weak-label check is a sanity table, never a row.

| capability | phase | evaluation set (n, provenance) | metric | threshold | verdict kind |
|---|---|---|---|---|---|
| Embeddings and semantic search | P5 | Diagnostic benchmark, 20 queries (10 lexical + 10 descriptive), binary, incremental blind pooling, one annotator (RR-06) | macro P@5 primary, MRR@10; ANN recall@10 vs exact | engineering gate: ID-set identities and completeness (`EMBED_GATE`); relevance hypotheses BM25 ≥ kNN lexical, kNN ≥ BM25 descriptive, hybrid ≥ BM25 overall | gate PASS/FAIL; relevance **REPORTED** |
| Complaint-theme labelling | P6 | Audit set 200 (120 enriched + 80 prevalence-representative, reported separately); development 200; 40 relabelled ≥ 5 days later (RR-08) | macro-F1 over supported themes; per-theme P/R/F1/support; intra-annotator κ | audit macro-F1 ≥ 0.70 **and** no supported theme recall < 0.50 (ADR-0003) | PASS/FAIL |
| ↳ three-system sub-row | P6 | identical audit rows | macro-F1: LLM · MLlib text classifier · star-only | classifier > star-only and ≥ 70 % of LLM (ADR-0002) | PASS/FAIL |
| ↳ local-vs-hosted sub-row | P6 | identical development and audit rows, identical frozen prompt | `llama3.2:3b` as % of hosted macro-F1 | none | REPORTED |
| RAG | P7 (conditional) | 30 frozen questions: 20 answerable (10 product-scoped + 10 temporal) + 10 unanswerable (4 absent attribute · 3 zero-review scope · 3 out-of-domain) | five rows below | five integer thresholds below | gate PASS/FAIL; four quality targets PASS/FAIL; `NOT_RUN` if cut |

### RAG row in full

**Questions.** Templates frozen before P7 starts; slots instantiated from gold, never
hand-picked. Product-scoped: *"What complaints appear in the cited reviews for product X
within <scope>?"* Temporal: *"What complaints appear in the cited reviews from product X's
recent window, and how do those examples contrast with cited reviews from its baseline
window?"* Under the fallback question: low-rated vs high-rated periods, wording *"contrast"*,
windows `low_rated` / `high_rated`, non-chronological. Every question manifest carries exact
start/end bounds or an explicit whole-lifetime declaration; answerability, retrieval and
citation scope all read the same bounds.

**Slot selection, frozen.** Top 5 eligible decline candidates by the RR-09 rating-decline
ranking (bootstrap lower bound), each with its nearest matched control by frozen matching
distance, ties by product id. Same 10 products in both answerable families; unique within a
family; reused controls deduplicated. Fewer than 5 candidates: fill from unused matched
controls ranked by distance then id, then from products meeting the protocol minimum counts
in the holdout ranked by evaluable-point count then id. `slot_role = candidate | control |
filler`; a filler is never presented as a finding. Every filler independently satisfies the
answerability rule. If the fallback-period rule is not in the frozen protocol configuration
when questions are instantiated, fallback temporal questions are `NOT_RUN`, not improvised.

**Answerability, per family, frozen before RAG execution.** *Evidence scan* = lexical
candidate discovery over every silver review in scope (frozen per-question synonym list),
followed by manual semantic validation; regex alone proves nothing. Product-scoped:
answerable iff ≥ 3 manually validated complaint-bearing reviews in scope. Temporal: ≥ 3 in
*each* window separately. Unanswerable iff 0 validated after the frozen synonym scan plus
manual check; the 1–2 band is excluded from the set. Praise and neutral mentions never
count. Matched ids, validation decisions, and confirmation that each supporting id is
present in the evaluated index snapshot are committed with the manifest.

**Answer key, written at authoring time, before any answer exists.** Fields:
`required_propositions` (minimum content for a fully adequate answer — for temporal, at least
one supported complaint from each window plus an explicit qualitative contrast),
`acceptable_themes`, `supporting_review_ids`, `forbidden_claims` (prevalence,
increase/decrease, "more often", "became", representativeness, causation),
`expected_refusal_reason` for unanswerable. Theme-shift direction is diagnostic metadata,
hidden from generator and judge. Development questions get light keys (propositions + ids).

**Retrieval.** Product-scoped: production hybrid, product and window filters applied to BM25
and kNN independently before fusion, top 10. Temporal: the production hybrid run once per
window, up to 5 per window, max context 10 — an evaluation wrapper around the production
retriever, not one unpartitioned request. No minimum context; BM25 fills what kNN cannot;
actual retrieved-set size and vector-cohort coverage reported per question. This closes the
map's fog on `k` and the sparse-vector fallback.

**Output contract** (`conf/rag-answer.schema.json`): `{status: answered|refused, answer,
claims: [{text, citations: [{review_id, window}]}], refusal_reason}`. Refusal: null answer,
no claims, no citations, non-empty reason. Window is verified from stored review metadata,
never trusted from the model. Strict JSON, one identical retry on API *or* parse failure,
attempts stored separately from valid refusals.

**Generator and ledger.** Haiku 4.5. Development set = 10 questions, same templates,
pre-2020 windows of non-candidate products; ≤ 5 prompt versions × 10 = 50 calls. Frozen
evaluation = 30 calls, run once after the prompt freezes; the 30 never tune anything. **P7
ledger: hard ceiling 200 calls / $2**, its own line, not ADR-0003's. Idempotency key covers
question id, model and prompt versions, inference settings, retriever spec, index and silver
snapshot ids, ordered retrieved ids and their content hashes. Check $2 against measured
top-10 prompt length before execution.

**Judge.** Philip alone, rubric frozen before answers are revealed. Two axes per answered
answerable question: *groundedness* (fully / partially / unsupported; fully = every material
claim supported, one unsupported material claim blocks it) and *adequacy* (fully / partially
/ does not, against the key). Refused recorded as its own state. Stated as a single-annotator
project evaluation, not a reliability study.

**Metrics and integer thresholds, fixed denominators.**

| # | row | denominator | threshold | kind |
|---|---|---|---|---|
| 1 | citation/scope contract: every non-refused answer ≥ 1 citation; every citation ∈ that question's retrieved set and correct scope; refusals carry no claims or citations | 30 | 30/30 | engineering gate |
| 2 | grounded-answer success (fully grounded; refusal = failure) | 20 answerable | ≥ 16 | quality target |
| 3 | adequate-answer success (fully adequate; refusal = failure) | 20 answerable | ≥ 14 | quality target |
| 4 | correct abstention | 10 unanswerable | ≥ 8, three strata reported beside | quality target |
| 5 | false refusal | 20 answerable | ≤ 2 | quality target |

Wilson 95 % beside every rate, raw counts always, the three-level distributions on both axes
reported as diagnostics. Footnote on the table: *small-benchmark evidence, single annotator,
pragmatic project targets, not reliability estimates.* Row 1 fails → P7 engineering gate
fails and reopens. Row 1 passes, any of 2–5 fails → `built, evaluated, below target`, number
shown.

**Disagreements.** One primary label per failed row by precedence, one optional secondary:
`scope_violation` → `failed_abstention` (answered a verified-unanswerable) →
`retrieval_miss` (validated support confirmed in the index snapshot, not retrieved) →
`over_refusal` (adequate support retrieved, generator refused) → `generation_unsupported` →
`generation_omission_or_inadequacy` → `judge_uncertain`. Adjudicated once after scoring;
prompt, key and questions unchanged after.

### One table shape

Columns: `capability · phase · evaluation set (n, provenance/hash) · metric · value
[interval] · threshold · verdict · evidence path`. Verdicts: **PASS / FAIL** for any frozen
threshold whether its inputs came from code or human labels; **REPORTED** for descriptive
results and hypotheses with no acceptance threshold; **NOT_RUN** for a conditional capability
cut under the recorded RR-12 decision. Sub-rows indented under their capability. Use
"evaluation set", never "gold set".

### The command

Each capability's gate script writes `data/eval/<capability>.json` validated against
`conf/eval-artifact.schema.json`, recording protocol/config hash, model and prompt/spec
identity, evaluation-population identity, `pipeline_run_id`, timestamp, `status`, and
`cut_reason` where applicable. `scripts/eval_table.py` (`make eval-table`) renders the one
table and hard-fails on a missing or schema-invalid artefact unless the capability is
declared cut in config. The proof that every phase gate prints a number is each producing
script; the renderer only aggregates.

### Labelling range → RR-12

≈ **17–21 h** of Philip, un-parallelisable, 17 explicitly the optimistic bound: 400 theme
labels ≈ 6.7 h, 40 relabels ≈ 1 h, ≈ 600 pooled relevance judgements ≈ 3.5 h, RAG authoring +
structured keys ≈ 3.3 h, RAG judging ≈ 1.5 h, plus taxonomy merge, per-query relevance rules,
answerability validation, and two adjudication passes. Cut order inherited: conditional RAG
first as one coherent cut; if P6 must shrink, preserve the 80-row representative stratum and
redesign taxonomy/support as a unit; never delete audit strata ad hoc.

### Dependency recorded, not decided here

The low-/high-rated fallback-period rule (minimum counts, selection, tie-break) is **not
yet in ADR-0001**. P3 owns it: frozen from pre-2020 data, before holdout identities or
outcomes are inspected, values in the machine-readable protocol configuration. RAG records
that configuration's identity and reuses its windows unchanged.

**Handoffs:** RR-11 (evaluation page is a demo surface; no local RAG row, local RAG is a
demo move at most), RR-12 (17–21 h range; RAG first cut; P7 ledger), RR-13 (`RAG_GATE` =
row 1; `make eval-table`), RR-14 (README AI table must show the verdict vocabulary).

## Amendment from RR-16 (2026-09-07)

Eval artefacts carry `pipeline_run_id` (the primary subject being judged) **and**
`participating_run_ids[]` — the complete, deduplicated set of runs whose outputs the
comparison consumed, including the primary — plus the concrete snapshot/artefact identities
consumed. RR-17 owns this general contract; ADR-0006 references its application to RAG
(`rag_answers` is the run that produces the answers). Declared cuts are read from
`conf/lineage_chain.toml`, shared with `scripts/gate_lineage.py`.

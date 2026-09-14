---
status: accepted
date: 2026-09-06
---

# Frozen RAG evaluation protocol

RAG is evaluated once, on thirty frozen questions, by one human judge, against thresholds
committed before the prompt is written. Twenty questions are answerable — ten product-scoped,
ten temporal — and ten are unanswerable; the answerability of every question is established
by an **evidence scan** (lexical candidate discovery followed by manual semantic validation)
over the scoped silver reviews, never by what the retriever returned. Slots are filled by the
RR-09 rating-decline ranking and frozen matching distances, never chosen by hand or by
theme-shift strength. We chose a human judge over an LLM judge because the brief's own
warning — LLMs produce confident wrong answers — makes an LLM judging an LLM circular with a
single annotator; and we chose per-window retrieval with a forbidden-claims list over
injecting the theme-shift aggregate as context, so RAG stays an evidence-summariser and the
theme-shift table remains the only quantitative authority on the opening question.

## Rules

- Templates freeze before P7 starts; the thirty evaluation questions run **once**, after the
  prompt freezes, and never tune anything. Prompt development uses a separate ten-question
  set instantiated on pre-2020 windows of non-candidate products.
- Answerable: ≥ 3 manually validated complaint-bearing reviews in scope (temporal: in each
  window separately). Unanswerable: 0 after the frozen synonym scan plus manual check. The
  1–2 band is excluded. Every question manifest carries exact scope bounds; answerability,
  retrieval and citation checking read the same bounds.
- Slot selection: top five candidates by the frozen decline ranking, nearest matched
  control each; `slot_role = candidate | control | filler`; fillers by a frozen mechanical
  rule, never presented as findings. Fallback-period windows come from the P3 protocol
  configuration unchanged; absent that rule, fallback questions are `NOT_RUN`.
- Answer key written at authoring time: `required_propositions`, `acceptable_themes`,
  `supporting_review_ids`, `forbidden_claims` (prevalence, increase/decrease,
  representativeness, causation), `expected_refusal_reason`. Theme-shift direction is
  hidden from generator and judge.
- Temporal retrieval: production hybrid run per window, filters applied to BM25 and kNN
  before fusion, up to five per window. Product-scoped: filtered production hybrid, top ten.
- Output is claim-level: each claim carries citations `{review_id, window}`; window is
  verified from stored metadata. Refusals carry no claims, no citations, a non-empty reason.
- Metrics use fixed denominators and integer thresholds: citation/scope contract 30/30
  (engineering gate); grounded-answer success ≥ 16/20 and adequate-answer success ≥ 14/20
  with refusal counted as failure; correct abstention ≥ 8/10, three strata reported;
  false refusal ≤ 2/20. Gate failure reopens P7; quality failure ships as *built, evaluated,
  below target*. **Amended 2026-09-14 (RR-24):** `RAG_GATE` blocks on *mechanical facts*
  only — seal identity, 30/30 recorded, ledger attribution, retrieval sizes and scope, every
  cited handle resolving to the question's own retrieved set and to a stored month inside the
  declared window, the run contract. The 30/30 citation contract keeps its bar but reports:
  `RAG_CONTRACT answers_ok=N/30 bar=30/30 verdict=PASS|FAIL`. An uncited claim, a refusal
  carrying claims or citations, or a parse failure is *generator behaviour*: it is never
  fixed after the thirty are seen, and the question scores as a failure in `RAG_QUALITY`
  (a parse failure counts as a refusal on an answerable question and as a failed abstention
  on an unanswerable one).
- Disagreement labels by precedence: `scope_violation` → `failed_abstention` →
  `retrieval_miss` (support confirmed in the index snapshot) → `over_refusal` →
  `generation_unsupported` → `generation_omission_or_inadequacy` → `generation_malformed`
  (added 2026-09-14, RR-24: the output was rejected by the answer validator) →
  `judge_uncertain`.
- P7 has its own ledger: 200 calls / $2, separate from ADR-0003's fully allocated 12,800.
  Idempotency covers question, prompt and model versions, inference settings, retriever
  spec, snapshot ids, and ordered retrieved ids with content hashes.
- **Reopening (added 2026-09-14, RR-24).** The thirty run once *per identity*. A second
  evaluation run is allowed only for a defect in the answer path that could not have been
  chosen on the held-out answers — a validator rejecting content nobody read — and never for
  the prompt, model, decoding or retrieval. It goes through `--reopen "<reason>"`, which
  archives the prior seal beside the new one, embeds the reason in the seal and the gate
  artefact (`reopened_from=<run_id>`), and asserts `answers_identical_to_run1=k/k` over every
  question the prior run parsed — a difference is a `RAG_GATE` FAIL. The prior run's verdict
  stays in the evaluation table as a prior row. First use: run `ffbbe884` failed on a 15-word
  `subject` cap that sat on the boundary in development; the cap stopped being a rejection
  reason and `max_attempts` became 1, because an identical retry of a `temperature 0`, seeded
  decoder returned identical output both times.
- **Cut, 2026-09-14: Philip declines the judging pass.** `RAG_QUALITY` is defined above as
  Philip's judgement over the sealed thirty, and no human judgement exists, so the capability
  is **cut** in `conf/lineage_chain.toml` with a written reason (ADR-0011's word for "a reason
  instead of a number"; the row prints `NOT_RUN`). What exists instead is one agent pass by a
  different model family (`annotator=claude`, `label_source=model`,
  `eval/rag/agent-judged-quality.json`, judgements in `eval/rag/judgements-agent.jsonl`):
  grounded 11/20 and adequate 10/20 sit under their bars of 16 and 14; abstention 10/10 and
  false refusal 1/20 clear theirs of 8 and ≤ 2. Its header states it is not `RAG_QUALITY` and
  that a human pass supersedes it entirely; it is published as a pre-check, not as the
  measurement. The four bars do not move. `make gate-rag` no longer writes
  `eval/rag_quality/gate.json` and removes a stale one; the judging worksheet and importer stay
  in place, so reversing the cut is the human pass plus a one-line `status` change.

## Consequences

- Every AI capability reports through one summary table (`make eval-table`) with verdicts
  PASS / FAIL / REPORTED / NOT_RUN; a threshold earns PASS/FAIL whether its inputs came from
  code or from human labels.
- Roughly 17–21 hours of single-annotator labelling across P4–P7 are now committed; RAG is
  the first coherent cut if that budget fails.
- The RAG evaluation artefact's `pipeline_run_id` is the `rag_answers` run that produced the
  thirty answers; `participating_run_ids[]` lists every run it consumed (RR-16 amendment,
  2026-09-07, contract owned by RR-17).

# 12 — RAG answer generation and `RAG_GATE`

**What to build:** a category manager can ask about a product's decline and get an answer
that cites the reviews it rests on — or a refusal, when the corpus cannot answer. The
citation and scope contract is checked at **30/30** and blocks: a system that cites nothing
cannot ship as working.

Retrieval reuses the P4/P5 hybrid retriever unchanged. The phase adds one prompt and one
evaluation, nothing more.

`RAG_GATE` is the contract only:

- every non-refused answer cites at least one review from its own retrieved set, in correct
  scope;
- refusals carry **neither** claims nor citations — a refusal that carries either is a
  contract violation, so hedged non-answers cannot pass as abstentions.

RAG carries its own call ledger, so its cost is attributed to it rather than pooled with
labelling.

**Blocked by:** 01 (the artefact contract) and 11 (the frozen questions).

**Status:** closed 2026-09-14 (run 2 `c4ae0dc5`, `RAG_GATE=PASS`, `RAG_CONTRACT 29/30 bar=30/30 verdict=FAIL`)

- [x] All 30 frozen questions answered in one run, with retrieved sets recorded per question
- [x] `RAG_GATE` prints its named constituents then a terminal verdict line, blocking at 30/30
- [x] Trip case: a refusal carrying a citation must violate the contract
- [x] Trip case: an answer citing a review outside its retrieved set must violate the contract
- [x] A vacuous run — zero answers checked — cannot print PASS
- [x] RAG's own call ledger records every model call with its run id
- [x] The evaluation artefact validates against the contract

## Reopened 2026-09-14 (RR-24) — the rerun

Run `ffbbe884` printed `RAG_GATE=FAIL contract=27/30`. Two rows were `parse_failed` on the
15-word `subject` cap (a validator defect; content never read), one carried an uncited
absence claim (generator behaviour; not fixed). Decision in `RR-24`; ADR-0006/0011 amended.

- [x] Run 1's four artefacts committed unchanged as the first result on record
- [x] `conf/rag-answer-spec.json`: `subject_max_words` no longer rejects (length reported),
      `max_attempts: 1`; prompt `rag-v5`, model, seed, retrieval untouched
- [x] `rag_run --reopen "<reason>"`: archives `answer-seal.json` → `answer-seal.1.json`,
      writes seal 2 with the reason, refuses without a reason, refuses if the prompt/model/
      seed/retrieval identity moved
- [x] Gate prints `RAG_REOPEN reopened_from=ffbbe884 answers_identical_to_run1=28/28 ok=`,
      **FAIL** on any difference; trip case: one altered parsed answer must fail
- [x] `RAG_GATE` blocks on mechanical constituents only; `RAG_CONTRACT … bar=30/30
      verdict=` is printed and written to the artefact as a non-blocking line
- [x] Trip cases from the original checklist still trip `RAG_CONTRACT`
- [x] `make eval-table` shows run 1's `RAG_GATE=FAIL` as a prior row beside run 2

### Closed 2026-09-14 — what the rerun measured

- `make rag-reopen REASON="…"` → run `c4ae0dc5`, 30 calls, `parse_failed=0`, answered 19,
  refused 11, ledger 62/200. The reopen check reproduced seal 1's config hash from today's
  spec with the sealed commit's `limits`, so the only movement was the limits.
- `make gate-rag`: `RAG_REOPEN reopened_from=ffbbe884 answers_identical_to_run1=28/28 ok=true`;
  `RAG_CONTRACT answers_ok=29/30 bar=30/30 … claims_cite=1 verdict=FAIL` (`temporal-01`, claim 2,
  the absence claim, recurred as predicted); `RAG_GATE … constituents_ok=7/7 RAG_GATE=PASS`.
- The two rescued rows: `product_scoped-02` answered with 5 claims (`subject_words=16`);
  `zero_review_scope-02` refused cleanly (`subject_words=19`, `retrieved=[]`). Both lengths
  match run 1's rejection messages, so run 2 parsed the bytes run 1 threw away.
- Live trip: one altered parsed answer → `answers_identical_to_run1=27/28 … RAG_GATE=FAIL`.
- Run 1 archived as `answers.1.json`, `answer-seal.1.json`, `gate.1.json`, byte-identical to
  commit `269ea7a`; `make eval-table` renders it as `rag (prior) … FAIL … ffbbe884`.
- Ticket 13 is unblocked on `RAG_GATE=PASS`; `temporal-01` scores as one unsupported
  material claim there.

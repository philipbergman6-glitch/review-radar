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

**Status:** ready-for-agent

- [ ] All 30 frozen questions answered in one run, with retrieved sets recorded per question
- [ ] `RAG_GATE` prints its named constituents then a terminal verdict line, blocking at 30/30
- [ ] Trip case: a refusal carrying a citation must violate the contract
- [ ] Trip case: an answer citing a review outside its retrieved set must violate the contract
- [ ] A vacuous run — zero answers checked — cannot print PASS
- [ ] RAG's own call ledger records every model call with its run id
- [ ] The evaluation artefact validates against the contract

# 11 — Freeze the 30 RAG questions

**What to build:** the frozen question set P7 is evaluated against — 20 answerable and 10
unanswerable, across three strata — settled and committed **before any answer is
generated**, so the question set cannot drift toward what the system happens to do well.

Two rules shape it:

- **Answerability is decided by evidence scan and manual validation, never by what the
  retriever returned.** Deciding it from retriever output grades the retriever twice.
- **Question slots are drawn from the actual decline ranking**, so RAG answers concern the
  real result rather than a convenient demo topic.

Each question carries a structured answer key. Keys **forbid prevalence and direction
claims** — a retriever's ranking is never a representative sample. Temporal questions
contrast cited examples only; the quantitative theme-shift claim stays where it is
computed, in the gold table.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] 30 questions committed: 20 answerable, 10 unanswerable, across the three strata
- [ ] Answerability decided by evidence scan and manual validation, with the scan recorded
- [ ] Slots drawn from the decline ranking, and the draw is reproducible
- [ ] Every question carries a structured answer key
- [ ] Keys forbid prevalence and direction claims
- [ ] No answers have been generated against these questions yet

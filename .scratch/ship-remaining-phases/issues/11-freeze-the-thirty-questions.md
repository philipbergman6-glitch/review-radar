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

**Status:** done — 2026-09-11. `conf/rag-questions.json` is frozen: 30 questions, 20 answerable
(10 product-scoped + 10 temporal over the same ten products) and 10 unanswerable
(4 absent-attribute · 3 zero-review-scope · 3 out-of-domain), spec hash `fdaf85463c0f`.

- [x] 30 questions committed: 20 answerable, 10 unanswerable, across the three strata
- [x] Answerability decided by evidence scan and manual validation, with the scan recorded
- [x] Slots drawn from the decline ranking, and the draw is reproducible
- [x] Every question carries a structured answer key
- [x] Keys forbid prevalence and direction claims
- [x] No answers have been generated against these questions yet

## Resolution

**The ranking is real, and it is the thing that picked the products.** RR-09's statistic —
percentile bootstrap lower bound of (baseline mean − recent mean), 2000 draws, per-episode seed —
did not exist in code before this ticket; the gold job stores episodes, not a ranking over them.
`src/ai/rag_questions.bootstrap_lower_bound` implements it and `make rag-ranking` ranks all 708
matched episodes into `eval/rag/decline-ranking.json`. Ranking by the lower bound rather than the
raw drop is the whole point: the top raw drops on this corpus are eleven-review windows, and the
bound puts them where they belong.

**What the corpus did to the answerability bar, and what it cost.** ADR-0006 needs three manually
validated complaint-bearing reviews *in every window a question declares*. The top five ranked
candidates could not carry that. Eight of their first twenty windows matched fewer than three
reviews before anyone read one, and of the 244 reviews the scan eventually put on a worklist, **91
were praise or neutral mentions** — the lexical scan's precision is about 63%, which is exactly why
ADR-0006 makes the human reading, not the regex, the decision.

So the walk descended, which ADR-0006 already permits by saying *top five **eligible** candidates*:

- A **lexical pre-filter** (`scripts/rag_prefilter.py`, `MIN_LEXICAL_MATCHES = 6`, twice the bar,
  fixed before any count was read) narrowed 177 products in the ranking head to 48. It is
  necessary and never sufficient — validation only ever removes reviews.
- The first draw took ranks 3, 6, 8, 11, 23. Manual validation **rejected the rank-8 pair**:
  `B00SH5WGNE` carried 2 validated complaints in its baseline window and its control `B007QG7G3U`
  carried 2 in its recent window — both in the excluded 1–2 band, neither answerable nor honestly
  unanswerable.
- The redraw walked to rank 24 and confirmed ten products: ranks **3, 6, 11, 23, 24**, each
  candidate with its nearest matched control, all ten distinct.

Every skip is recorded in `eval/rag/slots.json` (19 of them), and `eval/rag/walked-past.json`
keeps the rejected pair in the scan so the reading that justified the descent survives the redraw.

**Answerability was decided against silver, never against a retriever.** No retrieval ran. The
scan reads `silver.reviews` directly with hand-written, complaint-oriented term lists in
`conf/rag-question-spec.json` — deliberately *not* `conf/theme-terms.json`, which was mined to
offer reviews for labelling and contains fragments like "see" and "few". 1,520 in-scope reviews,
462 lexical matches, 244 read, 153 judged complaint-bearing, every decision recorded in
`eval/rag/validation.jsonl` including the negatives.

**The annotator is the agent, and the manifest says so.** Same standing as P6's reference labels
(RR-21): machine-made, recorded as `agent`, never as `human`. Philip remains the sole judge of
*answers* under ADR-0006; this is judgement of *evidence*, and it is stated rather than implied.

**Two spec choices worth naming.**

- *Product-scoped scope is the episode's whole span* (`baseline_start … recent_end`), one declared
  window; temporal declares the two halves. Same evidence, same bounds, and citation scope-checking
  reads the same bounds answerability did.
- *Out-of-domain refusals claim only what was checked.* The three rarest probes still appear
  somewhere in the corpus — engine oil 4 / mortgage 4 / tax 6 in 694,252 reviews — so the refusal
  reason speaks about the declared window and the corpus-wide count rides on the question as a
  diagnostic. The absent-attribute probes were rewritten mid-ticket for the same reason: refusing
  "what do reviewers say about GPS?" tests nothing, so the four probes are recyclable packaging,
  cruelty-free status, sulfates and parabens, and expiry date — things a beauty shopper plausibly
  asks that these products' reviews never discuss.

**The freeze refuses four ways.** `scripts/rag_freeze_questions.py --freeze` will not write if the
ledger holds a `rag_answers` run or an answers file exists, if the manifest is already frozen, if
any of the 141 supporting review ids is absent from the evaluated `reviews` index generation (all
141 were checked and present), or if `check_manifest` finds any violation. Theme-shift direction
appears nowhere in a question, a proposition or a refusal reason, and a test enforces that.

**Artefacts.** `conf/rag-question-spec.json` (frozen) · `conf/rag-questions.json` (frozen) ·
`src/ai/rag_questions.py` · `scripts/rag_{slots,prefilter,evidence_scan,validate,freeze_questions}.py`
· `eval/rag/` · `tests/test_rag_questions.py` (28 tests, every check with a case that trips it) ·
`make rag-ranking|rag-prefilter|rag-slots|rag-scan|rag-validate|rag-questions-draft|rag-freeze`.

**What this hands ticket 12.** The questions, their scopes, their retrieval spec and their keys.
What it does not settle: the answer prompt, the ten-question development set on non-candidate
pre-2020 windows, `conf/rag-answer.schema.json`, and the P7 call ledger.

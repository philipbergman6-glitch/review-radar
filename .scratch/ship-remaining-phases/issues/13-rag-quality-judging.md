# 13 — `RAG_QUALITY` judging

**What to build:** the four quality verdicts for P7, printed beside their thresholds and
**non-blocking**, so a below-target RAG still ships as evaluated rather than as unfinished.

Fixed integer denominators, all frozen before judging:

- grounded ≥ 16/20
- adequate ≥ 14/20
- abstention ≥ 8/10
- false refusal ≤ 2/20

Philip is sole judge, with the seven-step disagreement precedence already fixed. The
thresholds do not move for a weak result — a miss prints `REPORTED` beside its bar and P7's
status becomes `built, evaluated, below target`.

**Blocked by:** 12's rerun — answers must exist under run 2 with `RAG_GATE=PASS`;
`RAG_CONTRACT` may be FAIL (RR-24, 2026-09-14).

**Scoring a contract violation (RR-24):** the question is a failure — an uncited claim is
unsupported (not fully grounded; adequacy judged on cited claims only); a parse failure
counts as a refusal on an answerable question and a failed abstention on an unanswerable one.
Disagreement label `generation_malformed` sits before `judge_uncertain`. Denominators stay
20/20/10/20.

**Status:** harness built 2026-09-14; **blocked on Philip's judging pass** (HITL — the agent does not judge)

- [ ] All 30 answers judged under the fixed precedence — **owed by Philip** (`make rag-judge`, then `rag-judge-check` → `rag-judge-import` → `gate-rag`)
- [x] Four verdicts printed, each with its threshold beside its value (`RAG_QUALITY_TARGET <row>=k/n bar>=|<=b wilson95=[..] verdict=`)
- [x] `RAG_QUALITY` never blocks and never changes `RAG_GATE`'s verdict (exit code is `RAG_GATE`'s alone; `test_quality_never_changes_the_reproducibility_verdict`)
- [x] Denominators are the frozen integers, not recomputed from what was answered (`score` iterates the manifest; a mis-sized manifest hard-fails)
- [x] The quality artefact validates against the contract and appears in `make eval-table` (`eval/rag_quality/gate.json`, NOT_RUN with its reason until the import)
- [x] Quality and reproducibility verdicts remain visually separated in the table (kind=quality)

## Built 2026-09-14 — the harness, and what the judge owes

- `src/gates/rag_quality.py` — pure scorer. Judged: groundedness and adequacy on the 19
  answered answerable rows, `refusal_reason_matches_key` on the 10 refusals (diagnostic).
  Derived, never judged: a refusal or malformed output on an answerable question is a false
  refusal and a failure on both axes; an unanswerable question abstains correctly only if the
  refusal is contract-clean; an uncited claim caps `fully` → `partially` (RR-24 §5); a ticked
  forbidden claim caps adequacy the same way (the key forbids it, so the answer does not meet
  the key — reading the key literally, no new bar). Primary disagreement label derived by the
  fixed precedence, `generation_malformed` in its RR-24 slot; a parse failure is not filed as
  `over_refusal` because it is not a decision the model made.
- `scripts/rag_judge.py --export|--check|--import`; `docs/RAG_JUDGING.md` is the rubric and
  its sha256 rides on the import run. Import is all thirty or none, judged once per answers
  run, refuses a file judged against another run id or a drifted answers digest.
- `rag_judgements` is a ledger job (migration 0007, contract v1, chain edge to `rag_answers`
  joined on the answers file digest); `rag_quality` claims it beside `rag_answers`.
- `.scratch/rag-judge/` — offline page (`make rag-judge`), same shape as the P6 labeller; the
  worksheet withholds slot role, decline rank, episode id, evidence and probe.
- `make gate-rag` prints `RAG_QUALITY` after `RAG_GATE`; today: `… targets_met=0/4 judged=0/30
  … verdict=NOT_RUN`. `make eval-table` errors 5 → 4.
- 26 new tests (`tests/test_rag_quality.py`, 3 in `test_runs_contracts.py`); suite 634 green.

**Mechanical facts the export already shows, before any judging:**
- `product_scoped-01` (answerable, 18 validated complaints in key) **refused** with
  `support_retrieved=0/18` — a `retrieval_miss`, not an over-refusal: the hybrid retriever
  returned none of the validated complaint reviews for that window. False refusal count is
  already 1/20 (bar ≤ 2) before Philip opens the page.
- `temporal-01` carries one uncited absence claim (`support_retrieved=1/18`); grounded is capped
  at `partially`, so grounded is at most 18/20 going in.
- Ten of ten unanswerable questions refused cleanly, so abstention is 10/10 before the
  reason-match diagnostic.

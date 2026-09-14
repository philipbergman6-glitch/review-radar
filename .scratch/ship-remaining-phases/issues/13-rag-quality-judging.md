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

**Status:** ready-for-agent

- [ ] All 30 answers judged under the fixed seven-step precedence
- [ ] Four verdicts printed, each with its threshold beside its value
- [ ] `RAG_QUALITY` never blocks and never changes `RAG_GATE`'s verdict
- [ ] Denominators are the frozen integers, not recomputed from what was answered
- [ ] The quality artefact validates against the contract and appears in `make eval-table`
- [ ] Quality and reproducibility verdicts remain visually separated in the table

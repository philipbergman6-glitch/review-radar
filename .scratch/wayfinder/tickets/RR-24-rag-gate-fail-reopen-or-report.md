---
id: RR-24
title: Is the first RAG_GATE FAIL a reproducibility failure to reopen, or a measured result to report
type: grilling
status: closed
assignee: agent (decided 2026-09-14 with Philip; ADR-0006 and ADR-0011 amended)
blocked-by: []
blocks: []
---

## Question

Ticket 12 opened the thirty frozen questions once under the seal (`eval/rag/answer-seal.json`,
run `ffbbe884`, prompt `rag-v5`, commit `5e8358b7`) and `scripts/gate_rag.py` printed
`RAG_GATE=FAIL` at `contract=27/30` (`eval/rag/gate.json`, 2026-09-14). ADR-0006 says "gate
failure reopens P7" and also "the thirty evaluation questions run **once** … and never tune
anything". The seal refuses a second run under any changed identity, and every fix to the
three violations changes the identity. The two rules collide, and which one yields is a
decision, not a build step.

### What failed `[observed]`

| question | violation | mechanism |
|---|---|---|
| `product_scoped-02` | `parse_failed` after 2 attempts | validator: `subject has 16 words, limit 15` |
| `zero_review_scope-02` | `parse_failed` after 2 attempts | validator: `subject has 19 words, limit 15` |
| `temporal-01` | claim 2 carries no citation | the claim is an *absence* claim: "complaints about product durability are not present in the cited reviews for the baseline window" |

Supporting facts, all `[observed]` in the repo:

- `subject_max_words: 15` lives in `conf/rag-answer-spec.json` `limits`, and `limits` is
  hashed into `inference_config_hash` (`src/ai/rag_answers.py:128`). Loosening it is a new
  identity; `check_seal` (`src/ai/rag_run.py:99`) refuses it.
- On the development set `dev-product_scoped-01`'s subject was exactly 15 words — the model
  echoed the product name, ASIN and date range. The cap sat on the boundary before the freeze
  and "10/10 clean" hid it.
- Decoding is `temperature 0, seed 20260913`. Both attempts on each failed question returned
  identical token counts (393/393, 99/99): the "one identical retry" is a no-op for a
  deterministic decoder. The rejected raw text is not stored (`attempts[]` keeps only counts
  and the error), so nothing can be re-parsed without a new call — though the new call would
  return the same bytes.
- `rag-v4` added "never write a claim that says something is absent" in prose and `rag-v5`
  kept it; on the evaluation set the pattern recurred once in twenty answerable questions.
- `zero_review_scope-02` retrieved nothing (`retrieved: []`) — the correct output was a
  refusal, and the model's 99-token output was rejected before anyone saw whether it was one.

### What the rules say

- ADR-0006: contract 30/30 is the "engineering gate"; "gate failure reopens P7; quality
  failure ships as *built, evaluated, below target*". Questions run once, never tune.
- ADR-0011: RAG_GATE = "the 30/30 citation and scope contract only", tripping cases "one
  answer citing a review outside its window; a refusal that still carries a claim" — both are
  generator behaviour, chosen consciously as blocking.
- `CONTEXT.md` *Engineering gate*: "a **mechanical** contract a phase must satisfy to count
  as built". Whether a frozen model writing an uncited claim on a held-out question is
  "mechanical" is exactly the term this ticket has to sharpen.
- RR-23 precedent (P6): on the evaluation sets a `parse_failed` row "still scores as an empty
  prediction for every system" — a failure is a measured result, not a rerun.
- Ticket 13 is "blocked by 12 — answers must exist and have passed the contract gate".

### The decision

Which of these is the route, and what does each ADR say afterwards:

1. **Reopen and rerun** under a second seal (validator loosened, prompt unchanged) — literal
   ADR-0006; the absence claim will almost certainly recur, and any prompt change after seeing
   the thirty is tuning on the held-out set.
2. **Report the FAIL as the measured result** and reclassify: the blocking gate keeps only
   what is mechanical (seal identity, 30/30 recorded, ledger attribution, retrieval sizes,
   citations resolving to the retrieved set and window, run contract); generator behaviour
   (`parse_failed`, an uncited claim, a refusal carrying claims) becomes a reported
   `RAG_CONTRACT 27/30 bar=30/30 verdict=FAIL` line and each such question scores as a failure
   in `RAG_QUALITY`. Threshold unmoved, first result on record, ticket 13 unblocked.
3. **Stop P7 at FAIL**: no judging, quality `NOT_RUN`, status "built, gate failed".

## Answer (2026-09-14)

**Split the three violations by what could have produced them, and treat each half by its own
rule.** Philip took the recommendation after one push-back ("sure you don't want to fix it?"),
which corrected round one's error of lumping the parse failures with the absence claim.

1. **The two `parse_failed` rows are a validator defect, fixed and rerun once.** The
   15-word subject cap rejected content nobody read, on a field nothing downstream consumes;
   the rejected text was never stored, so the fix is verifiably blind. Spec edit, one new
   identity: `subject_max_words` stops being a rejection reason (length is reported instead;
   `claim_max_words` and `max_claims` stay, they bound what the judge reads) and
   `max_attempts` becomes 1, because an identical retry of a `temperature 0, seed` decoder is a
   no-op (both attempts returned identical token counts). Prompt `rag-v5`, model, seed and
   retrieval are untouched.
2. **The reopen goes through a sanctioned path, never around the seal.** `rag_run` gains
   `--reopen "<reason>"`: it archives seal 1 as `answer-seal.1.json`, writes seal 2 with the
   reason embedded, and the gate artefact carries `reopened_from=ffbbe884 reason=…`. Run 2 must
   print `answers_identical_to_run1=28/28` (parsed answer hashes of every question that
   succeeded in run 1) and **FAIL if any differ** — determinism is asserted, not assumed. Run
   1's `RAG_GATE=FAIL contract=27/30` stays in the evaluation table as a prior row; nothing is
   erased.
3. **`temporal-01`'s uncited absence claim is generator behaviour and is not fixed.** A
   prompt change after seeing the thirty is tuning on the held-out set. Run 2 will print
   29/30; that is the result.
4. **`RAG_GATE` is reclassified so that only mechanical facts block** — facts a correct
   pipeline makes true regardless of what the model wrote: seal identity, 30/30 recorded,
   ledger attribution, retrieval sizes and scope, every cited handle resolving to the
   question's own retrieved set and to a stored month inside the declared window, run
   contract, and (run 2 onward) identity with the prior run. **Generator behaviour reports**:
   `RAG_CONTRACT answers_ok=29/30 bar=30/30 verdict=FAIL` — an uncited claim, a refusal
   carrying claims or citations, a parse failure. The 30/30 bar does not move; it stops
   blocking. Handle resolution stays mechanical (an unknown handle would put a citation on
   the slide that points nowhere) and has not tripped since `rag-v2`.
5. **Scoring in `RAG_QUALITY` (ticket 13)**: a question whose answer violates the contract
   is a failure for that question — `temporal-01` cannot be fully grounded (one unsupported
   material claim) and is judged for adequacy on its cited claims only. Had a `parse_failed`
   row survived, it would score as a refusal on an answerable question and as a failed
   abstention on an unanswerable one. Disagreement precedence gains `generation_malformed`
   between `generation_omission_or_inadequacy` and `judge_uncertain`. Denominators stay
   20/20/10/20.
6. **Recorded, not fixed**: the cap was a boundary case visible in development
   (`dev-product_scoped-01` sat at exactly 15 words) that the freeze missed; the design
   doc's *how we avoided fooling ourselves* section says so in one line.

## Consequences

- ADR-0006 amended: reopen rule, `RAG_CONTRACT` reported, `generation_malformed`, spec
  limits. ADR-0011 P7 row amended. `CONTEXT.md` *Engineering gate* sharpened with
  *mechanical fact* vs *generator behaviour*.
- Ticket 12 reopens for the rerun (checklist appended); ticket 13 is blocked on run 2's
  `RAG_GATE=PASS`, not on `RAG_CONTRACT`.
- Run 1's artefacts (`eval/rag/answers.json`, `answer-seal.json`, `call-ledger.jsonl`,
  `gate.json`) are committed as the first result on record before anything reruns.

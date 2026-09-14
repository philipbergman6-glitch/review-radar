# Judging the thirty — the `RAG_QUALITY` worksheet

**Why you are doing this.** P7's blocking gate already passed: `RAG_GATE=PASS` says the thirty
frozen questions were answered once under the sealed identity and every citation resolves to
the question's own retrieved set inside its window. It says nothing about whether the answers
are *right*. That is `RAG_QUALITY`, four numbers over fixed denominators, and you are the only
judge (ADR-0006, RR-17). It is **reported, never blocking**: a miss prints FAIL beside its bar
and P7 ships as *built, evaluated, below target*. Nothing you write here can reopen the phase,
and nothing here moves a bar.

## What is already fixed, and cannot move

| | |
|---|---|
| The questions | `conf/rag-questions.json`, 30, frozen 2026-09-12, spec hash `fdaf85463c0f` |
| The answers | `eval/rag/answers.json`, run `c4ae0dc5` (seal 2, reopened once under RR-24) |
| The targets | grounded ≥ 16/20 · adequate ≥ 14/20 · abstention ≥ 8/10 · false refusal ≤ 2/20 |
| The denominators | the manifest's 20 answerable and 10 unanswerable, never "how many were answered" |
| The rubric | this file; its sha256 is recorded on the import run |

The importer is **all thirty or none**. A partial file is refused, because a judged subset is a
denominator nobody froze.

## The flow

```
make rag-judge-export    # eval/rag/judging-worksheet.json: questions, answers, keys, facts
make rag-judge           # .scratch/rag-judge/index.html — open it in a browser, judge, export
make rag-judge-check     # validate eval/rag/judgements.jsonl without importing
make rag-judge-import    # ledger run `rag_judgements`; writes eval/rag/judgements.json
make gate-rag            # prints RAG_QUALITY beside RAG_GATE; writes eval/rag_quality/gate.json
make eval-table
```

The page keeps your progress in the browser between sessions. When every row is valid, export
and save the file as `eval/rag/judgements.jsonl`.

## What you judge, and what is derived

You never decide whether a question was answerable, refused, or malformed — those are facts
the pipeline recorded, and the scorer reads them. Your row's shape follows from them:

| the answer did | you record |
|---|---|
| answered an **answerable** question (19 rows) | `groundedness`, `adequacy`, `forbidden_claim_present` |
| refused an **unanswerable** question (10 rows) | `refusal_reason_matches_key` |
| refused an **answerable** question (1 row: `product_scoped-01`) | nothing — it is a false refusal by definition |
| malformed output (0 rows this run) | nothing — derived |

On every row you may set `uncertain`, one `secondary_label` from the precedence, and a `note`.
None of the three changes a count.

### Groundedness — is every material claim supported by what it cites?

A **material claim** is a sentence in the answer that asserts something about what the reviews
say. The `subject` line and a bare "the cited reviews complain about X" are claims; "no other
complaints were cited" is not a claim about the reviews' content unless it asserts an absence.

- **fully** — every material claim is supported by the text of the review(s) it cites, and for
  a temporal question each cited review sits in the window the claim says it does.
- **partially** — at least one material claim is supported and at least one is not.
- **unsupported** — no material claim is supported by its citations.

A claim with **no citation is unsupported** (RR-24 §5). The page flags them; the scorer caps a
`fully` to `partially` on any row that carries one. `temporal-01` is the known case.

Read the cited review, not the whole retrieved set: a claim supported by a review it did not
cite is unsupported.

### Adequacy — does the answer meet the key?

The key is `required_propositions` (all must hold), `acceptable_themes`, and `forbidden_claims`.

- **fully** — every required proposition is satisfied by cited claims, and no forbidden claim
  is made.
- **partially** — some required propositions are satisfied, or all are but the answer also
  makes a forbidden claim.
- **does_not** — no required proposition is satisfied, or the answer is about something else.

Judge adequacy **on the cited claims only**: an uncited claim cannot satisfy a proposition.

`forbidden_claim_present` is a separate tick: set it whenever the answer asserts prevalence
("most reviews"), direction ("complaints increased"), representativeness, or causation, even
in passing. The scorer caps `fully` adequacy to `partially` when it is set — the key forbids
the claim, so an answer that makes it does not meet the key. Reading the key literally is the
rule; no new bar is being invented.

### Refusal reason — does it match the key?

For the ten unanswerable questions the answer refused. Set `refusal_reason_matches_key` to
true when the stated reason is the key's `expected_refusal_reason` in substance (no reviews in
the window; the attribute is never discussed; the subject is outside the corpus). This is
**diagnostic**: a correct abstention is counted from the refusal itself, and a refusal with a
wrong reason still counts. It is reported beside the count, not inside it.

### The disagreement precedence

The scorer assigns one primary label per failed row from the fixed order:
`scope_violation` → `failed_abstention` → `retrieval_miss` → `over_refusal` →
`generation_unsupported` → `generation_omission_or_inadequacy` → `generation_malformed` →
`judge_uncertain`. You may add **one** secondary label where you think the primary misses the
real cause — say why in the note.

## The one rule that makes this worth doing

Judge each row **once**, from the page, against the key and the cited reviews. Do not edit
the answers, the questions, or the prompt afterwards — a second answer run on the thirty is
ruled out (ADR-0006), and a rubric changed after reading the answers is a rubric fitted to
them. If you find the rubric itself is wrong, say so in a note and finish the pass; the fix is
an ADR amendment, not a silent rescore.

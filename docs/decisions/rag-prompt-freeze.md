# P7's answering prompt is frozen at `rag-v5`

**Date:** 2026-09-13 · **Ticket:** 12 · **ADR:** 0006

ADR-0006 lets the thirty evaluation questions run **once**, after the prompt freezes. This
records what was frozen, on what evidence, and what that evidence could not be.

## What was selected on

The ten-question **development** set (`eval/rag/dev/questions.json`): pre-2020 windows of
products that appear in neither the evaluation manifest, the decline ranking nor the slot draw,
drawn by the frozen seed. It carries **no answer keys and no answerability verdict**, so the
only thing a version can be selected on is **behaviour under the contract** — never answer
quality. Scoring quality would need keys, and the only keyed set is the held-out thirty.

That is a real limit and it is stated rather than smoothed over: `rag-v5` is the version whose
citations resolve and whose refusal branch is reachable. Whether its answers are *good* is
`RAG_QUALITY`'s question (ticket 13), and it is asked once, afterwards.

## The versions, and what each one moved

| version | contract on development | out-of-domain refused | what changed |
|---|---|---|---|
| `rag-v1` | violations on 2 of 2 tried | — | cited list positions (`"3"`, `"4"`) and mis-transcribed a 64-hex id by one character |
| `rag-v2` | 10/10 clean | 0 of 3 | citations became per-question handles (`R1`, `R2`, …) |
| `rag-v3` | 10/10 clean | 1 of 3 | a named-subject test before writing |
| `rag-v4` | 10/10 clean | 1 of 3 | "never write a claim that says something is absent" |
| `rag-v5` | 10/10 clean | **3 of 3** | `subject` and `subject_supported` emitted **before** any claim |

The step that mattered was structural, not rhetorical. `qwen3:8b` emitted `claims` before
`refused`, so it wrote the complaints it could see and only then decided it had not refused —
and having written them, it never had. Asked in prose three separate ways not to, it kept
turning absence into a claim reading *"complaints about flight delays are not present in the
cited reviews"*. Reordering the output schema so the subject and its support are written first
moved it immediately.

`rag-v5` also refuses two development *temporal* questions. Those refusals are correct: both
products' retrieved reviews are praise, and the development products were drawn by review count
rather than by complaint content, so some genuinely have nothing to complain about.

## What is frozen

`conf/rag-answer-spec.json` `status = "frozen"`, `frozen_prompt = rag_v5 / rag-v5`, frozen at
the commit the development runs were made at. The evaluation run refuses any other prompt, and
`eval/rag/answer-seal.json` refuses a second evaluation run under a changed identity.

Development cost: 40 model calls of ADR-0006's 200-call ledger.

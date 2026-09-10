# 05 — Run `label-v5` on the development set

**What to build:** the fifth and final prompt version in ADR-0003's budget, run over the
same 200 development rows `label-v4` was run over, producing labels that parse. The
existing `make label-themes` path does the running; this ticket's real work is proving
v5 parses at volume and fixing it if it does not.

`label-v4` returned 59/200 `parse_failed` — a plumbing cost of roughly 0.14 macro-F1. The
v5 prompt was written directly against that failure census (it forbids repeating a theme
id and tightens quotes to "3 to 6 words, never more than 12"), but it has never been run at
volume. The 24-row `label-v5` artefact already in `eval/themes/` is a **scorer fixture**
committed before any score existed, not a result — it must not be mistaken for one.

If v5's parse rate is no better than v4's, that is a finding to record, not a reason to
write a sixth prompt. The budget is five.

**Blocked by:** None — can start immediately.

**Status:** done — landed 2026-09-10

- [x] `label-v5` has run over all 200 development rows with `label_source=llm`
- [x] The parse-failure census is printed by named validation cause and committed
- [x] The run registered a run contract and its labels carry the run id
- [x] The 24-row fixture is annotated in place as a scorer fixture, not an evaluation
- [x] No sixth prompt version is written — the ADR-0003 budget is five
- [x] A parse failure is recorded as an empty prediction, distinguishable from "no themes"

## What was actually found

**The premise above is wrong in one respect, and the correction is the finding.** `label-v5`
*had* already run at volume: the labels landed on 2026-09-07 under run
`e30176a8-14c6-4cd7-9c23-9e78604fce83` (176 rows) on top of `43be4670-3809-4f93-9284-4680c0f485eb`
(24 rows), all under the same `inference_config_hash` `b84a2ce22193` today's code still computes.
What never happened was *scoring* them: the committed artefact was written from the 24-row state
in commit `03f53f2`, before the 176 arrived, which is the whole of why it reads 0.1208. Re-running
`make label-themes SAMPLE_NAME=development PROMPT=label_v5` proved it — 200 assigned, 200 cached,
0 to infer — under a fresh run contract `abb4a875-ed8e-4c9b-98aa-3ccbcb765cf8`.

**Parse rate, development, 200 rows, `qwen3:8b`:**

| prompt | parse_failed | rate | diagnostic ceiling over answered rows |
| --- | --- | --- | --- |
| `label-v4` | 59 | 0.295 | 0.5663 over 141 answered, 9 supported themes |
| `label-v5` | 50 | 0.250 | 0.5969 over 150 answered, 8 supported themes |

v5's prompt was written against v4's failure census and moved the rate by 9 rows. The census says
where: `quote_too_long` collapsed from 27 rows to 2 — the "3 to 6 words, never more than 12"
instruction worked — while `quote_not_verbatim` *rose* from 24 to 38 and is now the dominant cause.
Shorter quotes are easier to paraphrase than to copy. `repeated_theme` fell 19 → 12 despite both the
tightened wording and the structural `maxItems: 3` cap.

Violations still cluster away from slot 0 (`{0: 15, 1: 23, 2: 27}`): the model's first theme is
usually fine and it fails on the ones it padded to reach the limit.

The ceiling is a diagnostic, not a score, and it is not the pass rule. Whether v5 or v4 is the
frozen prompt is ticket 06's decision, on the scored number over all 200 rows.

**No sixth prompt.** The budget is five and the finding is recorded instead.

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

**Status:** ready-for-agent

- [ ] `label-v5` has run over all 200 development rows with `label_source=llm`
- [ ] The parse-failure census is printed by named validation cause and committed
- [ ] The run registered a run contract and its labels carry the run id
- [ ] The 24-row fixture is annotated in place as a scorer fixture, not an evaluation
- [ ] No sixth prompt version is written — the ADR-0003 budget is five
- [ ] A parse failure is recorded as an empty prediction, distinguishable from "no themes"

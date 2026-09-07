---
id: RR-21
title: Reverse RR-19 — the agent labels all 400, Philip adjudicates a measured subset
type: grilling
status: closed
assignee: philipbergman (decided 2026-09-07)
blocked-by: [RR-19, RR-20]  # both closed
blocks: []
---

## Question

`RR-19` decided on 2026-09-07 that **Philip hand-labels both 200-row sets** plus 40 repeats,
and stated the reason: "doing that here makes the labeller and the judge the same model
family, so macro-F1 would measure self-consistency, not accuracy." ADR-0003 was amended to
match, and the P4/P5 `judge=claude` compromise was explicitly ruled inadmissible for P6.

Later the same day Philip reversed it: "i want you to do the labelling."

The concern that produced RR-19's answer has not gone away. The primary labeller is
`qwen3:8b`; if the ground truth is written by Claude against the same frozen taxonomy, the
two annotators' errors correlate, and the audit macro-F1 stops estimating "does the model
label correctly" and starts estimating "does the model agree with another LLM." The
frozen 0.70 bar is the headline number the theme-shift table rests on, so an unbounded
inflation of it is a validity problem, not a workload one.

Decide who produces the ground truth, and what is done about the correlated-error risk.

## Answer (2026-09-07)

**The agent labels all 400; Philip hand-labels a stratified 50 of the audit set; the
agreement between them is measured and published.** This supersedes RR-19's ground-truth
clause. RR-19's host decision (`qwen3:8b` primary, `llama3.2:3b` comparison, hosted Haiku
`NOT_RUN`) is untouched.

The concern was raised once and Philip reversed it knowingly, choosing the option that
prices the risk rather than the one that removes it or the one that ignores it. What that
buys: the circularity stops being an unbounded caveat in prose and becomes **a number on the
evaluation table** — Claude-vs-Philip agreement on a random stratified 50 of the audit set,
with a Wilson interval. A high agreement rate is evidence the ground truth is sound; a low
one is evidence the audit macro-F1 is inflated, and by roughly how much.

Binding conditions, all of which are protocol and none of which may be relaxed later:

1. **Blind labelling.** The agent labelling job sees only the review `title`, `text`, and the
   frozen taxonomy. It must never see `qwen3:8b`'s output, its own earlier labels for the
   same review, the star rating, the product, or the window the review came from. Any
   labelling run that had access to the model under test is void.
2. **Provenance is on every row.** `label_source="human"` is reserved for Philip.
   Agent ground truth is written with `label_source="agent_reference"` and its own
   `model_id`, distinct from both `local_llm` (the system under test) and `human`. No table,
   chart, or sentence may present agent labels as human labels.
3. **The adjudication set is drawn before Philip sees anything.** 50 audit reviews, stratified
   over the ten themes plus `other`, drawn by the seeded `draw_key` already used for the
   discovery sample. Philip labels them from the same tool and the same taxonomy, blind to
   the agent's labels for those rows.
4. **The reported number is agreement, not correction.** Philip's 50 do not overwrite the
   agent's labels and are not used to re-tune anything. They produce one measured row:
   per-theme and overall agreement, Cohen's kappa, and the Wilson interval.
5. **The 0.70 macro-F1 bar does not move** — frozen in ADR-0003 before any measurement,
   unchanged through RR-19 and unchanged here.
6. **The evaluation table states the limitation in the verdict column, not a footnote.** The
   P6 row reads as measured against agent-generated ground truth with the Philip-agreement
   figure beside it. If agreement is weak, the theme-shift table inherits the caveat.

What is lost relative to RR-19, stated plainly so no later reader has to infer it: the 40
five-day-apart repeats measured *human* intra-annotator stability, which is what justified
treating one person's labels as a reference standard. Re-running them against an agent at
temperature 0 measures sampling noise, not annotator stability, and would produce a
near-perfect kappa that means nothing. **The repeat-kappa measurement is therefore dropped
for the agent-labelled sets and reported as `NOT_RUN`, reason "intra-annotator stability is
not defined for a deterministic labeller."** It is not silently replaced with a number that
looks like it.

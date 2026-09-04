---
id: RR-09
title: The question the presentation opens with
type: grilling
status: open
assignee: unassigned
blocked-by: []
blocks: [RR-13, RR-14, RR-18]
---

## Question

The audit puts results & insights at 2/15 today and warns of "60% are 5-star" findings; the
grade table's projected shortfall is "no sharp question yet". Two candidates are on the
table:

- **"Which products got worse, and why?"** — rating decline across the 1,902 products with
  ≥50 reviews, explained by an aspect shift. Uses gold's drift calculation and gives the
  aspect phase something to be *for*.
- **"Which reviews should you not trust?"** — review bursts and incentivised-review
  detection: 6,139 exact duplicate `(user, product, timestamp)` triples, unverified-purchase
  spikes, near-duplicate text found via embeddings. Uses the embedding phase for something
  other than search.

The artifact says "decide on the data, after gold exists" — but gold is execution and this
map ends before it. So the decision this ticket must actually make is one of:

- commit to one question now, on the profile evidence already measured; or
- commit to the **procedure** that picks it: the specific numbers to compute from gold, and
  the threshold at which each question is judged to have a real answer behind it.

Either way the resolution must name:

1. the opening question (or the decision rule and its inputs);
2. what a *good* answer looks like as a number, so the gate stops being "three insights
   written down";
3. whether the second candidate survives as a supporting thread or is dropped — under three
   weeks, two insight threads may be one too many;
4. what the answer is if the data does not support the chosen question — the fallback, named
   in advance rather than improvised.

Discipline to hold, from the artifact's own gate: "an outlier you cannot narrate is a bug,
not an insight." Each finding is reproduced on a slice in pandas and must match Spark.

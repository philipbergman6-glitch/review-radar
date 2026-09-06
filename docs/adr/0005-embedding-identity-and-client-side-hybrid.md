---
status: accepted
date: 2026-09-06
---

# Embeddings carry a hashed identity; hybrid retrieval is fused client-side

The vector cohort is every deduplicated silver review whose `text` has at least twenty
whitespace-separated words — the profile's own predicate, frozen verbatim — never the
≥ 50-review product set, whose minimum count belongs to the protocol freeze. Vectors live in
`gold.review_embeddings` under an **embedding identity** (model, pinned revision, sequence
length, normalisation, text preparation version, dimension) whose canonical hash is the
cache key; execution settings (batch size, threads, device) are recorded in a run manifest
and never hashed, so a cheaper run does not invalidate a cache. Hybrid retrieval is two
Elasticsearch calls fused in Python by unweighted reciprocal rank fusion, ties broken by
review id.

## Rules

- Membership is decided by `text` alone; the embedded input is title plus text. The LLM wire
  contract keeps title and text as separate fields and is not shared.
- The gate compares ID sets — silver cohort, active-spec Iceberg rows, vector-bearing ES
  documents — never counts.
- Retrieval quality is evaluated twice and the tables are never merged: a **controlled
  comparison** on the vector cohort (BM25 filtered, kNN, hybrid filtered) and the
  **production rows** (unfiltered BM25, production hybrid over all reviews plus cohort kNN).
- Hypotheses on macro P@5 are frozen before the first kNN result is inspected and reported
  as held or not; no relevance number is a pass line.
- An unjudged document is never scored non-relevant; pools are expanded and every system is
  recomputed on the complete judgements.

## Considered and rejected

- **RRF server-side** — HTTP 403 on the basic licence in 8.17; a trial licence would expire
  before the demo and hide the per-hit decomposition the demo shows.
- **Tie-break by BM25 rank** — silently weights the fusion toward lexical matches.
- **Hashing batch size and thread count into the identity** — would force a full re-embed
  for a change that cannot alter a vector.
- **Truncating to 64 or 128 tokens** — saves minutes on a sixteen-minute job at the price of
  an unevaluated quality caveat.
- **Gating on hybrid ≥ BM25 or on "five disagreeing queries"** — outcomes chosen before any
  retrieval has run would be numbers picked for convenience.

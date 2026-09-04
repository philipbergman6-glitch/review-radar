---
id: RR-06
title: Embedding scope and how the hybrid retriever fuses
type: grilling
status: open
assignee: unassigned
blocked-by: [RR-01, RR-04, RR-05]
blocks: [RR-11, RR-13, RR-14, RR-17]
---

## Question

With the throughput number and the RRF licence answer in hand, settle the Elasticsearch and
embeddings phase.

1. **Scope.** Embed the ≥20-word reviews of the 1,902 products with ≥50 reviews, or all
   349,059 ≥20-word reviews? The first is fast and defensible; the second is hours of CPU.
   State the chosen population as a **count**, because the phase gate is "index document
   count equals the silver cohort count" and that identity needs both sides defined.
2. **Fusion.** RRF if the licence permits it, manual rank fusion if not — and either way,
   the one-sentence explanation of how the two rankings combine.
3. **Vectors in the lake.** Store the embeddings in Iceberg as well as Elasticsearch, so the
   index can be rebuilt from the lake without re-embedding? This is cheap insurance against
   a demo-day index loss and makes the ES index derived rather than primary.
4. **What "5 queries where kNN ≠ BM25" means concretely.** The gate needs a rule for what
   counts as a disagreement (different top-1? no overlap in top-5?) and how the 20-query
   relevance judgement is scored, since "read aloud once" is not a number.

The framing to protect, from `docs/course-coverage.md` Finding 3: the course establishes
BM25 and names its weakness verbatim (`BM25 does not consider the semantic meaning of the
query terms or the documents`), while `dense_vector` and `kNN` appear zero times in 860
slides. The demo measures that named weakness and closes it in the same engine. Avoid the
overstatement the audit red-teams — BM25 is a strong baseline and still wins on model
numbers and brand names; hybrid is the honest claim, not "embeddings are better".

Also settle here, or hand back to `RR-01`: whether the explicit mapping and custom analyzer
(lowercase + stop + stemmer, a `.keyword` subfield for aggregations, `dense_vector` as its
own explicit field) are part of this phase's deliverables and its gate.

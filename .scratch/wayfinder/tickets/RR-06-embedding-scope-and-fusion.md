---
id: RR-06
title: Embedding scope and how the hybrid retriever fuses
type: grilling
status: closed
assignee: philip
closed: 2026-09-06
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

## Input from RR-01 (closed 2026-09-06)

The ES phase is split: **P4 Search** (mapping contract, analyzers, review search index over
*every* validated silver row with non-empty text, `product_month` projection, Kibana) precedes
**P5 Embeddings**, which this ticket now scopes. Consequences: item 1 is only the *vector*
cohort — the `dense_vector` field is already declared in the Search mapping contract and is
simply absent on non-cohort documents; item 4's 20-query judgement set is **built in Search**
(frozen queries, blind pooled judging, P@5 + MRR@10 or nDCG@10) and reused here — define
"disagreement" against it rather than building a second set; it is not the RAG gold set. Item
3 is settled in principle by ADR-0004: ES indices are serving projections, so vectors in
Iceberg are the rebuild source, and the question left is only the Iceberg table shape. The
"hand back to RR-01" question is answered: mapping and analyzer are Search's, not this
phase's.

## Grilling log

Two rounds, 2026-09-06. Q1–Q9 in round one, Q10–Q16 in round two; Philip's answers
amended nearly every recommendation and are recorded in the Answer as decided, not as
proposed. Facts used: RR-04 (RRF 403 on basic), RR-05 (rates, cohort counts, memory),
`scripts/profile_data.py:52` (the word predicate), `docker-compose.yml:95,107` (ES 1 GB
heap, 2 GB cap), ADR-0003 (`gold.review_theme_labels` shape), ADR-0004 (projection rule).

## Answer

Closed 2026-09-06. Terms in `CONTEXT.md` under *Embeddings and retrieval*; identity and
retriever rules in `docs/adr/0005-embedding-identity-and-client-side-hybrid.md`.

**1. Vector cohort = every deduplicated, validated silver review whose `text` has ≥ 20
whitespace-separated words.** Title never affects membership. Predicate frozen verbatim from
the profile, with explicit null handling, in one importable function used by the profile,
the embedder and the gate:

```python
text.isNotNull() & (trim(text) != "") & (size(split(trim(text), r"\s+")) >= 20)
```

Edge-case tests: null, blank, tabs, repeated whitespace, Unicode whitespace. The raw ceiling
is 349,059 `[observed, profile]`; the gate value is whatever deduplicated silver yields, and
the gate compares **ID sets**, not counts: silver cohort IDs = Iceberg IDs for the active
embedding specification = ES IDs where the vector field exists. Rejected: the ≥ 50-review
product cohort (123,510) — it couples retrieval to a minimum count that belongs to the
protocol freeze (ADR-0001); every non-empty review (~700k) — doubles cost for text too short
to carry meaning and contradicts README's stated population.

**2. Embedded input** = `title.strip() + ". " + text.strip()` when title is non-empty, else
`text.strip()`; one deterministic representation, versioned as `text_preparation_version`.
It shares normalisation helpers with the LLM path but **not** the final serialisation: the
LLM wire contract sends title and text as separate JSON fields (`docs/LLM_LABEL_RUNBOOK.md`)
and is not to be changed. `prepared_text_sha256` hashes this exact input.

**3. Embedding identity vs run manifest.** `conf/embedding-spec.json` has two sections.
*Identity* (hashed, canonical JSON → `embedding_spec_hash`): `model_id`
(`sentence-transformers/all-MiniLM-L6-v2`), `model_revision` (pinned HF commit),
`tokenizer_revision` if separately pinned, `max_seq_length: 256`,
`normalize_embeddings: true`, `text_preparation_version`, `dim: 384`, pooling config.
*Run manifest* (recorded, not hashed): `batch_size: 64`, `torch_threads: 4` (explicit, never
"default"), device, dtype, library versions, `pipeline_run_id`. Changing batch size must not
invalidate the cache. Truncation stays at 256: the 128/64-token savings are minutes
(RR-05) and would add an unevaluated quality caveat; "128 keeps p90 intact" was withdrawn
(p90 ≈ 140 tokens before title and special tokens).

**4. Iceberg table `gold.review_embeddings`** (same namespace as the label cache, both
model-derived, keyed by review). Columns: `source_review_id`, `source_silver_snapshot_id`,
`prepared_text_sha256`, `embedding_spec_hash`, `model_id`, `model_revision`,
`text_preparation_version`, `max_seq_length`, `normalize_embeddings`, `dim`,
`vector array<float>`, `embedded_at`, `pipeline_run_id`. Uniqueness on
`(source_review_id, embedding_spec_hash)`; a changed text upserts the current row for that
spec, Iceberg snapshots keep history. Skip rule: a row with the same review, spec hash and
prepared-text hash is a cache hit. Silver is never widened with a vector column. Embedding
runs **driver-side** in a separate Python process, streaming bounded batches from Spark —
never `collect()` of the whole cohort, never torch inside Spark workers (map Out of scope).

**5. Fusion = unweighted client-side reciprocal rank fusion.** Two ES calls, each
`rank_window_size = 50`: BM25 `match` on the Search-chosen analyzer; `knn` with `k = 50`,
`num_candidates = 200` (frozen initial engineering value, see item 8). Score
`Σ 1/(60 + rank)`, final size 10, ties broken by **stable review id** — never by BM25 rank,
which would smuggle a lexical preference into equal-weight fusion. Slide sentence: *"Each
engine ranks the reviews its own way; a review's hybrid score is the sum of 1/(60 + its
rank) in each list, so a review near the top of either list rises and one near the top of
both wins."* The per-hit decomposition (BM25 rank, kNN rank, each contribution) is the demo
view server-side RRF could not expose (RR-04). ES field: `similarity: cosine`, `int8_hnsw`
(8.17 default for indexed float vectors) — a memory-risk decision under the 2 GB container
cap, part of the Search mapping contract, not of the embedding identity. No memory
arithmetic is claimed for it; item 9 measures instead.

**6. Two retrieval tables, never conflated.** *Controlled retrieval comparison* — identical
population, the vector cohort: BM25 filtered to vector-bearing docs · kNN · hybrid with both
components filtered. *Production retriever rows* — what the endpoint serves: unfiltered BM25
over every non-empty review, and the **production hybrid** (unfiltered BM25 + kNN over the
cohort). The production hybrid is evaluated and pooled too; unfiltered BM25 alone is not
"what the user gets".

**7. Judgement set — handoff into Search (RR-01) and RR-17.** The 20 frozen queries are
authored in Search as **10 lexical** (brand, model number, named ingredient, spelled
attribute) + **10 descriptive** (naturally phrased information need; *not* engineered for
zero term overlap). Frozen before the first kNN result is inspected: query text, stratum,
information need, per-query relevance rule, evaluation population, primary metric and
aggregation, directional hypotheses. Hypotheses, against **macro-average P@5** per stratum
and overall: BM25 ≥ kNN on lexical; kNN ≥ BM25 on descriptive; hybrid ≥ BM25 overall — all
three are hypotheses to test, and the balanced set is a *diagnostic benchmark*, not an
estimate of production query prevalence. **Binary** relevance; internal states `relevant ·
not_relevant · cannot_judge`, plus `unjudged` as a pooling state; `cannot_judge` is resolved
before scoring or the query is reported incomplete, never coerced to non-relevant. Metrics:
P@5 primary, MRR@10 secondary, every per-query result reported.

**Incremental pooling.** Search judges its analyzer pool. Embeddings then pools every
previously unjudged document in the top 10 of kNN, controlled hybrid and production hybrid,
deduplicated, randomised, retriever hidden, and recomputes **every** system's metrics only
once the expanded judgements are complete; an unjudged top-10 document is never scored as
non-relevant. Report new judgements, total unique judgements, pool provenance. Hybrid must be
pooled because its top 10 draws from ranks 11–50 of its components.

**8. ANN fidelity baseline** (not a quantisation baseline — it measures int8 + HNSW +
`num_candidates` + implementation together). For the 20 queries, exact cosine top-10 over
the float Iceberg vectors, blockwise over contiguous float32, same normalised query
embeddings, deterministic ties; report per-query and mean recall@10 of the ES kNN result.
Pre-registered revision rule `[assumed, stated here]`: if mean recall@10 < 0.95, raise
`num_candidates` to 400, then 800; freeze the first that passes, record every attempt. This
uses no relevance labels, so it is not judgement-set leakage. Done before the judged table.

**9. Memory and latency, reported.** ~1 s continuous sampling of `bd-es` during indexing and
the query benchmark; container restart count and OOM status before/after; request errors and
timeouts; peak memory beside the 2 GB cap; ES `took` and client latency separately; a
labelled warm run after a cold run; retriever/query order randomised over several
repetitions so p50/p95 are more than one observation, and labelled descriptive.

**10. Gate `scripts/gate_embeddings.py` → `EMBED_GATE=PASS|FAIL`.** PASS iff: ID-set
equality across silver cohort, active-spec Iceberg rows and vector-bearing ES docs; exactly
one current vector per cohort review and none outside it; text hash, spec hash, model
revision, dim and source snapshot match; every vector finite, dim 384; all 20 queries succeed
for BM25, kNN and hybrid with 10 distinct results where the benchmark expects 10; judgements
complete through rank 10 for every evaluated system; no request error, OOM or restart during
the run; P@5/MRR@10 tables emitted overall and by stratum. **Outcomes, reported not gated:**
the ranking tables and whether the hypotheses held; the disagreement distribution
(per-query top-5 intersection size and Jaccard, with "≤ 1 shared" kept as one descriptive
count — the old "5 queries where kNN ≠ BM25" is retired as a gate); ANN recall@10; memory
and latency. Exact printed names freeze in RR-13.

**Handoffs written into:** RR-01 addendum (stratified query authoring, frozen fields, pooling
states, incremental pooling), RR-11 (hybrid decomposition as a demo move), RR-13 (gate
constituents above), RR-14 (README "349,059" is a raw ceiling, ES row now BM25 + kNN +
client-side fusion), RR-17 (embeddings evaluation row: two tables, metrics, hypotheses,
pooling); RAG fog line sharpened on the map.

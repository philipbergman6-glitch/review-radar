---
id: RR-04
title: Reciprocal rank fusion on the Elasticsearch basic licence
type: research
status: closed
assignee: philipbergman (claimed 2026-09-04)
blocked-by: []
blocks: [RR-06]
---

## Question

The plan is a hybrid BM25 + kNN retriever fused with reciprocal rank fusion. The audit flags
that RRF's licence tier is unverified ("hybrid (RRF, available in ES 8.17 — verify licence
tier)"), and the running cluster is Elasticsearch **8.17.0**, confirmed green with kNN by
the healthcheck (`docs/AUDIT_REPORT_2026-09-01.md` §2).

Answer, against primary Elastic documentation for 8.17 specifically:

1. Is the `rrf` retriever available under the **basic** (free) licence in 8.17, or does it
   require a paid tier? Quote the licence line from the docs, with the URL.
2. If it is gated, what exactly happens on a basic cluster — an error, or a silent fallback?
3. What is the supported syntax in 8.17 (`retriever` block vs the older `rank: { rrf: … }`
   form), and did it change between 8.x minors?
4. What are the knobs — `rank_window_size`, `rank_constant` — and their defaults?
5. If RRF is unavailable: what is the manual fallback? Two queries scored separately in the
   client and fused by rank position, and what that costs in explainability when the answer
   to "how does your hybrid ranking work?" has to be given out loud.

Also worth capturing while there: whether `dense_vector` kNN search itself has any basic-tier
limits in 8.17 (dimensions, `num_candidates`, `index: true` requirements), since the whole
semantic-search demo rests on it.

Bear in mind `docs/course-coverage.md` Finding 3: the course teaches BM25 and explicitly
names its weakness (`No semantic understanding`), and never mentions `dense_vector` or `kNN`
(0 occurrences across 860 slides). The side-by-side comparison is the strongest AI framing
available — so a fusion method that cannot be explained line by line is a real cost, not a
detail.

**Deliverable.** Findings as a Markdown file in the repo, linked from this ticket.

## Resolution

**Not available on basic.** In 8.17 RRF is `LicensedFeature.momentary("rank-rrf", License.OperationMode.ENTERPRISE)`
(`x-pack/plugin/rank-rrf/.../RRFRankPlugin.java` at v8.17.0); the subscriptions page lists
"Reciprocal Rank Fusion (RRF) for hybrid search" under Enterprise only. Verified live on `bd-es`
(8.17.0, `_license.type = basic`) 2026-09-04: both `retriever.rrf` and legacy `rank.rrf` return
HTTP 403 `security_exception: current license is non-compliant for [Reciprocal Rank Fusion (RRF)]`
at parse time — hard error, no fallback. kNN alone returns 200; `dense_vector`/kNN has no licence
gate (only engineering limits: dims ≤ 4096, `num_candidates` ≤ 10000, `index: true` required).
Documented syntax is `retriever.rrf` with `rank_constant` (default 60) and `rank_window_size`
(default = `size`); `rank`/`sub_searches` deprecated since 8.16.

**Recommendation for RR-06:** fuse client-side — two ES calls (BM25 + kNN, window 50) and
`score = Σ 1/(60 + rank)` in Python — which also lets the demo show the per-hit rank decomposition
the server-side RRF cannot expose. No trial licence.

Findings: `docs/research/RR-04-rrf-licence-es817.md`.

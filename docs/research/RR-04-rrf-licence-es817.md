# RR-04 — Reciprocal rank fusion on the Elasticsearch 8.17 basic licence

Resolves wayfinder ticket RR-04 (`.scratch/wayfinder/tickets/RR-04-hybrid-fusion-rrf-licence.md`).
Date: 2026-09-04. Target: Elasticsearch **8.17.0** (the cluster in `docker-compose.yml`, container `bd-es`).

Tags: `[observed]` = quoted from a primary doc / source file / the live cluster; `[inferred]` = my
deduction from observed facts; `[assumed]` = not verified.

## TL;DR

**RRF is not available on the basic licence in 8.17.** Both the `retriever.rrf` form and the legacy
`rank.rrf` form fail at request-parse time with HTTP 403
`current license is non-compliant for [Reciprocal Rank Fusion (RRF)]`. The feature is gated to
`License.OperationMode.ENTERPRISE` in source. `dense_vector` + kNN is fully available on basic.
Do the fusion client-side: two ES queries, RRF by rank position in Python, `score = Σ 1/(60 + rank)`.

## Q1. Licence tier of `rrf` in 8.17

- `[observed]` The 8.17 reference pages carry **no licence note at all** — a grep for
  `licen|subscription|enterprise|platinum` over
  https://www.elastic.co/guide/en/elasticsearch/reference/8.17/rrf.html and
  https://www.elastic.co/guide/en/elasticsearch/reference/8.17/retriever.html returns zero matches.
  So the ticket's "quote the licence line from the docs" cannot be satisfied from the reference docs;
  the gate is documented only on the subscriptions page and in source.
- `[observed]` Subscriptions page https://www.elastic.co/subscriptions, row
  `Reciprocal Rank Fusion (RRF) for hybrid search` — raw markup of the tier cells (the page is React,
  cells are icons): `plan-basic … hyphen`, `plan-gold … hyphen`, `plan-platinum … hyphen`,
  `plan-enterprise … check-icon`. Same pattern for the row
  `Retrievers: linear, rule, RRF, text similarity re-ranker`. **RRF = Enterprise only.**
  `[assumed]` the `plan-basic` column is the one displayed as "Free and open / Basic"; the header
  labels are JS-rendered and were not extracted.
- `[observed]` Source, tag v8.17.0,
  https://raw.githubusercontent.com/elastic/elasticsearch/v8.17.0/x-pack/plugin/rank-rrf/src/main/java/org/elasticsearch/xpack/rank/rrf/RRFRankPlugin.java
  lines 25–29:
  ```java
  public static final LicensedFeature.Momentary RANK_RRF_FEATURE = LicensedFeature.momentary(
      null,
      "rank-rrf",
      License.OperationMode.ENTERPRISE
  );
  ```
- `[observed]` Elastic pricing FAQ https://www.elastic.co/pricing/faq says "The full suite of
  Elasticsearch Relevance Engine (ESRE) tools are available under the Platinum or Enterprise licenses,
  including … Hybrid ranking that combines retrieval techniques with reciprocal rank fusion (RRF)".
  `[inferred]` The FAQ's "Platinum or" is contradicted by the subscriptions table and the source
  constant; source wins — Platinum would also be refused.
- `[observed]` Live cluster `GET /_license`: `"type" : "basic"`, `"status" : "active"`.
  `GET /_xpack?categories=features` has **no `rrf` key** at all (feature keys present:
  aggregate_metric, analytics, archive, ccr, data_streams, data_tiers, enrich, enterprise_search,
  eql, esql, frozen_indices, graph, ilm, logsdb, logstash, ml, monitoring, rollup,
  searchable_snapshots, security, slm, spatial, sql, transform, universal_profiling, voting_only,
  watcher). `enterprise_search`: `{"available": false, "enabled": true}`;
  `ml`: `{"available": false, "enabled": true}`; `esql`: `{"available": true, "enabled": true}`.
  `[inferred]` `_xpack` cannot be used to pre-check RRF availability; the only check is to send the
  request.

## Q2. What happens on a basic cluster — error, not silent fallback

- `[observed]` Source: the check runs in `fromXContent`, i.e. while parsing the request body,
  before any shard work.
  https://raw.githubusercontent.com/elastic/elasticsearch/v8.17.0/x-pack/plugin/rank-rrf/src/main/java/org/elasticsearch/xpack/rank/rrf/RRFRetrieverBuilder.java
  lines 89–91:
  ```java
  if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
      throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
  }
  ```
  Identical block in `RRFRankBuilder.java` lines 59–61 (legacy form).
  `LicenseUtils.newComplianceException` (x-pack/plugin/core/.../license/LicenseUtils.java lines
  45–53) builds `ElasticsearchSecurityException("current license is non-compliant for [{}]",
  RestStatus.FORBIDDEN, feature)`.
- `[observed]` **Live probe on `bd-es` (8.17.0, basic), 2026-09-04.** Index `rr04_rrf_probe`
  (`text` field + `dense_vector` dims=4, `index: true`, `similarity: cosine`), 3 docs, deleted
  afterwards (`HEAD /rr04_rrf_probe` → 404).

  | request | HTTP | body |
  |---|---|---|
  | top-level `knn` alone | 200 | 3 hits, scores 0.9969 / 0.8904 / 0.5560 |
  | `retriever: {standard: …}` alone | 200 | 2 hits (BM25) |
  | `retriever: {knn: …}` alone | 200 | 3 hits |
  | **(a)** `retriever: {rrf: {retrievers: [{standard…},{knn…}], rank_window_size: 10, rank_constant: 60}}` | **403** | see below |
  | **(b)** legacy `sub_searches: [...]` + `knn` + `rank: {rrf: {rank_window_size: 10, rank_constant: 60}}` | **403** | identical |
  | **(b2)** legacy `query` + `knn` + `rank: {rrf: {}}` | **403** | identical |

  Verbatim body for (a), (b), (b2):
  ```json
  {"error":{"root_cause":[{"type":"security_exception","reason":"current license is non-compliant for [Reciprocal Rank Fusion (RRF)]","license.expired.feature":"Reciprocal Rank Fusion (RRF)"}],"type":"security_exception","reason":"current license is non-compliant for [Reciprocal Rank Fusion (RRF)]","license.expired.feature":"Reciprocal Rank Fusion (RRF)"},"status":403}
  ```
  Hard error, no partial results, no fallback to the first retriever.
- `[observed]` Same message reported by a self-hosted 8.9.1 user:
  https://discuss.elastic.co/t/is-rrf-available-in-the-free-self-hosted-version-of-elastic-search/342354
- `[inferred]` A 30-day trial licence (`POST /_license/start_trial`) would unlock it, because trial
  runs at Enterprise operation mode. Not tested; not recommended for a graded demo (expires, and
  turns a "free stack" claim into a footnote).

## Q3. Syntax in 8.17 and how it changed across 8.x

- `[observed]` 8.17 rrf.html documents only the retriever form:
  ```json
  { "retriever": { "rrf": {
      "retrievers": [ { "standard": { "query": {…} } }, { "knn": {…} } ],
      "rank_window_size": 50, "rank_constant": 20 } } }
  ```
  and states under "Reciprocal rank fusion using sub searches": "RRF using sub searches is no longer
  supported. Use the retriever API instead."
- `[observed]` https://www.elastic.co/guide/en/elasticsearch/reference/8.17/retrievers-overview.html:
  "A retriever is an abstraction that was added to the Search API in 8.14.0 and was made generally
  available in 8.16.0." and "Compare to `RRF` with `sub_searches` approach (which is deprecated as
  of 8.16.0)".
- `[observed]` 8.16.0 release notes
  (https://www.elastic.co/guide/en/elasticsearch/reference/8.17/release-notes-8.16.0.html),
  Deprecations › Search: "Adding deprecation warnings for rrf using rank and sub_searches #114854".
  8.17.0 release notes contain no rrf syntax change (only "Fix for propagating filters from compound
  to inner retrievers [#117914]").
- `[observed]` Source v8.17.0 `server/.../SearchSourceBuilder.java` lines 102–103:
  `new ParseField("sub_searches").withAllDeprecated("retriever")` and
  `new ParseField("rank").withAllDeprecated("retriever")`; `RRFRankBuilder` is annotated
  `@Deprecated` (line 42). So in 8.17 the legacy form still parses (with a deprecation warning
  header) — the live probe confirms it reaches the licence check — but is undocumented.
- `[inferred]` Timeline: 8.8–8.13 `rank.rrf` + `sub_searches` (only form); 8.14 `retriever` added
  (preview); 8.16 retriever GA, legacy deprecated; 8.17 legacy still parsed. `[assumed]` legacy
  form removed in 9.x — not checked.
- `[observed]` The legacy knob name `window_size` appears nowhere in 8.17 docs or v8.17.0 source;
  only `rank_window_size`.

## Q4. Knobs and defaults (8.17 rrf.html / retriever.html, identical wording)

- `[observed]` `rank_constant` — "(Optional, integer) This value determines how much influence
  documents in individual result sets per query have over the final ranked result set. A higher
  value indicates that lower ranked documents have more influence. This value must be greater than
  or equal to 1. Defaults to 60." Source: `RRFRetrieverBuilder.java` line 54
  `public static final int DEFAULT_RANK_CONSTANT = 60;`.
- `[observed]` `rank_window_size` — "(Optional, integer) This value determines the size of the
  individual result sets per query. A higher value will improve result relevance at the cost of
  performance. The final ranked result set is pruned down to the search request's size.
  rank_window_size must be greater than or equal to size and greater than or equal to 1. Defaults
  to the size parameter." `[inferred]` effective default 10 (`SearchService.DEFAULT_SIZE = 10`).
- `[observed]` `retrievers` — "(Required, array of retriever objects) … Each child retriever
  carries an equal weight as part of the RRF formula. Two or more child retrievers are required."
- `[observed]` "if k from a knn search is larger than rank_window_size, the results are truncated
  to rank_window_size."

## Q5. Manual fallback — client-side RRF

Two ES calls (BM25 `match` on the review text; top-level `knn` on the `dense_vector` field), each
asking for `rank_window_size` hits (say 50), then in Python:

```
score(d) = Σ_{lists L containing d}  1 / (k + rank_L(d))      k = 60 (ES default), rank is 1-based
```

sort by `score` desc, take `size`. This is exactly the formula ES implements (rrf.html shows the
same pseudo-code with `k` = `rank_constant`). Cost: two round-trips instead of one and the
`rank_window_size` cap is enforced by us, not the engine. Benefit for the spoken explanation:
every fused score decomposes into "BM25 rank 2 → 1/62, kNN rank 5 → 1/65", which can be shown
per-hit in the demo table — something the server-side RRF cannot expose (it returns only the
fused `_score`, and `explain` is not supported for rrf). Given course-coverage Finding 3 (course
teaches BM25 and names "No semantic understanding"; `dense_vector`/kNN 0 mentions), the
line-by-line explainability is the point of the demo, so the fallback is arguably the better
design even where RRF were licensed. `[inferred]`

## `dense_vector` / kNN limits on basic in 8.17

- `[observed]` Subscriptions page row `Vector search`: check-icon in `plan-basic`, `plan-gold`,
  `plan-platinum`, `plan-enterprise`. Row `Retrievers: Standard, kNN, pinned, rescorer`: check in
  basic. No licence note on dense-vector.html or knn-search.html (grep zero). Live probe: kNN
  returned 200 on the basic cluster.
- `[observed]` https://www.elastic.co/guide/en/elasticsearch/reference/8.17/dense-vector.html:
  - `dims` — "Number of vector dimensions. Can't exceed 4096." Live: `dims: 4096, index: true`
    → 200; `dims: 4097` → 400 `The number of dimensions should be in the range [1, 4096] but was [4097]`.
  - `element_type` — "The supported data types are float (default), byte, and bit." bit: "the
    number of dimensions must be a multiple of 8". Live: `byte` + `dot_product`, `bit` dims=8, and
    `index_options: {type: int8_hnsw}` all accepted.
  - `index` — "If true, you can search this field using the kNN search API. Defaults to true."
    "Indexing is enabled by default for dense vector fields and indexed as int8_hnsw." Live: a field
    mapped with only `dims` comes back as `index: true, similarity: cosine, index_options:
    {type: int8_hnsw, m: 16, ef_construction: 100}`. A field with `index: false` → kNN 400
    `to perform knn search on field [vn], its mapping must have [index] set to [true]`.
  - `similarity` — "Defaults to l2_norm when element_type: bit otherwise defaults to cosine";
    values `l2_norm`, `dot_product` ("all vectors must be unit length" for float), `cosine`
    ("automatically normalizes vectors … to unit length"), `max_inner_product`.
  - `index_options.type`: hnsw, int8_hnsw (default), int4_hnsw, bbq_hnsw, flat, int8_flat,
    int4_flat, bbq_flat.
- `[observed]` https://www.elastic.co/guide/en/elasticsearch/reference/8.17/search-search.html:
  `num_candidates` — "Needs to be greater than k, or size if k is omitted, and cannot exceed
  10,000. … Defaults to Math.min(1.5 * k, 10_000)." `k` — "must be less than or equal to
  num_candidates. Defaults to size." Live: `num_candidates: 10001` → 400
  `[num_candidates] cannot exceed [10000]`; `k: 11, num_candidates: 10` → 400
  `[num_candidates] cannot be less than [k]`.
- `[observed]` Not free: ELSER ("To use ELSER, you must have the appropriate subscription level",
  https://www.elastic.co/guide/en/machine-learning/8.17/ml-nlp-elser.html; subscriptions table:
  Platinum + Enterprise), Elastic Rerank / Elastic Inference Service (Enterprise). Live `_xpack`
  shows `ml.available: false`. `[inferred]` Embeddings must therefore be computed outside ES (the
  plan's local `EMBED_MODEL`) and written as plain float vectors — which is what the pipeline does.

## Recommendation for RR-06

Keep hybrid = BM25 + kNN, fuse client-side with RRF (`k = 60`, window 50) in the demo/query layer;
do not depend on the `rrf` retriever, do not start a trial licence. Show the per-hit rank
decomposition in the side-by-side table.

## Method

Docs fetched 2026-09-04 (8.17 reference, subscriptions raw HTML, pricing FAQ); source read at git
tag `v8.17.0` via raw.githubusercontent.com; live probes with curl against `http://localhost:9200`
(security disabled in `docker-compose.yml`, no credentials). Probe index deleted; cluster unchanged.

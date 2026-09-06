---
id: RR-01
title: Where Kibana and the explicit ES mapping live
type: grilling
status: closed
assignee: philip
closed: 2026-09-06
blocked-by: []
blocks: [RR-06, RR-11, RR-13, RR-14]
---

## Question

Two documents put Kibana in two different places, and the explicit Elasticsearch mapping is
promised in a phase that does not index anything.

- `docs/course-coverage.md` Decisions §1: "Kibana — ADD. **Committed to Phase 2.**" and §2:
  "Explicit ES mapping + custom analyzer — ADD. **Committed to Phase 2.**"
- The build-plan artifact's Phase 2 is silver only (parse, quarantine, dedupe, JDBC join);
  Elasticsearch first appears in Phase 4; Kibana appears only inside the Phase 8 open
  decision "Streamlit, or a notebook plus Kibana".

So: fix the phase numbering for P2–P8 such that (a) the Kibana **container** has a home,
(b) the Kibana **dashboard over gold** has a home, (c) the explicit mapping and custom
analyzer have a home, and (d) each phase still has one coherent gate.

Decide and record:

1. Does Kibana-the-container become infrastructure done at any time (a compose service
   against the already-running Elasticsearch), separate from Kibana-the-dashboard? If so,
   which phase owns each?
2. Do the explicit mapping and custom analyzer belong to the Elasticsearch phase where
   indexing is actually written, or do they stay a separate committed item?
3. Does the numbering P2 Silver / P3 Gold / P4 ES+embeddings / P5 Aspects / P6 RAG /
   P7 Stream / P8 Deliverables survive, or does it change? If it changes, the new numbering
   is authoritative for every other ticket on this map.

Constraint to honour: `docs/course-coverage.md` §2 argues the mapping work "costs almost
nothing and is the difference between 'used Elasticsearch' and 'understood Elasticsearch'"
against the largest deck (264 pages, `analyzer` 54, `tokenizer` 13, `inverted index` 7).
Wherever it lands, it must not be droppable when time gets tight.

The resolution must state the final phase list with one line each, because several other
tickets refer to phases by number.

## Grilling log

### Round 1 — 2026-09-06

Facts checked first `[observed]`: `docker-compose.yml` has kafka, minio, minio-init,
elasticsearch 8.17.0 (`xpack.security.enabled: false`, 1 GB heap, `mem_limit: 2g`), postgres
— **no Kibana service**. Kibana reads only Elasticsearch, so "a dashboard over gold" implies
a gold-derived index nobody had named. Audit §9 P4 bundles mapping, BM25, embeddings, kNN and
RRF; RR-09/07/08/16 have since rewritten Gold, Aspects and lineage, so the phase list needed
restating regardless. Phase numbers are cited outside this ticket only in RR-14 ("Phase 2")
and RR-11 ("Phase 8").

Settled `[observed, Philip]`:

1. **Kibana splits into container and dashboard, both owned by the Search phase.** The
   container is infrastructure behind a compose profile `ui`; the dashboard is a Search
   deliverable over the `product_month` serving projection. `make health` may report Kibana,
   but availability alone completes nothing.
2. **Mapping and analyzer are the first Elasticsearch deliverable of Search**, enforced as a
   *mapping contract*: explicit settings and mappings, `"dynamic": "strict"`, indexer-side
   required-field validation (strict mapping does not enforce presence), analyzer tests, and
   a contract test that verifies field types and rejects unknown fields. Corrections to the
   proposal: no `.keyword` subfield on the full review body; stemming is **tested, not
   assumed**; the `dense_vector` field is defined in Search for one stable schema but may be
   absent from documents until Embeddings.
3. **The ES phase splits** (option b): Search (mapping, analysis, BM25, indexing, Kibana)
   then Embeddings (MiniLM, kNN, fusion, relevance evaluation). One baseline before one
   comparison; no debugging indexing, vectors, ranking and visualisation at once.
4. **Kibana runs under a compose profile.** `make up` = core; `make up-ui` = core + Kibana,
   pinned to exactly 8.17.0. Saved-object export lives in the repo; import is idempotent;
   Kibana gets a health check; the runbook documents the extra memory.
5. **Gold before Search.** `product_month` is a derived projection rebuilt deterministically
   from Iceberg gold; Search consumes stable data products rather than inventing schema while
   Gold moves. Under schedule pressure, simplify Gold's machinery rather than reverse the
   dependency.
6. **Phase list** accepted with two structural changes from Philip: RAG and Stream are
   *conditional* (see Answer — recorded as input to RR-12, not cut here), and Deliverables is
   a continuous **track**, not a ninth phase. Names are the durable references; numbers are
   ordering labels.

### Round 2 — 2026-09-06

Kibana memory `[observed, Elastic docs]`: pre-9.4 Kibana sizes the Node heap at 50% of
available memory; Elastic recommends 2 GB per instance for advanced features. Starting
point `[inferred]`: `mem_limit: 1g`, `NODE_OPTIONS=--max-old-space-size=768`, confirm with
`docker stats` when Search lands; the runbook's Supabase-stop step is mandatory for `up-ui`.

Settled `[observed, Philip]`:

7. **Review search index population** = every deduplicated, validated silver row with
   non-empty review text. Gate identity `reviews_index_docs == silver_searchable_reviews`;
   no hard-coded count. Deterministic document ids from the stable silver review identifier
   so rebuilds are idempotent.
8. **Stemming evaluation.** Two analyzers on the same index: `text` (stemmed) and
   `text.unstemmed` (not "exact" — lowercasing and stop removal are not exact analysis).
   A frozen 20-query relevance set, judged on the pooled union of both analyzers' results
   with the analyzer hidden; report P@5 at least, MRR@10 or nDCG@10 also. The judgement set
   is reused for BM25, kNN and hybrid, but is **not** the RAG gold set (RAG needs
   answerability, citation and faithfulness judgements). The chosen default is recorded in a
   small decision/config file; mapping comments explain mechanics only.
9. **`product_month` projection.** Never delete the live index. Build
   `product_month_<gold_snapshot_id>` → validate count, mapping and snapshot metadata →
   atomic alias swap → retain the previous generation, clean up separately. Deterministic
   `(product_id, month)` ids; every document carries `source_gold_snapshot_id`; the
   alias-to-snapshot match is part of the lineage gate (RR-16). The Search dashboard reads
   only the alias. Project only the gold population P3 actually materialises — do not
   silently expand 112,590 products across their lifetimes.
10. **Search gate** is one composite script, `scripts/gate_search.py`, printing every
    constituent then `SEARCH_GATE=PASS|FAIL`. Pass only when: reviews count identity;
    `product_month` count identity; alias points at the expected gold snapshot; contract
    tests N/N; required-field validation passes; unknown-field rejection demonstrated; saved
    objects import and the dashboard exists; the judgement set holds all 20 completed
    queries. Analyzer scores are outcomes, not thresholds — the gate checks the comparison
    was completed and recorded; the higher-scoring analyzer becomes the frozen BM25 default.
11. **Deliverables track** owns artefacts, rehearsal records and the playable backup; RR-11
    owns the ordered demo moves. Acceptance, not a rehearsal count: two consecutive complete
    rehearsals, each within the time limit, every move succeeding or using its documented
    fallback, a playable backup within the limit, and README, design doc, slides and runbook
    present.
12. **Glossary**: *Review search index*, *Serving projection*, *Mapping contract* added to
    `CONTEXT.md`. *Phase* and *Track* are planning mechanics and live on the map, not in the
    glossary. Embeddings stored in Iceberg are derived data, not a projection; a future
    vector-enabled reviews index is one.

## Answer

Closed 2026-09-06 after two grilling rounds (log above). Terms in `CONTEXT.md` under
*Search serving*; the projection rule in `docs/adr/0004-elasticsearch-holds-serving-projections.md`.

**1. Kibana** has two deliverables, one home: the **Search phase**. The container is a
compose-profile service (`ui`, `make up-ui`, image pinned 8.17.0, health-checked, memory
documented in the runbook). The dashboard is a repo-stored saved-object export, imported
idempotently, reading only the `product_month` alias.

**2. The explicit mapping and custom analyzer** are the first deliverable of Search, as a
**mapping contract** (`"dynamic": "strict"` + indexer required-field validation + executable
contract and analyzer tests). Undroppable structurally: the indexer cannot write without it,
and the gate cannot pass without the tests. Stemmed vs unstemmed is a measured result on a
frozen 20-query set, not an assumption. The vector field is declared here, populated in
Embeddings.

**3. The numbering changes.** Authoritative list — names are durable, numbers are order:

- **P2 Silver** — catalogue load, parsing, quarantine, collision classification,
  deterministic dedupe, JDBC enrichment, reconciliation, lineage row.
- **P3 Gold** — product-month calendar spine and metrics, frozen decline rule, holdout
  execution, numerical findings.
- **P4 Search** — mapping contract, analyzers, review search index (BM25), `product_month`
  serving projection, profile-gated Kibana, reproducible dashboard.
- **P5 Embeddings** — MiniLM vectors, kNN, client-side fusion, relevance evaluation on the
  Search judgement set.
- **P6 Themes** — frozen complaint taxonomy, LLM labelling, human audit, theme-shift
  results; MLlib and star-only baselines within their time cap.
- **P7 RAG** — *conditional* — grounded retrieval, citations, answerability and
  faithfulness evaluation.
- **P8 Stream** — *conditional* — paced replay, event-time watermark, late-event metrics;
  no pipeline rebuild merely to claim streaming.
- **Deliverables** — a *track*, not a phase — design doc, slides, README, runbook, recorded
  backup, developed throughout; acceptance criteria in the log (item 11).

*Conditional* means the cut rule is decided in RR-12, which inherits Philip's protected-core
sentence: **Silver → Gold → Search/Kibana → one evaluated AI capability → numerical insights
→ polished deliverables.** Nothing is cut on this ticket; the map's "do not cut scope yet"
stands.

**Gate** for Search: `scripts/gate_search.py`, constituents in log item 10, final line
`SEARCH_GATE=PASS|FAIL`. Thresholds freeze in RR-13.

**Handoffs written into:** RR-06 (index split, sparse vector field, judgement-set reuse,
projection term), RR-10 (Stream conditional), RR-11 (dashboard reads the alias only; the
Deliverables track owns rehearsals), RR-12 (protected core, conditional phases), RR-13
(Search gate constituents, Deliverables acceptance), RR-14 (coverage doc "Phase 2" →
Search), RR-16 (alias-to-snapshot lineage), RR-17 (judgement set is not the RAG gold set),
RR-18 (Deliverables continuous).

## Addendum from RR-06 (closed 2026-09-06) — requirements on the Search judgement set

Search authors the 20 queries as **10 lexical + 10 descriptive** (naturally phrased; not
engineered for zero term overlap) and freezes, before any kNN result is seen: query text,
stratum, information need, per-query binary relevance rule, evaluation population, primary
metric (macro-average P@5; MRR@10 secondary) and the RR-06 directional hypotheses. Judgement
states are `relevant · not_relevant · cannot_judge`, plus `unjudged` as a pooling state;
`cannot_judge` is never coerced. Search judges only its analyzer pool; Embeddings pools the
new kNN/hybrid top-10 docs later and recomputes everything, so store judgements with pool
provenance and keep the judging CLI reusable, retriever hidden, presentation randomised.
The vector field is `similarity: cosine`, `int8_hnsw`, dim 384, in the mapping contract.

---
status: accepted
date: 2026-09-06
---

# Elasticsearch holds serving projections, never the system of record

Iceberg on MinIO is the system of record for every layer. Every Elasticsearch index is a
**serving projection**: rebuilt deterministically from an Iceberg table or snapshot, with
deterministic document ids, under an explicit **mapping contract** (`"dynamic": "strict"`,
indexer-side required-field validation, executable contract and analyzer tests). Losing an
index costs a rebuild, never data; the demo-day index can be regenerated from the lake.

## Rules

- The review search index holds every deduplicated, validated silver row with non-empty
  text; document ids derive from the silver review identifier, so re-indexing is idempotent.
- `product_month` is rebuilt per gold snapshot as `product_month_<gold_snapshot_id>`,
  validated, then reached by an atomic alias swap; the previous generation is retained and
  cleaned up separately; every document carries `source_gold_snapshot_id`. Readers,
  including Kibana, use only the alias.
- The `dense_vector` field is declared in the contract before any vectors exist; documents
  outside the embedding cohort simply omit it.
- Analyzer choice (stemmed vs unstemmed) is a measured result on a frozen judged query set,
  recorded in a decision file, not an assumption in a mapping comment.

## Considered and rejected

- **Elasticsearch as primary store for the AI layer** — one fewer write path, but an index
  loss would be a data loss, and the course's own framing (lakehouse + search engine) is the
  opposite.
- **Dynamic mappings** — cheaper today; forfeits the text-analysis credit the largest
  course deck teaches and makes field drift invisible.
- **Delete-and-reindex the live projection** — simplest; leaves the dashboard empty or
  half-built during every rebuild and breaks the lineage identity.

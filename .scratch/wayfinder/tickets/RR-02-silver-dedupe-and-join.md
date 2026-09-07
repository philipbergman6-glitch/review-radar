---
id: RR-02
title: Silver dedupe rule and catalogue-join strategy
type: grilling
status: closed
assignee: philip
closed: 2026-09-07
blocked-by: []
blocks: [RR-13, RR-14, RR-16]
---

## Question

Two coupled decisions the silver phase cannot start without.

**1. The dedupe rule.** The profile finds 6,139 duplicate `(user_id, parent_asin,
timestamp)` triples. Which row survives?

- keep first by Kafka `(partition, offset)` — deterministic, replay-stable, trivially
  explainable; or
- keep the row with the highest `helpful_vote` — arguably the "best" record, but not
  replay-stable and needs a tiebreak.

Whichever wins must survive the follow-up: "your pipeline is exactly-once, so where did
6,139 duplicates come from?" (they are in the source file, not introduced by the pipeline —
that distinction is the answer, and it should be stated in the resolution). Note also that
the duplicates are themselves a candidate insight — review bursts / incentivised reviews —
so the decision should say whether dedupe **drops** them or **quarantines** them somewhere
the gold layer can still count.

**2. The catalogue join.** `products` is empty today (audit F2: `select count(*) from
products` → 0). Once loaded with 112,590 rows, how does silver join it?

- JDBC read per micro-batch from PostgreSQL — literally "enrichment from SQL", matches the
  brief's option, but re-reads per batch; or
- snapshot the catalogue into an Iceberg table once and broadcast the join — faster, still
  sourced from PostgreSQL, but the SQL step becomes a one-off load rather than part of the
  stream.

The audit calls the second "faster and still 'enrichment from SQL'". Decide, and state the
answer to "so is PostgreSQL actually in your pipeline, or just holding the Iceberg
catalogue?" — today the honest answer is the latter (audit §3, course technologies row).

**Gate consequence.** The silver gate is
`count(bronze) == count(silver) + count(rejects) + duplicates_removed`, printed by the job.
Both decisions change what `duplicates_removed` means, so the resolution must state the
exact printed identity.

## Input from RR-09 (closed 2026-09-04)

- The 6,139 figure counts **key-collision groups**, not rows (`scripts/profile_data.py:99`).
  Silver must classify collisions: **exact duplicate** (all meaningful fields agree),
  **conflicting** (content or rating differs), **unresolvable**. Deterministic survivor rule
  for conflicting; non-survivors kept in an auditable table, not dropped.
- Print both the number of collision groups and the number of rows removed; the gate
  identity must name which.
- Framing is a **provenance result** only: collisions come from the source file;
  exactly-once says nothing about repetition inside the source. Never "trust",
  "incentivised", "bursts" — those are out of scope (map, Out of scope).
- The pandas reproduction (RR-09) re-implements the dedupe from the written rule with no
  shared code, so the rule must be one paragraph of logic.

## Resolution (2026-09-07)

Grilled in two rounds; collision anatomy measured on the raw file first. Decision detail in
**ADR-0007** (`docs/adr/0007-silver-batch-dedupe-and-catalogue-join.md`); terms in
`CONTEXT.md` *Data quality*. Numbers below tagged `[observed]` come from
`scripts/profile_collisions.py` over `data/raw/All_Beauty.jsonl`; everything else is
provisional until silver prints it.

### The measured facts

| measure | value |
|---|---|
| collision groups `(user_id, parent_asin, timestamp)` | 6,139 `[observed]` |
| rows in those groups | 13,415 `[observed]` |
| exact-duplicate groups | 6,138 `[observed]` |
| conflicting groups | 1 — differs only in `helpful_vote` `[observed]` |
| groups disagreeing on rating or text | 0 `[observed]` |
| group sizes | 2: 5,379 · 3: 514 · ≥4: 246 (max 10) `[observed]` |
| looser key `(user_id, parent_asin)` | 6,437 groups — would wrongly merge 298 genuine repeat reviews `[observed]` |
| `asin` mapping to >1 `parent_asin` | 0 `[observed]` |
| empty-text rows inside collision groups | 26 `[observed]` |

The source's collisions are byte-identical repeats. "6,139 duplicates" (README, coverage
doc) undercounts: the removed-row figure is 7,276 if every group has one survivor (RR-14).

### 1. Processing model — batch is authoritative

Silver is a bounded Spark batch pinned to an explicit bronze Iceberg snapshot id. Each run
atomically replaces `silver.reviews`, `silver.rejects` and `silver.review_collisions`
(no append, no per-snapshot copies). If P8 survives RR-12, streaming silver is a separate
demo table/path that reconciles with batch after the bounded replay; it never replaces
authoritative silver (input to RR-10 item 4).

### 2. Validation first, then collisions

Fixed precedence, four mutually exclusive reasons: `unparsable_json` → `missing_key_field`
(`user_id`, `parent_asin`, `timestamp` null/blank) → `invalid_rating` (null, non-numeric,
non-finite, non-integral, or ∉ {1,2,3,4,5}) → `timestamp_out_of_range` (`< 1995-01-01T00:00Z`
or `> ingested_at` of that bronze row; two named UTC constants; never job time). Rejects
carry raw payload, reason, optional diagnostics. Empty title or text is **not** a reject.
Rejects never enter collision classification.

### 3. Collision classes and the survivor rule

Over valid rows, grouped by `review_id`:

- **exact** — all retained fields agree (rating, title, text, verified_purchase,
  helpful_vote, asin, images). One row survives, the rest are removed.
- **conflicting** — same rating, some other retained field differs. Survivor = highest
  `helpful_vote` (null < every integer), then longest `text` (null = 0), then lowest
  SHA-256 of the canonical row. Content-only, no arrival order: replay-stable by
  construction. Verified purchases are **not** preferred (would bias the verified-mix
  robustness check).
- **unresolvable** — rating disagreement. No survivor; the whole group goes to the
  collision table, none reaches silver.

`silver.review_collisions` holds every row of every group: `collision_group_id`
(= `review_id`), `collision_class`, `differing_fields`, `is_survivor`, selection rank and
reason, canonical row hash, raw payload, Kafka coordinates, `source_bronze_snapshot_id`.

### 4. Identity and encoding

`review_id = SHA-256(canonical(user_id, parent_asin, timestamp_ms))`. Canonical encoding:
fields in fixed order, each as 4-byte big-endian length + UTF-8 bytes; null = length
`0xFFFFFFFF`, empty string = length 0; identity fields (`user_id`, `parent_asin`, `asin`)
trimmed and validated before encoding, `title`/`text` preserved exactly; numbers as decimal
ASCII, rating `1`–`5`, booleans `true`/`false`, images as compact JSON with sorted object
keys and array order preserved; hex lowercase. Survivor hash covers the seven retained
fields. One Python function and one Spark expression, tested against a golden-vector table
(null vs empty, Unicode, embedded separators, booleans, numeric extremes). That test proves
encoding parity; the independent pandas reproduction (RR-09) proves the whole rule.

### 5. Catalogue: loader and join

`scripts/load_products.py`: stream meta JSONL with orjson → psycopg `COPY` into a staging
table → validate count and key uniqueness → transactional replace of `products`. Each load
gets a `pipeline_runs.run_id`, stored as `catalogue_load_id` on every product (`loaded_at`
is operational metadata, not identity). Price: trimmed, frozen optional-dollar decimal
regex; ranges and junk → NULL. Loader prints `source_rows`, `parsed_prices`,
`missing_prices`, `invalid_prices`, `rows_loaded`, `final_count`. No `details` keys are
parsed or loaded; `details` remains in the raw metadata JSONL (no raw product Iceberg table
exists; add one only when a consumer needs it).

Silver reads `products` over Spark JDBC once per run, requires exactly one distinct
`catalogue_load_id`, and **left broadcast-joins** on `parent_asin` — never inner. Silver
carries `product_title`, `main_category`, `store`, `price`; downstream reads product
attributes from silver, never PostgreSQL. Prints `unmatched_review_rows` and
`unmatched_parent_asins`; asserts the join did not change cardinality.

**"Is PostgreSQL in the pipeline?"** Yes, in two roles: Iceberg catalogue (atomic metadata
pointer swap) and the relational catalogue source every silver run reads over JDBC. It is
the ingestion of the catalogue that is a one-off load; the enrichment happens in the
transformation.

### 6. Silver shape

`silver.reviews`, partitioned by `review_month` (UTC first day of the event's calendar
month): `review_id, parent_asin, asin, user_id, event_ts, review_month, rating (int), title,
text, text_word_count, verified_purchase, helpful_vote, image_count, product_title,
main_category, store, price, kafka_partition, kafka_offset, source_bronze_snapshot_id`.
Image URLs omitted (bronze keeps them). `text_word_count` = Python-equivalent whitespace
split incl. Unicode whitespace, null/blank → 0; tested on tabs, repeated spaces, Unicode
whitespace (RR-05 found a 14-row Spark/Python discrepancy).

### 7. The printed gate

```
SILVER_REJECT_REASON reason=unparsable_json rows=N        (one line per reason, zeros too)
SILVER_REJECT_REASON reason=timestamp_out_of_range rows=N below_1995=N after_ingest=N
SILVER_COLLISIONS groups=G exact_groups=E conflicting_groups=C unresolvable_groups=U table_rows=T removed=X
SILVER_GATE bronze_snapshot=<id> catalogue_load_id=<run_id> catalogue_rows_read=112590
  bronze_rows=701528 reject_rows=R silver_rows=S collision_rows_removed=X
  unmatched_review_rows=0 unmatched_parent_asins=0 join_cardinality_ok=true
  review_id_unique=true identity=PASS gate_scope=full SILVER_GATE=PASS
```

`collision_rows_removed = table_rows − groups + unresolvable_groups`. Pass (full scope):
`bronze_rows == reject_rows + silver_rows + collision_rows_removed`,
`count(distinct review_id) == silver_rows`, join cardinality preserved, both unmatched
counts zero. Sample scope prints the same, threshold on unmatched not applied,
`gate_scope=sample`. Determinism is proven separately: run the same bronze snapshot and
catalogue load twice, compare ids, counts, classifications and a deterministic output
digest (`--verify-rerun`). Expected today: groups 6,139 `[observed]`; the rest provisional.

`pipeline_runs` row per silver run: bronze input snapshot, silver output snapshot,
`catalogue_load_id`, counts above, status, `SILVER_SPEC_VERSION` (manually bumped),
`git_commit_sha`, `worktree_dirty` flag (input to RR-16).

### Handoffs

RR-10 (batch authoritative, streaming beside), RR-13 (gate lines above, determinism rerun
as its own printed check), RR-16 (row contents, loader row, load id), RR-14 (7,276 vs
6,139 wording; PostgreSQL two-roles sentence). Execution note: `make_sample.py` must make
the sampled catalogue cover every sampled review product.

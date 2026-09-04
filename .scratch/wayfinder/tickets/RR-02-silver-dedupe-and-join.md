---
id: RR-02
title: Silver dedupe rule and catalogue-join strategy
type: grilling
status: open
assignee: unassigned
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

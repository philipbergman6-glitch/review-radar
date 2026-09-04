---
id: RR-01
title: Where Kibana and the explicit ES mapping live
type: grilling
status: open
assignee: unassigned
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

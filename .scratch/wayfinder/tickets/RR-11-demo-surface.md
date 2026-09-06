---
id: RR-11
title: Demo surface — Streamlit, or a notebook plus Kibana
type: prototype
status: open
assignee: unassigned
blocked-by: [RR-01, RR-06, RR-08]
blocks: [RR-13, RR-14, RR-18]
---

## Question

What does the grader actually watch? Presentation & demo is 10% and stands at 1/10; the audit
notes the demo currently "has nothing to watch (5 s)".

The options, from the artifact and `docs/course-coverage.md`:

- **Streamlit** — interactive, but `src/serving/` is an empty package, it runs on the host
  alongside a 4 GB Spark driver and torch, and it is not a course technology.
- **A notebook plus Kibana** — cheaper, and Kibana is course-expected verbatim
  (`We will mostly use Kibana throughout this course`, 52 mentions, `Beats  Kafka  Logstash
  Elasticsearch  Kibana` as the taught ELK stack). `notebooks/` is currently empty.
- Some split: Kibana dashboard for the aggregate view, notebook for the search and RAG
  comparisons.

This is a **prototype** ticket, not a discussion: build the cheapest possible mock of the
demo surface — a static wireframe or a stub dashboard — and react to it. The question "how
should it look" is the one that decides this, and it is answered faster by looking than by
arguing.

Inputs the resolution needs, which is why this is blocked:

- the final phase numbering, so Kibana's home is fixed (`RR-01`);
- what the search comparison actually shows on screen (`RR-06`);
- what the aspect evaluation produces, since a precision/recall table wants a page, not a
  dashboard tile (`RR-08`).

The resolution must produce **the list of demo moves in order**, with the surface each one
runs on and its rough time on the clock — the artifact's Phase 8 says the surface should be
chosen "after the four demo moves are known", so naming them is part of this ticket.
Constraint: total live demo of five minutes, following a runbook that has run clean twice,
with a recorded backup. Streamlit only if a demo move genuinely needs interactivity.

Fold in `docs/DEMO_RUNBOOK.md`'s existing live sequence (healthcheck → producer → bronze →
Iceberg snapshots → exactly-once proof, ~65 s for the last) and say which of those survive
once the new phases exist. Also decide whether the deferred-but-cheap Iceberg time-travel
comparison earns a slot.

## Input from RR-08 (closed 2026-09-06)

- The demo **never makes a hosted call**. Every label shown is a cached
  `gold.review_theme_labels` row (raw response retained, auditable on stage).
- One optional live move: `llama3.2:3b` labelling a single review (~4 s, RR-03) under the
  frozen prompt, with a cached fallback. Requires `ollama serve` in the pre-demo checklist.
- The evaluation output is a page (per-theme table, coverage counts, disagreement examples),
  not a dashboard tile.

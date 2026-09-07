---
id: RR-19
title: P6 labeller host under no hosted access, and who hand-labels
type: grilling
status: closed
assignee: philipbergman (decided 2026-09-07)
blocked-by: [RR-03, RR-08, RR-17]  # all closed
blocks: []
---

## Question

ADR-0003 froze **Haiku 4.5 via the Batch API** as P6's primary labeller and made the pass
rule `audit macro-F1 >= 0.70, no supported theme recall < 0.50` against **200 hand-labelled
development + 200 hand-labelled audit reviews**. Two facts block executing it as written:

1. **No hosted access.** `src/common/config.py:55` reads `ANTHROPIC_API_KEY` with default
   `""`; it is not set in the session environment either `[observed 2026-09-07]`. RR-03
   already recorded the key as "declared but empty". Nothing in P4/P5 ever made a hosted
   call: the P4/P5 relevance labels carry `judge=claude` and are disclosed as
   model-generated (`docs/decisions/embeddings-retrieval.md`), which is the agent labelling
   in-session, not the Anthropic API.
2. **Ground truth has no owner.** RR-17 costed the hand labelling at 17-21 h and ADR-0003
   assumes a human produces both 200-row sets. P4/P5 substituted agent labels; doing that
   here makes the labeller and the judge the same model family, so macro-F1 would measure
   self-consistency, not accuracy.

Decide the host, the ground-truth owner, and what happens to the frozen 0.70 bar.

## Answer (2026-09-07)

**Host: local, `qwen3:8b` via Ollama, chosen on a measured smoke test.** Philip declined
hosted Haiku (no key, no spend) and chose the local route. The model named in ADR-0003's
local row, `llama3.2:3b`, was then measured against `qwen3:8b` on 8 low-rated `All_Beauty`
reviews with a schema-constrained request (temperature 0, `format` = the output schema,
`think:false`, identical v0 prompt, 8 candidate themes) `[observed 2026-09-07]`:

| model | s/review | JSON valid | behaviour |
|---|---:|---:|---|
| `llama3.2:3b` | 4.3 | 8/8 | **degenerate** - emitted all 8 themes on 5 of 8 reviews |
| `qwen3:8b` | 6.7 | 8/8 | discriminating - 1-6 themes, varying sensibly with the text |

Client-side concurrency does not help: 3 threads gave 6.08 s/review against 6.7 serial, so
the local GPU is the bottleneck and P6 plans on **serial throughput**. `qwen3:14b` (9.3 GB)
is ruled out on this host: 16 GB RAM with an 8 GB Colima stack.

- `qwen3:8b` is the **primary labeller**; only its frozen labels feed the theme-shift table.
- `llama3.2:3b` keeps exactly the role ADR-0003 reserved for the local model: a
  development/audit **comparison row** and the single live demo call.
- Hosted Haiku becomes a **NOT_RUN** row in the evaluation table with the reason stated
  ("no hosted access provisioned"), not a silent omission.
- Bulk runs execute with the compose stack down where memory demands it.

**Ground truth: Philip hand-labels both sets** - 200 development and 200 audit, plus the 40
audit rows relabelled at least five days later for intra-annotator kappa. This is the only
route on which the pass rule is a real validation claim, and it is the difference between P6
and the P4/P5 model-judged compromise. Agent labelling is **not** used for P6 ground truth.
The labelling tool (frozen taxonomy on screen, one review at a time, resumable, order fixed
by seed) is a P6 deliverable and must exist before the discovery taxonomy freezes, because
Philip labels while the machine work continues.

**The 0.70 bar does not move.** It was frozen before any measurement and stays frozen under
the weaker labeller. If `qwen3:8b` fails it, P6 reports `verdict=FAIL` with the per-theme
table, the theme-shift table inherits an explicit caveat, and nothing is re-tuned to pass.
Changing a threshold after seeing the host is exactly the discretion ADR-0001 exists to
prevent.

**Budget.** The dollar cap and the 12,800 hosted-call ceiling in ADR-0003 lapse - local
inference has no marginal cost. They are replaced by a **time** budget at ~6.7 s/review
serial: discovery 600 (~1.1 h), prompt development 200 x <=5 versions (~1.9 h), classifier
training pool 3,000 (~5.6 h), post-2020 candidate/control inference (cap 8,000, ~15 h at the
cap; the real set is expected 1-3k), audit 200 (~0.4 h). Batch-vs-standard API mode
collapses to `api_mode = "local"`. `gold.review_theme_labels` keeps its schema, idempotency
key and MERGE semantics unchanged; `model_id` carries `qwen3:8b`.

**Data minimisation is unchanged and now stronger**: only `title` and `text` reach the
model, and the model runs on this machine, so nothing leaves it at all.

Recorded as an amendment to ADR-0003 (2026-09-07).

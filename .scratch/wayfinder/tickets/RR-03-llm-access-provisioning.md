---
id: RR-03
title: LLM access — what is actually provisioned
type: task
status: closed
assignee: philipbergman (claimed 2026-09-04)
blocked-by: []
blocks: [RR-08]
---

## Question

Nothing to decide — establish the facts the aspect-sentiment decision waits on. Provisioning
state was answered "unsure" during charting, so the model-host choice cannot be judged yet.

Establish and record:

1. **Hosted.** Is there an Anthropic API key available to this project, and is a spend cap
   set? `.env` and `.env.example` exist in the repo root (708 bytes each) — check what keys
   they declare without printing any secret value. Record only the variable name and whether
   it resolves to a non-empty value.
2. **Local.** Ollama is **not installed** `[observed 2026-09-04: \`ollama not found\`]`. The
   review of 2026-09-04 recommends it: the brief §6.2 names it, it removes the audit's
   "live LLM dependency" demo risk, and "local LLM inference inside a Spark pipeline" is
   the stronger CV line. So this ticket **installs it and measures**: `brew install ollama`,
   pull a 3B-class model (`llama3.2:3b` or `qwen2.5:3b`, ~2 GB `[assumed — record the real
   size]`), and record tokens/second on a real review prompt and peak RSS, once with Spark
   stopped and once with the bronze job running. Constraint `[inferred]`: 16 GB host, Colima
   8 GB, Spark driver 4 GB — an 8B model at Q4 (~5 GB) is unlikely to coexist; the numbers
   decide. Also record whether the `ollama` Python client or plain HTTP is the sane call
   path from a Spark `foreachBatch` or from a driver-side batch script.
3. **Cost envelope for hosted.** The audit's arithmetic: ~700k reviews × ~200 tokens ≈ 140M
   input tokens for the full corpus — which is why only a stratified subset is enriched.
   Compute the actual token and dollar estimate for the ~5,000-review subset the artifact
   proposes, so the subset-size decision has a number.

**Redact every secret value.** Record variable names and yes/no, never the key.

Resolution records: which hosts are available today, the local model's tok/s and RSS under
both conditions, what remains to be provisioned, and the subset cost estimate for both
hosts (hosted = dollars; local = wall-clock hours for the same subset). It decides nothing about taxonomy or host — that is
`RR-08 Aspect taxonomy and model host`.

## Resolution

Facts only — decides nothing. Full record with every command and output:
`docs/research/RR-03-llm-access-provisioning.md`.

- **Hosted.** `ANTHROPIC_API_KEY` is declared in `.env` and `.env.example` and is **empty** in
  both `[observed 2026-09-04]`; no `ANTHROPIC_*`/`OPENAI_*` in the shell env; no other LLM key
  declared. `anthropic>=0.40` is already in `pyproject.toml`. Spend cap: unknown — check console
  (no key exists to check). To provision: key + console cap, into `.env` only.
- **Local.** Ollama **0.33.2 installed** (`brew install ollama`); `llama3.2:3b` pulled —
  **2.0 GB on disk, 2.5 GB loaded, 100% GPU (Metal)**. Real 110-word review + 9-aspect JSON
  prompt = 267 prompt tokens, 192 output tokens, ~3.8 s/review.
  - A, Spark stopped: **54.0 / 53.8 / 54.1 tok/s** generation, 1,170–1,569 tok/s prompt eval,
    runner peak RSS **2.40–2.49 GB**, free memory 33%.
  - B, bronze running (`--trigger 5s` on `reviews.eos`, 4 GB driver, local[6], micro-batch in
    flight): **52.3 / 53.8 / 54.1 tok/s**, RSS 2.40–2.43 GB, free memory 28%. No measurable
    loss; caveat: the batch had not committed before the job was stopped.
  - All 4 `bd-*` containers **and 9 foreign Supabase containers** were up during both; lock
    acquired with 0 s wait, so uncontended.
  - Knobs (Ollama FAQ, verbatim): `OLLAMA_NUM_PARALLEL` default 1, RAM scales by
    `NUM_PARALLEL * CONTEXT_LENGTH`; `OLLAMA_MAX_LOADED_MODELS` default 3/GPU; `OLLAMA_MAX_QUEUE`
    512, 503 when full; `OLLAMA_KEEP_ALIVE` default 5m, overridable per call.
  - Call path: `ollama` package **not** in `pyproject.toml`/`uv.lock`; recommendation is plain
    HTTP `POST localhost:11434/api/generate` (`stream:false`, `format:json`) from a
    **driver-side batch script** with a thread pool = `OLLAMA_NUM_PARALLEL`, not a UDF — the
    server serialises requests anyway and the same script hosts the Haiku backend.
  - `ollama serve` is stopped; restart:
    `OLLAMA_FLASH_ATTENTION=1 OLLAMA_KV_CACHE_TYPE=q8_0 /opt/homebrew/opt/ollama/bin/ollama serve`
    (or `brew services start ollama`).
- **Cost envelope.** Haiku 4.5 = $1 in / $5 out per MTok (`platform.claude.com/docs/en/docs/about-claude/pricing`,
  2026-09-04; Batch API −50%). **5,000 reviews: $6.14 standard / $3.07 batch**
  (1.34 M in + 0.96 M out); **700k: ~$859 / ~$430**. Output is 78% of the bill.
  **Local: 5,000 reviews ≈ 5.3 h serial**; 700k ≈ 735 h — subset forced on both hosts.
- **Not measured:** 8B-Q4 coexistence with Spark, `OLLAMA_NUM_PARALLEL` > 1, `qwen2.5:3b`,
  Anthropic-tokenizer count, actual console spend cap.

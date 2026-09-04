# RR-05 — MiniLM embedding throughput on this host

Measured 2026-09-04 for wayfinder ticket `RR-05`. Facts are tagged `[observed]` (printed by a
command run on this host), `[inferred]` (arithmetic on observed numbers), or `[assumed]`.

## Bottom line

| | rate | source |
|---|---|---|
| Demo-conditions rate, default settings (batch 64, 256 tokens, 4 torch threads) | **373.7 reviews/s** | `[observed]` run 1 |
| Best rate at full text (batch 64, 256 tokens, 10 threads) | **422.0 reviews/s** | `[observed]` run 3 |
| Best rate overall (batch 64, truncate to 64 tokens, 10 threads) | **905.5 reviews/s** | `[observed]` run 3 |
| Same config with the Supabase stack running | 368.2 reviews/s (0.985× of 373.7) | `[observed]` run 2 |
| Peak RSS, batch 256 | 1,794.7 MB (4 threads) / 2,008.0 MB (10 threads) | `[observed]` |
| Peak RSS, batch 64 | 740.4 MB | `[observed]` |

Both candidate cohorts fit in well under an hour of CPU at every measured rate — the
larger one takes **15.6 min** at the default rate and **6.4 min** at the fastest one
(`[inferred]`, tables below). Embedding scope is therefore not CPU-bound on this host; the
constraint that remains for `RR-06` is memory alongside the 4 GB Spark driver.

## Command

Script: `scripts/bench_embed.py` (committed, repo-owned so the command is reproducible).
It reads real review text from `data/sample/All_Beauty.sample.jsonl`, loads the model, makes
one untimed warm-up `encode` call, then times **only `model.encode`** per configuration.

```bash
# run 1 — demo conditions (Supabase stopped), torch default threads
.venv/bin/python scripts/bench_embed.py --n 4000 \
  --configs 16:256,64:256,256:256,64:128,64:64 --label supabase-stopped

# run 2 — contended (Supabase running), single config
.venv/bin/python scripts/bench_embed.py --n 4000 --configs 64:256 --label supabase-running

# run 3 — demo conditions, all 10 cores
OMP_NUM_THREADS=10 .venv/bin/python scripts/bench_embed.py --n 4000 \
  --configs 64:256,64:128,64:64,256:256 --label supabase-stopped-10threads
```

Runs 1–3 were driven by a shell script that acquired a host-wide measurement lock
(`mkdir host-measure.lock`, shared with a concurrent Ollama measurement), stopped the
Supabase containers with `docker stop $(docker ps -q --filter name=supabase_)`, ran the
bench, restarted exactly the nine containers recorded beforehand with `docker start`, and
released the lock. Lock was acquired on the first attempt both times `[observed]`, so no run
overlapped the Ollama measurement.

## Conditions

| item | value | tag |
|---|---|---|
| CPU | Apple M5, 10 cores (`sysctl -n machdep.cpu.brand_string`, `hw.ncpu`) | `[observed]` |
| RAM | 16 GiB (`hw.memsize` = 17,179,869,184) | `[observed]` |
| Python / torch / sentence-transformers | 3.11.15 / 2.13.0 / 6.0.1, all from the project `.venv` (no install needed) | `[observed]` |
| Device | CPU (`device="cpu"` forced; MPS not used) | `[observed]` |
| `torch.get_num_threads()` | 4 by default; 10 with `OMP_NUM_THREADS=10` | `[observed]` |
| Model | `sentence-transformers/all-MiniLM-L6-v2` — the value of `EMBED_MODEL` in `src/common/config.py:51` | `[observed]` |
| Model download | 87 MB in `~/.cache/huggingface/hub` (`model.safetensors` 90,868,376 bytes); loads in 3.83 s; 384-dim output; default `max_seq_length` 256 | `[observed]` |
| `docker ps` during runs 1 and 3 | `bd-postgres bd-kafka bd-minio bd-es` only | `[observed]` |
| `docker ps` during run 2 | the four `bd-*` plus 9 `supabase_*_compliance-ai-app` containers, all Up (healthy) 20 s after restart | `[observed]` |
| `pgrep -fl spark` | empty (no JVM / Spark running) | `[observed]` |
| `pgrep -fl ollama` | `ollama serve` (pid 9600) and one `llama-server` child with a model resident, both idle under the lock | `[observed]` |
| Reviews embedded per config | 4,000 (first 4,000 non-empty texts of the sample) | `[observed]` |

Note on the runbook's container filter: `docs/DEMO_RUNBOOK.md` §2 uses the compose-project
label; `--filter name=supabase_` selected the same nine running containers here
`[observed]` (a tenth, `supabase_edge_runtime_compliance-ai-app`, was already `Exited` three
weeks earlier and was left alone).

## Sample text — length distribution

The 4,000 timed reviews `[observed]`:

| metric | words | chars |
|---|---|---|
| mean | 75.7 | 405.8 |
| p50 | 49 | 255 |
| p90 | 171 | 930 |
| max | 1,511 | 10,205 |
| ≥ 20 words | 2,973 of 4,000 (74%) | |

This is **longer than the corpus** (`docs/phase0-profile.txt`: mean 32.8 words, p50 102 chars)
because `data/sample/` is the head of the raw file (`scripts/make_sample.py`), and longer
than either cohort (mean 56.9 / 57.6 words, next section). The rates below are therefore
conservative for the cohorts `[inferred]`: sentence-transformers sorts inputs by length and
pads per batch, so shorter inputs run faster at 256-token truncation. At 64-token truncation
the length difference is mostly cut away and the rate is close to what the cohort would see.

## Throughput

### Run 1 — Supabase stopped, 4 torch threads `[observed]`

| batch | max_seq_length | seconds (4,000 reviews) | reviews/s | peak RSS so far (MB) |
|---|---|---|---|---|
| 16 | 256 | 10.78 | 371.0 | 644.7 |
| 64 | 256 | 10.70 | **373.7** | 740.4 |
| 256 | 256 | 12.63 | 316.7 | 1,794.7 |
| 64 | 128 | 7.88 | 507.9 | 1,794.7 |
| 64 | 64 | 4.93 | **812.0** | 1,794.7 |

### Run 3 — Supabase stopped, 10 torch threads `[observed]`

| batch | max_seq_length | seconds | reviews/s | peak RSS so far (MB) |
|---|---|---|---|---|
| 64 | 256 | 9.48 | **422.0** | 1,124.2 |
| 64 | 128 | 6.97 | 573.5 | 1,302.5 |
| 64 | 64 | 4.42 | **905.5** | 1,303.2 |
| 256 | 256 | 10.28 | 389.0 | 2,008.0 |

### Run 2 — Supabase running (contended), 4 threads `[observed]`

| batch | max_seq_length | seconds | reviews/s | peak RSS (MB) |
|---|---|---|---|---|
| 64 | 256 | 10.86 | 368.2 | 651.2 |

### What the numbers say `[inferred]`

- **Batch size barely matters**: 16 vs 64 is within noise; 256 is 15% *slower* and costs
  1.0–1.3 GB more RSS. Use 64.
- **Truncation is the lever**: 128 tokens gives 1.36×, 64 tokens 2.17× over 256. Whether 64
  tokens is acceptable is a quality question for `RR-06`: the cohort's mean review is ~57
  words (≈ 75 tokens `[assumed]` at ~1.3 tokens/word), so 64-token truncation clips roughly
  half the cohort; 128 tokens keeps p90 (107 words ≈ 140 tokens) nearly intact.
- **Threads**: 10 threads give only 1.13× over torch's default 4 (M5 has 4 performance
  cores; the extra efficiency cores add little `[assumed]`). Not worth fighting for.
- **Contention from idle Supabase containers is ~1.5%** (373.7 → 368.2 rev/s), not the 2×
  the runbook cites for the producer. The producer number was a VM-internal
  (Kafka-in-Colima) measurement; torch runs on the host, outside the VM, and idle containers
  consume almost no host CPU. Stopping Supabase still matters for VM memory (runbook §2),
  not for embedding speed.
- **Memory**: 424 MB after model load; 740 MB peak at batch 64; ~1.8–2.0 GB at batch 256.
  With the 4 GB Spark driver (`src/common/spark.py`) on a 16 GB host, batch 64 in a separate
  process is comfortable; batch 256 inside Spark Python workers would not be.

## Cohort sizes — counted from `data/raw/All_Beauty.jsonl` `[observed]`

Scanned with a 25-line Python script (json + Counter, 1.5 s; the file is 326 MB, 701,528
lines). Product = `parent_asin`, which the profile also uses (112,565 distinct).

| quantity | count | profile (`docs/phase0-profile.txt`) |
|---|---|---|
| reviews | 701,528 | 701,528 |
| empty text | 720 | 720 |
| reviews ≥ 20 words | 349,073 | 349,059 (Δ 14 — whitespace-split difference between Python `str.split` and Spark's tokeniser `[assumed]`) |
| products with ≥ 50 reviews | 1,902 | 1,902 |
| reviews in those products | 241,730 | — |
| **cohort (a): ≥ 20 words in products with ≥ 50 reviews** | **123,510** | — |
| **cohort (b): all ≥ 20 words** | **349,073** (projections below use the ticket's 349,059) | 349,059 |

Length of the cohorts `[observed]`: (a) mean 57.6 words, p50 41, p90 108, max 1,735;
(b) mean 56.9, p50 40, p90 107, max 2,585.

## Projections — wall-clock = cohort size ÷ measured rate `[inferred]`

Rates are single-process `model.encode` only; add model load (3.8 s) and I/O.

### Cohort (a): 123,510 reviews

| condition | rate (rev/s) | seconds | wall-clock |
|---|---|---|---|
| Supabase stopped, default (batch 64, 256 tok, 4 thr) | 373.7 | 330 | **5.5 min** |
| Supabase running, same config | 368.2 | 335 | 5.6 min |
| Supabase stopped, 10 threads, 256 tok | 422.0 | 293 | 4.9 min |
| Supabase stopped, 128 tok (4 thr) | 507.9 | 243 | 4.1 min |
| Supabase stopped, 64 tok (4 thr) | 812.0 | 152 | 2.5 min |
| Supabase stopped, 64 tok, 10 thr (best) | 905.5 | 136 | **2.3 min** |

### Cohort (b): 349,059 reviews

| condition | rate (rev/s) | seconds | wall-clock |
|---|---|---|---|
| Supabase stopped, default (batch 64, 256 tok, 4 thr) | 373.7 | 934 | **15.6 min** |
| Supabase running, same config | 368.2 | 948 | 15.8 min |
| Supabase stopped, 10 threads, 256 tok | 422.0 | 827 | 13.8 min |
| Supabase stopped, 128 tok (4 thr) | 507.9 | 687 | 11.5 min |
| Supabase stopped, 64 tok (4 thr) | 812.0 | 430 | 7.2 min |
| Supabase stopped, 64 tok, 10 thr (best) | 905.5 | 386 | **6.4 min** |

Because the timed sample is ~30% longer than the cohorts (75.7 vs ~57 mean words), the
256-token rows overstate the cost; the true figures are likely somewhat lower `[inferred]`.
Even so, the ticket's "under an hour of CPU" bound holds for the full ≥20-word corpus with a
3.8× margin at default settings.

## Raw artefacts

JSON output of each run (`--out`) and the driver logs were kept in the session scratchpad
only; every number above is transcribed from them. Re-run the commands in §Command to
regenerate.

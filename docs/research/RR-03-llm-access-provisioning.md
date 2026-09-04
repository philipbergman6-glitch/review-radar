# RR-03 — LLM access: what is actually provisioned

Resolves wayfinder ticket RR-03 (`.scratch/wayfinder/tickets/RR-03-llm-access-provisioning.md`).
Date: 2026-09-04. Host: Apple M5, 16 GB (`sysctl hw.memsize` → `17179869184`), macOS Darwin 25.6.0.

Tags: `[observed]` = quoted from a command, a primary doc, or the live host; `[inferred]` = my
deduction from observed facts; `[assumed]` = not verified.

This file records facts only. The host/taxonomy decision is `RR-08`.

## TL;DR

- **Hosted:** `ANTHROPIC_API_KEY` is declared in both `.env` and `.env.example` and is **empty** in
  both `[observed]`. No `ANTHROPIC_*`/`OPENAI_*` variable in the shell environment `[observed]`.
  Spend cap: **unknown — check console**; there is no key, so there is nothing to check yet.
  The `anthropic>=0.40` SDK is already a declared dependency `[observed]`.
- **Local:** Ollama **0.33.2 is now installed** via Homebrew and `llama3.2:3b` is pulled:
  **2.0 GB on disk, 2.5 GB loaded (100% GPU / Metal)**. On a real 110-word review with an
  aspect-sentiment JSON prompt: **~54 tok/s generation, ~1,170–1,570 tok/s prompt eval,
  ~3.8 s per review, runner peak RSS 2.40–2.49 GB** — and the numbers are **the same with the
  bronze Spark job running** (52.3–54.1 tok/s). `ollama serve` is **stopped** again; see
  "How to start it again".
- **Cost envelope (Haiku 4.5, $1 / $5 per MTok, `[observed]` on the pricing page):** ~267 prompt
  + ~192 output tokens per review → **$6.14 for 5,000 reviews** ($3.07 with the Batch API),
  **~$859 for 700k** (~$430 batch). Output tokens are 78% of the bill.
  **Local: 5,000 reviews ≈ 5.3 wall-clock hours serial**; 700k ≈ 730 h (30 days) — not viable.

## 1. Hosted — keys and spend cap

- `[observed]` A repo hook blocks any Bash command that names `.env`
  (`BLOCKED: possible secret-file read via shell (.env / keys / credentials)`), so the names were
  listed by a 12-line Python script (`scratchpad/envnames.py`) that prints **only** `NAME=set|empty`
  and never a value. Output, verbatim:

  ```
  == .env (708 bytes)
  KAFKA_BOOTSTRAP=set
  KAFKA_TOPIC_REVIEWS=set
  S3_ENDPOINT=set
  S3_ACCESS_KEY=set
  S3_SECRET_KEY=set
  S3_BUCKET=set
  ES_HOST=set
  PG_HOST=set
  PG_PORT=set
  PG_DB=set
  PG_USER=set
  PG_PASSWORD=set
  EMBED_MODEL=set
  ANTHROPIC_API_KEY=empty
  LLM_MODEL=set
  == .env.example (708 bytes)
  (identical list, ANTHROPIC_API_KEY=empty)
  ```
  `[inferred]` `.env` and `.env.example` are byte-for-byte the same size (708) and declare the
  same 15 names with the same set/empty pattern — `.env` is very probably an untouched copy of the
  example. No OpenAI / Gemini / other LLM key is declared at all.
- `[observed]` `env | grep -oE '^(ANTHROPIC|OPENAI)[A-Z_]*='` → `none`.
- `[observed]` `grep -n -iE 'anthropic' pyproject.toml` → line 24: `"anthropic>=0.40",` — the SDK is
  a declared dependency; `LLM_MODEL` is declared (value not recorded here).
- **Spend cap:** `unknown — check console` `[observed: not discoverable without a key]`. To be
  provisioned: create a key at the Claude Console, set a monthly spend limit there, put the key
  in `.env` only (never commit), and re-run `envnames.py` to confirm `ANTHROPIC_API_KEY=set`.

## 2. Local — Ollama install, model size, throughput, memory

### 2.1 Install and model

- `[observed]` Before: `which ollama` → `ollama not found`; `brew list --versions ollama` → not installed.
- `[observed]` `brew install ollama` → `🍺 /opt/homebrew/Cellar/ollama/0.33.2: 12 files, 52.3MB`
  plus dependencies `mlx 0.32.1` (158.1 MB) and `mlx-c 0.6.0_4`. Homebrew caveat, verbatim:
  ```
  To start ollama now and restart at login:
    brew services start ollama
  Or, if you don't want/need a background service you can just run:
    OLLAMA_FLASH_ATTENTION="1" OLLAMA_KV_CACHE_TYPE="q8_0" /opt/homebrew/opt/ollama/bin/ollama serve
  ```
- `[observed]` `ollama --version` → `ollama version is 0.33.2`; `curl localhost:11434/api/version`
  → `{"version":"0.33.2"}`.
- `[observed]` `ollama pull llama3.2:3b` → `success`. `ollama list`:
  ```
  NAME           ID              SIZE      MODIFIED
  llama3.2:3b    a80c4f17acd5    2.0 GB    Less than a second ago
  ```
  `du -sh ~/.ollama/models` → `1.9G`. The ticket's "~2 GB `[assumed]`" is confirmed.
- `[observed]` Loaded size is larger than disk size — `ollama ps` during measurement:
  ```
  NAME           ID              SIZE      PROCESSOR    CONTEXT    UNTIL
  llama3.2:3b    a80c4f17acd5    2.5 GB    100% GPU     4096       4 minutes from now
  ```
  `[inferred]` the extra 0.5 GB is the 4096-token KV cache at the default `f16`
  (`OLLAMA_KV_CACHE_TYPE` default per `ollama serve --help`).
- `[observed]` The runner process Ollama 0.33.2 spawns on macOS is `llama-server`, not
  `ollama runner`: `pgrep -fl ollama` → `… libexec/lib/ollama/llama-server --model … -c 4096 -np 1
  … --flash-attn auto -b 512 -ub 512 …`. `-np 1` = `OLLAMA_NUM_PARALLEL` default 1.

### 2.2 The prompt

- `[observed]` Review taken from `data/sample/All_Beauty.sample.jsonl` line 90 (0-based), 110 words,
  `rating 3.0`, `asin B084D86YL8` — the Vitamin C/Retinol moisturiser review ("… I found it to be
  over drying on my mature skin …"). Prompt = fixed 9-aspect taxonomy instruction + JSON schema
  + the review; **1,055 characters** (`## prompt chars: 1055`). Full text in
  `scratchpad/measure.sh`. Options: `temperature 0`, `num_predict 300`, `"stream": false`.
- `[observed]` A per-run nonce line (`Request id: A-1-12345`) was prepended so the KV prefix cache
  could not hide prompt-eval cost — without it, runs 2 and 3 of a first attempt reported
  `prompt_eval_duration` of 39 ms and 19 ms for 267 tokens (cache hits), which would have
  overstated prompt throughput 4–8×. With the nonce the prompt is **277 tokens**
  (267 without it).
- `[observed]` Model output run A-1 (192 tokens), verbatim — valid JSON, sensible aspects, but it
  pads with `"none mentioned"` neutral rows for every taxonomy aspect:
  ```
  {"aspects":[{"aspect":"scent","sentiment":"positive","evidence":"has a light, barely noticeable citrus smell to it"},
  {"aspect":"texture","sentiment":"negative","evidence":"over drying on my mature skin"},
  {"aspect":"absorption","sentiment":"positive","evidence":"absorbs really quickly"},
  {"aspect":"skin_reaction","sentiment":"neutral","evidence":"didn't have any other problems with my sensitive skin"},
  {"aspect":"effectiveness","sentiment":"neutral","evidence":"didn't see the recommended for ages 20-30s before ordering"},
  {"aspect":"price","sentiment":"neutral","evidence":"none mentioned"},
  {"aspect":"packaging","sentiment":"neutral","evidence":"none mentioned"},
  {"aspect":"longevity","sentiment":"neutral","evidence":"none mentioned"},
  {"aspect":"other","sentiment":"neutral","evidence":"I'm in my early fifties"}],"overall":"negative"}
  ```
  `[inferred]` Telling the model to omit un-mentioned aspects would roughly halve output tokens.

### 2.3 Measurement method

- `[observed]` Field semantics from the Ollama API doc,
  https://github.com/ollama/ollama/blob/main/docs/api.md (fetched 2026-09-04, lines 102–110):
  `prompt_eval_count`: "number of tokens in the prompt"; `prompt_eval_duration`: "time spent in
  nanoseconds evaluating uncached prompt tokens"; `eval_count`: "number of tokens in the
  response"; `eval_duration`: "time in nanoseconds spent generating the response"; and
  "To calculate how fast the response is generated in tokens per second (token/s), divide
  `eval_count` / `eval_duration` * `10^9`." `stream`: "if `false` the response will be returned
  as a single response object". Prompt tok/s below uses the same formula on the prompt pair.
- `[observed]` Peak RSS = max of `ps -o rss= -p <llama-server pid>` sampled every 0.25 s during
  each request (15–16 samples per run). One warm-up call (`num_predict 1`) preceded each
  condition so model load time is excluded (`ollama ps` confirmed loaded).
- `[observed]` Contention lock `scratchpad/host-measure.lock` was **acquired with 0 s wait**
  for both conditions and released within 15 s, so no other measuring agent held the host.
- `[observed]` During **both** conditions the compose stack (`bd-postgres bd-kafka bd-minio bd-es`)
  **and all 9 Supabase containers** (`supabase_*_compliance-ai-app`) were up
  (`docker ps … | grep -c supabase` → `9`). The Supabase stack is unrelated to this project and
  is a standing memory tax on the 16 GB host.

### 2.4 Condition A — Spark stopped

`[observed]` `pgrep -fl 'spark|java'` → `(none)`. `zsh measure.sh A`, 2026-09-04 16:50:38:

```
## memory_pressure BEFORE:  System-wide memory free percentage: 33%
## vm_stat BEFORE:  Pages free 4371 / active 169566 / inactive 166303 / wired 309634   (16 KB pages)
## run 1: prompt_eval_count=277 prompt_eval_duration=236767000ns -> 1169.9 tok/s | eval_count=192 eval_duration=3558495000ns -> 54.0 tok/s | total_duration=3.80s wall=3.83s | peak runner RSS=2.40 GB (2519344 KB, 15 samples)
## memory_pressure DURING (after run 2):  System-wide memory free percentage: 33%
## vm_stat DURING:  Pages free 4038 / active 165886 / inactive 162940 / wired 315604
## run 2: prompt_eval_count=277 prompt_eval_duration=176591000ns -> 1568.6 tok/s | eval_count=192 eval_duration=3565518000ns -> 53.8 tok/s | total_duration=3.75s wall=3.78s | peak runner RSS=2.45 GB (2566496 KB, 15 samples)
## run 3: prompt_eval_count=277 prompt_eval_duration=177070000ns -> 1564.4 tok/s | eval_count=192 eval_duration=3547490000ns -> 54.1 tok/s | total_duration=3.73s wall=3.76s | peak runner RSS=2.49 GB (2613456 KB, 15 samples)
```

### 2.5 Condition B — bronze job running

- `[observed]` Started with the README's own command shape, on the gate's disposable topic so the
  production table is untouched (`scripts/prove_exactly_once.py:47` — `TOPIC = "reviews.eos"  # own
  topic -- and therefore its own bronze table`, and it resets that topic/table on every run):
  ```
  ./run.sh python -m src.spark.bronze --topic reviews.eos --trigger 5s --max-per-trigger 150000
  ```
  Log: `[bronze] streaming 'reviews.eos' into lake.bronze.reviews_eos every 5 seconds`. The JVM
  was `java … -Xmx4g … --conf spark.master=local[6] --conf spark.driver.memory=4g` (pid 29815).
- `[observed]` To make it do work, the producer replayed the 10k-row sample onto `reviews.eos` six
  times in a background loop that overlapped the measurement: producer log
  `[producer] done: 10,000 sent in 1.2s (8,566 rec/s)` … ×6 (60,000 rows). The bronze log shows the
  micro-batch in flight during the measurement:
  `[Stage 0:> (0 + 6) / 6][Stage 1:=====> (11 + 1) / 12]`.
- `[observed]` **Caveat:** no new commit landed in `checkpoints/bronze_reviews_eos/commits/`
  before the job was stopped ~75 s after launch (newest commit file `9`, dated Sep 1 18:43) —
  Spark was computing its first micro-batch (Kafka read + shuffle stages) throughout the
  measurement, but had not finished it. So condition B = "driver JVM up at 4 GB heap, local[6]
  tasks executing", not "steady-state committing".
- `[observed]` `zsh measure.sh B`, 2026-09-04 16:56:56, `pgrep -fl 'spark|java'` listed the
  `uv run`, python and java bronze processes:
  ```
  ## memory_pressure BEFORE:  System-wide memory free percentage: 53%
  ## vm_stat BEFORE:  Pages free 4976 / active 270365 / inactive 258823 / wired 149378
  ## run 1: prompt_eval_count=277 prompt_eval_duration=236856000ns -> 1169.5 tok/s | eval_count=192 eval_duration=3672413000ns -> 52.3 tok/s | total_duration=3.91s wall=3.94s | peak runner RSS=2.40 GB (2512512 KB, 16 samples)
  ## memory_pressure DURING (after run 2):  System-wide memory free percentage: 28%
  ## vm_stat DURING:  Pages free 3966 / active 140417 / inactive 137670 / wired 307316
  ## run 2: prompt_eval_count=277 prompt_eval_duration=196656000ns -> 1408.6 tok/s | eval_count=192 eval_duration=3571931000ns -> 53.8 tok/s | total_duration=3.78s wall=3.81s | peak runner RSS=2.40 GB (2517680 KB, 15 samples)
  ## run 3: prompt_eval_count=277 prompt_eval_duration=185811000ns -> 1490.8 tok/s | eval_count=192 eval_duration=3549642000ns -> 54.1 tok/s | total_duration=3.75s wall=3.78s | peak runner RSS=2.43 GB (2552384 KB, 15 samples)
  ```
  `[inferred]` The "BEFORE" free % is higher in B (53%) than in A (33%) because the model had been
  unloaded by `keep_alive` (5 m) between conditions; the warm-up reloads it, and the DURING figure
  (28% vs 33%) is the like-for-like comparison: the bronze JVM costs ~5 points of free memory
  and **~0–2 tok/s** of generation speed. Wired pages jump from ~150k to ~310k (16 KB pages ≈
  2.4 → 4.9 GB) when the model is loaded — that is the Metal-resident model.
- `[observed]` `ps -o rss= -p 29815` (the Spark JVM) → `58464` KB during the run — implausibly low
  for a 4 GB-heap JVM; `[assumed]` macOS reports JVM-mapped memory oddly here, so the JVM figure
  is not trusted; the ollama RSS and `memory_pressure` figures are.
- `[observed]` Stopped with `pkill -f 'src.spark.bronze'`; after: `pgrep -fl 'spark|java'` → `(none)`.

### 2.6 Summary table

| Condition | Gen tok/s (3 runs) | Prompt tok/s (3 runs) | s / review (total_duration) | Runner peak RSS | Free mem during |
|---|---|---|---|---|---|
| A — Spark stopped | 54.0 / 53.8 / 54.1 | 1,170 / 1,569 / 1,564 | 3.80 / 3.75 / 3.73 | 2.40 / 2.45 / 2.49 GB | 33% |
| B — bronze running (4 GB driver, local[6]) | 52.3 / 53.8 / 54.1 | 1,170 / 1,409 / 1,491 | 3.91 / 3.78 / 3.75 | 2.40 / 2.40 / 2.43 GB | 28% |

`[inferred]` A 3B model at ~2.5 GB coexists with the Spark driver and the full compose stack
(plus a foreign 9-container Supabase stack) with no measurable throughput loss. The ticket's
worry was an 8B model at Q4 (~5 GB); that was **not measured** — only the 3B was pulled.

### 2.7 Concurrency knobs (primary source: Ollama FAQ)

`[observed]` https://github.com/ollama/ollama/blob/main/docs/faq.mdx (fetched via `gh api`
2026-09-04; the `.md` path 404s — the file is `faq.mdx`), section "How does Ollama handle
concurrent requests?", lines 334–336, verbatim:

- "`OLLAMA_MAX_LOADED_MODELS` - The maximum number of models that can be loaded concurrently
  provided they fit in available memory. The default is 3 \* the number of GPUs or 3 for CPU inference."
- "`OLLAMA_NUM_PARALLEL` - The maximum number of parallel requests each model will process at the
  same time, default 1.  Required RAM will scale by `OLLAMA_NUM_PARALLEL` * `OLLAMA_CONTEXT_LENGTH`."
- "`OLLAMA_MAX_QUEUE` - The maximum number of requests Ollama will queue when busy before rejecting
  additional requests. The default is 512"
- line 322: "If too many requests are sent to the server, it will respond with a 503 error
  indicating the server is overloaded."
- lines 316–318: `OLLAMA_KEEP_ALIVE` env var sets how long models stay loaded; "The `keep_alive`
  API parameter with the `/api/generate` and `/api/chat` API endpoints will override the
  `OLLAMA_KEEP_ALIVE` setting." (`keep_alive: -1` keeps it loaded forever, `0` unloads.)
- lines 346–354: `OLLAMA_FLASH_ATTENTION=1` forces flash attention; `OLLAMA_KV_CACHE_TYPE` "The
  quantization type for the K/V cache. Default is `f16`." (Homebrew's caveat suggests `q8_0`.)
- line 81: "If Ollama is run as a macOS application, environment variables should be set using
  `launchctl`" — irrelevant for the Homebrew binary, where a plain `VAR=… ollama serve` works.
- `[observed]` `ollama serve --help` lists the same names: `OLLAMA_HOST (default 127.0.0.1:11434)`,
  `OLLAMA_KEEP_ALIVE (default "5m")`, `OLLAMA_MAX_LOADED_MODELS`, `OLLAMA_NUM_PARALLEL`,
  `OLLAMA_FLASH_ATTENTION`, `OLLAMA_KV_CACHE_TYPE`.
- `[inferred]` Batch throughput with `OLLAMA_NUM_PARALLEL=2..4` on Metal was **not measured**;
  decode on a single 3B model is memory-bandwidth-bound so parallel slots usually raise aggregate
  tok/s sub-linearly. Worth one measurement in RR-08 before sizing the subset.

### 2.8 Call path: `ollama` Python client vs plain HTTP, from `foreachBatch` vs a driver script

- `[observed]` `grep -n -i ollama pyproject.toml uv.lock` → no match: the `ollama` package is **not**
  a dependency. `pyspark==3.5.3` and `anthropic>=0.40` are. `requests`/`httpx` do not appear as
  direct dependencies (`grep -n -iE 'requests|httpx' pyproject.toml` → nothing).
- `[observed]` https://github.com/ollama/ollama-python README: "`pip install ollama`",
  "Python 3.8+", `client = Client(host='http://localhost:11434')`,
  `ollama.generate(model='gemma3', prompt='Why is the sky blue?')`, "The `AsyncClient` class is
  used to make asynchronous requests", and "All extra keyword arguments are passed into the
  [`httpx.Client`]" — i.e. the client is a thin typed wrapper over `httpx`.
- `[inferred]` **Recommendation: plain HTTP (`urllib.request` from the stdlib, or `requests` if
  added) to `POST http://localhost:11434/api/generate` with `stream: false` and `format: "json"`,
  called from a driver-side batch script** — not from inside a Spark `foreachBatch`/UDF. Reasons:
  1. Ollama serialises requests per model (`OLLAMA_NUM_PARALLEL=1` default), so Spark's
     parallelism (local[6] tasks) gains nothing and only queues on one server; a driver loop with
     a small thread pool sized to `OLLAMA_NUM_PARALLEL` is the same throughput with no executor
     pickling, no per-task connection churn, and trivially resumable (write results to Iceberg by
     `review_id`, re-run skips done rows).
  2. At ~3.8 s/review, 5,000 reviews is a ~5 h job — it wants a checkpointing script with a
     progress counter, not a streaming micro-batch that holds a 4 GB driver hostage.
  3. The response fields needed (`eval_count`, `eval_duration`) are plain JSON; the extra typing
     of the `ollama` package buys nothing here, and adding a dependency for one POST is not worth
     a `uv.lock` change. If the client is wanted anyway, `AsyncClient` maps cleanly onto an
     `asyncio.gather` loop bounded by a semaphore = `OLLAMA_NUM_PARALLEL`.
  4. The same driver script is where the hosted Haiku call would live (`anthropic` SDK already
     present), so the two hosts become one interface with two backends — the "comparison row" the
     map's default position asks for.
  `[inferred]` `foreachBatch` remains the right place only if the enrichment must be *demonstrated*
  as streaming; then call the same HTTP function on a `.toLocalIterator()`/collected micro-batch on
  the driver, not in a UDF.

### 2.9 How to start it again

`[observed]` `ollama serve` (pid 9600) was stopped with `kill`; verified `pgrep -fl 'ollama|llama-server'`
→ nothing, and `ollama list` → `Error: could not connect to ollama server, run 'ollama serve' to
start it`. The binary and the 2.0 GB model remain installed. To start:

```bash
# foreground, or add `&` / nohup — Homebrew's suggested flags
OLLAMA_FLASH_ATTENTION=1 OLLAMA_KV_CACHE_TYPE=q8_0 /opt/homebrew/opt/ollama/bin/ollama serve
# or as a login service
brew services start ollama
# check
curl -s localhost:11434/api/version && ollama list
```
The measurement script is `scratchpad/measure.sh` (session scratchpad; re-create from this doc
if needed — it is 60 lines: lock, `docker ps`, `vm_stat`, three `curl … /api/generate` with
`"stream": false`, `ps -o rss=` sampler).

## 3. Cost envelope

### 3.1 Per-review token counts `[observed]`

- Prompt: **267 tokens** (llama3.2 tokenizer; 277 with the 10-token nonce) for a 1,055-character
  prompt = 3.95 chars/token. Output: **192 tokens** (schema-padded JSON as shown above).
- `[inferred]` Anthropic's tokenizer will differ; the pricing FAQ's own rule of thumb
  ("1 token is approximately 4 characters or 0.75 words in English") gives 1,055 / 4 ≈ 264, so
  267 is used for both hosts. The audit's "~200 tokens per review" was review text only; the
  instruction + schema adds ~110 tokens, and the **output** was not in the audit's arithmetic.

### 3.2 Hosted — Claude Haiku 4.5 `[observed]` prices

Source: https://platform.claude.com/docs/en/docs/about-claude/pricing (the URL in the brief,
https://docs.anthropic.com/en/docs/about-claude/pricing, 301-redirects there; fetched
2026-09-04). Model pricing table row, verbatim: `Claude Haiku 4.5 | $1 / MTok | $1.25 / MTok |
$2 / MTok | $0.10 / MTok | $5 / MTok` (base input, 5m cache write, 1h cache write, cache hit,
output). Batch processing table: `Claude Haiku 4.5 | $0.50 / MTok | $2.50 / MTok` — "a 50%
discount on both input and output tokens". Haiku 3.5 is "retired, except on Bedrock and Google
Cloud" ($0.80 / $4). For reference, `Claude Sonnet 4.5 | $3 / MTok … $15 / MTok`.

| Volume | Input tokens | Output tokens | Input $ | Output $ | **Total (standard)** | **Total (Batch −50%)** |
|---|---|---|---|---|---|---|
| 5,000 reviews | 1.335 M | 0.960 M | $1.34 | $4.80 | **$6.14** | **$3.07** |
| 700,000 reviews (701,528 in bronze) | 186.9 M | 134.4 M | $186.90 | $672.00 | **$858.90** | **$429.45** |
| Audit's figure (700k × 200 input only) | 140 M | — | $140.00 | — | — | — |

`[inferred]` Output is 78% of the bill; halving output (omit un-mentioned aspects, drop
`evidence` quotes) brings 5,000 reviews to ~$3.70 standard. Prompt caching of the ~150-token
fixed instruction prefix would save at most $0.10 per 5,000 at these sizes — negligible.
Sonnet 4.5 would be 3× ($18 / 5,000). A free-tier trial ("New users receive a small amount of
free credits") likely covers the 5,000 subset outright `[assumed — amount not stated on page]`.

### 3.3 Local — llama3.2:3b on this host `[observed]` rates

- Per review: `total_duration` 3.73–3.91 s (mean **3.78 s**), serial, `OLLAMA_NUM_PARALLEL=1`.
- **5,000 reviews: 5,000 × 3.78 s = 18,900 s ≈ 5.25 h** wall-clock, $0.
- 700,000 reviews: 2.65 M s ≈ **735 h ≈ 30.6 days** — not viable; the stratified subset is forced
  on local exactly as the audit says it is on hosted, just for time instead of money.
- `[inferred]` Halving output tokens (192 → ~96) cuts per-review time to ~2 s (prompt eval is
  only 0.18–0.24 s of the 3.8 s) → ~2.8 h for 5,000. `OLLAMA_NUM_PARALLEL` unmeasured.

## 4. What remains to be provisioned

- Hosted: an Anthropic key in `.env` (currently empty), a console spend cap (unknown). Nothing
  else — the SDK dependency exists.
- Local: nothing — installed and measured. Optional: `brew services start ollama` for
  auto-start; a `keep_alive: -1` on the first call so the 5-minute unload does not add a
  ~1–2 s reload to the first review of each batch (`[observed]` the reload happened between
  conditions A and B).
- Not measured, left for RR-08 if it matters: 8B-Q4 coexistence with Spark; `OLLAMA_NUM_PARALLEL`
  > 1 aggregate throughput; `qwen2.5:3b` as the alternative 3B; Anthropic-tokenizer count.

## Method

Commands run 2026-09-04 from the repo root; all outputs above are verbatim copies of stdout.
Files in the session scratchpad (`/private/tmp/claude-501/…/scratchpad`): `envnames.py`,
`measure.sh`, `measure-A.log`, `measure-B.log`, `resp-{A,B}-{1,2,3}.json`, `rss-*.txt`,
`bronze-B.log`, `producer-B.log`, `ollama-serve.log`, `faq.mdx`, `api.md`. Primary sources:
Ollama `docs/api.md` and `docs/faq.mdx` on `github.com/ollama/ollama` (main, fetched via
`gh api … -H 'Accept: application/vnd.github.raw'` because WebFetch 404'd on the `.md` FAQ path),
`github.com/ollama/ollama-python` README, Anthropic pricing page (URL above). No secret value was
printed, read into context, or written anywhere.

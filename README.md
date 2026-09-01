# Review Radar — streaming review intelligence

BIU 8688697201 — Big Data and AI, final project (solo, approved by the instructor).

Reviews replay through Kafka into a Spark Structured Streaming job that lands them
unparsed — and exactly once across an unclean restart — as an Iceberg table on a MinIO
object store, with the Iceberg catalogue in PostgreSQL. **That is what runs today.**
Everything downstream of it — silver/gold transformation, the SQL product-catalogue
join, Elasticsearch indexing, and an AI layer (embeddings + semantic search, LLM aspect
sentiment, RAG) — is designed but not built. The table below is the honest split, and it
stays in this README until it is all in the "built" column.

## Status

| component | state | evidence / what is missing |
|---|---|---|
| Kafka replay producer | **built** | `src/ingest/producer.py`; 701,528 records on `reviews.raw` |
| Bronze ingest → Iceberg on MinIO | **built** | `src/spark/bronze.py`; 701,528 rows, 96.5 MB of Parquet |
| Iceberg JDBC catalogue in Postgres | **built** | `iceberg_tables` row for `bronze.reviews_raw` |
| Exactly-once proof (kill + restart) | **built** | `scripts/prove_exactly_once.py` — see below |
| Data profiling | **built** | [`docs/phase0-profile.txt`](docs/phase0-profile.txt) |
| Silver / gold transformation | *planned* | nothing written yet |
| Product catalogue + JDBC enrichment | *planned* | `products` table exists and is **empty**; no loader script |
| Elasticsearch index (BM25 + kNN) | *planned* | cluster is up and healthy; **no indices** |
| AI: embeddings, semantic search | *planned* | `src/ai/` is an empty package |
| AI: LLM aspect sentiment + validation | *planned* | — |
| AI: RAG question answering | *planned* | — |
| Streamlit app | *planned* | `src/serving/` is an empty package |
| Design doc, slides, demo runbook | *planned* | `docs/` holds the profile and the audit only |

## Dataset

**Amazon Reviews 2023** — McAuley Lab, UC San Diego.
<https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023>

Two semi-structured JSONL sources that we join:

| file | content |
|---|---|
| `raw/review_categories/<CATEGORY>.jsonl` | one JSON object per review — free text, rating, nested `images[]` |
| `raw/meta_categories/meta_<CATEGORY>.jsonl` | one JSON object per product — nested `images[]`/`videos[]`, `features[]`, `description[]`, free-form `details{}` |

- **Dev category** `All_Beauty` — 0.33 GB reviews + 0.21 GB metadata (701,528 reviews).
- **Full category** `Beauty_and_Personal_Care` — 11.0 GB reviews + 2.8 GB metadata (~23M reviews).

Public dataset, no personal data of ours — which is what makes sending review text to a
hosted LLM acceptable here (see the brief's rule on third-party services).

## What the data actually looks like

Measured with Spark over all 701,528 dev-category reviews — full output in
[`docs/phase0-profile.txt`](docs/phase0-profile.txt).

| property | value | why it matters |
|---|---|---|
| reviews / products / users | 701,528 / 112,590 / 631,986 | real volume, not a toy table |
| review text | mean 33 words, median 102 chars, max 14,989 chars | genuinely unstructured |
| reviews ≥ 20 words | 349,059 (49.8%) | the population worth embedding |
| empty text | 720 (0.10%) | must be filtered before the AI step |
| rating mix | 60.0% ★5, 14.6% ★1, 6.1% ★2 | strongly J-shaped — accuracy is a useless metric here |
| time span | 2000-11-01 → 2023-09-09, peak 126,753 in 2020 | supports time-windowed drift detection |
| verified purchases | 634,969 (90.5%) | a trust signal we can weight by |
| reviews per product | median 2, p99 72, max 1,962 | heavy skew → partitioning/salting matters |
| products with ≥ 50 reviews | 1,902 | the cohort where trends are statistically meaningful |
| `price` populated | 15.7% | real data-quality problem, handled explicitly |
| review→product join match | 112,565 / 112,565 (100%) | the join is clean |
| duplicate `(user, product, ts)` | 6,139 | justifies a real dedupe step |

Two real dirt findings that shaped the design:

1. Product `details` is a free-form dict whose keys collide under Spark's default
   case-insensitive resolution (`Assembly Required` vs `assembly required`), which makes
   schema inference fail with `COLUMN_ALREADY_EXISTS`. We declare schemas explicitly and
   keep `details` as raw JSON — schema-on-read. See `src/common/schemas.py`.
2. `price` is a string that is null, `"9.99"`, `"$9.99"` or a range. It is parsed
   defensively, never coerced silently.

## Architecture

```
data/raw/*.jsonl
      │
      │  replay producer (controllable rate)
      ▼
┌──────────────┐
│    Kafka     │  reviews.raw, 6 partitions — decouples ingest from processing
└──────┬───────┘
       ▼
┌─────────────────────────────────────────────┐        ┌──────────────┐
│      Spark Structured Streaming             │◀──JDBC─│  PostgreSQL  │
│  bronze            [built]                  │ (plan) │  products    │
│  silver → gold     [planned]                │        │  (empty)     │
└──────┬──────────────────────────────┬───────┘        └──────────────┘
       │ Iceberg tables               │ enriched docs (planned)
       ▼                              ▼
┌──────────────┐              ┌──────────────────┐
│ MinIO (S3)   │              │  Elasticsearch   │
│  lakehouse   │              │  BM25 + kNN      │
│    [built]   │              │    [planned]     │
└──────────────┘              └────────┬─────────┘
                                       ▼
                          ┌────────────────────────┐
                          │  Streamlit app         │
                          │  semantic search · RAG │
                          │       [planned]        │
                          └────────────────────────┘
```

Solid today: producer → Kafka → bronze → Iceberg/MinIO, with Postgres as the Iceberg
catalogue. Everything marked `[planned]` is architecture, not code.

**Course technologies — in use now:** Kafka (streaming) · Spark (Structured Streaming) ·
Iceberg (table format) · MinIO (object store) · PostgreSQL (Iceberg JDBC catalogue) ·
Docker (containers) · JSON (semi-structured) · free text (unstructured).
**Planned:** Elasticsearch (NoSQL + vector search), PostgreSQL in its second role as the
SQL enrichment source.

**AI capabilities** (brief §6.2) — *none built yet*. Planned, in this order:
(b) embeddings + semantic search, (a) LLM aspect sentiment with a measured validation,
(c) RAG on top of (b). (e), (f) and (g) are explicitly out of scope for now.

## Setup

Requirements: macOS/Linux, Docker (Colima or Docker Desktop), `uv`, JDK 17.

```bash
brew install openjdk@17 docker-compose colima uv     # if not present
colima start --cpu 6 --memory 8 --disk 80            # if using Colima

cp .env.example .env                                 # fill ANTHROPIC_API_KEY for the LLM steps
uv sync                                              # Python 3.11 env
docker compose up -d                                 # Kafka, MinIO, Elasticsearch, Postgres
docker compose ps                                    # all four should be "healthy"
```

Load the schema (only needed if the Postgres volume already existed):

```bash
docker exec -i bd-postgres psql -U bigdata -d catalog < conf/postgres-init/01_schema.sql
```

Get the data and profile it:

```bash
./run.sh python scripts/download_data.py --category All_Beauty
./run.sh python scripts/make_sample.py
./run.sh python scripts/profile_data.py --category All_Beauty
```

`./run.sh` is a thin wrapper that pins `JAVA_HOME` to JDK 17 and runs inside the uv
environment — Spark 3.5 will not start otherwise.

## Running the pipeline

Check the stack first — it prints one line per component and exits non-zero if any of the
five (Kafka, MinIO, Elasticsearch, Postgres, Spark) is not reachable:

```bash
./run.sh python scripts/healthcheck.py
```

**1 — replay the reviews into Kafka.** Creates the topic if it does not exist (6
partitions), keys every record by `parent_asin`, and reports throughput as it goes.

```bash
./run.sh python -m src.ingest.producer --category All_Beauty
#  --limit N     stop after N records (0 = the whole file, the default)
#  --rate R      cap at R records/sec (0 = as fast as the broker accepts, the default)
#  --topic T     default reviews.raw
#  --source PATH override the input .jsonl
```

**Running on the sample.** `data/sample/` is committed (10k reviews, 5k products — the
first N lines of each raw file, so `scripts/make_sample.py` reproduces it exactly). It
lets you replay step 1 without the 0.54 GB download:

```bash
./run.sh python -m src.ingest.producer --source data/sample/All_Beauty.sample.jsonl
```

Everything downstream — bronze, the verify script, the exactly-once gate — then runs
against 10k rows instead of 701,528. `scripts/download_data.py` and
`scripts/profile_data.py` still need the full raw files.

**2 — drain the topic into the bronze Iceberg table.** `--trigger once` processes
everything available and stops; a duration like `--trigger 5s` keeps the query running.
The table and checkpoint are derived from the topic name, so `reviews.raw` can only ever
write to `lake.bronze.reviews_raw` / `checkpoints/bronze_reviews_raw`.

```bash
# the command that produced the current 701,528-row table
./run.sh python -m src.spark.bronze --trigger once --max-per-trigger 150000
#  --reset             drop the table and checkpoint first (DESTRUCTIVE)
#  --reset-only        drop them and exit without streaming
#  --starting-offsets  earliest (default) | latest
```

`--max-per-trigger` is the memory throttle: 150,000 records per micro-batch gave five
commits and ~10 s wall for the full category on this machine. The code default is
100,000; the larger value is a deliberate choice for the bulk load, not a hidden one.

**3 — see what landed** (row count, snapshot history, and a time-travel read of the
first snapshot):

```bash
./run.sh python scripts/verify_iceberg.py
```

**4 — the exactly-once gate.** Loads a known number of records, starts bronze, `SIGKILL`s
it mid-stream, restarts it from the checkpoint, and asserts no loss, no duplicates, *and*
that the kill actually interrupted work in progress (a run that drained before the kill
now fails instead of printing a vacuous PASS).

```bash
./run.sh python scripts/prove_exactly_once.py --records 120000 --kill-after 25
```

It runs against its own topic (`reviews.eos`) and therefore its own table
(`bronze.reviews_eos`) — it cannot touch the production one. About 65 s end to end; full
Spark output in `checkpoints/eos_run.log`.

### Measured producer throughput

The producer replays the full 701,528-review file in roughly 1.5–3 s: **232,000–484,000
rec/s**, measured with

```bash
./run.sh python -m src.ingest.producer --topic audit.perf   # whole file, --rate 0
```

on an Apple Silicon laptop (16 GB) with the stack running under Colima. Quote the range,
not a point estimate: the high end is an otherwise-idle host, the low end is the same
command with nine unrelated containers competing for the VM. The number is host- and
load-dependent, so a single figure is not reproducible.

## Service endpoints

| service | endpoint | credentials |
|---|---|---|
| Kafka | `localhost:9092` | — |
| MinIO API / console | `localhost:9000` / `localhost:9001` | `minioadmin` / `minioadmin` |
| Elasticsearch | `localhost:9200` | security disabled (local only) |
| PostgreSQL | `localhost:5432` | `bigdata` / `bigdata`, db `catalog` |

## Resource envelope

Everything here shares one 16 GB laptop, and the budget is tighter than it looks.

| what | budget | where |
|---|---|---|
| Colima VM | 8 GB / 6 CPU | `colima start --cpu 6 --memory 8` |
| Kafka container | 2 GB (`-Xmx1G` heap) | `docker-compose.yml` |
| Elasticsearch container | 2 GB (1 GB heap) | `docker-compose.yml` |
| MinIO container | 1 GB | `docker-compose.yml` |
| Postgres container | 512 MB | `docker-compose.yml` |
| Spark driver (on the host, not in the VM) | 4 GB | `src/common/spark.py` |
| `.venv` on disk | 1.7 GB (torch alone is 489 MB) | `uv sync` |

Container caps total 5.5 GB of the 8 GB VM, and the Spark driver's 4 GB is host memory on
top of that. It fits — but only if the VM is running this stack alone. Containers from
another project sharing the same Colima VM measurably slow the producer (see the
throughput range above), so stop them before a demo:

```bash
docker ps --format '{{.Names}}'      # expect only bd-* containers
```

## Layout

```
conf/postgres-init/   catalogue + audit schema
data/raw/             downloaded JSONL (git-ignored)
data/sample/          10k reviews + 5k products, committed (see "Running on the sample")
docs/                 profiling output, audit report
scripts/              download, sample, profile, healthcheck, verify, EOS gate
src/common/           config + explicit Spark schemas
src/ingest/           Kafka producer
src/spark/            bronze job (silver / gold: planned)
src/ai/               empty package (planned)
src/serving/          empty package (planned)
tests/                unit tests for the bronze naming + gate verdict logic
run.sh                JAVA_HOME + uv wrapper
```

## Credits

Dataset: Hou et al., *Bridging Language and Items for Retrieval and Recommendation*,
McAuley Lab, UCSD (Amazon Reviews 2023).

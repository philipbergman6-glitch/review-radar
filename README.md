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
| Silver transformation | **built** | `src/spark/silver.py`; 701,528 → 694,252 rows, 0 rejects, 6,139 collision groups (7,276 rows removed); `scripts/gate_silver.py`, `scripts/reproduce_silver.py` (pandas, no shared code) |
| Gold transformation | **built** | `src/spark/gold.py` + `src/gold/rule.py` (`conf/decline_rule.toml`, provisional, holdout from 2020-01); 13,122 products materialised on a 473,268 product-month spine, 1,599 evaluable, 964 alerts = 964 episodes; GOLD_GATE=PASS, rerun identical |
| Product catalogue + JDBC enrichment | **built** | `src/catalogue/load_products.py` (COPY under a `catalogue_load_id`, 112,590 rows); silver broadcast-joins it over Spark JDBC |
| Elasticsearch review search index (BM25) | **built** | mapping contract `conf/es/reviews.contract.json` (strict, two analyzers, vector field declared); `src/serving/index_reviews.py`; 693,547 docs behind alias `reviews`; 7/7 analyzer cases |
| Elasticsearch `product_month` projection + Kibana | **built** | `src/serving/index_product_month.py` (473,268 docs, alias swap, `source_gold_snapshot_id` lineage); Kibana under `make up-ui`; dashboard `conf/kibana/product_month_dashboard.ndjson` ([screenshot](docs/assets/kibana-product-month.png)) |
| Search judgement set + analyzer decision | **built** (SEARCH_GATE=PASS; labels model-judged, see docs/decisions/search-analyzer.md) | 20 frozen queries `conf/search/queries.json`; pool of 273 docs in `eval/search/pool.jsonl`; `make judge-search` then `make eval-search`; `scripts/gate_search.py` prints SEARCH_GATE |
| AI: embeddings, semantic search | **built** (EMBED_GATE=PASS; labels model-judged, see docs/decisions/embeddings-retrieval.md) | `conf/embedding-spec.json` (MiniLM-L6-v2, hashed identity, ADR-0005); `src/ai/embed.py` → `lake.gold.review_embeddings` (345,418 vectors, cohort ≥ 20 words, pinned silver snapshot); `text_vector` populated on alias `reviews` via `make index-reviews-vectors`; `knn` + client-side RRF `hybrid` in `src/serving/search.py`; ANN recall@10 0.96 vs exact (`eval/embeddings/ann_recall.json`); H-E1 not held, H-E2/H-E3 held; `scripts/gate_embeddings.py` prints EMBED_GATE |
| AI: LLM aspect sentiment + validation | *planned* | — |
| AI: RAG question answering | *planned* | — |
| Demo notebook | *planned* | `notebooks/` empty (ADR-0009) |
| Demo runbook | **built** | [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) — Kafka retention, memory, pre-demo checklist |
| Design doc, slides | *planned* | `docs/` holds the profile, the audit and the runbook |

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
│  bronze            [built]                  │ [built]│  products    │
│  silver            [built]                  │        │  112,590 rows│
│  gold              [built]                  │        │              │
└──────┬──────────────────────────────┬───────┘        └──────────────┘
       │ Iceberg tables               │ serving projections (silver, gold)
       ▼                              ▼
┌──────────────┐              ┌──────────────────┐
│ MinIO (S3)   │              │  Elasticsearch   │
│  lakehouse   │              │  BM25 [built]    │
│    [built]   │              │  kNN [built]     │
└──────────────┘              └────────┬─────────┘
                                       ▼
                          ┌────────────────────────┐
                          │  Demo notebook         │
                          │  semantic search · RAG │
                          │  [kNN built · RAG planned] │
                          └────────────────────────┘
```

Solid today: producer → Kafka → bronze → silver → gold on Iceberg/MinIO, with Postgres as
the Iceberg catalogue, and two Elasticsearch serving projections (`reviews`, `product_month`)
with a Kibana dashboard over the gold one, the second now carrying MiniLM vectors for kNN and
hybrid retrieval. Everything marked `[planned]` is architecture, not code.

**Course technologies — in use now:** Kafka (streaming) · Spark (Structured Streaming) ·
Iceberg (table format) · MinIO (object store) · PostgreSQL (Iceberg JDBC catalogue) ·
Docker (containers) · JSON (semi-structured) · free text (unstructured) · Elasticsearch
(inverted index, custom analyzers, BM25, dense_vector int8 HNSW kNN) · Kibana · PostgreSQL as the SQL
enrichment source · sentence-transformers (MiniLM embeddings).

**AI capabilities** (brief §6.2) — (b) embeddings + semantic search is built (kNN, hybrid RRF,
two evaluation tables, ANN recall; see `docs/decisions/embeddings-retrieval.md`). Planned next, in
this order: (a) LLM aspect sentiment with a measured validation, (c) RAG on top of (b). (e), (f)
and (g) are explicitly out of scope for now.

## Setup

Requirements: macOS/Linux, Docker (Colima or Docker Desktop), `uv`, JDK 17.

```bash
brew install openjdk@17 docker-compose colima uv     # if not present
colima start --cpu 6 --memory 8 --disk 80            # if using Colima

cp .env.example .env                                 # set the MinIO/Postgres credentials; ANTHROPIC_API_KEY for the LLM steps
uv sync                                              # Python 3.11 env + installs the repo as an editable package
make up                                              # docker compose up -d; all four should be "healthy"
```

There are no default credentials. `docker compose` and `src/common/config.py` both read
`S3_ACCESS_KEY`, `S3_SECRET_KEY` and `PG_PASSWORD` from `.env` and fail loudly if any is
missing. `make` with no target lists every entrypoint (`up`, `health`, `produce`, `bronze`,
`verify`, `eos`, `test`, `lint`); the sections below show the underlying commands and their
flags.

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
make health          # = ./run.sh python scripts/healthcheck.py
```

**1 — replay the reviews into Kafka.** Creates the topic if it does not exist (6
partitions), keys every record by `parent_asin`, and reports throughput as it goes.

```bash
make produce         # = ./run.sh python -m src.ingest.producer --category All_Beauty
#  --limit N     stop after N records (0 = the whole file, the default)
#  --rate R      cap at R records/sec (0 = as fast as the broker accepts, the default)
#  --topic T     default reviews.raw
#  --source PATH override the input .jsonl
```

**Running on the sample.** `data/sample/` is committed (10k reviews, 5k products — the
first N lines of each raw file, so `scripts/make_sample.py` reproduces it exactly). It
lets you replay step 1 without the 0.54 GB download:

```bash
make produce-sample  # -> topic reviews.raw.sample (never the production topic)
make bronze-sample   # -> lake.bronze.reviews_raw_sample
make silver-sample   # -> lake.silver.reviews_sample / rejects_sample / review_collisions_sample
./run.sh python scripts/reproduce_silver.py --scope sample
make gold-sample     # -> lake.gold.product_month_sample / evaluation_points_sample / decline_episodes_sample
make index-reviews-sample index-product-month-sample   # -> ES aliases reviews_sample / product_month_sample
make embed-sample    # -> lake.gold.review_embeddings_sample (MiniLM, ~20 s on CPU)
make index-reviews-vectors-sample   # -> new reviews_sample generation carrying text_vector
```

The sample flows through its own topic and tables, so it runs against 10k rows without
touching the 701,528-row production tables. Silver's catalogue join still reads the full
`products` table (`make catalogue`), and `scripts/download_data.py` /
`scripts/profile_data.py` still need the full raw files.

**2 — drain the topic into the bronze Iceberg table.** `--trigger once` processes
everything available and stops; a duration like `--trigger 5s` keeps the query running.
The table and checkpoint are derived from the topic name, so `reviews.raw` can only ever
write to `lake.bronze.reviews_raw` / `checkpoints/bronze_reviews_raw`.

```bash
# the command that produced the current 701,528-row table
make bronze          # = ./run.sh python -m src.spark.bronze --trigger once --max-per-trigger 150000
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
make verify          # = ./run.sh python scripts/verify_iceberg.py
```

**4 — the exactly-once gate.** Loads a known number of records, starts bronze, `SIGKILL`s
it mid-stream, restarts it from the checkpoint, and asserts no loss, no duplicates, *and*
that the kill actually interrupted work in progress (a run that drained before the kill
now fails instead of printing a vacuous PASS).

```bash
make eos             # = ./run.sh python scripts/prove_exactly_once.py --records 120000 --kill-after 25
```

It runs against its own topic (`reviews.eos`) and therefore its own table
(`bronze.reviews_eos`) — it cannot touch the production one. About 65 s end to end; full
Spark output in `checkpoints/eos_run.log`.

**5 — embeddings and semantic search** (P5). The spec `conf/embedding-spec.json` hashes only
what changes vector meaning (model, revision, sequence length, normalisation, text prep, cohort
threshold; ADR-0005); batch size and device are execution settings and never hashed.

```bash
make embed                  # silver snapshot -> lake.gold.review_embeddings (345,418 vectors, ~18 min CPU)
make index-reviews-vectors  # new `reviews` generation with text_vector, alias swap, ID sets checked
make pool-embeddings        # pool knn / hybrid / cohort systems for the 20 frozen queries
make export-judgements      # unjudged pool docs -> eval/search/unjudged.jsonl (retriever hidden)
./run.sh python scripts/judge_search.py --import labels.jsonl --judge-name <name>
make ann-recall             # HNSW recall@10 vs exact cosine, 200 docs + 20 queries
make eval-embeddings        # two tables, H-E1..3 verdicts -> docs/decisions/embeddings-retrieval.md
make gate-embeddings        # EMBED_GATE=PASS|FAIL
```

Retrieval systems (`src/serving/search.py`): `bm25_stemmed` (default), `knn` (k=50,
num_candidates=200, cohort only), `hybrid` (client-side RRF, k=60, window 50, ties by
review_id; the ES RRF endpoint needs an Enterprise licence). Labels are model-judged
(`claude`) with a 4-label human audit set; the decision doc states this.

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
| MinIO API / console | `localhost:9000` / `localhost:9001` | `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `.env` |
| Elasticsearch | `localhost:9200` | security disabled (local only) |
| PostgreSQL | `localhost:5432` | `PG_USER` / `PG_PASSWORD` from `.env`, db `catalog` |
| Kibana (`make up-ui`) | `localhost:5601` | security disabled (local only) |

## Resource envelope

Everything here shares one 16 GB laptop, and the budget is tighter than it looks.

| what | budget | where |
|---|---|---|
| Colima VM | 8 GB / 6 CPU | `colima start --cpu 6 --memory 8` |
| Kafka container | 2 GB (`-Xmx1G` heap) | `docker-compose.yml` |
| Elasticsearch container | 2 GB (1 GB heap) | `docker-compose.yml` |
| Kibana container (profile `ui`, optional) | 1 GB | `docker-compose.yml` |
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
.github/workflows/    CI: ruff + pytest on every push (no JDK yet — see the comment in ci.yml)
conf/postgres-init/   catalogue + audit schema
data/raw/             downloaded JSONL (git-ignored)
data/sample/          10k reviews + 5k products, committed (see "Running on the sample")
docs/                 profiling output, audit report
scripts/              download, sample, profile, healthcheck, verify, EOS gate, silver gate + reproduction, kibana import, search pool/judge/eval/gate, ANN recall, embeddings eval/gate
src/common/           config, Spark schemas, canonical review identity, run ledger
src/catalogue/        product catalogue loader (Postgres)
src/ingest/           Kafka producer
src/spark/            bronze + silver + gold jobs
src/ai/               embedding spec + hash, MiniLM encoder, Spark embeddings job -> gold.review_embeddings
src/serving/          ES mapping contracts, projections (reviews, product_month), retrieval systems, judgements
tests/                unit tests for the bronze naming + gate verdict logic
Makefile              every entrypoint: up, health, catalogue, produce, bronze, silver, gate-silver, reproduce-silver, gold, index-reviews, index-product-month, up-ui, kibana-import, pool-search, judge-search, eval-search, gate-search, embed, index-reviews-vectors, pool-embeddings, export-judgements, ann-recall, eval-embeddings, gate-embeddings, verify, eos, test, lint
run.sh                JAVA_HOME + uv wrapper (make targets go through it)
```

## Credits

Dataset: Hou et al., *Bridging Language and Items for Retrieval and Recommendation*,
McAuley Lab, UCSD (Amazon Reviews 2023).

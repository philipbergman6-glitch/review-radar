# Review Radar — streaming review intelligence

BIU 8688697201 — Big Data and AI, final project (solo, approved by the instructor).

Reviews stream through Kafka into a Spark Structured Streaming job that cleans them,
enriches them against a SQL product catalogue, writes an Iceberg lakehouse on an S3
object store, and indexes them into Elasticsearch — where an AI layer adds embeddings,
LLM-derived aspect sentiment, semantic search, RAG question answering and anomaly detection.

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
│  bronze → silver → gold                     │        │  products    │
│  parse · clean · dedupe · join · KPIs       │        └──────────────┘
└──────┬──────────────────────────────┬───────┘
       │ Iceberg tables               │ enriched docs
       ▼                              ▼
┌──────────────┐              ┌──────────────────┐
│ MinIO (S3)   │              │  Elasticsearch   │
│  lakehouse   │              │  BM25 + kNN      │
└──────────────┘              └────────┬─────────┘
                                       ▼
                          ┌────────────────────────┐
                          │  Streamlit app         │
                          │  semantic search · RAG │
                          │  anomalies · narrative │
                          └────────────────────────┘
```

**Course technologies used:** Kafka (streaming) · Spark (batch + streaming) · Iceberg
(table format) · MinIO (object store) · Elasticsearch (NoSQL + vector search) · PostgreSQL
(RDBMS/SQL enrichment) · Docker (containers) · JSON (semi-structured) · free text (unstructured).

**AI capabilities** (brief §6.2): (a) LLM enrichment, (b) embeddings + semantic search,
(c) RAG, (e) streaming AI enrichment, (f) ML anomaly detection, (g) insight narrative.

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

## Service endpoints

| service | endpoint | credentials |
|---|---|---|
| Kafka | `localhost:9092` | — |
| MinIO API / console | `localhost:9000` / `localhost:9001` | `minioadmin` / `minioadmin` |
| Elasticsearch | `localhost:9200` | security disabled (local only) |
| PostgreSQL | `localhost:5432` | `bigdata` / `bigdata`, db `catalog` |

Memory is capped per container in `docker-compose.yml` (ES 1 GB heap, Kafka 1 GB) because
Spark runs on the host on the same 16 GB machine.

## Layout

```
conf/postgres-init/   catalogue + audit schema
data/raw/             downloaded JSONL (git-ignored)
data/sample/          10k-review sample, committed for the submission
docs/                 profiling output, design document
scripts/              download, sample, profile
src/common/           config + explicit Spark schemas
src/ingest/           Kafka producer
src/spark/            bronze / silver / gold jobs
src/ai/               embeddings, LLM enrichment, anomaly detection
src/serving/          Streamlit app
run.sh                JAVA_HOME + uv wrapper
```

## Credits

Dataset: Hou et al., *Bridging Language and Items for Retrieval and Recommendation*,
McAuley Lab, UCSD (Amazon Reviews 2023).

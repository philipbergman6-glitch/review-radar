# Review Radar — streaming review intelligence

BIU 8688697201 — Big Data and AI, final project (solo, approved by the instructor).

Reviews replay through Kafka into a Spark Structured Streaming job that lands them
unparsed — and exactly once across an unclean restart — as an Iceberg table on a MinIO
object store, with the Iceberg catalogue in PostgreSQL. From there a batch lakehouse
carries them through silver and gold into two Elasticsearch serving projections, and an
AI layer reads the review text. **Phases P2–P5 are built with passing gates; P6 Themes is
mid-flight; P7 RAG, P8 Stream and the presentation deliverables are designed but not
built.** The table below is the honest split, and it stays in this README until it is all
in the "built" column.

The project answers one question, fixed before the analysis was run (ADR-0001):

> **Which products experienced a sustained decline in customer ratings, and which
> complaint themes increased during that decline?** — asked by a category manager.

## Status

Phases and their gates. Names are the durable reference; numbers are ordering labels
(ADR-0004). Verdicts use one vocabulary — **PASS / FAIL / REPORTED / NOT_RUN** — and a
gate's PASS/FAIL carries *reproducibility* claims only; quality targets print their own
non-blocking verdict, so a phase is never reopened by a quality miss (ADR-0011). A phase
is complete only at `scope=full`.

| phase | state | gate → verdict | evidence / what is missing |
|---|---|---|---|
| P0 Foundation | **built** | healthcheck 5/5, profile byte-identical on re-run | [`docs/phase0-profile.txt`](docs/phase0-profile.txt); four compose services memory-capped for 16 GB |
| P1 Ingest — Kafka → bronze | **built** | exactly-once proof `PASS` | `src/ingest/producer.py` (701,528 records on `reviews.raw`); `src/spark/bronze.py` (701,528 rows, 96.5 MB Parquet); `scripts/prove_exactly_once.py` — see below |
| P2 Silver — parse, quarantine, dedupe, join | **built** | `SILVER_GATE=PASS`, rerun identical | `src/spark/silver.py`; 701,528 → 694,252 rows, 0 rejects, **6,139 collision groups → 7,276 rows removed**; `scripts/gate_silver.py`, `scripts/reproduce_silver.py` (independent pandas, no shared code); ADR-0007 |
| ↳ Product catalogue + JDBC enrichment | **built** | loader gate: `rows_loaded == final_count`, key unique | `src/catalogue/load_products.py` — `COPY` into staging then a transactional swap under a `catalogue_load_id`, 112,590 rows; silver broadcast-joins it over Spark JDBC each run |
| P3 Gold — product-month spine, decline episodes | **built** | `GOLD_GATE=PASS`, rerun identical | `src/spark/gold.py` + `src/gold/rule.py`; 13,122 products on a 473,268 product-month spine, 1,599 evaluable, 964 alerts = 964 episodes. `conf/decline_rule.toml` is **provisional** — thresholds freeze at the protocol freeze, holdout from 2020-01 (ADR-0001) |
| P4 Search — mapping contract, projections, Kibana | **built** | `SEARCH_GATE=PASS` | Mapping contract `conf/es/reviews.contract.json` (`dynamic: strict`, two analyzers, vector field declared), `src/serving/index_reviews.py`, 693,547 docs behind alias `reviews`, 7/7 analyzer cases. `src/serving/index_product_month.py` (473,268 docs, alias swap, `source_gold_snapshot_id` on every doc). Kibana under `make up-ui`, dashboard `conf/kibana/product_month_dashboard.ndjson` ([screenshot](docs/assets/kibana-product-month.png)). 20 frozen queries `conf/search/queries.json`, 273-doc blind pool. ADR-0004 |
| ↳ analyzer decision (quality) | **built** | `REPORTED` | Stemmed vs `text.unstemmed` measured on the frozen set; labels are **model-judged** (`judge=claude`) with a human audit subset — [`docs/decisions/search-analyzer.md`](docs/decisions/search-analyzer.md) says so rather than implying hand labels |
| P5 Embeddings — vectors, kNN, hybrid | **built** | `EMBED_GATE=PASS` (identities + completeness only) | `conf/embedding-spec.json` (MiniLM-L6-v2, hashed identity, ADR-0005); `src/ai/embed.py` → `lake.gold.review_embeddings`, 345,418 vectors on the ≥ 20-word cohort against a pinned silver snapshot; `text_vector` on alias `reviews`; `knn` + **client-side** RRF `hybrid` in `src/serving/search.py` |
| ↳ retrieval quality | **built** | `REPORTED` | ANN recall@10 **0.96** vs exact (`eval/embeddings/ann_recall.json`); **H-E1 not held**, H-E2 / H-E3 held; two tables kept separate (controlled cohort vs production) — [`docs/decisions/embeddings-retrieval.md`](docs/decisions/embeddings-retrieval.md) |
| P6 Themes — complaint-theme labelling | *mid-flight* | `THEMES_GATE` — not yet run at full scope | Ten-theme complaint taxonomy frozen as `conf/theme-taxonomy.json` v1 (discovered pre-2020, `over_ceiling=no`); `conf/theme-terms.json` v1 decides which reviews are *offered*, never what they are labelled. Primary labeller is local **`qwen3:8b`** via Ollama — `ANTHROPIC_API_KEY` is empty, so hosted Haiku ships as a `NOT_RUN` row (ADR-0003 as amended by RR-19) |
| ↳ theme quality (does **not** block P7) | *planned* | `THEMES_QUALITY` — macro-F1 ≥ 0.70, no theme recall < 0.50 | The bars did **not** move for the weaker labeller; a miss is reported FAIL with the caveat on the theme-shift table |
| ↳ ground-truth agreement | *planned* | `THEMES_AGREEMENT` → `REPORTED` | The agent labels all 400 blind (`label_source="agent_reference"`, never `human`); Philip hand-labels a stratified 50 of the audit set and the agreement is **published as a number** — per-theme and overall Cohen's kappa with Wilson intervals. Repeat-kappa is `NOT_RUN`: intra-annotator stability is undefined for a deterministic labeller |
| ↳ MLlib classifier baseline | *planned* | scored inside `THEMES_QUALITY` | `spark.ml` CountVectorizer → IDF → per-theme logistic regression, trained on the 3,000-row LLM-labelled pool, scored beside the LLM labeller and a per-theme star-only baseline. A baseline, never a predictor; `label_source="classifier"` (ADR-0002) |
| P7 RAG — grounded, cited answers | *planned* (conditional) | `RAG_GATE` = the 30/30 citation and scope contract; `RAG_QUALITY` non-blocking | 30 frozen questions (20 answerable, 10 unanswerable in three strata); thresholds are fixed-denominator integers set before measurement — grounded ≥ 16/20, adequate ≥ 14/20, abstention ≥ 8/10, false refusal ≤ 2/20; Philip is sole judge. ADR-0006 |
| P8 Stream — reconciled projection beside batch | *planned* (conditional) | `STREAM_GATE=PASS\|FAIL run_kind=control\|demo` | Paced replay of the **sorted** file into its own topic `reviews.stream`; `withWatermark` 30 d + `dropDuplicatesWithinWatermark`; lateness is **injected** (near lag 7 d must be accepted, far lag 730 d must be dropped, counts known before the run) and a control run must print zero natural drops. ADR-0010 |
| Lineage — the run ledger | **built** | `LINEAGE_GATE gate_mode= chain_clean= publication_ready= chain_links_checked=N` — `make gate-lineage` | `pipeline_runs` is the run ledger: one row per execution attempt of every job, UUID `run_id` stamped into every Iceberg snapshot, ES doc and eval artefact (ADR-0008). [`scripts/gate_lineage.py`](scripts/gate_lineage.py) walks the chain declared in [`conf/lineage_chain.toml`](conf/lineage_chain.toml) — artefact → ledger row → its outputs and the runs its inputs name — and prints how many links it checked, so a pass over an empty chain is impossible. `chain_clean` is the verdict; `publication_ready` is the stricter question and stays false while phases are pending. Every phase gate prints `run_contract_registered` for the jobs the chain gives it |
| Deliverables — demo notebook | *planned* | `DEMO_GATE … rehearsals=N max_elapsed_s= docs_present=`, threshold `rehearsals ≥ 2` | `notebooks/` empty. Stage is one notebook kernel + Kibana, **not Streamlit**; ten live moves in 4:40 of a 5:00 budget, recorded backup under `docs/demo/<date>/` (ADR-0009) |
| Deliverables — demo runbook | **built** | — | [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) — Kafka retention, memory, pre-demo checklist. §3's move list is superseded by ADR-0009/ADR-0010 and is rewritten when the notebook is built |
| Deliverables — design doc, slides | *planned* | counted by `docs_present=` in `DEMO_GATE` | Shape decided, not written: **eight sections, two pages hard, links not appendices** for the doc; **eight slides plus a title** for the talk, **9:30** of the 10:00 ceiling (5:00 slides + 4:40 demo + 20 s slack). Outlines live in the `RR-18` ticket and are cited, not copied |

**Nothing here is omitted for being unbuilt.** Every phase appears with its gate verdict;
unreached phases print `NOT_RUN` plus a `cut_reason` from `conf/lineage_chain.toml`, where
`"not reached by submission date"` is a legitimate reason. The renderer that assembles this
into the one-shape evaluation table (`make eval-table`, *not yet written*) hard-fails on an
evaluation artefact that is missing *and* not declared cut — a capability is either a number
or a written reason, never silence.

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
| reviews ≥ 20 words | 349,059 (49.8%) | the **raw ceiling** of the vector cohort — measured on the raw file, before dedupe and validation. The cohort actually embedded is every *deduplicated silver* review passing the same predicate: 345,418 vectors, the number the gate reconciles against |
| empty text | 720 (0.10%) | must be filtered before the AI step |
| rating mix | 60.0% ★5, 14.6% ★1, 6.1% ★2 | strongly J-shaped — accuracy is a useless metric here |
| time span | 2000-11-01 → 2023-09-09, peak 126,753 in 2020 | supports time-windowed drift detection |
| verified purchases | 634,969 (90.5%) | a trust signal we can weight by |
| reviews per product | median 2, p99 72, max 1,962 | heavy skew → partitioning/salting matters |
| products with ≥ 50 reviews | 1,902 | the cohort where trends are statistically meaningful |
| `price` populated | 15.7% | real data-quality problem, handled explicitly |
| review→product join match | 112,565 / 112,565 (100%) | the join is clean |
| collision groups on `(user, product, ts)` | 6,139 groups over 13,415 rows | justifies a real dedupe step. 6,138 groups are byte-exact, 1 conflicts on `helpful_vote`, **0 disagree on rating** — so silver removed 7,276 rows keeping one survivor each. The group count is not the removed-row count |

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
┌───────────────────────────────────────┐
│                Kafka                  │  reviews.raw, 6 partitions [built]
│                                       │  reviews.stream, paced + sorted [planned]
└──────┬─────────────────────────┬──────┘
       ▼                         ▼
┌────────────────────────┐  ┌──────────────────────┐    ┌────────────────────────┐
│ Spark — batch          │  │ Spark — streaming    │    │      PostgreSQL        │
│  bronze       [built]  │  │  watermark 30 d      │◀───│  1. Iceberg catalogue  │
│  silver       [built]  │◀─┼──── JDBC [built] ────┼────│  2. products, 112,590  │
│  gold         [built]  │  │  reconciled beside   │    │     rows — the SQL     │
│                        │  │  batch     [planned] │    │     enrichment source  │
└──────┬─────────────┬───┘  └──────────┬───────────┘    │  3. pipeline_runs —    │
       │             │                 │                │     the run ledger     │
       │ Iceberg     │ serving         │ stream.alerts  └────────────────────────┘
       ▼             │ projections     ▼
┌──────────────┐     │        ┌──────────────────┐
│ MinIO (S3)   │     │        │   Iceberg        │
│  lakehouse   │     │        │   [planned]      │
│    [built]   │     │        └──────────────────┘
└──────────────┘     ▼
        ┌──────────────────────────────┐
        │        Elasticsearch         │
        │  reviews: BM25 [built]       │
        │           kNN  [built]       │
        │           hybrid RRF, client-side [built]
        │  product_month [built] ──▶ Kibana [built]
        └──────────────┬───────────────┘
                       ▼
          ┌────────────────────────────┐
          │  Demo notebook  [planned]  │
          │  one kernel, ten moves     │
          │  search · themes · RAG     │
          └────────────────────────────┘
```

Solid today: producer → Kafka → bronze → silver → gold on Iceberg/MinIO, and two
Elasticsearch serving projections (`reviews`, `product_month`) with a Kibana dashboard over
the gold one, the first carrying MiniLM vectors for kNN and hybrid retrieval. Everything
marked `[planned]` is architecture, not code.

**PostgreSQL is in this pipeline three times, and they are different jobs.** (1) It is the
**Iceberg catalogue** — the thing that swaps the pointer to the current metadata file in a
transaction, which an object store cannot do because it has no atomic rename. (2) It is the
**relational enrichment source**: `products` holds 112,590 rows loaded by `COPY` under a
`catalogue_load_id`, and every silver run reads it over Spark JDBC and broadcast-joins it —
this is the course's "SQL enrichment", and it is a real join, not a mention. (3) It holds
`pipeline_runs`, the **run ledger** that gives every job execution a `run_id` stamped into
the Iceberg snapshots, ES docs and eval artefacts it produced. Conflating the three is the
easiest way to sound like Postgres is there once, decoratively.

**Course technologies — in use now:** Kafka (streaming) · Spark (Structured Streaming) ·
Iceberg (table format) · MinIO (object store) · PostgreSQL (Iceberg JDBC catalogue) ·
Docker (containers) · JSON (semi-structured) · free text (unstructured) · Elasticsearch
(inverted index, custom analyzers, BM25, dense_vector int8 HNSW kNN) · Kibana · PostgreSQL as the SQL
enrichment source · sentence-transformers (MiniLM embeddings).

**AI capabilities** (brief §6.2) — three, each with an evaluation table whose thresholds
were set *before* the measurement:

- **(b) embeddings + semantic search — built.** kNN, client-side RRF hybrid, two evaluation
  tables kept separate, ANN recall@10 0.96; `docs/decisions/embeddings-retrieval.md`.
- **(a) complaint-theme labelling — mid-flight.** A frozen ten-theme complaint taxonomy over
  review text, with a `spark.ml` classifier and a star-only baseline scored against it on a
  held-out post-2020 audit set (ADR-0002, ADR-0003). *Not* "aspect sentiment" — the taxonomy
  is complaint-only and the output is a theme set per review, not a polarity.
- **(c) RAG on top of (b) — planned, conditional.** Grounded answers with claim-level
  citations over the P4/P5 retriever (ADR-0006).

(d), (e), (f) and (g) are out of scope, with reasons in `docs/course-coverage.md`.

None of the AI layer earns *course-technology* credit — `dense_vector`, `kNN` and `KRaft`
are 0 mentions across the decks, `LLM` 2 and `RAG` 1. It scores under **AI capability**
(25%). Its one honest bridge to the course is deck 5's own line, `BM25 does not consider
the semantic meaning of the query terms or the documents` — the course names the gap, and
P5 measures it in the same engine.

## Setup

Requirements: macOS/Linux, Docker (Colima or Docker Desktop), `uv`, JDK 17.

```bash
brew install openjdk@17 docker-compose colima uv     # if not present
colima start --cpu 6 --memory 8 --disk 80            # if using Colima

cp .env.example .env                                 # set the MinIO/Postgres credentials
uv sync                                              # Python 3.11 env + installs the repo as an editable package
make up                                              # docker compose up -d; all four should be "healthy"
```

There are no default credentials. `docker compose` and `src/common/config.py` both read
`S3_ACCESS_KEY`, `S3_SECRET_KEY` and `PG_PASSWORD` from `.env` and fail loudly if any is
missing. `make` with no target lists every entrypoint (`up`, `health`, `produce`, `bronze`,
`verify`, `eos`, `test`, `lint`, `check`); the sections below show the underlying commands
and their flags. `make check` green is the standing bar before any push, and CI
(`.github/workflows/ci.yml`) runs `ruff check` + `pytest -q` on every push.

**The LLM step needs no hosted key.** P6 labels locally with `qwen3:8b` through Ollama
(`ollama serve`, ~6.7 s/review on this host), because `ANTHROPIC_API_KEY` is empty — a
constraint the project states rather than works around. `llama3.2:3b` stays as the
comparison row.

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
conf/                 frozen artifacts: es/ mapping contracts, kibana/ dashboard, search/ queries,
                      prompts/, decline_rule.toml, embedding-spec.json, theme-taxonomy.json,
                      theme-terms.json, theme_sampling.toml, postgres-init/ + postgres-migrations/
data/raw/             downloaded JSONL (git-ignored)
data/sample/          10k reviews + 5k products, committed (see "Running on the sample")
docs/adr/             eleven ADRs — the decision record; every claim below links to one
docs/decisions/       measured outcome docs (search analyzer, embeddings retrieval)
docs/                 profiling output, audit report, demo runbook, course coverage, LLM label runbook
eval/                 per-capability evaluation artefacts (search, embeddings, themes)
scripts/              download, sample, profile, healthcheck, verify, EOS gate; per-phase gates
                      (silver, search, embeddings, themes) and their independent reproductions;
                      kibana import, search pool/judge/eval, ANN recall; the P6 chain —
                      taxonomy proposal/scoring, theme-term freeze, blind export, adjudication,
                      star baseline, sentiment check
src/common/           config, Spark schemas, canonical review identity, run ledger
src/catalogue/        product catalogue loader (Postgres, COPY under a catalogue_load_id)
src/ingest/           Kafka producer
src/spark/            bronze + silver + gold jobs
src/gold/             the decline rule (conf/decline_rule.toml), provisional until the freeze
src/ai/               embedding spec + hash, MiniLM encoder, Spark embeddings job, theme labelling
src/serving/          ES mapping contracts, projections (reviews, product_month), retrieval systems
tests/                unit tests per layer: bronze naming, gate verdict logic, mapping contract,
                      analyzer cases
Makefile              every entrypoint — `make` with no target lists all of them
run.sh                JAVA_HOME + uv wrapper (make targets go through it)
```

## Decision record

Eleven ADRs in [`docs/adr/`](docs/adr/). The ones a reader should start with:

| ADR | what it fixes |
|---|---|
| 0001 | Temporal holdout from 2020-01 and the protocol freeze — thresholds are set on pre-2020 data and applied unchanged |
| 0004 | Elasticsearch holds serving projections; the mapping contract is `dynamic: strict` |
| 0005 | Embedding identity is hashed over meaning-changing settings only; hybrid fusion is client-side |
| 0008 | `pipeline_runs` is the run ledger, and every artefact carries its `run_id` |
| 0011 | A gate's PASS/FAIL carries reproducibility only; quality targets never block a phase |

Two rules run through all of them and are worth stating plainly, because they are what
separates this from a project that reports whatever it happened to measure:

1. **Thresholds are set before the measurement, and they do not move afterwards.** When the
   labeller had to drop from hosted Haiku to a local 8B model, the 0.70 macro-F1 bar stayed
   where it was; a miss is reported as a miss.
2. **A quality miss never reopens a phase.** Reopening on a quality result means tuning
   against a held-out set, which is the specific thing the holdout exists to prevent.

## Credits

Dataset: Hou et al., *Bridging Language and Items for Retrieval and Recommendation*,
McAuley Lab, UCSD (Amazon Reviews 2023).

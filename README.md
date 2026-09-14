# Review Radar — streaming review intelligence

BIU 8688697201 — Big Data and AI, final project (solo, approved by the instructor).

Reviews replay through Kafka into a Spark Structured Streaming job that lands them
unparsed — and exactly once across an unclean restart — as an Iceberg table on a MinIO
object store, with the Iceberg catalogue in PostgreSQL. From there a batch lakehouse
carries them through silver and gold into two Elasticsearch serving projections, and an
AI layer reads the review text. **P2–P8 are built and every reproducibility gate passes; the
written deliverables are complete. What is short is printed rather than hidden:
`GOLD_CALIBRATION` (power 0.1857 vs 0.80) and `THEMES_QUALITY` (macro-F1 0.4583 vs 0.70) are
measured misses against bars set beforehand, and `THEMES_AGREEMENT` / `RAG_QUALITY` are
**cut** — Philip declined both judging passes on 2026-09-14, so each carries a written reason
and no number. `make eval-table` renders all 18 capabilities and exits 0.**
The table below is the honest split, and it stays in this README until it is all in the
"built" column. The two-page version is [`docs/DESIGN.md`](docs/DESIGN.md); the talk is
[`docs/SLIDES.md`](docs/SLIDES.md).

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
| P3 Gold — product-month spine, decline episodes | **built** | `GOLD_GATE=PASS`, rerun identical | `src/spark/gold.py` + `src/gold/rule.py`; 5,966 materialised products on a 263,792 product-month spine, 12,109 evaluable points, 425 alerts = 425 episodes. `conf/decline_rule.toml` is **frozen** (B6 R12 δ0.3 P3 min_reviews 20), sealed in one commit after `scripts/calibrate_gold.py` chose it and before any holdout point was evaluated — ADR-0001, ADR-0012, ADR-0014. `GOLD_CALIBRATION=FAIL` (non-blocking, run `6e1c0d3a`): placebo 0.80 alerts/month under the ceiling of 1; detection power at δ=0.3 **0.1857** [0.1678, 0.2051] vs target 0.80 — the highest of 116 admissible configurations, reported as the miss it is. **Holdout result: 139 of 502 eligible products alerted in 2020–2023 (27.7%)** |
| P4 Search — mapping contract, projections, Kibana | **built** | `SEARCH_GATE=PASS` | Mapping contract `conf/es/reviews.contract.json` (`dynamic: strict`, two analyzers, vector field declared), `src/serving/index_reviews.py`, 693,547 docs behind alias `reviews`, 7/7 analyzer cases. `src/serving/index_product_month.py` (263,792 docs, alias swap, `source_gold_snapshot_id` on every doc). Kibana under `make up-ui`, dashboard `conf/kibana/product_month_dashboard.ndjson` ([screenshot](docs/assets/kibana-product-month.png)). 20 frozen queries `conf/search/queries.json`, 273-doc blind pool. ADR-0004 |
| ↳ analyzer decision (quality) | **built** | `REPORTED` | Stemmed vs `text.unstemmed` measured on the frozen set; labels are **model-judged** (`judge=claude`) with a human audit subset — [`docs/decisions/search-analyzer.md`](docs/decisions/search-analyzer.md) says so rather than implying hand labels |
| P5 Embeddings — vectors, kNN, hybrid | **built** | `EMBED_GATE=PASS` (identities + completeness only) | `conf/embedding-spec.json` (MiniLM-L6-v2, hashed identity, ADR-0005); `src/ai/embed.py` → `lake.gold.review_embeddings`, 345,418 vectors on the ≥ 20-word cohort against a pinned silver snapshot; `text_vector` on alias `reviews`; `knn` + **client-side** RRF `hybrid` in `src/serving/search.py` |
| ↳ retrieval quality | **built** | `REPORTED` | ANN recall@10 **0.96** vs exact (`eval/embeddings/ann_recall.json`); **H-E1 not held**, H-E2 / H-E3 held; two tables kept separate (controlled cohort vs production) — [`docs/decisions/embeddings-retrieval.md`](docs/decisions/embeddings-retrieval.md) |
| P6 Themes — complaint-theme labelling | **built** | `THEMES_GATE=PASS` 7/7, `scope=full` (run `e39584c3`) | Ten-theme complaint taxonomy frozen as `conf/theme-taxonomy.json` v1 (discovered pre-2020, `over_ceiling=no`); `conf/theme-terms.json` v1 decides which reviews are *offered*, never what they are labelled. Primary labeller is local **`qwen3:8b`** via Ollama — `ANTHROPIC_API_KEY` is empty, so hosted Haiku was replaced, not run, and has no row of its own (ADR-0003 amendment a, RR-19); [`docs/LLM_LABEL_RUNBOOK.md`](docs/LLM_LABEL_RUNBOOK.md) is the labelling procedure |
| ↳ theme quality (does **not** block P7) | **built, evaluated, below target** | `THEMES_QUALITY=FAIL` — macro-F1 **0.4583** [0.3647, 0.5223] vs bar 0.70; min supported recall 0.4667 vs 0.50 | Measured once on the held-out 200-review audit set. Beside it on the same pass: MLlib classifier **0.3899** [0.3074, 0.4639] and the star-only floor **0.2968** [0.2612, 0.3493] — the labeller's interval is **disjoint** from the floor's, the classifier's is not. The bars did **not** move for the weaker labeller; 27/200 labellings failed to parse (13.5%) and each scores as an empty prediction |
| ↳ ground-truth agreement | **cut** (Philip declined the pass, 2026-09-14) | `THEMES_AGREEMENT=NOT_RUN` — written reason, no number | The agent labels all 400 blind (`label_source="agent_reference"`, never `human`). RR-21 defined this row as agent against **Philip's** hand-labelled 50; he declined it, so the capability is cut in `conf/lineage_chain.toml` and the correlated-error caveat is unpriced (ADR-0003 amendment e). Beside it, as what they are and not as this measurement: two in-session agent passes over the same blind 50, kappa **0.926** and **0.864** (`eval/themes/agent2-consistency-audit.json`, `agent3-adjudication-audit.json`) — same model family on both sides, so reproducibility, not accuracy. Repeat-kappa is `NOT_RUN`: intra-annotator stability is undefined for a deterministic labeller |
| ↳ MLlib classifier baseline | **built** | scored inside `THEMES_QUALITY`, macro-F1 0.3899 | `spark.ml` CountVectorizer → IDF → per-theme logistic regression, trained on the 3,000-row LLM-labelled pool, scored beside the LLM labeller and a per-theme star-only baseline. A baseline, never a predictor (ADR-0002); `label_source="classifier"` is one of the five values ADR-0011 fixes in `src/ai/labels.py` |
| P7 RAG — grounded, cited answers | **built** | `RAG_GATE=PASS` 8/8 `scope=full` (run `c4ae0dc5`); 30/30 answered; 29/30 clear the citation contract, the single violation being an uncited claim rather than a bad citation — every cited handle resolves to its own retrieved set and to a month inside the declared window, which is the half that blocks. `RAG_QUALITY=NOT_RUN` — **cut**, Philip declined the judging pass on 2026-09-14; written reason, no number | 30 frozen questions (20 answerable, 10 unanswerable in three strata); thresholds are fixed-denominator integers set before measurement — grounded ≥ 16/20, adequate ≥ 14/20, abstention ≥ 8/10, false refusal ≤ 2/20; Philip is sole judge and declined, so the capability is cut in `conf/lineage_chain.toml` ([`docs/RAG_JUDGING.md`](docs/RAG_JUDGING.md) is the unused worksheet). Run `c4ae0dc5` was reopened from `ffbbe884` under RR-24; the prior 6/7 `FAIL` stays on record as the `rag (prior)` row, and 28/28 shared answers are identical. Beside it, as a pre-check and not as the measurement: one agent pass by a different model family (`annotator=claude`, `eval/rag/agent-judged-quality.json`) reads grounded **11/20** and adequate **10/20** under their bars, abstention **10/10** and false refusal **1/20** over theirs. ADR-0006 |
| P8 Stream — sorted topic + paced replay | **built** | run contracts `sort_replay`, `stream_produce` | `conf/stream_replay.toml` frozen *before* the first run (3,000 rec/s, slice sizes, lags). `src/ingest/sort_replay.py` orders the file by `(timestamp, review_id, line digest)` in 4.8 s — 701,528 rows, 0 rejects, 2000-11-01 → 2023-09-09, input and output digests in the ledger. `src/ingest/stream_producer.py` paces it into `reviews.stream` with an event-time clock; measured 2,999 rec/s against the frozen 3,000 and the contract fails a run more than 10% off. The sort reproduces silver's dedupe arithmetic independently — see below |
| ↳ P8 Stream — reconciled projection beside batch | **built**, both runs pass | `STREAM_GATE=PASS run_kind=control` 11/11 (run `74dde108`), `STREAM_GATE=PASS run_kind=demo` 18/18 (run `c95be84a`), both `scope=full`, both reconciled against the frozen gold run `0cbdcc0d` | `withWatermark` 30 d + `dropDuplicatesWithinWatermark(review_id)`, `foreachBatch` summing per-batch contributions into `stream.product_month`. The **control** run replays the sorted file untouched and prints zero drops over 129,330 product-months compared, zero differing. The **demo** run releases slices held back in counts frozen before it ran -- 2,000 rows 7 days late (accepted), 2,000 rows 730 days late (dropped) -- onto `reviews.stream.demo`: exactly 2,000 dropped, and all 882 differing plus 177 gold-only product-months explained by the dropped rows, none only in the stream. Near acceptance is *derived*, not observed per row, and the gate line says so. `distinct_users` is not projected (not summable). ADR-0010 |
| Lineage — the run ledger | **built** | `LINEAGE_GATE gate_mode=development chain_clean=true publication_ready=false chain_links_checked=101` → `PASS` 7/7 (run `4447727f`) — `make gate-lineage` | `pipeline_runs` is the run ledger: one row per execution attempt of every job, UUID `run_id` stamped into every Iceberg snapshot, ES doc and eval artefact (ADR-0008). [`scripts/gate_lineage.py`](scripts/gate_lineage.py) walks the chain declared in [`conf/lineage_chain.toml`](conf/lineage_chain.toml) — artefact → ledger row → its outputs and the runs its inputs name — and prints how many links it checked, so a pass over an empty chain is impossible. `chain_clean` is the verdict; `publication_ready` prints beside it and is `false` by construction — the walk pins superseded runs on purpose (frozen frames, the pre-freeze calibration run, P8's two projections), so publication mode is declined, not pending (ADR-0013). Every phase gate prints `run_contract_registered` for the jobs the chain gives it |
| Deliverables — demo notebook | **built**, rehearsed three times | `DEMO_GATE rehearsals=3/2 max_elapsed_s=259 all_cells_ok=true stream_running=true backup_playable=true docs_present=4/4` → `PASS` — `make gate-demo` | [`notebooks/demo.ipynb`](notebooks/demo.ipynb) — one kernel holding one Spark session, one ES client and one Postgres connection; every cell calls [`src/serving/demo.py`](src/serving/demo.py), so the stage runs the pipeline's own code path. Ten moves budgeted at 4:40 of 5:00 and measured at 4:19 (`max_elapsed_s=259`), Kibana carrying exactly one; moves 2 and 10 now run the **P8 demo run** on `reviews.stream.demo`, so the control topic survives a rehearsal. A **rehearsal is an executed export** under [`docs/demo/`](docs/demo), never a self-report: the cells' own recorded timestamps give the duration, move 10's `DEMO_LIVE` line gives the projection's micro-batches, and a `stream_produce` ledger run that *began inside* those timestamps is the second, independent record that the stream was live. `make rehearse` produces one; ADR-0009 is why the stage is a notebook |
| Deliverables — demo runbook | **built** | — | [`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) — Kafka retention, memory, pre-demo checklist, and §3's live section rewritten as the ten moves the notebook runs (ADR-0009/ADR-0010) |
| Deliverables — design doc, slides | **built** | `docs_present=4/4`, so `DEMO_GATE=PASS` | [`docs/DESIGN.md`](docs/DESIGN.md) — eight sections, links not appendices; its §4 phase table is one row per capability in `conf/lineage_chain.toml` and `tests/test_written_deliverables.py` fails if it disagrees with `make eval-table`. [`docs/SLIDES.md`](docs/SLIDES.md) — eight slides plus a title, **290 s of slides + 280 s of demo = 9:30** of the 10:00 ceiling, slide 2 shipping two variants with the fork rule and the numbers that called it, five prepared Q&A answers each carrying a number, and the four declines in their binding wording |

**Nothing here is omitted for being unbuilt.** Every phase appears with its gate verdict;
unreached phases print `NOT_RUN` plus a `cut_reason` from `conf/lineage_chain.toml`, where
`"not reached by submission date"` is a legitimate reason. The renderer that assembles this
into the one-shape evaluation table (`make eval-table`) hard-fails on an evaluation artefact
that is missing *and* not declared cut — a capability is either a number or a written reason,
never silence. Today every capability clears that test: 18 rendered, 0 errors — the freeze
landed, so `gold_calibration` prints a number, and the two declined passes print their reasons.

## Dataset

**Amazon Reviews 2023** — McAuley Lab, UC San Diego.
<https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023>

Two semi-structured JSONL sources that we join:

| file | content |
|---|---|
| `raw/review_categories/<CATEGORY>.jsonl` | one JSON object per review — free text, rating, nested `images[]` |
| `raw/meta_categories/meta_<CATEGORY>.jsonl` | one JSON object per product — nested `images[]`/`videos[]`, `features[]`, `description[]`, free-form `details{}` |

- **Dev category** `All_Beauty` — 0.33 GB reviews + 0.21 GB metadata (701,528 reviews).
- **Full category** `Beauty_and_Personal_Care` — 11.0 GB reviews + 2.8 GB metadata (23.9M reviews per the dataset card; sizes as downloaded, not re-measured).

Public dataset, no personal data of ours — which is what makes sending review text to a
hosted LLM acceptable here (see the brief's rule on third-party services).

## What the data actually looks like

Measured with Spark over all 701,528 dev-category reviews — full output in
[`docs/phase0-profile.txt`](docs/phase0-profile.txt); the collision split on the last row
comes from `eval/silver/gate.json`.

| property | value | why it matters |
|---|---|---|
| reviews / catalogue products / users | 701,528 / 112,590 / 631,986 | real volume, not a toy table |
| review text | mean 33 words, median 102 chars, max 14,989 chars | genuinely unstructured |
| reviews ≥ 20 words | 349,059 (49.8%) | the **raw ceiling** of the vector cohort — measured on the raw file, before dedupe and validation. The cohort actually embedded is every *deduplicated silver* review passing the same predicate: 345,418 vectors, the number the gate reconciles against |
| empty text | 720 (0.10%) | must be filtered before the AI step |
| rating mix | 60.0% ★5, 14.6% ★1, 6.1% ★2 | strongly J-shaped — accuracy is a useless metric here |
| time span | 2000-11-01 → 2023-09-09, peak 126,753 in 2020 | supports time-windowed drift detection |
| verified purchases | 634,969 (90.5%) | a trust signal we can weight by |
| reviews per product | median 2, p99 72, max 1,962 | heavy skew → partitioning/salting matters |
| products with ≥ 50 reviews | 1,902 | the cohort where trends are statistically meaningful |
| `price` populated | 15.7% | real data-quality problem, handled explicitly |
| review→product join match | 112,565 reviewed products / 112,565 (100%) | the join is clean |
| collision groups on `(user, product, ts)` | 6,139 groups over 13,415 rows | justifies a real dedupe step. 6,138 groups are byte-exact, 1 conflicts on `helpful_vote`, **0 disagree on rating** — so silver removed 7,276 rows keeping one survivor each. The group count is not the removed-row count |

**The stream's sort reproduces silver's dedupe arithmetic, from raw JSONL, sharing no code
with it.** `make sort-replay` orders the file by `(timestamp, review_id, line digest)` and
records what it saw: 701,528 rows, **694,252 distinct review ids**, **7,276 key-collision
rows**, of which **7,275 are byte-identical and exactly one is not** (the split lives in the sort's ledger row, not in an eval artefact). Those are silver's
numbers — 694,252 surviving rows, 7,276 removed, 6,138 byte-exact groups and one conflicting
on `helpful_vote` — arrived at by a different program reading the source file directly. The
count is stored in the sort's ledger row because ADR-0010 dedupes the stream on `review_id`,
so it is also the number of rows the streaming job will drop, known before that job exists.

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
│                                       │  reviews.stream, paced + sorted [built]
└──────┬─────────────────────────┬──────┘
       ▼                         ▼
┌────────────────────────┐  ┌──────────────────────┐    ┌────────────────────────┐
│ Spark — batch          │  │ Spark — streaming    │    │      PostgreSQL        │
│  bronze       [built]  │  │  watermark 30 d      │◀───│  1. Iceberg catalogue  │
│  silver       [built]  │◀─┼──── JDBC [built] ────┼────│  2. products, 112,590  │
│  gold         [built]  │  │  reconciled beside   │    │     rows — the SQL     │
│                        │  │  batch       [built] │    │     enrichment source  │
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
          │  Demo notebook    [built]  │
          │  one kernel, ten moves     │
          │  search · themes · RAG     │
          └────────────────────────────┘
```

Solid today: producer → Kafka → bronze → silver → gold on Iceberg/MinIO, the streaming
projection reconciled beside batch, and two Elasticsearch serving projections (`reviews`,
`product_month`) with a Kibana dashboard over the gold one, the first carrying MiniLM vectors
for kNN and hybrid retrieval. The one box still marked `[planned]` is `stream.alerts`: the
streaming projection writes `stream.product_month`, and the alert table ADR-0010 sketches on
top of it is architecture, not code.

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
- **(a) complaint-theme labelling — built, and below its bar.** A frozen ten-theme complaint taxonomy over
  review text, with a `spark.ml` classifier and a star-only baseline scored against it on a
  held-out post-2020 audit set (ADR-0002, ADR-0003). *Not* "aspect sentiment" — the taxonomy
  is complaint-only and the output is a theme set per review, not a polarity.
- **(c) RAG on top of (b) — built.** Grounded answers with claim-level citations over the
  P4/P5 retriever; 30/30 answered, 29/30 clear the contract (ADR-0006). The four quality
  targets are `NOT_RUN`: the judging pass was declined on 2026-09-14 (`docs/RAG_JUDGING.md`).

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
make up                                              # docker compose up -d && ps; kafka/minio/es/postgres "healthy", bd-minio-init exits 0
```

There are no default credentials. `docker compose` and `src/common/config.py` both read
`S3_ACCESS_KEY`, `S3_SECRET_KEY` and `PG_PASSWORD` from `.env` and fail loudly if any is
missing; compose also requires `PG_USER`, which `config.py` defaults to `bigdata`. `make`
with no target lists every target (about 130); the sections below walk the ones a full run
needs, with the underlying commands and their flags. `make check` green is the standing bar before any push, and CI
(`.github/workflows/ci.yml`) runs `ruff check` + `pytest -q` on every push.

**The LLM step needs no hosted key.** P6 labels locally with `qwen3:8b` through Ollama
(`ollama serve`, ~6.7 s/review on this host), because `ANTHROPIC_API_KEY` is empty — a
constraint the project states rather than works around. `llama3.2:3b` stays as the
comparison row.

Load the schema (only needed if the Postgres volume already existed), then apply the
migrations — checksummed and idempotent, so it is safe to run every time:

```bash
docker exec -i bd-postgres psql -U bigdata -d catalog < conf/postgres-init/01_schema.sql
make pg-migrate
```

Get the data, profile it, and load the product catalogue silver joins against:

```bash
./run.sh python scripts/download_data.py --category All_Beauty
./run.sh python scripts/make_sample.py
./run.sh python scripts/profile_data.py --category All_Beauty
make catalogue       # meta_All_Beauty.jsonl -> Postgres products, under a catalogue_load_id
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
#  --partitions P  default 6
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
make sort-replay-sample stream-produce-sample stream-aggregate-sample   # P8 on the sample -> reviews.stream.sample
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
Spark output in `checkpoints/eos_run.log`. A driver killed outside this harness leaves its
ledger row `running`; `make reconcile-run RUN_ID=<id> REASON="..."` closes it as `failed`,
never as success.

**4b — silver, gold, the freeze, and the search projections** (P2–P4). Each job prints its
gate line; the two `reproduce-*` targets re-derive the tables in pandas sharing no code with
the Spark jobs.

```bash
make silver               # parse, quarantine, dedupe, catalogue join -> lake.silver.*; prints SILVER_GATE
make gate-silver          # SILVER_GATE on the current snapshot
make reproduce-silver     # SILVER_REPRO_GATE: independent pandas derivation, 13 checks
make gold                 # product-month spine, evaluation points, decline episodes; prints GOLD_GATE
make reproduce-gold       # GOLD_REPRO_GATE, 11 checks
make calibrate-gold       # placebo rate + injected-decline power on pre-2020 only; prints GOLD_CALIBRATION
make freeze-rule          # THE PROTOCOL FREEZE: writes the chosen thresholds into conf/decline_rule.toml
#  DRY_RUN=1                 show what would be written
#  ACCEPT_POWER_SHORTFALL=1  freeze although power is under 0.80 -- what happened on 2026-09-14
make index-reviews index-product-month   # ES projections behind aliases reviews / product_month
make kibana-import        # the product_month dashboard
make pool-search eval-search gate-search # blind pool, analyzer decision, SEARCH_GATE
```

The freeze has happened once and the rule is sealed (ADR-0001, ADR-0012); `make freeze-rule`
refuses to move it, and gold, the stream and the notebook all read the frozen file.

**5 — embeddings and semantic search** (P5). The spec `conf/embedding-spec.json` hashes only
what changes vector meaning (model, revision, sequence length, normalisation, text prep, cohort
threshold; ADR-0005); batch size and device are execution settings and never hashed.

```bash
make embed                  # silver snapshot -> lake.gold.review_embeddings (345,418 vectors, ~18 min CPU)
make index-reviews-vectors  # new `reviews` generation with text_vector, alias swap, ID sets checked
make pool-embeddings        # pool knn, hybrid, bm25_cohort, hybrid_cohort for the 20 frozen queries
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

**6 — the sorted topic and the paced replay** (P8). The replay protocol
`conf/stream_replay.toml` is frozen: rate, slice sizes and lags were committed before the
first run, because a rate chosen after watching a run would make the demo's timing claim a
description rather than a prediction. Both jobs refuse to start while it says
`status = "provisional"`.

```bash
make sort-replay          # raw -> All_Beauty.sorted.jsonl, event-time order, digests in the ledger
make stream-produce       # resets reviews.stream, then paced replay at 3,000 rec/s (control run, no injection)
#  --limit N     stop after N records (0 = the whole file, the default)
make stream-produce-sample   # the sorted sample -> reviews.stream.sample, never the full topic
```

The sort is over an *index* of `(timestamp, review_id, line digest, byte offset)`, not over
the file: the 16 GB envelope does not hold 0.33 GB of JSON plus a sorted copy of it. It
takes 4.8 s for the full category. The digest is in the sort key because `review_id` is not
unique in this source — that is the key-collision finding above — so without it the order of
7,276 rows would depend on how the file was downloaded.

The replay paces against an absolute schedule rather than sleeping a fixed interval per
record, and the run contract fails a run whose measured rate is more than 10% off the frozen
target: `701,528 / 3,000 = 234 s`, so twenty-three years of event time pass in 3:54 of wall
clock while the rest of the demo runs. Lateness is **not** injected here — that is the
control run ADR-0010 requires. `make stream-demo` runs the same replay with `--inject`: `src/ingest/lateness.py` draws the two slices from the frozen config by
`slice_rank(seed, salt, review_id)`, holds each drawn row back until the first natural row
at or after its event time plus its lag, and simulates the watermark over that order —
a plan not predicted to land exactly on the frozen counts fails before Kafka is opened.
Spark judges a batch's late rows by the watermark in force one batch earlier, which the
model reproduces and `tests/test_stream_spark.py` checks against
`numRowsDroppedByWatermark`. The demo goes onto its own topic because the lineage gate
resolves a Kafka output by comparing the topic's record count with the run's acked count.

```bash
make stream-aggregate     # the watermarked product-month projection of reviews.stream; drains and stops
make gate-stream          # STREAM_GATE for the control run, reconciled against the frozen gold
make stream-demo          # resets reviews.stream.demo, replays with --inject, then projects it (the notebook backgrounds this at move 2)
make gate-stream-demo     # STREAM_GATE for the demo run -- also requires the control artefact
```

**7 — the evaluation table, the gates and the deliverables.** One row per capability declared
in `conf/lineage_chain.toml`; the command exits non-zero on a capability that has neither an
artefact nor a `cut_reason`. P6 and P7 each have a chain of some twenty targets (`make` lists
them under `theme-*` and `rag-*`); their gates are the two lines below, and
[`docs/LLM_LABEL_RUNBOOK.md`](docs/LLM_LABEL_RUNBOOK.md) is the P6 procedure.

```bash
make gate-themes                 # THEMES_GATE, then THEMES_QUALITY beside its bar
make gate-rag                    # RAG_GATE, then RAG_QUALITY (cut) beside its bars
make eval-table                  # the whole submission, one row per capability
make gate-lineage                # development-mode walk: artefact -> ledger row -> outputs -> the runs its inputs name; prints LINEAGE_GATE
make gate-lineage-publication    # the publication bar; cannot pass by construction (ADR-0013), kept so the stale count prints
make demo                        # open the notebook in Jupyter Lab (ten moves, one kernel)
make demo-run-sheet              # the running order, its seconds, any move marked pending
make rehearse                    # execute the notebook into docs/demo/<date>/, then re-run stream-aggregate-demo + gate-stream-demo (run it twice)
make gate-demo                   # DEMO_GATE: 2 rehearsals, <= 300 s each, 4/4 written deliverables
```

The written deliverables are [`docs/DESIGN.md`](docs/DESIGN.md) (two pages, eight sections),
[`docs/SLIDES.md`](docs/SLIDES.md) (the talk, 9:30 of a 10:00 ceiling),
[`docs/DEMO_RUNBOOK.md`](docs/DEMO_RUNBOOK.md) and this README — the gate checks all four are
present. [`docs/QA-FAILS.md`](docs/QA-FAILS.md) is the viva sheet for the two measured misses.
`tests/test_written_deliverables.py` checks the
design doc's phase table against `conf/lineage_chain.toml` and the live gate artefacts, the
deck's running order against its own clock, and the four declines against their binding
wording — so a document cannot quietly drift away from the system it describes.

### Measured producer throughput

The producer replays the full 701,528-review file in roughly 1.5–3 s: **232,000–484,000
rec/s**, measured with

```bash
./run.sh python -m src.ingest.producer --topic audit.perf   # whole file, --rate 0
```

on an Apple Silicon laptop (16 GB) with the stack running under Colima. Quote the range,
not a point estimate: the high end is an otherwise-idle host, the low end is the same
command with nine unrelated containers competing for the VM. Only the low end was reproduced
by the 2026-09-01 audit (231,849 rec/s); the high end is the pre-audit claim. The number is
host- and load-dependent, so a single figure is not reproducible.

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
| `.venv` on disk | 1.8 GB (torch alone is 516 MB) | `uv sync` |

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
conf/                 frozen artifacts: lineage_chain.toml (the declared chain), es/ mapping contracts,
                      kibana/ dashboard, search/ queries, prompts/, decline_rule.toml (sealed),
                      stream_replay.toml, embedding-spec.json, theme-taxonomy.json, theme-terms.json,
                      theme_sampling.toml, theme-classifier.json, theme-label-spec.json,
                      theme-star-baseline.json, rag-questions.json + rag-*-spec.json, two JSON schemas
                      (eval artefact, LLM label), postgres-init/ + postgres-migrations/
data/raw/             downloaded JSONL (git-ignored)
data/sample/          10k reviews + 5k products, committed (see "Running on the sample")
docs/adr/             fourteen ADRs — the decision record; every claim below links to one
docs/decisions/       measured outcome docs (search analyzer, embeddings retrieval, theme- and RAG-prompt freezes)
docs/                 DESIGN.md (the two-page design doc), SLIDES.md (the talk), DEMO_RUNBOOK.md,
                      profiling output, audit prompt + report, QA-FAILS.md (viva sheet), course
                      coverage, LLM label runbook, RAG judging worksheet (declined), research/
                      notes, theme-taxonomy/ (discovery-run inputs to the frozen taxonomy), assets/
docs/demo/            the recorded backup: executed rehearsal notebooks, HTML, Kibana PNG, EOS transcript
eval/                 per-capability evaluation artefacts -- eval/<capability>/gate.json is what
                      `make eval-table` and `make gate-lineage` both read
scripts/              download, sample, profile, healthcheck, verify, EOS gate; per-phase gates
                      (silver, gold, search, embeddings, themes, rag, stream, demo, lineage) and
                      the two independent reproductions; calibrate_gold + freeze_rule; eval_table,
                      pg_migrate, reconcile_run, rehearse_demo; kibana import, search
                      pool/judge/eval, ANN recall; the P6 chain — taxonomy proposal/scoring,
                      theme-term freeze, blind export, adjudication, star baseline, sentiment
                      check; the P7 chain — question freeze, validation, prefilter, judging
src/common/           config, Spark schemas, canonical review identity, run ledger, the evaluation
                      artefact contract, SparkSession builder
src/catalogue/        product catalogue loader (Postgres, COPY under a catalogue_load_id)
src/ingest/           Kafka producer; the P8 event-time sort and its paced stream replay
src/spark/            bronze + silver + gold jobs; the P8 streaming projection; MLlib theme classifier;
                      theme sample frames
src/gates/            pure verdict functions, one module per gated capability; scripts/gate_*.py do the I/O
src/gold/             the decline rule (conf/decline_rule.toml, frozen) + its calibration
src/ai/               embedding spec + hash, MiniLM encoder, Spark embeddings job, theme labelling,
                      RAG answering, Ollama client, Wilson intervals, audit seal
src/serving/          ES mapping contracts, projections (reviews, product_month), retrieval systems,
                      the frozen judgement set, demo.py (one function per live move)
tests/                unit tests per layer: bronze naming, gate verdict logic, mapping contract,
                      analyzer cases, the written deliverables against the live artefacts
tests/integration/    ledger-on-Postgres and search-on-ES tests; skipped when the stack is down
notebooks/            demo.ipynb — the demo stage; make rehearse executes it into docs/demo/
.scratch/             planning record: the wayfinder map + RR decision tickets, the 19 ship tickets,
                      builders for the two declined human-judging pages
reference/ lessons/   teaching-workspace HTML (architecture map, glossary, lessons) + assets/ and
                      learning-records/ — not part of the submission
docker-compose.yml    the bd-* stack; CONTEXT.md is the project glossary
Makefile              every entrypoint — `make` with no target lists all of them
run.sh                JAVA_HOME + uv wrapper (make targets go through it)
```

## Decision record

Fourteen ADRs in [`docs/adr/`](docs/adr/), numbered in the order they were decided:

| ADR | what it fixes |
|---|---|
| 0001 | Temporal holdout from 2020-01 and the protocol freeze — thresholds are set on pre-2020 data and applied unchanged |
| 0002 | MLlib is a theme-classifier baseline trained on LLM labels, never a predictor |
| 0003 | A frozen, complaint-only taxonomy is labelled by an LLM; amended (a)–(e) through 2026-09-14 — local `qwen3:8b`, the agreement row cut |
| 0004 | Elasticsearch holds serving projections; the mapping contract is `dynamic: strict` |
| 0005 | Embedding identity is hashed over meaning-changing settings only; hybrid fusion is client-side |
| 0006 | The RAG evaluation protocol is frozen before measurement; amended 2026-09-14 when the judging pass was declined |
| 0007 | Silver is a snapshot-pinned batch with content-only dedupe and a JDBC catalogue join |
| 0008 | `pipeline_runs` is the run ledger, and every artefact carries its `run_id` |
| 0009 | The demo stage is one notebook, with Kibana for the decline view |
| 0010 | Streaming is a reconciled projection beside batch, with injected lateness |
| 0011 | A gate's PASS/FAIL carries reproducibility only; quality targets never block a phase |
| 0012 | The decline rule is calibrated before it is frozen, and the placebo ceiling never moves |
| 0013 | `publication_ready` is unreachable by construction; publication mode is declined out loud |
| 0014 | The placebo permutes the spine, and power injects a step into products the rule leaves alone |

Two rules run through all of them and are worth stating plainly, because they are what
separates this from a project that reports whatever it happened to measure:

1. **Thresholds are set before the measurement, and they do not move afterwards.** When the
   labeller had to drop from hosted Haiku to a local 8B model, the 0.70 macro-F1 bar stayed
   where it was; a miss is reported as a miss (ADR-0003 amendment a, ADR-0012).
2. **A quality miss never reopens a phase.** Reopening on a quality result means tuning
   against a held-out set, which is the specific thing the holdout exists to prevent.

## Credits

Dataset: Hou et al., *Bridging Language and Items for Retrieval and Recommendation*,
McAuley Lab, UCSD (Amazon Reviews 2023).

# Review Radar

A streaming data pipeline that turns 700k Amazon reviews into one answer for a category manager:

> **Which products had a sustained decline in ratings, and which complaint themes grew during that decline?**

Solo final project for Bar-Ilan University's *Big Data and AI* course. Everything runs on one 16 GB laptop in Docker.

## How it works

```
reviews (JSONL) ─▶ Kafka ─▶ Spark Structured Streaming ─▶ Iceberg tables on MinIO (S3)
                                                            bronze ─▶ silver ─▶ gold
                                                                                 │
                 PostgreSQL: Iceberg catalogue,                                  ▼
                 product table, run ledger            Elasticsearch (BM25 + vector search) ─▶ Kibana
                                                                                 │
                                                                                 ▼
                                                      AI layer: complaint themes, cited RAG answers
```

1. **Ingest.** A producer replays reviews into Kafka. Spark streams them, unparsed, into a bronze Iceberg table.
2. **Clean.** Silver parses, quarantines bad rows, removes duplicates, and joins the product catalogue from PostgreSQL.
3. **Detect.** Gold builds a product-by-month table and flags sustained rating declines with a rule frozen before testing.
4. **Serve.** Elasticsearch indexes reviews for keyword, semantic and hybrid search. Kibana charts the declines.
5. **Explain.** A local LLM labels complaint themes, and a RAG layer answers questions with citations to real reviews.

## Results

| What | Result |
|---|---|
| Scale | 701,528 reviews, 112,590 products, 2000–2023 |
| Exactly-once ingest | Job killed mid-stream and restarted: no rows lost, none duplicated |
| Deduplication | 7,276 duplicate rows removed; re-derived by a separate pandas program with identical counts |
| Decline detection | 139 of 502 eligible products alerted on the 2020–2023 holdout (27.7%) |
| Stream vs batch | Streaming totals match batch exactly across 129,330 product-months |
| Late data | Injected 2,000 rows arriving 730 days late; exactly 2,000 dropped by the watermark |
| Vector search | 345,418 review embeddings; approximate search recall@10 of 0.96 against exact |
| RAG | 30 of 30 test questions answered; 29 of 30 meet the citation contract |

## What fell short

Targets were set before measuring and never moved afterwards. Two results missed them, and they are reported as misses.

- **Decline-rule sensitivity:** detects 19% of injected 0.3-star declines. The target was 80%.
- **Theme labelling:** macro-F1 of 0.46 against a target of 0.70. It still beats the star-rating baseline (0.30).
- **Human-judged quality** for theme labels and RAG answers was not run, so no number is claimed for either.

## Engineering choices worth a look

- **Frozen thresholds.** The decline rule was tuned on pre-2020 data, sealed in one commit, then applied to 2020–2023.
- **Independent checks.** Silver and gold are re-derived in pandas with no shared code, and the counts must match.
- **Run ledger.** Every job run gets an ID stamped into its tables, search documents and evaluation files.
- **Gates.** Each phase prints a PASS/FAIL line for reproducibility. `make eval-table` shows all 18 in one table.

## Run it

Needs Docker, `uv` and JDK 17. A 10k-review sample is committed, so no download is required for the first steps.

```bash
cp .env.example .env    # set MinIO and Postgres credentials
uv sync
make up                 # Kafka, MinIO, Elasticsearch, PostgreSQL
make health             # one line per service
make produce-sample     # sample reviews -> Kafka
make bronze-sample      # Kafka -> Iceberg
```

The full run, every command and every flag are in [`docs/REFERENCE.md`](docs/REFERENCE.md).

## More

- [Design document](docs/DESIGN.pdf) (two pages) and [slides](docs/slides/review-radar.pdf)
- [Executed demo notebook](docs/demo/2026-09-14-2/demo.executed.ipynb) and its [Kibana dashboard](docs/demo/2026-09-14-2/kibana-dashboard.png)
- [Full reference](docs/REFERENCE.md): phase-by-phase evidence, setup, resource budget, repo layout
- [Decision records](docs/adr/): fourteen ADRs explaining why each choice was made

Dataset: [Amazon Reviews 2023](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023), McAuley Lab, UC San Diego.

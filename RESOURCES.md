# Review Radar Resources

## Knowledge

### Project-internal (highest trust: it is what is being graded)
- [README.md](README.md)
  Status table, architecture diagram, measured data profile, run instructions. Use for: what
  is built vs planned, the exact commands.
- [docs/course-coverage.md](docs/course-coverage.md)
  Term counts and verbatim quotes from the five lecture decks. Use for: what the course
  taught, which deviations need a defence, where course credit is earned.
- [docs/phase0-profile.txt](docs/phase0-profile.txt)
  Measured data profile over all 701,528 dev-category reviews. Use for: any number about
  the data.
- [docs/AUDIT_REPORT_2026-09-01.md](docs/AUDIT_REPORT_2026-09-01.md)
  Independent audit of phases 0–1. Use for: what held, what did not, the revised plan.
- [docs/DEMO_RUNBOOK.md](docs/DEMO_RUNBOOK.md)
  Kafka retention, memory, pre-demo checklist.
- [.scratch/wayfinder/map.md](.scratch/wayfinder/map.md) and `tickets/`
  The open decisions for phases 2–8 and the ones closed so far. Use for: the planned half.
- [docs/research/](docs/research/)
  RR-03 LLM provisioning, RR-04 RRF licence, RR-05 embedding throughput. Use for: AI-layer
  facts measured on this host.
- Course brief: `docs/course/BIU BigData 8688697201 - Project.txt`
  Grading weights and the "understanding beats complexity" rule.

### Primary vendor docs
- [Spark Structured Streaming Programming Guide](https://spark.apache.org/docs/3.5.3/structured-streaming-programming-guide.html)
  Micro-batch model, triggers, checkpointing, fault-tolerance semantics. Use for: lessons 1–2.
- [Spark + Kafka integration guide](https://spark.apache.org/docs/3.5.3/structured-streaming-kafka-integration.html)
  `startingOffsets`, `maxOffsetsPerTrigger`, `failOnDataLoss`. Use for: every bronze option.
- [Iceberg: Spark Structured Streaming](https://iceberg.apache.org/docs/1.6.1/spark-structured-streaming/)
  Streaming writes, `fanout-enabled`, how the sink dedupes committed batches. Use for: lesson 2.
- [Iceberg: Spark queries — time travel and metadata tables](https://iceberg.apache.org/docs/1.6.1/spark-queries/)
  `.snapshots`, `VERSION AS OF`. Use for: the verify script and the demo move.
- [Apache Kafka documentation](https://kafka.apache.org/documentation/)
  Producer `acks`, idempotence, partitioning by key, KRaft. Use for: lesson 4.
- [Elasticsearch 8.17 reference](https://www.elastic.co/guide/en/elasticsearch/reference/8.17/index.html)
  Analyzers, mappings, `dense_vector`, kNN search. Use for: lesson 5.
- [Databricks: medallion architecture](https://www.databricks.com/glossary/medallion-architecture)
  The bronze/silver/gold vocabulary the deck-3 slide uses. Use for: lesson 3.
- [Amazon Reviews 2023 (McAuley Lab)](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023)
  Field definitions for review and meta files.

## Wisdom (Communities)
- Course instructor and cohort — the graders. Use for: testing whether a defence lands.
- [Apache Spark user mailing list](https://spark.apache.org/community.html) and
  [Iceberg Slack](https://iceberg.apache.org/community/)
  Use for: streaming-sink edge cases if the exactly-once story breaks under a new scenario.
- Philip has not stated a community preference yet.

## Gaps
- No primary source yet on Kafka 4.0 removing ZooKeeper mode; course-coverage says verify
  before claiming it. Needed for lesson 4.
- No trusted, short explainer on RAG evaluation that fits a solo project. Needed for lesson 6.

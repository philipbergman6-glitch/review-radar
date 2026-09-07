# Teaching notes

- Philip's global preference: extremely concise; sacrifice grammar for concision. Lessons
  should follow suit — short sentences, no padding.
- He values grounding: quote the source, cite file and line, tag observed vs inferred.
  Every lesson claim links to the repo file or a primary doc.
- Mission drafted by the agent from project memory on 2026-09-04, not yet confirmed by
  Philip. Ask him to confirm or correct it at the next session.
- Lesson plan (one per session, revise as decisions close):
  1. Trace one review disk → Kafka → bronze → Iceberg/MinIO (built) — done
  2. Exactly-once: what the checkpoint and the Iceberg sink each remember
  3. The lakehouse layers: bronze/silver/gold, what silver must fix (dedupe, price, details)
  4. Kafka design choices: key, partitions, acks, idempotence, KRaft vs ZooKeeper
  5. Elasticsearch: BM25, analyzers, explicit mapping, dense_vector kNN, client-side RRF
  6. AI layer: embeddings, aspect sentiment with a validation table, RAG
  7. Defending the deviations: MinIO/HDFS, no Connect sink, Iceberg's course credit
  8. The data by numbers: cold-question drill from `docs/phase0-profile.txt`
- Open the lesson with `open lessons/NNNN-*.html` after writing it.

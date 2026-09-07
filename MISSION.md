# Mission: Own every part of Review Radar

## Why
Philip is building the BIU 8688697201 Big Data & AI final project solo and submits on
2026-09-21. The brief says outright that understanding beats complexity, and Q&A plus AI
understanding together carry 30% of the grade. He must be able to explain, from memory
and in under three minutes, why every station in the pipeline exists, what it guarantees,
and what he would answer if the grader asks "why not X?". Beyond the grade, this project
goes on his CV and he wants the data-engineering and AI-on-data knowledge to be his own.

## Success looks like
- Draw the full architecture from memory and say what each arrow carries and why.
- Explain exactly-once across a kill and restart without looking at the code.
- Defend each deliberate deviation from the course stack (MinIO not HDFS, KRaft not
  ZooKeeper, Iceberg on top of the taught lakehouse, no Kafka Connect sink) in one breath.
- Explain the planned AI layer: BM25 vs kNN, embeddings, LLM aspect sentiment, RAG, and
  how each will be measured.
- Answer any cold question about the data itself with a number from the profile.

## Constraints
- Hard deadline 2026-09-21; 4–6 focused hours/day, most of it building, so lessons must
  be short (under 15 minutes) and pay back immediately in the design doc or the demo.
- Teach the built half from the code as it is; teach the planned half from the decision
  tickets, and update lessons when a decision closes.
- Ground every claim in the repo, the course decks (`docs/course-coverage.md`), or a
  primary vendor doc. No parametric recall.

## Out of scope
- Technologies the wayfinder map has ruled out: Kafka Connect sink, HDFS, GraphFrames,
  the 23M-review full category.
- General Spark/Kafka tutorials not tied to a decision in this project.

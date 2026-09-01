# What the course actually taught

**Purpose.** The rubric awards 20% for "use of course technologies." This file
records what the five lecture decks cover, so design decisions can cite the
course instead of guessing at it.

**Method.** The decks were converted with `pdftotext -layout` into
`docs/course/*.txt` and queried with `grep`. Nothing here is summarised or
recalled from reading — every claim is a term count or a verbatim quote.
Counts are `grep -oic <term>` (case-insensitive occurrences, not slides).

**The slides are gitignored** (copyright; 12,642 lines of diff noise). They are
NOT needed to use this file — that is the point of this file. Do not re-extract
them unless a genuinely new question arises that no quote below answers. To
regenerate: `for f in ./*.pdf; do pdftotext -layout "$f" "docs/course/$(basename "$f" .pdf).txt"; done`

## Deck map

| Deck | Pages | Subject | Leading term counts |
|------|-------|---------|---------------------|
| חלק 1 | 90  | Intro, big-data landscape, NoSQL | NoSQL 19, SQL 31 |
| חלק 2 | 132 | **Hadoop ecosystem** | Hadoop 73, HDFS 41, YARN 30, ZooKeeper 27, Oozie 25, MapReduce 18, Sqoop 15, Flume 12, Avro 10 |
| חלק 3 | 121 | **Spark** | Spark 217, SQL 49, Streaming 36, MapReduce 14, MLlib 18 |
| חלק 4 | 253 | **Kafka** | Kafka 356, ZooKeeper 34, Streaming 14 |
| חלק 5 | 264 | **Elasticsearch** | Elasticsearch 126, Kibana 52, Lucene 34, Logstash 14 |

Total ≈ 860 slides. Weight by pages: Elasticsearch and Kafka are the two
largest units; Hadoop is third; Spark is taught densely in the fewest pages.

## Findings that bind project decisions

### 1. Iceberg is NOT a course technology
One occurrence in 860 slides, inside a boilerplate list of Apache projects:

> `חלק 1:474  Apache AntUnit  Apache Commons BeanUtils … Apache Iceberg  Apache Mahout  Apache PDFBox …`

**Consequence.** Iceberg earns nothing under "use of course technologies" (20%).
Keep it — it is the strongest engineering in the pipeline — but score it under
*pipeline design* (25%), and be ready for "why a technology we did not teach?"

### 2. Kibana is expected and the project does not have it
Deck 5, verbatim:

> `We will mostly use Kibana throughout this course`
> `▪ Kibana is a user interface used for data visualization and for creating detailed …`
> `Beats  Kafka  Logstash  Elasticsearch  Kibana`   (the ELK stack as taught)

52 mentions; the project has zero. Cheapest available grade point: one more
compose service against the Elasticsearch already running, and it gives the
live demo a visual surface. **Recommended: add it.**

### 3. Elasticsearch is taught as BM25, and the deck names the exact gap our AI fills
Deck 5 teaches Lucene scoring, BM25 as the default, and states its limitation:

> `▪ The default algorithm in ES for calculating the score is BM25`
> `▪ BM25 is a ranking function measuring relevance based on term frequency and …`
> `▪ No semantic understanding`
> `  ▪ BM25 does not consider the semantic meaning of the query terms or the documents`

`dense_vector` = 0 mentions, `kNN` = 0 mentions across all decks.

**Consequence.** This is the strongest AI framing available: the course
establishes BM25 and explicitly names its weakness; the project measures that
weakness and closes it with embeddings + kNN in the same engine. Build the
semantic-search demo as a **side-by-side BM25 vs kNN comparison** — it is
directly legible to the grader and answers "why not just BM25?" pre-emptively.

### 4. Spark Structured Streaming and its exactly-once claim are taught
> `SPARK STRUCTURED STREAMING`
> `▪ End-to-end exactly-once fault-tolerance guarantees through checkpointing and …`

Also taught: DStream/RDD-based Spark Streaming (the older API), `withWatermark`,
`dropDuplicatesWithinWatermark`, `checkpoint`.

**Consequence.** Phase 1's SIGKILL proof demonstrates a guarantee the course
*asserts* but does not prove. That is a genuine differentiator — lead with it.

### 5. Spark MLlib is taught (deck 3 only, 18 mentions)
> `SPARK MLLIB` … `▪ MLlib contains many algorithms and utilities`
> `▪ As of Spark 2.0, the RDD-based APIs in the spark.mllib package have entered [maintenance]`

**Consequence.** An MLlib model scores under BOTH "course technologies" (20%)
and "AI capability" (25%). A hosted-LLM call scores only under the latter.
If the AI scope must be cut, cut toward MLlib, not away from it.

### 6. Sqoop — the course itself retires it
> `▪ Apache Sqoop moved into the Attic in June 2021`
> `▪ Apache Sqoop (Extract, Load), Apache Pig (Transform)`

**Consequence.** Cutting Sqoop is defensible using the course's own slide.
Quote it verbatim in the Q&A. Same framing covers Pig (3 mentions total).

### 7. HDFS is taught hands-on, not just conceptually
> `[~]$ hdfs dfs`
> `root@eff23c07f886:~# hdfs dfs -ls /`     (a Docker container prompt)
> `▪ HDFS command line shell uses 'put' command`

41 mentions, with live shell transcripts. This is the **weakest point** in the
current design's course-technology coverage — MinIO substitutes for it on
memory grounds, but HDFS was demonstrated, not merely described. Unresolved;
decide deliberately rather than by omission.

### 8. Oozie is taught as the Hadoop ETL orchestrator
> `• Hadoop ETL – Apache Oozie`
> `▪ A need for a general-purpose system to run multistage Hadoop jobs`

25 mentions. Cutting it needs a better argument than Sqoop's, since it is not
retired. Current argument (Spark subsumes multistage orchestration) is
reasonable but is an opinion, not a citation.

### 9. Kafka is taught on ZooKeeper; the project runs KRaft
`KRaft` = **0** mentions across all decks. The deck teaches:

> `▪ ZooKeeper is used for managing and coordinating Kafka broker`
> `▪ Zookeeper is already up and running`

Producer config *is* taught in detail and matches the project exactly:

> `Producer Configuration - acks` … `▪ acks=all` … `props.put("acks", "all");`

**Consequence.** Running KRaft is modern and correct, but it removes ZooKeeper
(27+34 = 61 mentions) from the demo. Prepare the answer: KRaft replaced
ZooKeeper as Kafka's metadata quorum and ZooKeeper mode is removed in Kafka 4.0.
Verify that claim before saying it.

## Not taught anywhere (0 mentions) — cannot claim course credit
`KRaft`, `dense_vector`, `kNN`, `MinIO`. Near-zero: `Iceberg` 1, `Airflow` 1,
`Flink` 1, `Delta` 2, `LLM` 2, `RAG` 1.

## Count corrections
An earlier pass reported `RAG` ≈ 70. That was a substring false positive —
`grep -oi rag` matches sto**rag**e (33), ave**rag**e, P**rag**ue, leve**rag**ing.
True count: **1**. Any term of three or fewer letters needs `-w` or a
substring audit before it is trusted.

---

# Validation pass (2026-09-01)

The nine findings above came from a hypothesis-driven term list — I grepped for
technologies I already had in mind, which cannot find what I did not think of.
This pass re-derives the topic inventory from the **decks' own section headers**
(`grep -xE "[A-Z][A-Z0-9 &/,'.()-]{4,60}"` after whitespace strip) and checks
each header against the build.

**Result: the first pass was incomplete.** Five additions and one correction.

## Correction to Finding 1 — the lakehouse IS taught
Deck 3 has a slide titled `Big Data & Lakehouse architecture` whose diagram is
labelled, verbatim:

> `Bronze      Silver          Gold`   (deck 3, lines 138 and 208)

`medallion` = 0 mentions, but the bronze/silver/gold layering is on the slide.
Finding 1 said Iceberg earns no course credit — still true (`Iceberg` = 1, in a
boilerplate list). But the **architecture** it implements is course-taught.
Frame it that way: "the course's lakehouse diagram, implemented with a table
format that gives it ACID snapshots." That is a much stronger answer than
defending Iceberg on its own.

## Addition A — Kafka Connect (43 mentions) is taught; the project ignores it
> `KAFKA CONNECT` … `Kafka Connect & Streams`
> `▪ Kafka Connect is a tool included with Kafka that imports and exports data to Kafka`
> `▪ Dedicated to importing data from external systems into Kafka topics, and …`

43 mentions is the **third-heaviest Kafka topic in the largest deck**. The
project's ingestion is a hand-rolled `confluent-kafka` producer
(`src/ingest/producer.py`). That producer is better engineered than a Connect
config and demonstrates `acks`/idempotence, which the deck also teaches — but
Connect being absent is a real coverage hole, not a neutral choice.
Also taught, lighter: `Kafka Streams` (5), `ksqlDB` (4).

## Addition B — Elasticsearch text analysis is taught heavily; the project has none
> `TEXT ANALYSIS` / `TEXT ANALYSIS CONCEPTS` / `ANALYZERS` / `INVERTED INDEX`

`analyzer` = 54, `tokenizer` = 13, `inverted index` = 7. Deck 5 also has
sections for `INDEX TEMPLATES`, `ROUTING`, `FUZZY QUERY`, `MULTI-MATCH QUERY`,
`COMPOUND QUERIES`, `FUNCTION SCORE QUERY`, `BOOSTING QUERY`, `GEO QUERIES`,
`DATA STREAM`, `X-PACK`, and `HANDS-ON`.

Grep of the repo for `analyzer|dense_vector|mappings` in `.py`: **no matches**
outside `scripts/healthcheck.py`. Nothing indexes into Elasticsearch yet.
When it is built, defining a custom analyzer and an explicit mapping — rather
than accepting dynamic defaults — converts the single largest deck (264 pages)
from partial credit into full credit for near-zero extra effort.

## Addition C — the course defines a Big Data project by the V's
> `▪ When to define a project as a Big Data project?`
> `▪ Project that involves collection and analyze data with at least on of the 4 V's`
> `dimensions to big data known as Volume, Variety, Velocity, Variability and [Veracity]`

The design doc should name which V's this project exercises and back each with
a measured number from `docs/phase0-profile.txt`. This is the instructor's own
definitional slide; matching it costs a paragraph.

## Addition D — Spark graph processing is taught
`GraphFrames` = 5, `GraphX` = 4, plus a `Graph` node in the deck-3 architecture
diagram. Not currently in scope. **Recommend leaving it cut** — co-review
graphs are a plausible but large detour, and the 25% understanding criterion
punishes breadth. Recorded so the omission is deliberate.

## Addition E — Kibana confirmed missing in the build
`docker-compose.yml` services: `kafka`, `minio`, `minio-init`, `elasticsearch`,
`postgres`. No Kibana. Confirms Finding 2 against the actual file.

## Headers checked and already covered
Deck 1 `CAP MODEL`, `RDBMS`, `BIG DATA DB`; deck 2 `HADOOP DISTRIBUTED [FS]`,
`ZOOKEEPER`, `PARQUET`, `APACHE FLUME/OOZIE/SQOOP`, `MR V1`, `HADOOP V2`;
deck 3 `SPARK CORE/SQL/ARCHITECTURE/MLLIB/STRUCTURED STREAMING`, `DATABRICKS`;
deck 4 `KAFKA BROKER/TOPICS/PRODUCER/CONSUMER/REPLICATION/ARCHITECTURE`,
`SASL SCRAM`, `TUNING KAFKA FOR OPTIMAL PERFORMANCE`; deck 5 `LUCENE`,
`ELASTICSEARCH SCORE`, `RESTFUL API`, `SEARCHING DATA`, `CRUD`, `BEATS`.
`ML/DL/LLM` and `THE SYNERGY` are diagram labels in the deck-3 architecture
picture, not taught units.

## Ranked answer to "do we need to add anything else?"
1. **Kibana** — expected verbatim, missing, ~10 lines of compose. Do it.
2. **ES explicit mapping + custom analyzer** — unlocks full credit on the
   largest deck; must be written anyway when indexing is built.
3. **State the V's in the design doc** — one paragraph, uses existing numbers.
4. **Kafka Connect** — real hole; cheapest fix is one sink/source connector
   alongside the hand-rolled producer, or a defensible written justification.
5. **HDFS** — still unresolved from Finding 7; taught hands-on.
6. **Oozie** — still an opinion-based cut (Finding 8).
7. **GraphFrames** — deliberately declined.

Everything else the decks teach is either already in the build or is a Hadoop
component covered by findings 7-8.

---

# Decisions (2026-09-01)

Resolutions for the seven ranked items above, with the argument to use in the
design doc and the Q&A. Items 1-3 are committed work; 4-7 are reasoned
positions that must survive a follow-up question.

## 1. Kibana — ADD. Committed to Phase 2.
Not a judgement call. Deck 5, verbatim: `We will mostly use Kibana throughout
this course`, and the ELK diagram is `Beats  Kafka  Logstash  Elasticsearch
Kibana`. One compose service against the running Elasticsearch, plus a saved
dashboard over the gold layer. It also solves a separate problem: the live demo
currently has no visual surface, and presentation is 10% of the grade.

## 2. Explicit ES mapping + custom analyzer — ADD. Committed to Phase 2.
Deck 5 is the largest deck (264 pages) and teaches `TEXT ANALYSIS`, `ANALYZERS`,
`INVERTED INDEX`, `tokenizer` (13), `analyzer` (54). Indexing has to be written
anyway; writing it with an explicit mapping and a deliberately chosen analyzer
instead of dynamic defaults costs almost nothing and is the difference between
"used Elasticsearch" and "understood Elasticsearch".

Concretely: define the review-text field with a custom analyzer (lowercase +
stop + stemmer), keep a `.keyword` subfield for aggregations, and give
`dense_vector` its own explicit field. Then the BM25-vs-kNN comparison in
Finding 3 runs against a mapping we can explain line by line.

## 3. Name the V's in the design doc — ADD. One paragraph.
Deck 2 defines the scope: `Project that involves collection and analyze data
with at least on of the 4 V's`. Each claim gets a measured number from
`docs/phase0-profile.txt` rather than an adjective:

- **Volume** — 701,528 reviews / 112,590 products; 311 MB JSONL compacting to
  96.5 MB Parquet+zstd (3.2x).
- **Velocity** — sustained 483,503 records/s into Kafka; Structured Streaming
  consumed 701,528 rows in ~11 s across 5 micro-batches.
- **Variety** — semi-structured JSON with a free-form `details` object whose
  keys collide on case (this is why `src/common/schemas.py` types it as a
  string), plus a relational catalogue joined from PostgreSQL.
- **Veracity** — 6,139 duplicate `(user_id, parent_asin, timestamp)` triples,
  720 empty-text reviews, `price` present on only 15.7% of products and dirty
  when present (`null` / `9.99` / `$9.99` / ranges).

Variability is the fifth V on the slide; the 2000-2023 span with a 2020 peak of
126,753 reviews covers it if asked.

## 4. Kafka Connect — DECLINE the connector, but the honest reason is narrow
The deck answers this itself. On what Connect ships with:

> `▪ The Kafka project does not itself develop any actual connectors (sources or
>   sinks) for Kafka Connect`
> `        ▪ Except for a trivial "file" connector`

Our source is a local JSONL replay, so the only Connect source that would apply
is the one the deck calls **trivial**. Swapping a hand-written producer that
demonstrates `acks=all`, `enable.idempotence`, keyed partitioning by
`parent_asin` and `BufferError` backpressure — all four taught in deck 4, with
`props.put("acks", "all")` appearing verbatim on a slide — for a two-line file
connector config would *lose* demonstrated understanding, not gain it. That is
the argument, and it is a good one.

**But be honest about its limit.** The deck also says:

> `▪ Kafka Connect connectors for JDBC, HDFS, S3, and Elasticsearch`

So a *sink* connector is not covered by the "trivial file connector" argument.
An Elasticsearch sink is a genuine, non-trivial, course-named use of Connect,
and we will be writing Kafka-to-Elasticsearch data movement regardless.

**Position:** decline a Connect *source* on the reasoning above. Treat a Connect
*Elasticsearch sink* as the top stretch item — take it if Phase 2 and 3 land
comfortably, since it closes a 43-mention hole for modest work. If it is not
taken, say so out loud in the design doc rather than leaving it unmentioned;
an acknowledged gap reads as judgement, an unmentioned one reads as ignorance.

Kafka Streams (5 mentions) and ksqlDB (4) stay cut — Spark already does the
stream processing and duplicating it would add surface without adding
understanding.

## 5. HDFS — DECLINE, on a resource argument, stated plainly
The hardest of the seven. Deck 2 teaches it hands-on, with live transcripts
(`root@eff23c07f886:~# hdfs dfs -ls /`), 41 mentions.

The argument: the machine is 16 GB with Colima allotted 8, already running
Kafka, Elasticsearch, PostgreSQL, MinIO and Spark. A NameNode plus DataNodes
does not fit alongside them, and the project needs S3-compatible object storage
for Iceberg regardless, so MinIO is not a substitute chosen out of preference —
it is the storage layer the table format requires.

The concession to make explicitly: HDFS and MinIO are not equivalent. HDFS is a
distributed filesystem with block-level replication and rack awareness; MinIO is
an object store with a flat key space and no rename. The design decision is
defensible; claiming they are interchangeable is not.

**Fallback if this feels too exposed:** a single-node HDFS container used for
exactly one demonstrated step — `hdfs dfs -put` of the raw JSONL, read back by
Spark — proves the API without carrying a cluster. Cheap insurance for 41
mentions. Take it if memory allows after Kibana is in.

## 6. Oozie, Sqoop, Pig — DECLINE. Sqoop has a citation; the others do not.
Sqoop is settled by the course's own slide:

> `▪ Apache Sqoop moved into the Attic in June 2021`

Quote it verbatim. Pig (3 mentions total) goes with it — the deck pairs them as
`Apache Sqoop (Extract, Load), Apache Pig (Transform)`, and Spark SQL performs
both in one engine.

Oozie has **no such escape hatch** — it is not retired, and deck 2 teaches it as
`Hadoop ETL - Apache Oozie` across 25 mentions with the stated motivation
`A need for a general-purpose system to run multistage Hadoop jobs`. The cut
rests on the claim that Spark Structured Streaming plus checkpointed jobs make a
separate XML workflow scheduler redundant for a pipeline of this shape. That is
an opinion, defensible but unsupported by any quote. **Own it as an opinion.**
The follow-up to prepare for is "so how do you orchestrate multi-stage jobs?" —
answer with the actual mechanism (checkpoint-gated stages, `availableNow`
triggers, the `pipeline_runs` audit table in PostgreSQL), not with a dismissal
of Oozie.

## 7. GraphFrames / GraphX — DECLINE, deliberately
`GraphFrames` 5, `GraphX` 4, plus a `Graph` node in the deck-3 architecture
diagram. A co-review or co-purchase graph is plausible on this dataset and would
be interesting. It is cut because the rubric's 25% understanding component
punishes breadth, and the AI scope is already the largest risk in the project.
Recorded here so the omission is a decision rather than an oversight.

## Standing rule
Every one of these cuts is a sentence in the design doc, not a silence. The
rubric line is "use of course technologies", and a technology consciously
declined with a stated reason demonstrates more command of the material than one
included without understanding. Silence on a 41-mention topic reads as a gap;
a paragraph on it reads as engineering judgement.

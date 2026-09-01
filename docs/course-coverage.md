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

# Independent audit — Review Radar (phases 0–1), 2026-09-01

Scope: graded brief (PDF §1–11), repo at commit `dc14483`, live stack. Everything below is
tagged **[observed]** (ran/read it), **[inferred]**, or **[assumed]**.

## 1. Verdict

Right path, wrong pace-of-truth. What exists (Kafka replay → Spark Structured Streaming →
Iceberg on MinIO with a JDBC catalog) is well built and the exactly-once claim survived an
adversarial test I designed myself. But the repo is ~20% of the architecture the README
describes in the present tense, the two things the rubric weights most (transformation +
AI, 50%) do not exist, the "SQL enrichment" source is an empty table, and the stream drains
in 5 seconds. **Single most important correction: stop widening. Load the product table,
build silver→gold, then build exactly three AI capabilities — (b) embeddings/semantic search,
(a) LLM aspect sentiment with a measured validation, (c) RAG on top — each with a number that
proves it works. Cut (e) streaming AI and (g) narrative until everything else is demoable.**

## 2. Reproduction results

| Claim | Result | Evidence |
|---|---|---|
| Healthcheck: 5 components healthy | **HELD** | `All components healthy.` Kafka 1 broker, MinIO buckets `['lakehouse','warehouse']`, ES 8.17.0 green + kNN, PG 16.14, Spark 3.5.3/Java 17.0.20.1 |
| Bronze holds 701,528 rows | **HELD** | Spark `COUNT 701528`, `DISTINCT(part,off) 701528`, NULL payloads 0, single `ingest_date=2026-09-01` |
| Parquet files in MinIO | **HELD** | boto3 list: 5 objects, 96.5 MB under `iceberg/bronze/reviews_raw/data/`; 16 metadata objects; Iceberg `files` table matches (149,998/149,998/149,996/149,996/101,540 rows) |
| Kafka topic holds exactly the dataset | **HELD** | `kafka-get-offsets` sums: `reviews.raw` = 701,528; `reviews.eos` = 120,000 |
| Profile numbers reproducible | **HELD** | Re-ran `profile_data.py`: `diff` against `docs/phase0-profile.txt` → identical (172 lines, 11.6 s) |
| EOS test: kill ~88k, recover rest, 0 dupes | **HELD (with caveats)** | My run: killed at 95,969, recovered 24,031, final 120,000, dupes 0, 65 s |
| EOS holds under other timings | **HELD** | Kill at 18 s: 47,987 before, 120,000 after, 0 dupes. Kill at 60 s: job had already finished (120,000 before kill) — script would still print PASS |
| EOS handles the *hard* case (sink committed, checkpoint not) | **HELD — not tested by the student's script; I forced it** | After the 18 s kill I deleted `checkpoints/bronze_reviews/commits/5`; restart re-ran batch 5 and Iceberg skipped it: snapshot `epochId` sequence `0..15` with no repeat, 0 dupes |
| Producer 483,503 rec/s | **NOT REPRODUCED** | Full file to scratch topic: `701,528 sent in 3.0s (231,849 rec/s)` — same order of magnitude, half the number (host had 9 unrelated Supabase containers running) |
| "data/sample committed for the submission" (README:142) | **FALSE** | `.gitignore:2` = `data/sample/`; `git ls-files` shows no sample; 15 MB of sample sits untracked |
| Postgres "product catalogue … SQL enrichment" | **NOT BUILT** | `select count(*) from products` → 0; `pipeline_runs` → 0; no loader script exists |
| Iceberg catalog pointer in Postgres | **HELD** | `iceberg_tables` row: `bronze.reviews_raw → …/00005-….metadata.json` |
| `spark.driver.memory 4g` actually applied | **HELD** | JVM `maxMemory` 4096 MB |

Side effects of the audit: the EOS test **drops the production bronze table** (same table
name, see finding F3) — I rebuilt it (`--reset --trigger once --max-per-trigger 150000`,
9.6 s wall, 701,528 rows, new snapshot ids). Deleted scratch topics `audit.perf`,
`reviews.eos`. Added `checkpoints/eos_audit.log` (git-ignored). No code changed.

## 3. Grade table

Current = phases 0–1 as they stand. Projected = remaining plan executed as written
(silver/gold, ES, six AI options, Streamlit).

| Criterion | W | Now | Why | Projected | Why it falls short of full |
|---|---|---|---|---|---|
| Data & pipeline | 25 | 8 | Ingest + raw landing only; zero transformation, no load of results, enrichment source empty | 22 | Static-file replay drained in 5 s reads as batch with extra steps unless paced/event-timed; no orchestration of stages |
| Course technologies | 20 | 11 | Kafka, Spark, Iceberg, MinIO, Docker genuinely working; ES empty; Postgres only used as Iceberg catalog (legit, but not the claimed role) | 18 | No HDFS (illustrative list, small cost); everything else covered incl. SQL enrichment |
| AI capability | 25 | 0 | Nothing exists beyond installed libraries | 17 | Six options for one person = shallow each; no evaluation design exists; "understanding" is the graded word |
| Results & insights | 15 | 2 | Profile is EDA, not insight | 10 | No sharp question yet; risk of "60% are 5-star" findings |
| Presentation & demo | 10 | 1 | No slides, design doc, or demo; README good but stops before the pipeline | 8 | Demo has nothing to watch (5 s); live LLM dependency |
| Understanding & Q&A | 5 | 3 | Docstrings show real reasoning; some constructs unexplained (F12) | 4 | Breadth dilutes |
| **Total** | 100 | **25** | | **79** | |

Projected with the revised plan in §7 (three AI capabilities, each evaluated; paced
event-time stream; one sharp insight thread): ~90.

## 4. Findings, ranked

**F1 — README describes a system that mostly does not exist. [observed]**
`README.md:5-8, 87-92, 146-149` present silver/gold, ES indexing, embeddings, RAG, anomaly
detection, Streamlit in the present tense; `src/ai/`, `src/serving/` contain only empty
`__init__.py`; ES has no indices. Rubric: presentation + honesty (§10). Fix: status table
("built / planned") at the top of README until it is true.

**F2 — The SQL enrichment source is empty and nothing writes the audit table. [observed]**
`products` = 0 rows, `pipeline_runs` = 0 rows; no `load_products` script; schema comments
(`01_schema.sql:3-7`) promise a JDBC join that cannot happen. Rubric: pipeline 25% + the
"enrich from SQL" option (§5.1). Fix: `scripts/load_products.py` (meta JSONL → Postgres via
Spark JDBC or COPY), and every job inserts a `pipeline_runs` row.

**F3 — The Phase-1 gate destroys the production table, and can pass vacuously. [observed]**
`prove_exactly_once.py:112` calls `bronze --reset-only`, and `bronze.py:36-37` hard-codes
`TABLE`/`CHECKPOINT` regardless of `--topic` → running the gate dropped the 701,528-row
table (I had to rebuild it). Lines 143-147 only print a NOTE when `partial == 0` or the
job finished before the kill; `ok` at line 168 ignores both, so PASS prints for a run that
tested nothing (my 60 s run). Fix: derive table + checkpoint from topic; assert
`0 < partial < records`.

**F4 — "Streaming" drains the whole dataset in 5 s and is not time-ordered. [observed]**
Snapshots: 5 commits 13:46:27→13:46:31. Producer docstring (`producer.py:10-14`) claims
per-product "arrival order … exactly the ordering guarantee the per-product trend analysis
needs", but the file is not chronological: 6,388 timestamp inversions in the first 10,000
lines; only 20 consecutive same-asin pairs. Arrival order ≠ event order; trend analysis
must sort by `timestamp` regardless. Rubric: pipeline + Q&A ("isn't this just batch?").
Fix: one-off sort of the replay file by timestamp; replay with `--rate` and time
compression; silver/gold use event-time windows with a watermark — then late/out-of-order
data becomes the demo, not the embarrassment.

**F5 — Deliverable gaps in the repo. [observed]**
No design doc (`docs/` = profile + audit prompt; README:143 claims one), no slides, no demo
runbook/recording, `notebooks/` empty, dataset sample untracked (README:142 vs
`.gitignore:2`), README has no instructions to run producer/bronze (§7 "README explaining
how to run it"). The 150k `--max-per-trigger` used for the real load is undocumented
(checkpoint `offsets/0` shows ~150k batches; code default is 100k). Fix: run section, un-ignore
`data/sample/`, 2-page design doc.

**F6 — Producer throughput claim not reproduced. [observed]** Commit `ab316d6` states
483,503 rec/s; I measured 231,849. Not wrong, but numbers in commit messages/README should
come with the command and conditions, or be dropped.

**F7 — The gate's log is empty of anything useful. [observed]** `checkpoints/eos_run.log`
ends at Spark's "To adjust logging level" line: `bronze.py:146/150` print without
`flush=True`, stdout is a file → block-buffered → lost on SIGKILL. Restart output goes to
DEVNULL (`prove_exactly_once.py:154`). Fix: `flush=True`, keep restart stderr.

**F8 — Resource envelope is tighter than the README says. [observed]** `.venv` 1.7 GB
(torch 489 MB). Colima VM 7.7 GB is shared with 9 Supabase containers from another project
(bd-es alone 1.47 GB RSS). Spark driver 4 GB + torch + Streamlit on the host. Rubric: demo.
Fix: `docker compose --profile` / stop unrelated stacks in the demo runbook; measure peak.

**F9 — Demo-day time bomb: Kafka retention 168 h + `failOnDataLoss=true`. [inferred]**
`docker-compose.yml:29`, `bronze.py:124`. A restart against a checkpoint whose offsets
have aged out fails hard (which is the correct choice). Mitigation in runbook: re-run
producer before demo, or set topic retention to -1 for the project.
*Mitigation now written up in `docs/DEMO_RUNBOOK.md` §1, with the `retention.ms=-1`
command verified against the broker. Not yet applied to the live topics — that is a
manual step in the runbook's T-1 day checklist. The runbook also records that both
current checkpoints sit at the log end, so the loud `failOnDataLoss` abort is not the
mode at risk today; the silent one (topic empties, bronze lands 0 rows) is.*

**F10 — Bronze partitioning is inert for a replay. [observed]** All 701,528 rows in one
`ingest_date` partition. Fine for bronze (that is the standard choice), but silver must
partition by review month or the time-window queries scan everything.

**F11 — `fanout-enabled=true` was unexplained. [resolved]** Kept, with the reason now
stated inline at the option in `bronze.py`: `ingest_date` is derived from wall-clock time,
so a micro-batch straddling midnight carries two partition values, and without fanout the
Iceberg sink requires each task's rows sorted by partition. Cost is at most two open
writers per task.

**F12 — Constructs the student must be able to explain in one sentence each (Q&A traps):**
`bronze.py:10-18` sink dedupe — answer: "Iceberg writes `spark.sql.streaming.queryId` +
`epochId` into each snapshot summary and skips an epoch ≤ the last committed one for the
same query id" (I observed the `epochId` keys). Follow-up trap: "what if you delete the
checkpoint?" — new query id, dedupe resets, duplicates appear; `spark.py:3-11` catalog
choice; `docker-compose.yml:19-23` three Kafka listeners; `prove_exactly_once.py:125,136`
`start_new_session` + `killpg` (why: the JVM is a child of the Python driver).

## 5. Design decisions

1. **HDFS cut — sound.** §5.1 says "at least one … the more, the better", and the list is
   illustrative. Four of the five named categories are covered (NoSQL, object store, table
   format, streaming) plus SQL. A single-node HDFS container is feasible (~1–1.5 GB) but
   adds nothing the project would *use*; a checkbox HDFS is worth ≤1–2 points and costs
   demo stability. Be ready for: "why is Iceberg-on-S3 different from Iceberg-on-HDFS?"
   (no atomic rename → catalog matters; that is exactly your JDBC-catalog story).
2. **Oozie/Sqoop/Pig cut — sound, but their *roles* are unfilled.** They appear only in §4
   "topics", not in the graded list. But nothing orchestrates bronze→silver→gold (Oozie's
   job) and the Sqoop-equivalent (RDBMS ↔ lake via Spark JDBC) is unimplemented (F2). Add a
   thin orchestrator (Makefile or a `run_pipeline.py` DAG that writes `pipeline_runs`).
3. **JDBC catalog — sound and correctly reasoned.** The Hadoop catalog commits by renaming
   a metadata file; S3-style stores do copy+delete, so concurrent commits can both succeed.
   Postgres already exists, so it is six config lines, not over-engineering. Honest
   caveat to volunteer: with one writer it never bites here; it is the principled choice.
4. **Static-file replay — sound if framed honestly, currently framed badly.** The brief's
   own example (§11) is "create a Kafka topic and load the dataset into it". Say: "the
   source is deterministic; the processing model is streaming: unbounded consumer,
   checkpointed offsets, micro-batches, exactly-once across a crash — demonstrated". What
   collapses is the 5-second drain and unordered events (F4). Fix pacing + event time.
5. **Six of seven AI options — over-scoped.** Keep (b) embeddings + semantic search (maps
   to the ES topic, local model, free); (a) LLM aspect sentiment on a stratified subset with
   a measured validation; (c) RAG — nearly free once (b) exists. Cut (e): torch inside Spark
   Python workers on 16 GB is the hardest thing to make reliable and the hardest to explain;
   if embeddings are computed in a `foreachBatch`, (e) is satisfied incidentally — do not
   build it separately. Defer (g) (needs its own validation discipline, adds little). (f)
   only as a simple, explainable statistic (per-product monthly rating z-score / review
   burst detection) that feeds an insight — not a model for its own sake.
6. **Dataset — good enough; the question is generic, not the data.** 700k free-text
   reviews with a clean metadata join is exactly what the brief's tip asks for. Sharpen the
   question: e.g. (i) review-burst / incentivised-review detection (6,139 exact dupes,
   unverified spikes, near-duplicate text via embeddings), (ii) aspect-level complaint
   drift for the 1,902 products with ≥50 reviews ("what changed when ratings dropped?").
   Drop the 23M-review "full category" ambition: embeddings for 23M texts on a laptop CPU is
   days; bronze/silver of 11 GB is fine if you want a scale story.

## 6. Red-team of prepared Q&A (note: the "plan" and its Q&A live only in a prior chat
session — nothing in the repo; that itself is a gap. Assessed against README/docstrings.)

- **Iceberg over Parquet** — correct as written in `spark.py`. Follow-up: "show me time
  travel on bronze" (snapshots exist — `verify_iceberg.py` proves it; make it a demo step)
  and "what does schema evolution buy you here?" Have an actual example.
- **CAP for Kafka / ES** — risk of overstatement. Kafka with `acks=all` + min ISR chooses
  consistency (partition unavailable rather than divergent); ES writes are primary-based
  and consistent-ish, reads are near-real-time (refresh interval) so stale by design.
  Follow-up you will get: "you run one broker and one ES node — what does CAP even mean
  here?" Answer: nothing operationally; explain the config that *would* decide it.
- **Embeddings over BM25** — overstated if phrased as "better". BM25 is a strong baseline;
  hybrid (RRF, available in ES 8.17 — verify licence tier) usually wins. Follow-up: "show
  me a query where BM25 loses" — prepare five, with side-by-side results.
- **Why stream a static file** — see decision 4. Follow-up: "what would change with a
  live source?" (nothing downstream of Kafka — that is the point of the broker).
- **LLM sentiment validation** — none designed yet. Minimum credible: stratified sample,
  star rating as weak label (1–2★ neg, 4–5★ pos, exclude 3★), 150–200 hand-labelled
  aspect rows, report per-class precision/recall (README:39 already notes accuracy is
  useless under J-shape — good). Follow-up: "what do the disagreements look like?"
- **Hosted-LLM calls allowed?** — yes per §6.2 ("hosted API … never send private or
  sensitive data"). README:25 argues "public dataset, not our personal data". Weak spot:
  beauty reviews contain health details (acne, eczema, alopecia) and `user_id`s. Send
  review text only, never ids; state that. Cost check: ~700k reviews × ~200 tokens ≈ 140M
  input tokens — enrich a subset, cache results in Iceberg so the demo never calls live.

## 7. What to cut
(e) streaming AI enrichment as a separate capability; (g) narrative until the end; the 23M
full-category run; HDFS; any second dashboard tech. Keep Streamlit only if the demo
needs interactivity — a notebook + Kibana may be cheaper.

## 8. What is missing, by cost to grade
1. Transformation + results load (silver/gold) — pipeline 25%.
2. Any AI capability + its evaluation — 25%.
3. Product table load + JDBC join — pipeline & tech.
4. ES index populated — tech 20%.
5. Insight thread with numbers — 15%.
6. Design doc, slides, demo runbook + recorded backup, README run section, sample tracked.
7. Orchestration + `pipeline_runs` lineage.
8. Event-time / paced replay.

## 9. Revised phase plan (gates are things you can print)
- **P2 Silver**: parse with explicit schema, quarantine bad rows to `silver.rejects`, dedupe
  `(user,product,ts)`, join `products` (loaded from meta), partition by review month,
  write `pipeline_runs`. Gate: `bronze = silver + rejects + dupes`, exactly.
- **P3 Gold**: per product-month rating/volume/verified-ratio/drift; write Iceberg gold.
  Gate: three insights written with numbers.
- **P4 ES + embeddings**: index silver docs (BM25) + MiniLM vectors for the ≥20-word cohort of
  the ≥50-review products (measure throughput first). Gate: 5 queries where kNN ≠ BM25,
  hybrid via RRF, 20-query mini relevance judgement.
- **P5 LLM aspect sentiment** on a stratified ~5k subset, cached in Iceberg; validation as
  in §6; aggregate to gold. Gate: precision/recall table.
- **P6 RAG** over ES with citations; 20 questions, faithfulness spot-check. Gate: table.
- **P7 Stream story**: time-sorted replay at `--rate`, event-time windows + watermark on
  silver→gold, live counters in the demo. Gate: late data visibly handled.
- **P8 Deliverables**: design doc, slides, runbook, recorded backup, README run section,
  60-second explanation per file (write them down).

# Demo runbook

What to do before and during the live demo so that nothing in the local stack
fails for a reason that has nothing to do with the project. Two things can bite:
**Kafka retention** (the topic quietly empties, or a restart aborts) and
**memory** (an unrelated Supabase stack sharing the Colima VM).

Everything below was checked against the running stack on 2026-09-01. Facts are
tagged **[observed]** (I ran it) or **[inferred]** (reasoned from config).

---

## 1. Kafka retention — the seven-day clock

`docker-compose.yml:29` sets `KAFKA_LOG_RETENTION_HOURS: 168`, and `reviews.raw`
carries no topic-level override, so it inherits the broker default of 7 days
**[observed]**:

```
$ docker exec bd-kafka /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:19092 \
    --entity-type topics --entity-name reviews.raw --describe
Dynamic configs for topic reviews.raw are:
      <-- empty: no override, broker default applies
```

`src/spark/bronze.py:124` sets `failOnDataLoss=true` — the right choice, because
the alternative is silently skipping a gap. It means a restart whose checkpointed
offsets have aged out of the topic **aborts** rather than continuing.

### What actually goes wrong, and when

Two distinct failure modes. They are not equally likely, and the loud one is not
the dangerous one.

**(a) Silent — the topic empties and bronze ingests nothing.** After 7 days the
segments are deleted. A fresh run (`--reset`, `startingOffsets=earliest`) then
reads an empty topic and lands 0 rows without any error. This is the mode that
ruins a demo, because it looks like the pipeline is broken.

**(b) Loud — `failOnDataLoss` aborts the restart.** Spark asks for its
checkpointed offset; if that offset is below the topic's log start offset, the
query fails. This requires a checkpoint sitting **mid-topic**.

Right now neither checkpoint is exposed: both sit exactly at the log end
**[observed]**, so a restart asks for an offset that still exists even if every
segment is deleted (`logStartOffset` rises to meet `logEndOffset`; it never
exceeds it) **[inferred]**.

```
$ tail -1 checkpoints/bronze_reviews_raw/offsets/4
{"reviews.raw":{"2":116020,"5":121980,"4":119508,"1":116193,"3":112425,"0":115402}}

$ docker exec bd-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:19092 --topic reviews.raw
reviews.raw:0:115402
reviews.raw:1:116193
reviews.raw:2:116020
reviews.raw:3:112425
reviews.raw:4:119508
reviews.raw:5:121980
```

Identical, partition for partition — the query is fully caught up. The exposure
appears the moment a run is interrupted and left mid-topic (a rehearsal you
Ctrl-C'd, an exactly-once gate run you stopped early) and then restarted a week
later.

### Fix — do this once, now

Pin retention to "never" for the two project topics. This is a project-lifetime
override, not a code change, and it removes both modes:

```bash
for t in reviews.raw reviews.eos; do
  docker exec bd-kafka /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server localhost:19092 \
    --entity-type topics --entity-name "$t" \
    --alter --add-config retention.ms=-1
done
```

Syntax verified against this broker on a scratch topic, which then reported
`retention.ms=-1 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=-1}`
**[observed]**.

The override lives in broker metadata, so it survives `docker compose restart`
but **not** `docker compose down -v` (that destroys the `kafka-data` volume, and
the topic with it). Re-run it after any volume wipe.

Note what this does *not* change: `failOnDataLoss=true` stays as it is. With
infinite retention there is no data for it to trip over, and leaving it on keeps
the honest answer to "what happens if data goes missing?" — it stops.

### Verify — one command, gives a yes/no

```bash
docker exec bd-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:19092 --topic reviews.raw --time -2
```

`--time -2` prints the **earliest** available offset per partition. All zeros =
nothing has been deleted, the full 701,528-record replay is still on the topic.
Any non-zero value means segments have gone and you should reload:

```bash
# if the topic still holds a partial remnant, delete it first -- otherwise the
# replay appends to what survived and bronze lands remnant + full file
docker exec bd-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 --delete --topic reviews.raw

./run.sh python -m src.ingest.producer --category All_Beauty
./run.sh python -m src.spark.bronze --trigger once --reset --max-per-trigger 150000
```

`--reset` drops the bronze table *and* its checkpoint, so the reload is a clean
rebuild rather than an append. It costs ~15 s. Deleting and recreating the topic
also discards the `retention.ms=-1` override, so re-apply it afterwards.

---

## 2. Memory — stop the Supabase stack

The Colima VM is 8 GB and is shared with nine containers from an unrelated
project (`compliance-ai-app`). Measured on 2026-09-01 **[observed]**:

| stack | containers | resident |
|---|---|---|
| this project (`bd-*`) | 4 | 2.34 GiB (ES alone 1.47 GiB) |
| Supabase (`compliance-ai-app`) | 9 | ~1.26 GiB |
| VM total | | 7,921 MB, 3,908 MB used, 4,013 MB available |

Freeing 1.26 GiB is not the only reason to stop them. The cost shows up in
throughput: with those nine containers running, the audit measured the producer
at **231,849 rec/s** (701,528 records in 3.0 s) **[observed]**, against an
original claim of 483,503 rec/s on an otherwise-idle host that the audit could
not reproduce (F6). Treat 2× as the plausible spread, not a promise — the point
is simply that a contended VM is measurably slower, so do not demo on one.

```bash
# stop the Supabase stack (leaves its data volumes intact)
docker stop $(docker ps -q --filter label=com.docker.compose.project=compliance-ai-app)

# confirm: only bd-* should be listed
docker ps --format '{{.Names}}'
```

The label filter selects exactly those nine containers **[observed]**. Use it
rather than `docker compose -p compliance-ai-app stop`: without that project's
compose file on the command line, Compose resolves the project to nothing and
the command silently does nothing **[observed]**. From that project's own
directory, `supabase stop` is equivalent.

Restart them after the demo with
`docker start $(docker ps -aq --filter label=com.docker.compose.project=compliance-ai-app)`,
or `supabase start` from that project's directory.

Host-side memory is separate from the VM and is not freed by the above: the
Spark driver takes 4 GB (`src/common/spark.py`) on the host. Quit other JVMs,
browsers with many tabs, and anything running torch before the demo.

---

## 3. Pre-demo checklist

Run top to bottom. Budget ~5 minutes on a warm stack, ~10 if the containers are
cold, plus ~15 s if the reload in §1 is needed.

**T-1 day**

- [ ] `retention.ms=-1` applied to `reviews.raw` and `reviews.eos` (§1)
- [ ] Full rehearsal of every command you plan to run live, in order
- [ ] After the rehearsal, leave **no** stream stopped mid-topic — let every
      `--trigger once` run finish, or re-drain until the checkpoint matches the
      log end (§1)

**T-30 min**

- [ ] Supabase stopped — `docker stop $(docker ps -q --filter label=com.docker.compose.project=compliance-ai-app)` (§2)
- [ ] `docker ps --format '{{.Names}}'` → only `bd-kafka`, `bd-minio`, `bd-es`,
      `bd-postgres`
- [ ] `colima ssh -- free -m` → ≥ 4 GB available in the VM
- [ ] `docker compose up -d && docker compose ps` → all four healthy
- [ ] `./run.sh python scripts/healthcheck.py` → `All components healthy.`
      (exit 0; it proves each service is usable from Python, not merely running)

**T-10 min**

- [ ] Earliest offsets all `0` (§1 verify command)
- [ ] `./run.sh python scripts/verify_iceberg.py` → bronze holds 701,528 rows
- [ ] Laptop on mains power, sleep disabled, notifications off
- [ ] Terminal font large enough to read from the back of the room

**Live, in order**

1. `./run.sh python scripts/healthcheck.py` — the stack is real
2. `./run.sh python -m src.ingest.producer --source data/sample/All_Beauty.sample.jsonl`
   — replay is visible and finishes in seconds
3. `./run.sh python -m src.spark.bronze --trigger once --max-per-trigger 150000`
   — no `--reset`: the existing checkpoint means only the 10,000 new records are
   read, not the 701,528 already ingested. That is the point worth saying out
   loud. Bronze goes 701,528 → 711,528.
4. `./run.sh python scripts/verify_iceberg.py` — snapshots + time travel
5. `./run.sh python scripts/prove_exactly_once.py --records 120000 --kill-after 25`
   — ~65 s; the one thing worth watching live. It uses its own topic
   (`reviews.eos`) and its own table, so it cannot damage the production bronze.

**If something fails live**

| symptom | cause | action |
|---|---|---|
| bronze lands 0 rows | topic aged out or was never loaded | re-run the producer, then bronze with `--reset` |
| bronze aborts with a data-loss error | checkpoint offsets no longer on the topic | re-run the producer, then bronze with `--reset` (drops the table and checkpoint together) |
| Spark will not start | wrong JDK | use `./run.sh`, which pins `JAVA_HOME` to JDK 17 |
| ES or Kafka unhealthy after a cold start | still warming up | `docker compose ps` and wait — Kafka's healthcheck allows 30 s start-up, ES 30 s |
| everything is slow | another container stack came back | `docker ps` and stop it (§2) |

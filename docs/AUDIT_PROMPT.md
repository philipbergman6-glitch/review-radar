# Audit prompt — paste into a fresh session

You are auditing a university final project that is already partly built. Your job is to
find what is wrong with it, not to confirm that it is good. Assume the previous session
was over-confident and that at least some of its claims do not survive contact with the
evidence. A review that finds nothing is a failed review — but do not invent problems
either; every criticism must be backed by something you read or ran.

## Context

- Project directory: `/Users/philipbergman/Documents/Coding_Projects/Big_Data`
- The graded brief is the PDF in that directory: `BIU BigData 8688697201 - Project.pdf`.
  **Read it first and in full.** It is the only source of truth for what is required.
  Everything else in the repo is one student's interpretation of it, which may be wrong.
- The student is doing this **solo**, with explicit instructor approval, although the brief
  specifies teams of 3. Judge scope against one person's capacity, not three.
- There is no deadline. Do not optimise for speed; optimise for a defensible result.
- The student's stated goal is a genuinely impressive project, and he wants to actually
  learn data engineering and AI-on-data rather than just pass.

## Environment (so you can run things)

- macOS, Apple M5, 16 GB RAM. Docker runtime is **Colima**, not Docker Desktop —
  if `docker ps` fails, run `colima start --cpu 6 --memory 8 --disk 80`.
- Bring the stack up with `docker compose up -d` from the project directory.
  Four services: Kafka, MinIO, Elasticsearch, PostgreSQL.
- **All Python must be run through `./run.sh`** (e.g. `./run.sh python scripts/healthcheck.py`).
  It pins `JAVA_HOME` to JDK 17 and uses the `uv` environment on Python 3.11.
  Plain `python` will fail: system Python is 3.14, which PySpark does not support.
- Raw data is git-ignored. If `data/raw/` is empty, run
  `./run.sh python scripts/download_data.py --category All_Beauty` (~0.5 GB).

## Start here

1. Read the PDF brief completely, including the grading table in section 9 and the AI
   options in section 6.2.
2. Read `README.md`, `docs/phase0-profile.txt`, then the code:
   `src/common/`, `src/ingest/producer.py`, `src/spark/bronze.py`,
   `scripts/prove_exactly_once.py`, `scripts/healthcheck.py`, `docker-compose.yml`.
3. Read the git log — the commit messages state specific claims. Check them.

## What to verify by running, not by reading

The previous session asserted all of the following. Reproduce each one or report that you
could not, with the actual output:

- All five components are healthy: `./run.sh python scripts/healthcheck.py`
- Exactly-once holds across an unclean kill:
  `./run.sh python scripts/prove_exactly_once.py --records 120000 --batch 8000 --kill-after 35`
  It claims: SIGKILL at ~88k of 120,000 rows, restart recovers the rest, **zero** duplicate
  `(partition, offset)` pairs. Does it actually pass? Is the test honest, or does it prove
  something weaker than it claims? Specifically: is `--kill-after` tuned so the job happens
  to be killed at a convenient moment? Would it still pass with different timings?
- The bronze table really holds 701,528 rows and the Parquet files really are in MinIO.
- The profiling numbers in `README.md` match `docs/phase0-profile.txt` and are reproducible.

Where a claim cannot be reproduced, say so plainly and show the failing output.

## Grade it against the actual rubric

Score each criterion out of its weight, with a one-line justification and the single most
valuable thing that would raise it:

| Criterion | Weight |
|---|---|
| Data and pipeline (working ETL/ELT over semi-/unstructured data) | 25% |
| Use of course technologies (NoSQL, HDFS, object store, table format, streaming) | 20% |
| AI capability (correctness, depth, and the student's understanding) | 25% |
| Results and insights | 15% |
| Presentation and demo | 10% |
| Understanding and Q&A | 5% |

Grade the project **as it stands today** (phases 0–1 built), and separately give a
projected grade **if the remaining plan is executed as written**. If the plan as written
would not reach full marks, say exactly which criterion falls short and why.

## Challenge the design decisions

For each of these, give a verdict — sound, risky, or wrong — with reasoning:

1. **HDFS was cut**, with MinIO covering the object-store requirement. The rubric names
   HDFS explicitly and says "the more, the better" for course technologies. Does cutting it
   cost marks? Would adding it be feasible in 16 GB, or is the trade-off correct?
2. **Oozie, Sqoop and Pig were cut** as superseded by Spark. Same question — is that
   defensible against a rubric line that rewards breadth of course technologies?
3. **Iceberg uses a JDBC catalog in Postgres** rather than a Hadoop catalog, justified by
   object stores lacking atomic rename. Is that reasoning correct? Is it over-engineering
   for a student project, or is it the right call?
4. **The "stream" is a replay of a static file.** Is that intellectually honest, and will it
   survive an examiner asking "so it isn't really streaming, is it?"
5. **Six of the seven AI options in §6.2 are planned.** The brief warns that understanding
   beats complexity and that a simpler project fully grasped beats a complex one that is
   not. Is this over-scoped for one person? Which AI capabilities should be cut to protect
   the 25% "understanding" component, if any?
6. **The dataset is one category of Amazon reviews.** Is it interesting enough to produce
   insights worth 15% of the grade, or is it a generic choice that will produce generic
   findings? Suggest something better if you believe there is one.

## Red-team the Q&A answers

`README.md` and the plan contain prepared answers on: why Iceberg over plain Parquet, where
Kafka and Elasticsearch sit on CAP, why embeddings over BM25, why streaming from a static
file, how LLM sentiment labels are validated, and whether sending review text to a hosted
model is allowed. **Check each for technical accuracy.** Flag anything that is wrong,
overstated, or would collapse under one follow-up question from an examiner — and give the
follow-up question you would ask.

## Find what is missing

- What does the brief require that the plan does not currently cover at all?
- Deliverables: source + README, a 1–2 page design document with an architecture diagram,
  the dataset link/sample, slides, and a live or recorded demo. Which exist? Which do not?
- What will realistically break during a live demo, and what is the mitigation?
- Is anything in the codebase too complex for the student to explain in a 3-minute Q&A? Name
  the specific file and construct.

## Output

Produce, in this order:

1. **Verdict** — one paragraph. Is this on the right path to a strong grade? If not, what is
   the single most important correction?
2. **Reproduction results** — what you ran, what happened, which claims held and which did not.
3. **Grade table** — current and projected, per criterion, with justifications.
4. **Findings** — ranked most to least severe. For each: the problem, the evidence
   (`file:line` or command output), why it matters against the rubric, and the fix.
5. **What to cut** — anything that adds risk or complexity without adding marks.
6. **What is missing** — gaps against the brief, ranked by cost to the grade.
7. **Revised phase plan** — if you would change the order, scope or gates, give the new version.

Ground every claim in something specific. Distinguish clearly between what you directly
observed, what you inferred, and what you are assuming. Where you are uncertain, say so
rather than guessing. Do not modify any code — this is an audit, not an implementation task.

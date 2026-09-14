"""The demo stage's helpers: one function per live move (ADR-0009, RR-11).

`notebooks/demo.ipynb` is a runbook, not a program. Every cell in it calls something here, so
the five minutes on stage exercise the pipeline's own code path rather than a parallel app
written for the projector -- the hybrid move fuses through `src/serving/search.py`, the
citation move re-runs `src/ai/rag_answers.contract_violations`, the evaluation move renders
through `src/common/evaluation.py`, and the gate moves print the artefact each gate wrote
rather than a number retyped beside it.

The shape the ADR fixes, and that this module makes literal:

* **One kernel, three connections.** `open_stage()` returns the one Spark session, the one
  Elasticsearch client and the one PostgreSQL connection the whole demo runs on. A move that
  needed a second Spark session would be a second JVM beside a 4 GB driver, on a laptop.
* **Ten moves, 4:40 of a 5:00 budget.** `MOVES` is the running order with its seconds; the
  total is asserted in `tests/test_demo_moves.py` rather than trusted to a comment.
* **A move whose phase is not built is placed and marked, never omitted.** `Move.pending`
  carries the written reason, and `run_sheet()` prints it, so the running order does not
  quietly shrink to whatever happens to work today.

Nothing here decides anything. Verdicts come from `src/gates/`, numbers from the artefacts
the gates wrote, and any move that would otherwise recompute a published claim reads it
instead.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai import rag_answers, theme_labels
from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.pg import connect
from src.common.spark import build
from src.ingest.replay_config import load_replay_config
from src.serving import search as S
from src.spark.silver import silver_names

#: Where a background replay's console output lands, so move 10 can show the clock it printed.
REPLAY_LOG = C.PROJECT_ROOT / "logs" / "demo-replay.log"

#: The Kibana dashboard move 5 switches to (`make kibana-import` created it).
KIBANA_DASHBOARD_ID = "pm-dashboard"

#: Move 6's query: D03 of the twenty frozen judged queries (`conf/search/queries.json`). Chosen
#: from that set because its fusion is legible on a projector -- nine of the fused top ten sit
#: in both legs -- not because it scores well; every one of the twenty is in the judged table
#: move 8 reports, and none of them was picked after the judging.
DEMO_QUERY = "nail polish chips after one day"

#: The frozen thirty and the answers to them, both already on disk when the demo starts.
RAG_QUESTIONS = C.PROJECT_ROOT / "conf" / "rag-questions.json"
RAG_ANSWERS = C.PROJECT_ROOT / "eval" / "rag" / "answers.json"
RAG_SLOTS = C.PROJECT_ROOT / "eval" / "rag" / "slots.json"


# ============================================================================ moves ====
@dataclass(frozen=True)
class Move:
    """One live move: what is shown, where, for how long, and what is said while it runs."""
    n: int
    title: str
    surface: str
    seconds: int
    says: str
    pending: str | None = None


#: Two switches between surfaces (terminal -> notebook at move 2, notebook -> Kibana and back
#: at move 5), charged to the clock because they happen on it.
SURFACE_SWITCH_S = 20

#: The live budget: 5:00 with 20 s of slack left unspent (RR-11).
LIVE_BUDGET_S = 280

MOVES: tuple[Move, ...] = (
    Move(1, "The stack is real", "terminal", 10,
         "four services proved usable from Python, in a real shell, before the notebook opens"),
    Move(2, "The paced replay and the streaming projection", "notebook", 20,
         "the file goes onto its own topic in event order at a constant rate; the event-time "
         "clock moves while the rest of the demo runs",
         pending="the injected near/far slices, the demo run's own topic and its own "
                 "STREAM_GATE are the P8 demo run (ADR-0010, ticket 16); until it exists this "
                 "replays the control topic without injection and move 10 prints the control "
                 "run's recorded gate"),
    Move(3, "Silver's reconciliation identity", "notebook", 15,
         "every bronze row is accounted for: rejected, survived, or removed as a duplicate"),
    Move(4, "Every run this data went through", "notebook", 30,
         "the silver row names the bronze snapshot it read, and time travel reads that "
         "snapshot back to the same count"),
    Move(5, "Decline candidates", "kibana", 45,
         "the opening question, answered over the serving projection"),
    Move(6, "Hybrid decomposition", "notebook", 35,
         "a review neither list ranks first wins the fusion"),
    Move(7, "Cached complaint-theme labels for the top decline candidate", "notebook", 30,
         "themes are hypotheses about what the text says, never a diagnosed cause, and P6's "
         "published macro-F1 is a reported miss"),
    Move(8, "The evaluation table", "notebook", 20,
         "one row per capability, every bar set before the number beside it existed"),
    Move(9, "RAG answers, claim by claim", "notebook", 30,
         "every claim cites a review inside the window the question declared; it contrasts "
         "cited examples and never says a complaint increased"),
    Move(10, "The stream's own gate", "notebook", 25,
         "the projection is reconciled against batch product-month by product-month",
         pending="the alerts table and the demo run's watermark drops come with the P8 demo "
                 "run (ticket 16)"),
)


def run_sheet() -> pd.DataFrame:
    """The running order with its clock, as the rehearsal reads it."""
    return pd.DataFrame([{"move": m.n, "title": m.title, "surface": m.surface,
                          "s": m.seconds, "says": m.says, "pending": m.pending or ""}
                         for m in MOVES])


def budget() -> dict[str, int]:
    """Where the five minutes go. `spare` is what the demo deliberately does not spend."""
    moves = sum(m.seconds for m in MOVES)
    return {"moves_s": moves, "switches_s": SURFACE_SWITCH_S,
            "live_s": moves + SURFACE_SWITCH_S, "budget_s": LIVE_BUDGET_S,
            "ceiling_s": 300, "spare_s": 300 - (moves + SURFACE_SWITCH_S)}


# ============================================================================ stage ====
@dataclass
class Stage:
    """The one kernel's three connections. Held for the whole demo, closed once at the end."""
    spark: SparkSession
    es: Elasticsearch
    pg: Any
    scope: str
    opened_at: datetime


def open_stage(*, scope: str = "full", app_name: str = "demo") -> Stage:
    """One Spark session, one Elasticsearch client, one PostgreSQL connection (ADR-0009)."""
    if scope not in ("full", "sample"):
        raise ValueError(f"scope must be 'full' or 'sample', got {scope!r}")
    spark = build(app_name)
    es = Elasticsearch(C.ES_HOST, request_timeout=60)
    pg = connect(autocommit=True)
    return Stage(spark=spark, es=es, pg=pg, scope=scope, opened_at=datetime.now(UTC))


def close_stage(stage: Stage) -> None:
    stage.pg.close()
    stage.es.close()
    stage.spark.stop()


def context(stage: Stage) -> pd.DataFrame:
    """What the export must carry for a rehearsal to be evidence: commit, scope, clock."""
    sha, dirty = runs.git_state()
    return pd.DataFrame([
        {"fact": "git_commit", "value": f"{sha or 'n/a'}{' (DIRTY)' if dirty else ''}"},
        {"fact": "scope", "value": stage.scope},
        {"fact": "opened_at", "value": stage.opened_at.isoformat(timespec="seconds")},
        {"fact": "elasticsearch", "value": C.ES_HOST},
        {"fact": "postgres", "value": f"{C.PG_HOST}:{C.PG_PORT}/{C.PG_DB}"},
        {"fact": "category", "value": C.CATEGORY},
    ])


# ================================================================= recorded gates ====
def recorded_gate(capability: str) -> list[str]:
    """The lines the named capability's gate printed, read back from its artefact.

    The demo shows the gate's own output rather than re-running it: a gate is a re-derivation
    over pinned snapshots and costs minutes, and retyping its number into a slide is exactly
    the drift `eval/<capability>/gate.json` exists to prevent. Hard-fails when the artefact is
    absent -- on stage a missing gate must stop the cell, not print an empty table.
    """
    path = E.EVAL_ROOT / capability / "gate.json"
    if not path.exists():
        shown = path.relative_to(C.PROJECT_ROOT) if path.is_relative_to(C.PROJECT_ROOT) else path
        raise FileNotFoundError(
            f"{shown} does not exist, so {capability} has no recorded gate to show. "
            f"Run that capability's gate target first.")
    doc = json.loads(path.read_text())
    fails = E.validate_artifact(doc)
    if fails:
        raise ValueError(f"{capability}: the recorded artefact does not match "
                         f"{E.SCHEMA_PATH.name}: " + "; ".join(fails))
    return list(doc["constituents"])


def show_gate(capability: str) -> None:
    for line in recorded_gate(capability):
        print(line)


def evaluation_table() -> str:
    """`make eval-table`, run as itself (move 8).

    A subprocess rather than a second call into `src/common/evaluation.py`, and deliberately:
    the renderer's rules about missing artefacts, superseded runs and sample scope are subtle,
    and a demo that re-implemented the load would be a second opinion about the table the
    design doc and the README are supposed to share. This is the command, and its output. It
    costs about a second and starts no JVM.
    """
    done = subprocess.run(["./run.sh", "python", "scripts/eval_table.py"], cwd=C.PROJECT_ROOT,
                          capture_output=True, text=True, check=False)
    out = done.stdout.rstrip()
    return out if done.returncode == 0 else f"{out}\n{done.stderr.rstrip()}".rstrip()


# ======================================================================== lineage ====
LEDGER_QUERY = """
    SELECT job_name, spec_version, status, started_at, records_in, records_out, run_id
      FROM pipeline_runs
     WHERE category = %s AND data_scope = %s
     ORDER BY started_at
"""


def ledger(stage: Stage, *, last: int = 0) -> pd.DataFrame:
    """Every run this data went through, oldest first; `last` keeps the most recent N (RR-16).

    Failed attempts included. The ledger holds one row per *attempt* (ADR-0008) and a demo
    that filtered to successes would be showing a cleaned-up history of itself.
    """
    rows = stage.pg.execute(LEDGER_QUERY, (C.CATEGORY, stage.scope)).fetchall()
    df = pd.DataFrame(rows, columns=["job", "spec", "status", "started_at", "in", "out", "run_id"])
    df["run_id"] = df["run_id"].astype(str).str.slice(0, 8)
    return df.tail(last) if last else df


def time_travel(stage: Stage) -> pd.DataFrame:
    """Silver's ledger row names a bronze snapshot; read that snapshot back and recount.

    The claim is a join, not an assertion: `records_in` was written by the silver run, the
    count comes from Spark reading the table `VERSION AS OF` the snapshot that run recorded,
    and the current count is there to show that the pinned read is not just the table as it
    stands today.
    """
    silver = runs.latest_success("silver", category=C.CATEGORY, data_scope=stage.scope)
    if silver is None:
        raise RuntimeError(f"no successful silver run for {C.CATEGORY}/{stage.scope}: "
                           "the lineage move has nothing to walk")
    bronze = silver["inputs"]["bronze"]
    snapshot_id = int(bronze["snapshot_id"])
    table = bronze["table"]
    pinned = stage.spark.sql(
        f"SELECT count(*) AS n FROM {table} VERSION AS OF {snapshot_id}").first()["n"]
    current = stage.spark.sql(f"SELECT count(*) AS n FROM {table}").first()["n"]
    snapshots = stage.spark.sql(
        f"SELECT count(*) AS n FROM {table}.snapshots").first()["n"]
    return pd.DataFrame([
        {"fact": "silver run", "value": silver["run_id"][:8]},
        {"fact": "silver records_in", "value": f"{silver['records_in']:,}"},
        {"fact": f"{table} pinned snapshot", "value": str(snapshot_id)},
        {"fact": "count VERSION AS OF that snapshot", "value": f"{pinned:,}"},
        {"fact": "matches records_in", "value": str(pinned == silver["records_in"]).lower()},
        {"fact": "count as the table stands now", "value": f"{current:,}"},
        {"fact": "snapshots on the table", "value": f"{snapshots:,}"},
    ])


# ========================================================================= kibana ====
def kibana_url() -> str:
    """The one move that leaves the notebook (ADR-0009): the decline dashboard, nothing else."""
    host = os.getenv("KIBANA_HOST", "http://localhost:5601")
    return f"{host}/app/dashboards#/view/{KIBANA_DASHBOARD_ID}"


# ========================================================================= search ====
def hybrid_decomposition(stage: Stage, query: str, *, size: int = 10,
                         alias: str = "reviews") -> pd.DataFrame:
    """One descriptive query, decomposed: BM25 rank, kNN rank, fused score (RR-06).

    Runs the production hybrid -- `src/serving/search.py`, the same function P5 was evaluated
    on -- and unpacks the per-leg ranks it already carries, so the table on screen is the
    retriever's own arithmetic rather than a recomputation of it.
    """
    alias = alias if stage.scope == "full" else f"{alias}_{stage.scope}"
    hits = S.run_system(stage.es, alias, "hybrid", query, size=size)
    return pd.DataFrame([{
        "fused_rank": h.rank,
        "bm25_rank": h.parts.get("bm25"),
        "knn_rank": h.parts.get("knn"),
        "fused_score": round(h.score, 6),
        "rating": h.source.get("rating"),
        "title": (h.source.get("title") or "")[:60],
        "review_id": h.review_id[:12],
    } for h in hits])


def fusion_story(table: pd.DataFrame) -> str:
    """The sentence move 6 is for: did a review neither leg ranked first win the fusion?"""
    if table.empty:
        return "the query retrieved nothing"
    top = table.iloc[0]
    bm25, knn = top["bm25_rank"], top["knn_rank"]
    both = len(table.dropna(subset=["bm25_rank", "knn_rank"]))
    seen = f"{both} of the fused top {len(table)} are in both legs"
    if bm25 == 1 or knn == 1:
        leg = "BM25" if bm25 == 1 else "kNN"
        return (f"the fused winner was already {leg}'s first hit "
                f"(bm25={bm25}, knn={knn}) -- fusion agreed rather than decided; {seen}")
    return (f"the fused winner is ranked {bm25} by BM25 and {knn} by kNN: neither list puts it "
            f"first, and the sum of 1/(60+rank) over both legs does; {seen}")


# ========================================================================= themes ====
def top_candidate(*, slots_path: Path = RAG_SLOTS) -> dict[str, Any]:
    """The best-ranked *eligible* decline candidate, with the windows its episode declares.

    "Eligible" is the qualifier the demo says out loud: the frozen slot set is drawn from the
    RR-09 ranking in order, and a product the evidence scan could not validate was skipped --
    so the top slot's `decline_rank` is the rank it holds in the ranking, not always 1.
    """
    slots = json.loads(slots_path.read_text())["slots"]
    candidates = [s for s in slots if s["slot_role"] == "candidate"]
    if not candidates:
        raise ValueError(f"{slots_path.name} declares no candidate slot")
    return min(candidates, key=lambda s: s["decline_rank"])


def theme_evidence(stage: Stage, *, candidate: dict[str, Any] | None = None) -> pd.DataFrame:
    """The cached theme labels for candidate #1, by window (move 7).

    Every row shown was written by `make label-themes` into `gold.review_theme_labels` and is
    read back here: the demo makes no model call (RR-08). The counts are over the *labelled*
    reviews of this product, which are the seeded sample frames rather than its whole history,
    so the denominators are printed beside them and no share, direction or cause is claimed --
    P6 publishes a macro-F1, not a theme-shift finding, and move 8's table says so.
    """
    cand = candidate or top_candidate()
    labels = (stage.spark.read.table(theme_labels.table_name(stage.scope))
              .filter((F.col("label_status") == "succeeded")
                      & (F.col("label_source") == "local_llm"))
              .select("source_review_id", F.explode_outer("themes").alias("theme")))
    topic = C.TOPIC_REVIEWS if stage.scope == "full" else f"{C.TOPIC_REVIEWS}.{stage.scope}"
    reviews = (stage.spark.read.table(silver_names(topic)["reviews"])
               .filter(F.col("parent_asin") == cand["parent_asin"])
               .select("review_id", "review_month", "rating"))
    window = (F.when((F.col("month") >= cand["baseline_start"])
                     & (F.col("month") <= cand["baseline_end"]), "baseline")
               .when((F.col("month") >= cand["recent_start"])
                     & (F.col("month") <= cand["recent_end"]), "recent"))
    joined = (reviews.join(labels, reviews.review_id == labels.source_review_id)
              .withColumn("month", F.date_format("review_month", "yyyy-MM"))
              .withColumn("window", window)
              .filter(F.col("window").isNotNull()))
    rows = (joined.groupBy("window", F.col("theme.theme_id").alias("theme"))
            .agg(F.count("*").alias("labelled_mentions"),
                 F.countDistinct("review_id").alias("reviews"),
                 F.first("theme.evidence_quote").alias("quote"))
            .orderBy("window", F.desc("labelled_mentions"))
            .collect())
    return pd.DataFrame([{"window": r["window"], "theme": r["theme"] or "(no theme)",
                          "labelled_mentions": r["labelled_mentions"], "reviews": r["reviews"],
                          "quote": (r["quote"] or "")[:70]} for r in rows])


def candidate_headline(candidate: dict[str, Any] | None = None) -> str:
    c = candidate or top_candidate()
    return (f"top eligible candidate, decline rank {c['decline_rank']}: {c['parent_asin']} "
            f"({(c.get('product_title') or '')[:60]}): baseline {c['baseline_start']}"
            f"..{c['baseline_end']}, recent {c['recent_start']}..{c['recent_end']}")


# ============================================================================ RAG ====
def _rag_docs() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    questions = json.loads(RAG_QUESTIONS.read_text())["questions"]
    answers = json.loads(RAG_ANSWERS.read_text())["answers"]
    return questions, answers


def rag_questions() -> pd.DataFrame:
    """The frozen thirty, with what each answer did -- one line to pick a question from."""
    questions, answers = _rag_docs()
    by_id = {a["question_id"]: a for a in answers}
    out = []
    for q in questions:
        a = by_id.get(q["question_id"]) or {}
        parsed = a.get("parsed") or {}
        out.append({"question_id": q["question_id"],
                    "family": q["family"],
                    "answerability": q["answerability"],
                    "status": a.get("status", "none"),
                    "outcome": "refused" if parsed.get("refused") else "answered",
                    "claims": len(parsed.get("claims") or [])})
    return pd.DataFrame(out)


def clean_question(*, family: str = "temporal") -> str:
    """The first answered question of `family` whose citations all resolve in scope.

    Move 9 shows what a satisfied contract looks like, and it picks that question by running
    the contract rather than by remembering which one worked at rehearsal. The whole thirty,
    violations included, are one cell above in `rag_questions()`: this chooses an example, it
    does not choose what is reported.
    """
    questions, answers = _rag_docs()
    by_id = {a["question_id"]: a for a in answers}
    for q in questions:
        a = by_id.get(q["question_id"])
        if a is None or q["family"] != family or (a.get("parsed") or {}).get("refused"):
            continue
        if not rag_answers.contract_violations(dict(a), dict(q), list(a.get("retrieved") or [])):
            return q["question_id"]
    raise LookupError(f"no answered {family} question satisfies the citation contract")


def question_text(question_id: str) -> str:
    questions, _ = _rag_docs()
    return next(q["question"] for q in questions if q["question_id"] == question_id)


def citations(question_id: str) -> pd.DataFrame:
    """One frozen question, claim by claim, with every citation resolved (move 9).

    Each cited handle is looked up in that question's own retrieved set and its stored
    `review_month` checked against the window the claim names -- the same two mechanical rules
    `RAG_GATE` blocks on, re-run live rather than asserted from the gate's summary.
    """
    questions, answers = _rag_docs()
    q = next((x for x in questions if x["question_id"] == question_id), None)
    if q is None:
        raise KeyError(f"{question_id!r} is not one of the frozen thirty")
    a = next((x for x in answers if x["question_id"] == question_id), None)
    if a is None:
        raise KeyError(f"{question_id!r} has no recorded answer")
    retrieved = {r["cite_id"]: r for r in a.get("retrieved") or []}
    parsed = a.get("parsed") or {}
    if parsed.get("refused"):
        return pd.DataFrame([{"claim": "(refused)", "cite_id": "-", "window": "-",
                              "review_month": "-", "in_declared_window": "-",
                              "review": parsed.get("refusal_reason", "")[:90]}])
    out = []
    for i, claim in enumerate(parsed.get("claims") or [], start=1):
        for c in claim.get("citations") or []:
            row = retrieved.get(c.get("cite_id")) or {}
            out.append({"claim": f"{i}. {claim['claim'][:60]}",
                        "cite_id": c.get("cite_id"),
                        "window": c.get("window"),
                        "review_month": row.get("review_month", "(not retrieved)"),
                        "in_declared_window": str(c.get("window") in q["scope"]["windows"]
                                                  and c.get("window") == row.get("window")).lower(),
                        "review": (row.get("title") or row.get("text") or "")[:70]})
    return pd.DataFrame(out)


def citation_contract(question_id: str) -> str:
    """The contract's own verdict for this question, from the pipeline's own rule set."""
    questions, answers = _rag_docs()
    q = next(x for x in questions if x["question_id"] == question_id)
    a = next(x for x in answers if x["question_id"] == question_id)
    violations = rag_answers.contract_violations(dict(a), dict(q), list(a.get("retrieved") or []))
    if not violations:
        return f"RAG_CONTRACT {question_id}: no violation -- every citation resolves and is in scope"
    return f"RAG_CONTRACT {question_id}: " + "; ".join(violations)


# ========================================================================= stream ====
@dataclass
class Live:
    """The paced replay and the streaming query started at move 2 and read at move 10."""
    replay: subprocess.Popen | None
    query: Any
    topic: str
    started_at: datetime
    #: What the query's own `foreachBatch` observed, updated as batches land.
    seen: dict[str, Any] = field(default_factory=dict)

    @property
    def replay_running(self) -> bool:
        return self.replay is not None and self.replay.poll() is None


def replay_command(*, scope: str = "full", reset: bool = True) -> list[str]:
    """The argv move 2 runs: `make stream-produce`, spelled out so a test can read it."""
    cmd = ["./run.sh", "python", "-m", "src.ingest.stream_producer",
           "--category", C.CATEGORY, "--scope", scope]
    return [*cmd, "--reset"] if reset else cmd


def start_replay(*, scope: str = "full", reset: bool = True,
                 log: Path = REPLAY_LOG) -> subprocess.Popen:
    """Start the paced replay of the sorted file in the background (move 2).

    A separate process on purpose: the replay is `src/ingest/stream_producer.py`, it registers
    its own `stream_produce` ledger run, and it must keep sending for the whole demo while the
    kernel's Spark session does the other nine moves. Its console output -- including the
    event-time clock -- goes to `log`, which `replay_clock()` shows.

    `reset` is the destructive flag the demo wants and development does not: it deletes the
    stream topic so it holds exactly this replay.
    """
    log.parent.mkdir(parents=True, exist_ok=True)
    handle = log.open("w")
    return subprocess.Popen(replay_command(scope=scope, reset=reset), cwd=C.PROJECT_ROOT,
                            stdout=handle,
                            stderr=subprocess.STDOUT, text=True)


def replay_clock(*, lines: int = 6, log: Path = REPLAY_LOG) -> list[str]:
    """The last few lines the replay printed -- the event-time clock, moving."""
    if not log.exists():
        return ["(the replay has not written anything yet)"]
    return log.read_text().splitlines()[-lines:]


def start_live_projection(stage: Stage, *, trigger_s: int = 5) -> Live:
    """Start the projection's own transformations over the stream topic, in this kernel.

    Deliberately *not* the gated projection run. `make stream-aggregate` is the run the ledger
    records and `make gate-stream` re-derives; this is the same four functions from
    `src/spark/stream_product_month.py` -- `shaped`, `review_rows`, `deduplicated`,
    `batch_aggregates` -- run against the live topic so the audience sees micro-batches
    arriving, with nothing written to Iceberg and no ledger row claimed for what a projector
    showed. The number that gets published is move 10's recorded gate.
    """
    # Imported here rather than at the top: every other move works without the projection
    # module, and move 2 is the only one that needs Kafka to be part of the picture.
    from src.spark import stream_product_month as SP

    cfg = load_replay_config()
    topic = cfg.topic_for(stage.scope)
    seen = {"batches": 0, "rows": 0, "product_months": 0, "max_event_month": ""}

    def observe(batch, batch_id: int) -> None:
        batch = batch.persist()
        try:
            rows = batch.count()
            agg = SP.batch_aggregates(batch)
            seen["batches"] += 1
            seen["rows"] += rows
            seen["product_months"] = agg.count()
            newest = batch.agg(F.max("event_ts").alias("m")).first()["m"]
            seen["max_event_month"] = newest.strftime("%Y-%m") if newest else ""
            print(f"[live] batch {batch_id}: {rows:,} deduplicated rows, event time reached "
                  f"{seen['max_event_month']} ({seen['rows']:,} so far)", flush=True)
        finally:
            batch.unpersist()

    raw = (stage.spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
           .option("failOnDataLoss", "true")
           .load())
    stream = SP.deduplicated(
        SP.review_rows(SP.shaped(raw, ingested_at=datetime.now(UTC))), watermark=cfg.watermark)
    query = (stream.writeStream
             .outputMode("append")
             .queryName("demo_live_projection")
             .foreachBatch(observe)
             .trigger(processingTime=f"{trigger_s} seconds")
             .start())
    return Live(replay=None, query=query, topic=topic, started_at=datetime.now(UTC), seen=seen)


def stop_live_projection(live: Live, *, drain_s: int = 10) -> pd.DataFrame:
    """Stop the live query and report what it saw (move 10, first half)."""
    deadline = time.time() + drain_s
    while time.time() < deadline and live.query.status.get("isDataAvailable"):
        time.sleep(1)
    live.query.stop()
    seen = live.seen
    return pd.DataFrame([
        {"fact": "topic", "value": live.topic},
        {"fact": "micro-batches", "value": str(seen.get("batches", 0))},
        {"fact": "deduplicated rows", "value": f"{seen.get('rows', 0):,}"},
        {"fact": "event time reached", "value": seen.get("max_event_month", "")},
        {"fact": "elapsed_s",
         "value": f"{(datetime.now(UTC) - live.started_at).total_seconds():.0f}"},
    ])

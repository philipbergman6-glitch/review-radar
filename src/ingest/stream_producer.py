"""Replay the event-time-sorted review file into the stream topic at a constant rate.

This is P8's producer (ADR-0010, ticket 14), and it is a different job from
`src/ingest/producer.py` in three ways worth being able to say out loud:

* **Its own topic.** `reviews.raw` carries the file in line order for the bronze drain;
  `reviews.stream` carries it in event order for the watermark job. Same file, two
  orderings, two streams -- and sample never shares a topic with full (ADR-0008).
* **Constant record rate, from frozen configuration.** Not event-time-linear compression:
  that spends 61% of the demo clock on the 5% of rows before 2015. The rate lives in
  `conf/stream_replay.toml` and was committed before the first run.
* **An event-time clock.** The latest review timestamp processed is printed as the replay
  advances, so an audience watches twenty-three years of event time pass in under four
  minutes of wall clock and can see the two clocks are not the same clock.

It refuses an unsorted input. Order is the premise of every watermark claim downstream, so
a file whose timestamps go backwards is a hard failure here, naming the line -- not a
warning that ends up as an unexplained drop count two jobs later.

Lateness is *not* injected here. The held-back slices are frozen in the same config and are
ticket 16's to release; this producer sends every row at its natural position, which is what
makes it the control run ADR-0010 requires.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Producer

from src.common import runs
from src.common.config import CATEGORY, DATA_RAW, KAFKA_BOOTSTRAP
from src.ingest.producer import ensure_topic
from src.ingest.replay_config import load_replay_config
from src.ingest.sort_replay import file_sha256, rel

_stop = False


def _handle_sigint(signum, frame):
    global _stop
    _stop = True
    print("\n[stream] stop requested, flushing...", flush=True)


def _event_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d")


def reset_topic(bootstrap: str, topic: str, *, timeout: float = 60.0) -> None:
    """Delete the topic and wait for the broker to forget it.

    The stream topic is meant to hold exactly one replay: the lineage gate resolves the
    producer's Kafka output by comparing the topic's record count with the run's acked count,
    and two replays on one topic make that count belong to neither of them. Appending a second
    replay would also interleave two orderings of the same file, which is the one thing the
    sort job exists to prevent.
    """
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
    if topic not in admin.list_topics(timeout=10).topics:
        return
    print(f"[stream] RESET: deleting topic '{topic}'", flush=True)
    for fut in admin.delete_topics([topic], operation_timeout=30).values():
        fut.result(timeout=30)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if topic not in admin.list_topics(timeout=10).topics:
            return
        time.sleep(1)
    raise SystemExit(f"[stream] topic '{topic}' was still present {timeout:.0f}s after the "
                     "delete request; the broker has not finished removing it")


def sort_run_for(src_sha: str, *, category: str, scope: str) -> dict:
    """The `sort_replay` run that wrote the file about to be replayed, by digest.

    Matched on the output digest rather than on "the latest sort run": the replay's whole
    claim is that it carried one particular ordering, and a file left over from an earlier
    sort would otherwise be replayed under the newest sort's run id. A file no sort run
    produced is a hard failure -- an ordering with no lineage is what ticket 14 split this job
    in two to prevent.
    """
    run = runs.latest_success("sort_replay", category=category, data_scope=scope)
    if run is None:
        raise SystemExit(f"no successful sort_replay run for {category}/{scope}. Run "
                         "`make sort-replay` first -- the replay names the run that ordered "
                         "the file it sends.")
    recorded = run["outputs"]["sorted_file"]["sha256"]
    if recorded != src_sha:
        raise SystemExit(
            f"the file on disk (sha256 {src_sha[:12]}…) is not the file sort_replay "
            f"{run['run_id'][:8]} wrote (sha256 {recorded[:12]}…). Re-run `make sort-replay`, "
            "or point --source at the file that run produced; an ordering with no lineage is "
            "the thing ticket 14 split this job in two to prevent.")
    return run


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", default=CATEGORY)
    ap.add_argument("--scope", choices=("full", "sample"), default="full")
    ap.add_argument("--source", default=None,
                    help="override the sorted .jsonl path (default: the sort job's output)")
    ap.add_argument("--topic", default=None, help="override the topic from the frozen config")
    ap.add_argument("--limit", type=int, default=0, help="stop after N records (0 = whole file)")
    ap.add_argument("--reset", action="store_true",
                    help="delete the topic first, so it holds exactly this replay (destructive)")
    args = ap.parse_args()

    cfg = load_replay_config()
    topic = args.topic or cfg.topic_for(args.scope)
    src = (Path(args.source) if args.source
           else (DATA_RAW / f"{args.category}.jsonl").with_suffix(cfg.suffix))
    if not src.exists():
        raise FileNotFoundError(
            f"{src} not found. Run `make sort-replay` first -- the stream replays the sorted "
            "file, and the sort is its own ledgered job (ticket 14).")

    src_sha, src_bytes = file_sha256(src)
    sort_run = sort_run_for(src_sha, category=args.category, scope=args.scope)
    print(f"[stream] topic={topic} rate={cfg.records_per_second:,}/s "
          f"source={src.name} sha256={src_sha[:12]}… "
          f"ordered_by={sort_run['run_id'][:8]}", flush=True)

    signal.signal(signal.SIGINT, _handle_sigint)
    if args.reset:
        reset_topic(KAFKA_BOOTSTRAP, topic)
    # Retention off, and not as a convenience. Every record on this topic carries its *event*
    # time as its Kafka timestamp -- that is what makes the replay an event-time stream and
    # what lets a console consumer show 2003 going past -- and Kafka's log cleaner deletes
    # segments by that same timestamp. Under the broker's default 168 hours a replay of
    # twenty-three years of history is therefore deleted within minutes of being written: the
    # first control run read 212,469 of 701,528 records because 489,059 had already aged out
    # between the replay finishing and the projection starting. The topic holds a fixed
    # historical replay, so wall-clock retention has nothing to express about it.
    ensure_topic(KAFKA_BOOTSTRAP, topic, cfg.partitions,
                 config={"retention.ms": "-1", "retention.bytes": "-1"})

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        # Smaller linger than the bulk producer: at a paced rate the batching window is
        # latency the event-time clock would show as a stall, and throughput is capped by
        # the rate anyway, so there is nothing to win by waiting.
        "linger.ms": 5,
        "batch.size": 65_536,
        "queue.buffering.max.messages": 200_000,
        # `localhost` resolves to ::1 first on this host, and the broker listens on IPv4
        # only, so librdkafka prints a red FAIL line per connection attempt before falling
        # back. Harmless, but this producer runs on a projector in front of an examiner and
        # the first thing on screen should not look like an error.
        "broker.address.family": "v4",
    })

    run = runs.start(
        "stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
        category=args.category, data_scope=args.scope,
        inputs={"source": {"run_id": sort_run["run_id"], "path": rel(src),
                           "sha256": src_sha, "bytes": src_bytes},
                "protocol": {"path": rel(cfg.path),
                             "status": cfg.status, "config_hash": cfg.config_hash}},
        params={"topic": topic, "records_per_second": cfg.records_per_second,
                "run_kind": "control", "replay_config_hash": cfg.config_hash})

    sent = failed = 0
    first_ms = last_ms = None
    t0 = time.time()
    interval = 1.0 / cfg.records_per_second

    def on_delivery(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            if failed <= 5:
                print(f"[stream] DELIVERY FAILED: {err}", file=sys.stderr)

    try:
        with src.open("r", encoding="utf-8") as fin:
            for lineno, line in enumerate(fin, start=1):
                if _stop:
                    break
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)          # the sorted file is ours; a parse error is a bug
                ts = int(rec["timestamp"])
                if last_ms is not None and ts < last_ms:
                    raise SystemExit(
                        f"[stream] {src.name} line {lineno} goes backwards in event time "
                        f"({_event_date(ts)} after {_event_date(last_ms)}). The stream replays "
                        "the sort job's output; re-run `make sort-replay`.")
                first_ms = ts if first_ms is None else first_ms
                last_ms = ts

                key = (rec.get("parent_asin") or rec.get("asin")).encode()
                while True:
                    try:
                        producer.produce(topic, key=key, value=line.encode("utf-8"),
                                         timestamp=ts, on_delivery=on_delivery)
                        break
                    except BufferError:
                        producer.poll(0.5)

                sent += 1
                if sent % cfg.clock_every_records == 0:
                    el = time.time() - t0
                    print(f"[stream] event-time {_event_date(ts)}  |  {sent:>9,} sent  "
                          f"{sent / el:>7,.0f} rec/s  {el:>6.1f}s wall", flush=True)
                producer.poll(0)

                # Pace against an absolute schedule, not a per-record sleep: a fixed sleep
                # accumulates every poll and produce call as drift, and a replay that quietly
                # ran at 2,400 rec/s would make the demo's timing claim false.
                deadline = t0 + sent * interval
                slack = deadline - time.time()
                if slack > 0:
                    time.sleep(slack)

                if args.limit and sent >= args.limit:
                    break

        remaining = producer.flush(120)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}",
                    counts={"records_attempted": sent})
        raise

    el = time.time() - t0
    acked = sent - failed
    print(f"\n[stream] done: {sent:,} sent in {el:.1f}s ({sent / max(el, 1e-9):,.0f} rec/s), "
          f"event time {_event_date(first_ms)} → {_event_date(last_ms)}")
    if remaining:
        print(f"[stream] WARNING: {remaining} message(s) still queued at timeout", file=sys.stderr)

    if failed or remaining:
        runs.failed(run, notes=f"{failed} delivery failure(s), {remaining} still queued",
                    counts={"records_attempted": sent, "records_acked": acked})
        raise SystemExit(f"[stream] {failed} message(s) failed delivery")

    runs.success(
        run, records_in=sent, records_out=acked, records_rejected=0,
        outputs={"kafka": {"topic": topic}},
        counts={"records_attempted": sent, "records_acked": acked,
                "first_event_ms": first_ms, "last_event_ms": last_ms,
                "records_per_second_target": cfg.records_per_second,
                "records_per_second_actual": round(sent / max(el, 1e-9), 1),
                "elapsed_s": round(el, 1), "replay_config_hash": cfg.config_hash})


if __name__ == "__main__":
    main()

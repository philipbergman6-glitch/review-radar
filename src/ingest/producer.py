"""Replay the raw review JSONL into Kafka at a controllable rate.

Why replay from disk instead of calling a live API: the demo must not depend on
a third-party service being up. This is a faithful simulation of a review
firehose -- the processing model downstream is genuinely streaming -- but the
source is deterministic and repeatable.

Design notes worth being able to defend:

* key = parent_asin. The partition is chosen by hashing the key, so keying by
  product co-locates every review for one product on a single partition. That
  buys locality, not time-ordering: records land in the order this replay sends
  them, which is the order of lines in the raw file -- not review timestamp
  order. Anything that depends on event time must sort or window on the
  timestamp field downstream. Keying by review id would scatter a product's
  reviews across partitions and lose even the locality.
* We send the raw JSON line untouched. Parsing belongs in the bronze->silver
  step, so that a parsing bug never destroys data we can no longer recover.
* Malformed lines are counted and reported, never silently skipped.
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from src.common.config import CATEGORY, DATA_RAW, KAFKA_BOOTSTRAP, TOPIC_REVIEWS

_stop = False


def _handle_sigint(signum, frame):
    global _stop
    _stop = True
    print("\n[producer] stop requested, flushing...", flush=True)


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    """Create the topic if absent. Idempotent -- safe to run before every replay."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        n = len(existing[topic].partitions)
        print(f"[producer] topic '{topic}' exists with {n} partition(s)")
        return
    fut = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    fut[topic].result(timeout=20)
    print(f"[producer] created topic '{topic}' with {partitions} partitions")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", default=CATEGORY)
    ap.add_argument("--topic", default=TOPIC_REVIEWS)
    ap.add_argument("--partitions", type=int, default=6)
    ap.add_argument("--limit", type=int, default=0, help="stop after N records (0 = whole file)")
    ap.add_argument("--rate", type=float, default=0,
                    help="max records/sec (0 = as fast as possible)")
    ap.add_argument("--source", default=None, help="override the input .jsonl path")
    args = ap.parse_args()

    src = Path(args.source) if args.source else DATA_RAW / f"{args.category}.jsonl"
    if not src.exists():
        raise FileNotFoundError(f"{src} not found. Run scripts/download_data.py first.")

    signal.signal(signal.SIGINT, _handle_sigint)
    ensure_topic(KAFKA_BOOTSTRAP, args.topic, args.partitions)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        # Durability: wait for all in-sync replicas. Single broker locally, but this
        # is the setting that makes the delivery guarantee real on a cluster.
        "acks": "all",
        "enable.idempotence": True,      # no duplicates on internal retry
        "compression.type": "lz4",
        "linger.ms": 50,                 # small batching window -> far better throughput
        "batch.size": 262_144,
        "queue.buffering.max.messages": 200_000,
    })

    sent = 0
    bad = 0
    failed = 0
    t0 = time.time()
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    def on_delivery(err, msg):
        nonlocal failed
        if err is not None:
            failed += 1
            if failed <= 5:
                print(f"[producer] DELIVERY FAILED: {err}", file=sys.stderr)

    with src.open("r", encoding="utf-8") as fin:
        for line in fin:
            if _stop:
                break
            line = line.strip()
            if not line:
                continue
            try:
                # Parsed only to pull the partition key and reject garbage early.
                rec = json.loads(line)
                key = rec.get("parent_asin") or rec.get("asin")
            except json.JSONDecodeError:
                bad += 1
                continue
            if not key:
                bad += 1
                continue

            while True:
                try:
                    producer.produce(args.topic, key=key.encode(),
                                     value=line.encode("utf-8"), on_delivery=on_delivery)
                    break
                except BufferError:
                    # Local queue full: let librdkafka drain. This is backpressure,
                    # and it is the reason the producer cannot outrun the broker.
                    producer.poll(0.5)

            sent += 1
            if sent % 25_000 == 0:
                el = time.time() - t0
                print(f"[producer] {sent:>9,} sent  {sent / el:>9,.0f} rec/s  "
                      f"({el:.1f}s elapsed)", flush=True)
            if interval:
                time.sleep(interval)
            producer.poll(0)
            if args.limit and sent >= args.limit:
                break

    remaining = producer.flush(120)
    el = time.time() - t0
    print(f"\n[producer] done: {sent:,} sent in {el:.1f}s ({sent / max(el, 1e-9):,.0f} rec/s)")
    if bad:
        print(f"[producer] {bad:,} malformed/keyless lines skipped")
    if remaining:
        print(f"[producer] WARNING: {remaining} message(s) still queued at timeout", file=sys.stderr)
    if failed:
        raise SystemExit(f"[producer] {failed} message(s) failed delivery")


if __name__ == "__main__":
    main()

"""Phase 1 gate: kill the bronze job mid-stream and prove nothing was lost or
duplicated.

This is the claim that matters about a streaming pipeline, and it is easy to
assert and hard to demonstrate. So we demonstrate it:

  1. reset bronze, load a known number of records onto the topic
  2. start the bronze job and kill it (SIGKILL -- no clean shutdown, no chance to
     flush anything) part-way through
  3. record how many rows landed
  4. restart the job and let it drain
  5. assert  final == expected  and  duplicate (partition, offset) pairs == 0,
     *and* that the kill actually interrupted work in progress

Step 2 uses SIGKILL rather than SIGINT on purpose: a graceful stop would prove
only that the shutdown path works, not that the checkpoint is correct.

Step 5's second half matters as much as the first. A run where nothing had been
committed yet, or where the job had already drained the topic, satisfies
`final == expected` while having tested nothing at all -- and the earlier
version of this script printed PASS for exactly that. `failures()` treats both
as failures now.

This runs against its own topic and, since the table and checkpoint are derived
from the topic name, its own bronze table. It cannot reach the production one.
"""
from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.common.console import line_buffered_stdout

ROOT = Path(__file__).resolve().parents[1]

line_buffered_stdout()

ENV = {**os.environ,
       "JAVA_HOME": os.environ.get("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")}
ENV["PATH"] = f"{ENV['JAVA_HOME']}/bin:{ENV['PATH']}"
PY_BIN = str(ROOT / ".venv" / "bin" / "python")

TOPIC = "reviews.eos"          # own topic -- and therefore its own bronze table


def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, env=ENV, cwd=ROOT, check=True, **kw)


def reset_topic(topic: str) -> None:
    """Delete and recreate the topic so the record count is exact on a re-run."""
    from confluent_kafka.admin import AdminClient, NewTopic

    from src.common.config import KAFKA_BOOTSTRAP

    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    if topic in admin.list_topics(timeout=10).topics:
        for fut in admin.delete_topics([topic]).values():
            fut.result(timeout=30)
        # Deletion is asynchronous inside the broker; wait for it to disappear.
        for _ in range(30):
            if topic not in AdminClient(
                    {"bootstrap.servers": KAFKA_BOOTSTRAP}).list_topics(timeout=10).topics:
                break
            time.sleep(1)
    for fut in admin.create_topics([NewTopic(topic, num_partitions=6,
                                             replication_factor=1)]).values():
        fut.result(timeout=30)
    time.sleep(2)


def bronze_count(table: str) -> int:
    """Count bronze rows in a separate short-lived Spark session."""
    out = subprocess.run(
        [PY_BIN, "-c",
         ("from src.common.spark import build;"
          "s=build('count', cores='local[2]');"
          f"print('COUNT=%d' % s.table('{table}').count());"
          "s.stop()")],
        env=ENV, cwd=ROOT, capture_output=True, text=True, check=False)
    for line in out.stdout.splitlines():
        if line.startswith("COUNT="):
            return int(line.split("=")[1])
    raise RuntimeError(f"could not read count.\nstdout:\n{out.stdout}\nstderr:\n{out.stderr[-2000:]}")


def duplicate_offsets(table: str) -> int:
    out = subprocess.run(
        [PY_BIN, "-c",
         ("from src.common.spark import build;"
          "s=build('dupes', cores='local[2]');"
          f"t=s.table('{table}');"
          "d=t.groupBy('kafka_partition','kafka_offset').count().filter('count > 1').count();"
          "print('DUPES=%d' % d); s.stop()")],
        env=ENV, cwd=ROOT, capture_output=True, text=True, check=False)
    for line in out.stdout.splitlines():
        if line.startswith("DUPES="):
            return int(line.split("=")[1])
    raise RuntimeError(f"could not read dupes.\nstdout:\n{out.stdout}\nstderr:\n{out.stderr[-2000:]}")


def failures(*, expected: int, partial: int, final: int, dupes: int) -> list[str]:
    """Every reason this run is not a passing exactly-once demonstration.

    Empty list means PASS. Split out from main() so the degenerate cases can be
    tested without a Kafka broker and a 90-second run.
    """
    reasons: list[str] = []
    if partial <= 0:
        reasons.append(
            "the kill landed before anything was committed, so the restart had "
            "nothing to recover -- raise --kill-after")
    elif partial >= expected:
        reasons.append(
            f"the job finished before the kill ({partial:,} of {expected:,} rows "
            "already committed), so no interrupted batch was tested -- raise "
            "--records or lower --batch")
    if final != expected:
        reasons.append(
            f"final row count {final:,} != expected {expected:,} "
            f"({'lost' if final < expected else 'extra'} rows)")
    if dupes != 0:
        reasons.append(f"{dupes} duplicate (partition, offset) pairs")
    return reasons


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=120_000)
    ap.add_argument("--batch", type=int, default=10_000, help="maxOffsetsPerTrigger")
    ap.add_argument("--kill-after", type=float, default=25.0, help="seconds before SIGKILL")
    args = ap.parse_args()

    from src.spark.bronze import names_for
    table, checkpoint = names_for(TOPIC)

    print("=" * 74)
    print("STEP 1 - load a known number of records onto a clean topic")
    print("=" * 74)
    reset_topic(TOPIC)
    run([PY_BIN, "-m", "src.ingest.producer", "--topic", TOPIC,
         "--limit", str(args.records)], stdout=subprocess.DEVNULL)
    print(f"  {args.records:,} records on topic '{TOPIC}'")

    # Clear bronze *without* consuming, so the run below starts from zero rows.
    run([PY_BIN, "-m", "src.spark.bronze", "--reset-only", "--topic", TOPIC],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"  reset (0 rows): {table}, {checkpoint}")
    print("\n" + "=" * 74)
    print(f"STEP 2 - start bronze, then SIGKILL it after {args.kill_after:.0f}s")
    print("=" * 74)
    log = ROOT / "checkpoints" / "eos_run.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as lf:
        proc = subprocess.Popen(
            [PY_BIN, "-m", "src.spark.bronze", "--trigger", "3s",
             "--topic", TOPIC, "--max-per-trigger", str(args.batch)],
            env=ENV, cwd=ROOT, stdout=lf, stderr=subprocess.STDOUT,
            start_new_session=True)
        deadline = time.time() + args.kill_after
        while time.time() < deadline:
            if proc.poll() is not None:
                tail = log.read_text().strip().splitlines()[-25:]
                raise SystemExit(
                    "the bronze job exited on its own before the kill window "
                    f"(exit {proc.returncode}). Last output:\n  "
                    + "\n  ".join(tail))
            time.sleep(1)
    # Kill the whole process group: Spark's JVM is a child of the Python driver.
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait(timeout=60)
    print("  killed (SIGKILL -- no graceful shutdown, nothing flushed on the way out)")

    time.sleep(3)
    partial = bronze_count(table)
    print(f"  rows landed before the kill: {partial:,}")

    print("\n" + "=" * 74)
    print("STEP 3 - restart from the checkpoint and drain")
    print("=" * 74)
    # Append the restart to the same log rather than discarding it: when the
    # restart is what fails, its output is the only evidence of why (F7).
    with log.open("a") as lf:
        lf.write("\n" + "=" * 74 + "\nRESTART\n" + "=" * 74 + "\n")
        run([PY_BIN, "-m", "src.spark.bronze", "--trigger", "once",
             "--topic", TOPIC, "--max-per-trigger", str(args.batch)],
            stdout=lf, stderr=subprocess.STDOUT)
    print(f"  drained (full output in {log})")

    final = bronze_count(table)
    dupes = duplicate_offsets(table)

    print("\n" + "=" * 74)
    print("RESULT")
    print("=" * 74)
    print(f"  expected rows            : {args.records:,}")
    print(f"  rows before the kill     : {partial:,}")
    print(f"  rows after the restart   : {final:,}")
    print(f"  recovered by the restart : {final - partial:,}")
    print(f"  duplicate (partition, offset) pairs : {dupes}")
    print(f"  table under test         : {table}")

    reasons = failures(expected=args.records, partial=partial, final=final, dupes=dupes)
    print()
    if not reasons:
        print("  PASS - no loss, no duplication across an unclean restart.")
        return
    print("  FAIL - this run does not demonstrate exactly-once:")
    for r in reasons:
        print(f"    - {r}")
    sys.exit(1)


if __name__ == "__main__":
    main()

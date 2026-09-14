"""Sort the raw review file into event-time order, once, into its own file (ticket 14).

Why a separate job rather than a `--sorted` flag on the producer: event order becomes a
property of the *data*, recorded in the ledger with the digest of the file it came from and
the digest of the file it produced. A replay that sorted on the fly would make the ordering
a property of the run, so two replays of "the same" file could differ and nothing would say
so. The streaming job's watermark claims rest on order, so order needs lineage.

The sort is over an index, not the file. We stream the input once recording, per line, its
byte offset, its event timestamp and its review id; sort that list (about 700k tuples, tens
of MB); then write the output by seeking to each offset in turn. The file itself is never
held in memory twice, which is what keeps this inside the 16 GB envelope alongside a running
Docker stack.

Ordering is `(timestamp, review_id)` and nothing else. review_id is a hash of
(user, product, timestamp), so ties are broken by content and the output is byte-identical
on any machine, for any line order of the input -- which is the property that makes
`output_sha256` a meaningful thing to record.

Rows the identity cannot be computed for -- unparsable JSON, a null timestamp, a blank
user or product -- are *not* dropped silently. They are counted by named reason, written to
a rejects sidecar next to the output, and reported. The stream is a projection beside batch
(ADR-0010), so it must be able to say exactly which rows it never carried.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from src.common import runs
from src.common.canonical import review_id as compute_review_id
from src.common.config import CATEGORY, DATA_RAW, PROJECT_ROOT
from src.ingest.replay_config import load_replay_config

READ_CHUNK = 8 * 1024 * 1024


def file_sha256(path: Path) -> tuple[str, int]:
    """(hex digest, bytes) over the whole file."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(READ_CHUNK):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _reject_reason(rec: dict) -> str | None:
    """The first thing that makes a row unorderable, in a fixed precedence."""
    ts = rec.get("timestamp")
    if ts is None:
        return "null_timestamp"
    try:
        int(ts)
    except (TypeError, ValueError):
        return "unparsable_timestamp"
    if not str(rec.get("user_id") or "").strip():
        return "blank_user_id"
    if not str(rec.get("parent_asin") or "").strip():
        return "blank_parent_asin"
    return None


def build_index(src: Path) -> tuple[list[tuple[int, str, str, int]], Counter, list[tuple[int, str]]]:
    """One pass: (timestamp, review_id, line digest, byte offset) per orderable line.

    Offsets are taken from the binary stream, so a multi-byte character anywhere earlier in
    the file cannot shift them -- `tell()` on a text handle would.

    The line digest is in the key because `(timestamp, review_id)` is **not** a total order
    on this file: review_id is a hash of (user, product, timestamp), and the source contains
    6,139 key collision groups over 13,415 rows (`docs/phase0-profile.txt`) -- the fact
    silver's dedupe exists for. Ordering those by content digest makes the output
    byte-identical whatever order they arrived in: rows that share a digest are the same
    bytes, so which one is written first cannot change the file.
    """
    index: list[tuple[int, str, str, int]] = []
    reasons: Counter = Counter()
    rejects: list[tuple[int, str]] = []
    with src.open("rb") as f:
        offset = 0
        for raw in f:
            line_start = offset
            offset += len(raw)
            stripped = raw.strip()
            if not stripped:
                continue
            try:
                rec = json.loads(stripped)
            except json.JSONDecodeError:
                reasons["unparsable_json"] += 1
                rejects.append((line_start, "unparsable_json"))
                continue
            reason = _reject_reason(rec)
            if reason:
                reasons[reason] += 1
                rejects.append((line_start, reason))
                continue
            rid = compute_review_id(rec["user_id"], rec["parent_asin"], int(rec["timestamp"]))
            digest = hashlib.sha256(stripped).hexdigest()
            index.append((int(rec["timestamp"]), rid, digest, line_start))
    return index, reasons, rejects


def write_sorted(src: Path, out: Path, index: list[tuple[int, str, str, int]]) -> int:
    """Write the lines in index order. Returns rows written."""
    written = 0
    with src.open("rb") as fin, out.open("wb") as fout:
        for *_key, offset in index:
            fin.seek(offset)
            line = fin.readline().strip()
            fout.write(line + b"\n")
            written += 1
    return written


def write_rejects(src: Path, out: Path, rejects: list[tuple[int, str]]) -> None:
    """The rows the stream never carried, with the reason, in input order."""
    with src.open("rb") as fin, out.open("w", encoding="utf-8") as fout:
        for offset, reason in rejects:
            fin.seek(offset)
            line = fin.readline().strip().decode("utf-8", errors="replace")
            fout.write(json.dumps({"reason": reason, "byte_offset": offset, "line": line},
                                  ensure_ascii=False) + "\n")


def rel(path: Path) -> str:
    """Repo-relative when it can be, absolute otherwise, so a ledger row is portable.

    An override like `--source /tmp/x.jsonl` is legitimate (it is how the sample and the
    tests run) and must not crash the job on the way into the ledger.
    """
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path.resolve())


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d")


def sort_file(src: Path, out: Path) -> dict:
    """The whole job, minus the ledger. Returns the counts the contract wants."""
    index, reasons, rejects = build_index(src)
    if not index:
        raise SystemExit(f"[sort] {src} yielded no orderable rows; nothing to sort")

    index.sort(key=lambda t: t[:3])
    written = write_sorted(src, out, index)
    reject_path = out.with_suffix(out.suffix + ".rejects.jsonl")
    write_rejects(src, reject_path, rejects)

    distinct_ids = len({rid for _, rid, _, _ in index})
    distinct_keys = len({t[:3] for t in index})
    out_sha, out_bytes = file_sha256(out)
    return {
        "rows_in": written + sum(reasons.values()),
        "rows_sorted": written,
        "rows_rejected": sum(reasons.values()),
        "reject_reasons": dict(reasons),
        "distinct_review_ids": distinct_ids,
        # The stream's own count of what silver calls a key collision. ADR-0010 dedupes the
        # stream on review_id, so this is exactly how many rows that will drop -- recorded
        # here, before the streaming job exists, so ticket 15's reconciliation has a number
        # to reconcile against rather than one it derives from its own output.
        "key_collision_rows": written - distinct_ids,
        "distinct_sort_keys": distinct_keys,
        "byte_identical_rows": written - distinct_keys,
        "first_event_ms": index[0][0],
        "last_event_ms": index[-1][0],
        "output_sha256": out_sha,
        "output_bytes": out_bytes,
        "reject_path": rel(reject_path),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", default=CATEGORY)
    ap.add_argument("--scope", choices=("full", "sample"), default="full")
    ap.add_argument("--source", default=None, help="override the input .jsonl path")
    ap.add_argument("--out", default=None, help="override the output .jsonl path")
    args = ap.parse_args()

    cfg = load_replay_config()
    src = Path(args.source) if args.source else DATA_RAW / f"{args.category}.jsonl"
    if not src.exists():
        raise FileNotFoundError(f"{src} not found. Run scripts/download_data.py first.")
    out = Path(args.out) if args.out else src.with_suffix(cfg.suffix)
    if out.resolve() == src.resolve():
        raise SystemExit("[sort] refusing to sort a file onto itself")

    src_sha, src_bytes = file_sha256(src)
    print(f"[sort] {src.name}: {src_bytes:,} bytes, sha256 {src_sha[:12]}…", flush=True)

    run = runs.start(
        "sort_replay", runs.SORT_REPLAY_SPEC_VERSION,
        category=args.category, data_scope=args.scope,
        inputs={"source": {"path": rel(src),
                           "sha256": src_sha, "bytes": src_bytes},
                "protocol": {"path": rel(cfg.path),
                             "status": cfg.status, "config_hash": cfg.config_hash}},
        params={"tiebreak": cfg.tiebreak, "replay_config_hash": cfg.config_hash})
    try:
        counts = sort_file(src, out)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}")
        raise

    print(f"[sort] {counts['rows_sorted']:,} rows sorted "
          f"({_iso(counts['first_event_ms'])} → {_iso(counts['last_event_ms'])}) -> {out.name}")
    if counts["rows_rejected"]:
        print(f"[sort] {counts['rows_rejected']:,} unorderable row(s) by reason: "
              f"{counts['reject_reasons']} -> {counts['reject_path']}", file=sys.stderr)
    print(f"[sort] output sha256 {counts['output_sha256'][:12]}… "
          f"({counts['output_bytes']:,} bytes)")

    runs.success(
        run, records_in=counts["rows_in"], records_out=counts["rows_sorted"],
        records_rejected=counts["rows_rejected"],
        outputs={"sorted_file": {"path": rel(out),
                                 "sha256": counts["output_sha256"],
                                 "rows": counts["rows_sorted"]}},
        counts={k: v for k, v in counts.items() if k != "rows_in"} |
               {"replay_config_hash": cfg.config_hash})


if __name__ == "__main__":
    main()

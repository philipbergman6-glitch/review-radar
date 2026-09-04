"""Cut a small, deterministic sample out of the raw JSONL files.

Used for fast iteration and for the `data/sample/` artefact we hand in with the
project (the full files are far too big to commit).
"""
from __future__ import annotations

import argparse
from pathlib import Path

from src.common.config import CATEGORY, DATA_RAW, DATA_SAMPLE


def head_lines(src: Path, dst: Path, n: int) -> int:
    if not src.exists():
        raise FileNotFoundError(f"Raw file not found: {src}. Run scripts/download_data.py first.")
    dst.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with src.open("r", encoding="utf-8") as fin, dst.open("w", encoding="utf-8") as fout:
        for line in fin:
            fout.write(line)
            written += 1
            if written >= n:
                break
    return written


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default=CATEGORY)
    ap.add_argument("--reviews", type=int, default=10_000)
    ap.add_argument("--meta", type=int, default=5_000)
    args = ap.parse_args()

    r = head_lines(DATA_RAW / f"{args.category}.jsonl",
                   DATA_SAMPLE / f"{args.category}.sample.jsonl", args.reviews)
    m = head_lines(DATA_RAW / f"meta_{args.category}.jsonl",
                   DATA_SAMPLE / f"meta_{args.category}.sample.jsonl", args.meta)
    print(f"sample written: {r} reviews, {m} products -> {DATA_SAMPLE}")


if __name__ == "__main__":
    main()

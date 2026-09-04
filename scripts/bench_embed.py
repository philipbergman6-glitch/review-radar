"""Measure MiniLM sentence-embedding throughput on this host (wayfinder RR-05).

Times only ``model.encode`` (after a warm-up call) over real review text from
``data/sample/``, for a list of (batch_size, max_seq_length) configurations, and
prints every number plus the conditions it was taken under (CPU, threads, docker
containers, spark/ollama processes, peak RSS).

Example::

    .venv/bin/python scripts/bench_embed.py --n 4000 \
        --configs 16:256,64:256,256:256,64:128,64:64 --label supabase-stopped
"""
from __future__ import annotations

import argparse
import json
import platform
import resource
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from src.common.config import CATEGORY, DATA_SAMPLE, EMBED_MODEL


def sh(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30,
                              check=False).stdout.strip()
    except Exception as exc:  # noqa: BLE001 - conditions are best-effort, never fatal
        return f"<unavailable: {exc}>"


def load_texts(path: Path, n: int) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"sample not found: {path}")
    texts: list[str] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = json.loads(line).get("text") or ""
            if text.strip():
                texts.append(text)
            if len(texts) >= n:
                break
    if len(texts) < n:
        raise ValueError(f"only {len(texts)} non-empty reviews in {path}, wanted {n}")
    return texts


def pct(values: list[int], p: float) -> int:
    s = sorted(values)
    return s[min(len(s) - 1, round(p * (len(s) - 1)))]


def length_stats(texts: list[str]) -> dict:
    words = [len(t.split()) for t in texts]
    chars = [len(t) for t in texts]
    return {
        "n": len(texts),
        "words_mean": round(statistics.mean(words), 1),
        "words_p50": pct(words, 0.5),
        "words_p90": pct(words, 0.9),
        "words_max": max(words),
        "chars_mean": round(statistics.mean(chars), 1),
        "chars_p50": pct(chars, 0.5),
        "chars_p90": pct(chars, 0.9),
        "chars_max": max(chars),
        "ge20_words": sum(w >= 20 for w in words),
    }


def rss_mb() -> float:
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux reports kilobytes.
    return ru / (1024 * 1024) if sys.platform == "darwin" else ru / 1024


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=4000, help="number of reviews to embed per config")
    ap.add_argument("--configs", default="16:256,64:256,256:256,64:128,64:64",
                    help="comma list of batch:max_seq_length")
    ap.add_argument("--model", default=EMBED_MODEL)
    ap.add_argument("--label", default="", help="free-text condition label, e.g. supabase-stopped")
    ap.add_argument("--out", type=Path, default=None, help="optional JSON output path")
    ap.add_argument("--sample", type=Path, default=DATA_SAMPLE / f"{CATEGORY}.sample.jsonl")
    args = ap.parse_args()

    import torch  # imported late so --help is fast
    from sentence_transformers import SentenceTransformer

    texts = load_texts(args.sample, args.n)
    stats = length_stats(texts)

    conditions = {
        "timestamp_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "label": args.label,
        "cpu": sh(["sysctl", "-n", "machdep.cpu.brand_string"]) if sys.platform == "darwin" else platform.processor(),
        "ncpu": sh(["sysctl", "-n", "hw.ncpu"]) if sys.platform == "darwin" else "",
        "python": platform.python_version(),
        "torch": torch.__version__,
        "torch_threads": torch.get_num_threads(),
        "device": "cpu",
        "docker_ps": sh(["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"]).splitlines(),
        "pgrep_spark": sh(["pgrep", "-fl", "spark"]).splitlines(),
        "pgrep_ollama": sh(["pgrep", "-fl", "ollama"]).splitlines(),
        "model": args.model,
        "sample": str(args.sample),
    }

    t0 = time.perf_counter()
    model = SentenceTransformer(args.model, device="cpu")
    conditions["model_load_s"] = round(time.perf_counter() - t0, 2)
    conditions["model_default_max_seq_length"] = model.max_seq_length
    conditions["rss_after_load_mb"] = round(rss_mb(), 1)

    # warm-up: first call pays tokenizer/kernel init that must not be timed
    model.encode(texts[:64], batch_size=64, show_progress_bar=False)

    rows = []
    for cfg in args.configs.split(","):
        batch, max_len = (int(x) for x in cfg.split(":"))
        model.max_seq_length = max_len
        t0 = time.perf_counter()
        emb = model.encode(texts, batch_size=batch, show_progress_bar=False, convert_to_numpy=True)
        elapsed = time.perf_counter() - t0
        rows.append({
            "batch": batch, "max_seq_length": max_len, "n": len(texts),
            "seconds": round(elapsed, 2), "reviews_per_s": round(len(texts) / elapsed, 1),
            "dim": int(emb.shape[1]), "peak_rss_mb_so_far": round(rss_mb(), 1),
        })
        print(f"batch={batch:4d} max_seq={max_len:4d}  {len(texts)} reviews in {elapsed:7.2f}s  "
              f"= {len(texts)/elapsed:7.1f} reviews/s   peak RSS so far {rss_mb():7.1f} MB", flush=True)

    result = {"conditions": conditions, "sample_length": stats, "runs": rows,
              "peak_rss_mb": round(rss_mb(), 1)}
    print(json.dumps(result, indent=2))
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(result, indent=2))
        print(f"wrote {args.out}")


if __name__ == "__main__":
    main()

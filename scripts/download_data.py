"""Download one category of the Amazon Reviews 2023 dataset (McAuley Lab, UCSD).

Source: https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023
Licence/credit: McAuley-Lab, UCSD. Public dataset -- no personal data of ours
is involved, which is why sending review text to a hosted LLM is acceptable here.

Downloads are resumable (HTTP Range), so an interrupted run can be re-run safely.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common.config import CATEGORY, DATA_RAW  # noqa: E402

BASE = "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main"


def fetch(remote: str, local: Path) -> None:
    local.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BASE}/{remote}"
    print(f"-> {url}\n   {local}")
    # -C - resumes, -f fails loudly on HTTP errors instead of writing an error page.
    rc = subprocess.call(["curl", "-fL", "-C", "-", "--retry", "3", "--progress-bar",
                          "-o", str(local), url])
    if rc != 0:
        raise RuntimeError(f"Download failed (curl exit {rc}) for {url}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default=CATEGORY,
                    help="e.g. All_Beauty (dev, 0.3 GB) or Beauty_and_Personal_Care (full, 11 GB)")
    args = ap.parse_args()
    c = args.category
    fetch(f"raw/review_categories/{c}.jsonl", DATA_RAW / f"{c}.jsonl")
    fetch(f"raw/meta_categories/meta_{c}.jsonl", DATA_RAW / f"meta_{c}.jsonl")
    print("done")


if __name__ == "__main__":
    main()

"""Validate a partial reference-label file against the blind export, without importing.

The importer is all-or-nothing by design (RR-21), which makes it a poor feedback loop while
the agent is still labelling. This runs the *same* validator over whatever has been written
so far and prints one line per failure, so a mistyped evidence quote is caught in seconds
instead of at the end of a 200-row set.

Run:  ./run.sh python scripts/check_reference_labels.py --sample development
"""
from __future__ import annotations

import argparse
import json

from src.ai.labels import load_spec, load_taxonomy, validate_label
from src.common.config import PROJECT_ROOT

OUT_DIR = PROJECT_ROOT / "eval" / "themes"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit"])
    args = ap.parse_args()
    blind = {json.loads(x)["blind_id"]: json.loads(x)
             for x in (OUT_DIR / f"blind-{args.sample}.jsonl").read_text().splitlines() if x.strip()}
    path = OUT_DIR / f"reference-{args.sample}.jsonl"
    spec, tax = load_spec(), load_taxonomy()
    bad = seen = 0
    ids: set[str] = set()
    for n, line in enumerate(path.read_text().splitlines(), start=1):
        if not line.strip():
            continue
        seen += 1
        entry = json.loads(line)
        bid = entry.get("blind_id")
        if bid not in blind:
            print(f"line {n} {bid}: not in the blind export")
            bad += 1
            continue
        if bid in ids:
            print(f"line {n} {bid}: duplicate")
            bad += 1
        ids.add(bid)
        fails = validate_label({k: v for k, v in entry.items() if k != "blind_id"},
                               title=blind[bid]["title"], text=blind[bid]["text"],
                               theme_ids=tax.ids, limits=spec.limits)
        for f in fails:
            print(f"line {n} {bid}: {f}")
        bad += 1 if fails else 0
    missing = len(blind) - len(ids)
    print(f"REFERENCE_CHECK sample={args.sample} lines={seen} valid={seen - bad} invalid={bad} "
          f"missing={missing}")


if __name__ == "__main__":
    main()

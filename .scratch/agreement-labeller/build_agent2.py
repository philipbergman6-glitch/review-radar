"""Emit the second agent annotator's 50 labels, verifying every quote against the blind export.

Hard-fails rather than repairing: a quote that is not an exact whitespace-collapsed substring
of its own review's title or text is an annotator error, and silently trimming it would hide
exactly the thing the evidence rule exists to prevent.

Writes eval/themes/agent2-agreement-audit.jsonl. That file is deliberately NOT
`human-agreement-audit.jsonl`: `make agreement-import` stamps `label_source="human"`, which
RR-21 condition 2 reserves for Philip.

Run:  ./run.sh python .scratch/agreement-labeller/build_agent2.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
sys.path.insert(0, str(HERE))

from agent2_labels import LABELS, PASS_B_CHANGES  # noqa: E402

sys.path.insert(0, str(ROOT))
from src.ai.labels import load_spec, load_taxonomy, quote_is_evidence, validate_label, word_count  # noqa: E402

BLIND = ROOT / "eval" / "themes" / "blind-agreement-audit.jsonl"
OUT = ROOT / "eval" / "themes" / "agent2-agreement-audit.jsonl"


def main() -> None:
    blind = {}
    for line in BLIND.read_text().splitlines():
        if line.strip():
            r = json.loads(line)
            blind[r["blind_id"]] = r

    spec, tax = load_spec(), load_taxonomy()
    limits = spec.limits
    errors: list[str] = []

    missing = sorted(set(blind) - set(LABELS))
    extra = sorted(set(LABELS) - set(blind))
    if missing:
        errors.append(f"no label written for {len(missing)} review(s): {missing}")
    if extra:
        errors.append(f"labels written for ids not in the blind export: {extra}")

    rows = []
    for bid, r in blind.items():
        spec_row = LABELS.get(bid)
        if spec_row is None:
            continue
        themes = []
        for theme_id, quote in spec_row.get("themes", []):
            if theme_id not in tax.ids:
                errors.append(f"{bid}: {theme_id!r} is not in the frozen taxonomy")
            if not quote_is_evidence(quote, r["title"], r["text"]):
                errors.append(f"{bid}/{theme_id}: quote is not verbatim in the review: {quote!r}")
            if word_count(quote) > int(limits["label_quote_max_words"]):
                errors.append(f"{bid}/{theme_id}: quote has {word_count(quote)} words, "
                              f"limit {limits['label_quote_max_words']}")
            themes.append({"theme_id": theme_id, "evidence_quote": quote})

        other_phrase = spec_row.get("other")
        label = {
            "themes": themes,
            "other": {"present": other_phrase is not None, "phrase": other_phrase},
            "abstain": False,
            "overall_sentiment": spec_row["sentiment"],
            "label_confidence": spec_row["confidence"],
        }
        # The repo's own validator, run over exactly what will be written.
        for f in validate_label(label, title=r["title"], text=r["text"],
                                theme_ids=tax.ids, limits=limits):
            errors.append(f"{bid}: {f}")
        rows.append({"blind_id": bid, **label})

    if errors:
        for e in errors:
            print(f"ERROR {e}")
        raise SystemExit(f"AGENT2_BUILD refused: {len(errors)} error(s); nothing written")

    OUT.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows) + "\n")
    labelled = sum(1 for x in rows if x["themes"] or x["other"]["present"])
    hits: dict[str, int] = {t: 0 for t in tax.ids}
    for x in rows:
        for t in x["themes"]:
            hits[t["theme_id"]] += 1
    print(f"AGENT2_BUILD rows={len(rows)} with_complaint={labelled} "
          f"no_complaint={len(rows) - labelled} theme_mentions={sum(hits.values())} "
          f"pass_b_changes={len(PASS_B_CHANGES)} out={OUT.relative_to(ROOT)}")
    for t, n in sorted(hits.items(), key=lambda kv: -kv[1]):
        print(f"AGENT2_THEME {t:<20} n={n}")


if __name__ == "__main__":
    main()

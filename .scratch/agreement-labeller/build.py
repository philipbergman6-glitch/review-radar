"""Render the blind labelling page for Philip's stratified 50 (RR-21, ticket 10).

The 50 are the one set the agent must not label: `label_source="human"` is reserved for
Philip, and an agreement number written by the same model family that wrote the reference
labels measures self-consistency rather than accuracy. This script therefore builds a tool,
not labels -- it inlines the blind export and the frozen taxonomy into a single offline HTML
page, and the page writes `eval/themes/human-agreement-audit.jsonl` in the exact shape
`scripts/check_reference_labels.py` validates.

What the page is allowed to see is exactly what the blind export carries: `blind_id`, `title`
and `text`, plus the taxonomy. No star rating, no product, no window, and no output from the
system under test -- RR-21 binding condition 1.

Run:  ./run.sh python .scratch/agreement-labeller/build.py
      open .scratch/agreement-labeller/index.html
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

BLIND = ROOT / "eval" / "themes" / "blind-agreement-audit.jsonl"
TAXONOMY = ROOT / "conf" / "theme-taxonomy.json"
SPEC = ROOT / "conf" / "theme-label-spec.json"
TEMPLATE = HERE / "template.html"
OUT = HERE / "index.html"

ALLOWED_REVIEW_KEYS = {"blind_id", "title", "text"}


def main() -> None:
    rows = [json.loads(line) for line in BLIND.read_text().splitlines() if line.strip()]
    if not rows:
        raise SystemExit(f"{BLIND} is empty; run `make agreement-export` first")

    # Blindness is a protocol condition, so it is enforced here rather than assumed: anything
    # beyond the three blind fields would be a leak into the annotator's view.
    for r in rows:
        extra = set(r) - ALLOWED_REVIEW_KEYS
        if extra:
            raise SystemExit(f"{r.get('blind_id')}: blind export carries {sorted(extra)}, which "
                             "RR-21 condition 1 does not permit the annotator to see")
    ids = [r["blind_id"] for r in rows]
    if len(set(ids)) != len(ids):
        raise SystemExit("the blind export has duplicate blind_ids")

    taxonomy = json.loads(TAXONOMY.read_text())
    limits = json.loads(SPEC.read_text())["limits"]

    html = TEMPLATE.read_text()
    for marker, payload in (
        ("/*__REVIEWS__*/", rows),
        ("/*__TAXONOMY__*/", taxonomy),
        ("/*__LIMITS__*/", limits),
    ):
        if marker not in html:
            raise SystemExit(f"template is missing the {marker} marker")
        html = html.replace(marker, json.dumps(payload, ensure_ascii=False))

    OUT.write_text(html)
    print(f"LABELLER_BUILD rows={len(rows)} themes={len(taxonomy['themes'])} "
          f"quote_max_words={limits['label_quote_max_words']} "
          f"other_phrase_max_words={limits['other_phrase_max_words']} out={OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

"""Render the judging page for the thirty P7 answers (ADR-0006, RR-17, ticket 13).

The page is a tool, not a judgement: it inlines `eval/rag/judging-worksheet.json` into a
single offline HTML file and writes `eval/rag/judgements.jsonl` in the exact shape
`scripts/rag_judge.py --check` validates. The judge sees the question, the answer, every
retrieved review with its handle and window, the answer key, and the mechanical facts the
scorer will use -- and not the product's decline rank or slot role, which the export withholds.

Run:  ./run.sh python .scratch/rag-judge/build.py      (or `make rag-judge`)
      open .scratch/rag-judge/index.html
"""
from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent

WORKSHEET = ROOT / "eval" / "rag" / "judging-worksheet.json"
TEMPLATE = HERE / "template.html"
OUT = HERE / "index.html"

HIDDEN = {"slot_role", "decline_rank", "episode_id", "evidence", "probe"}


def main() -> None:
    if not WORKSHEET.exists():
        raise SystemExit(f"{WORKSHEET.relative_to(ROOT)} is missing; run `make rag-judge-export`")
    doc = json.loads(WORKSHEET.read_text())
    rows = doc["rows"]
    if len(rows) != 30:
        raise SystemExit(f"worksheet carries {len(rows)} rows, not the frozen thirty")
    for r in rows:
        leaked = set(r) & HIDDEN
        if leaked:
            raise SystemExit(f"{r['question_id']}: worksheet leaks {sorted(leaked)} to the judge")
    ids = [r["question_id"] for r in rows]
    if len(set(ids)) != len(ids):
        raise SystemExit("worksheet has duplicate question ids")

    html = TEMPLATE.read_text()
    marker = "/*__WORKSHEET__*/"
    if marker not in html:
        raise SystemExit(f"template is missing the {marker} marker")
    payload = {k: v for k, v in doc.items() if k != "rows"} | {"rows": rows}
    html = html.replace(marker, json.dumps(payload, ensure_ascii=False))
    OUT.write_text(html)
    kinds: dict[str, int] = {}
    for r in rows:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(f"RAG_JUDGE_BUILD rows={len(rows)} answers_run={doc['answers_run_id'][:8]} "
          f"{' '.join(f'{k}={v}' for k, v in sorted(kinds.items()))} "
          f"rubric={doc['rubric']['sha256'][:12]} out={OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

"""Second-agent consistency against `agent_reference` over the stratified 50.

This is NOT `THEMES_AGREEMENT`. RR-21 defines that as agent-vs-Philip and it stays NOT_RUN.
Both sides here were written by the same model family, so what this measures is how
reproducible the reference labelling is between annotator runs -- an upper bound on how much
of it is stable rather than arbitrary, and no evidence at all about accuracy.

It reads the files directly and writes nothing to Postgres, so no `label_source` is stamped
and `pipeline_runs` is untouched.

Run:  ./run.sh python .scratch/agreement-labeller/score_agent2.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from src.ai.agreement import agreement_report
from src.ai.labels import load_taxonomy

OUT_DIR = ROOT / "eval" / "themes"
REFERENCE = OUT_DIR / "reference-audit.jsonl"
AGENT2 = OUT_DIR / "agent2-agreement-audit.jsonl"
SUBSET = OUT_DIR / "agreement-subset-audit.json"
OUT = OUT_DIR / "agent2-consistency-audit.json"


def themes_by_id(path: Path) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for line in path.read_text().splitlines():
        if line.strip():
            r = json.loads(line)
            out[r["blind_id"]] = {t["theme_id"] for t in (r.get("themes") or [])}
    return out


def main() -> None:
    tax = load_taxonomy()
    a2 = themes_by_id(AGENT2)
    ref = themes_by_id(REFERENCE)
    ids = sorted(a2)
    absent = [i for i in ids if i not in ref]
    if absent:
        raise SystemExit(f"{len(absent)} of the 50 have no agent_reference label: {absent[:5]}")

    rep = agreement_report(agent={i: ref[i] for i in ids}, human=a2,
                           theme_ids=tax.ids, review_ids=ids)
    doc = {
        "what_this_is": "second-agent consistency: agent_reference vs a second in-session agent "
                        "annotator over the RR-21 stratified 50",
        "what_this_is_not": "THEMES_AGREEMENT, which RR-21 defines as agent_reference vs Philip "
                            "and which remains NOT_RUN",
        "caveat": "both sides are the same model family, so correlated error is unbounded here; "
                  "a high number is evidence of reproducibility, not of accuracy",
        "reference": str(REFERENCE.relative_to(ROOT)),
        "annotator_b": str(AGENT2.relative_to(ROOT)),
        "subset": json.loads(SUBSET.read_text()).get("draw_key") if SUBSET.exists() else None,
        **rep,
    }
    OUT.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")

    for t in rep["per_theme"]:
        k = "none" if t["kappa"] is None else round(t["kappa"], 3)
        print(f"AGENT2_THEME {t['theme_id']:<20} n={t['n']:>3} agree={t['agree']:>3} "
              f"ref={t.get('agent_positive', '?'):>2} b={t.get('human_positive', '?'):>2} "
              f"kappa={k}")
    lo, hi = rep["wilson_95"]
    print(f"AGENT2_CONSISTENCY reviews={rep['reviews']} themes={rep['themes']} "
          f"decisions={rep['n']} agreed={rep['agreed']} "
          f"agreement={rep['agreement']:.4f} wilson95=[{lo:.4f}, {hi:.4f}] "
          f"kappa={'none' if rep['kappa'] is None else round(rep['kappa'], 4)} "
          f"exact_set_match={rep['exact_set_match']}/{rep['reviews']}")
    print("AGENT2_NOTE this is not THEMES_AGREEMENT; the human slot is still empty")


if __name__ == "__main__":
    main()

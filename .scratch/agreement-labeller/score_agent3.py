"""Third-annotator adjudication against `agent_reference` over the stratified 50.

This is NOT `THEMES_AGREEMENT`. RR-21 defines that as agent_reference vs Philip and it stays
NOT_RUN; the human slot is still empty. What this adds over
`agent2-consistency-audit.json` is a pass produced by *adjudicating the taxonomy* --
reading each review against `conf/theme-taxonomy.json` `includes`/`excludes` and applying
the exclusion rules deliberately -- rather than by re-running the labelling prompt. It is
still the same model family as the reference, so correlated error is still unbounded and
none of this is evidence of accuracy.

Run:  ./run.sh python .scratch/agreement-labeller/score_agent3.py
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
AGENT3 = OUT_DIR / "agent3-agreement-audit.jsonl"
AGENT2 = OUT_DIR / "agent2-agreement-audit.jsonl"
SUBSET = OUT_DIR / "agreement-subset-audit.json"
OUT = OUT_DIR / "agent3-adjudication-audit.json"


def themes_by_id(path: Path) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for line in path.read_text().splitlines():
        if line.strip():
            r = json.loads(line)
            out[r["blind_id"]] = {t["theme_id"] for t in (r.get("themes") or [])}
    return out


def main() -> None:
    tax = load_taxonomy()
    a3 = themes_by_id(AGENT3)
    a2 = themes_by_id(AGENT2)
    ref = themes_by_id(REFERENCE)
    ids = sorted(a3)
    absent = [i for i in ids if i not in ref]
    if absent:
        raise SystemExit(f"{len(absent)} of the 50 have no agent_reference label: {absent[:5]}")

    rep = agreement_report(agent={i: ref[i] for i in ids}, human=a3,
                           theme_ids=tax.ids, review_ids=ids)
    disagreements = [
        {"blind_id": i, "reference": sorted(ref[i]), "adjudicated": sorted(a3[i]),
         "agent2": sorted(a2.get(i, set()))}
        for i in ids if ref[i] != a3[i]]
    doc = {
        "what_this_is": "third-annotator adjudication: agent_reference vs an agent applying the "
                        "frozen taxonomy's includes/excludes over the RR-21 stratified 50",
        "what_this_is_not": "THEMES_AGREEMENT, which RR-21 defines as agent_reference vs Philip "
                            "and which remains NOT_RUN",
        "caveat": "the same model family wrote both sides, so correlated error is unbounded; "
                  "this measures how much of the reference survives a deliberate reading of "
                  "the taxonomy, not whether either side is right",
        "reference": str(REFERENCE.relative_to(ROOT)),
        "annotator_b": str(AGENT3.relative_to(ROOT)),
        "subset": json.loads(SUBSET.read_text()).get("draw_key") if SUBSET.exists() else None,
        "disagreements": disagreements,
        **rep,
    }
    OUT.write_text(json.dumps(doc, indent=2, ensure_ascii=False) + "\n")

    lo, hi = rep["wilson_95"]
    print(f"AGENT3_ADJUDICATION reviews={rep['reviews']} themes={rep['themes']} "
          f"decisions={rep['n']} agreed={rep['agreed']} "
          f"agreement={rep['agreement']:.4f} wilson95=[{lo:.4f}, {hi:.4f}] "
          f"kappa={'none' if rep['kappa'] is None else round(rep['kappa'], 4)} "
          f"exact_set_match={rep['exact_set_match']}/{rep['reviews']}")
    for d in disagreements:
        print(f"AGENT3_DIFF {d['blind_id']} ref={d['reference']} adj={d['adjudicated']}")
    print("AGENT3_NOTE this is not THEMES_AGREEMENT; the human slot is still empty")


if __name__ == "__main__":
    main()

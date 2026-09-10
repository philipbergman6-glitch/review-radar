"""Select the prompt to freeze, on the development scores alone (ADR-0003, ticket 06).

Reads the score artefacts the three systems already wrote -- `label-v4`, `label-v5` and the
star-only floor -- checks they were measured the same way, applies the rule committed in
`docs/decisions/theme-prompt-freeze.md` *before* those numbers existed, and writes
`eval/themes/selection-development.json`.

Three things this script refuses to do quietly:

  * compare artefacts produced under different settings (`require_comparable` hard-fails);
  * select while the audit set has been opened -- it counts the `local_llm` audit labels and
    the `score-audit-*` artefacts, and both must be zero;
  * subtract parse failures from a score. The census is carried beside each system so the
    plumbing cost is separable, never so it can be netted off.

`--freeze` additionally writes the `frozen_prompt` block into `conf/theme-label-spec.json`,
which is what `scripts/gate_themes.py:check_prompt` re-derives the freeze from.

Run:  ./run.sh python scripts/select_prompt.py [--freeze]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
from typing import Any

from pyspark.sql import functions as F

from src.ai.label_themes import PROMPT_MAX_THEMES
from src.ai.labels import decoding_schema, load_spec, load_taxonomy
from src.ai.prompt_selection import Candidate, intervals_overlap, require_comparable, select
from src.ai.theme_labels import table_name
from src.common.config import PROJECT_ROOT
from src.common.spark import build

OUT_DIR = PROJECT_ROOT / "eval" / "themes"
SPEC_PATH = PROJECT_ROOT / "conf" / "theme-label-spec.json"
DECISION = "docs/decisions/theme-prompt-freeze.md"
BASELINE = "star_only"


def artefact(name: str) -> dict[str, Any]:
    path = OUT_DIR / name
    if not path.exists():
        raise SystemExit(f"missing score artefact {path.relative_to(PROJECT_ROOT)}; "
                         "every candidate is scored before anything is selected")
    return json.loads(path.read_text())


def settings(doc: dict[str, Any]) -> dict[str, Any]:
    """The score-moving settings an artefact declares, flattened for the comparability check."""
    out = {k: doc[k] for k in ("min_support", "seed", "bootstrap_draws", "taxonomy_hash",
                               "reference_source") if k in doc}
    out["reviews"] = doc["overall"]["reviews"]
    return out


def audit_untouched(spark, scope: str) -> dict[str, Any]:
    """Evidence for rule 7: the audit set has not been opened at the moment of the freeze."""
    table = table_name(scope)
    by_source: dict[str, int] = {}
    if spark.catalog.tableExists(table):
        by_source = {r["label_source"]: r["n"] for r in
                     (spark.table(table).filter(F.col("budget_line") == "audit")
                      .groupBy("label_source").agg(F.count("*").alias("n")).collect())}
    scored = sorted(p.name for p in OUT_DIR.glob("score-audit-*.json"))
    system_rows = by_source.get("local_llm", 0)
    return {"audit_label_rows_by_source": by_source, "audit_system_label_rows": system_rows,
            "audit_score_artefacts": scored, "untouched": system_rows == 0 and not scored}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", default="development", choices=["development"],
                    help="the selection frame; the audit set is not a choice here")
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--candidates", nargs="+", default=["label_v4", "label_v5"])
    ap.add_argument("--model", default=None)
    ap.add_argument("--freeze", action="store_true",
                    help="write the frozen_prompt block into conf/theme-label-spec.json")
    ap.add_argument("--force", action="store_true", help="re-freeze over an existing block")
    args = ap.parse_args()

    spec, tax = load_spec(), load_taxonomy()
    model = args.model or spec.model_id
    slug = model.replace(":", "_")
    existing = spec.raw.get("frozen_prompt")
    if args.freeze and existing and not args.force:
        raise SystemExit(f"conf/theme-label-spec.json already freezes {existing['version']}; "
                         "re-freezing after an audit number exists is what ADR-0003 forbids")

    systems: dict[str, dict[str, Any]] = {}
    censuses: dict[str, dict[str, Any] | None] = {}
    for name in args.candidates:
        prompt = spec.prompts[name]
        schema = decoding_schema(tax.ids, max_themes=PROMPT_MAX_THEMES.get(prompt.version))
        config_hash = spec.config_hash(name, schema, extra={"taxonomy": tax.file_hash},
                                       model_id=model)
        doc = artefact(f"score-{args.sample}-{prompt.version}-{slug}.json")
        if doc["inference_config_hash"] != config_hash:
            raise SystemExit(
                f"{prompt.version}: the committed score was written under config "
                f"{doc['inference_config_hash'][:12]}, but today's spec computes "
                f"{config_hash[:12]}; re-score before selecting")
        systems[prompt.version] = doc
        census_path = OUT_DIR / f"parse-census-{args.sample}-{prompt.version}-{slug}.json"
        censuses[prompt.version] = json.loads(census_path.read_text()) if census_path.exists() else None

    floor = artefact(f"score-{args.sample}-{BASELINE}.json")
    systems[floor["prompt_version"]] = floor
    censuses[floor["prompt_version"]] = None
    require_comparable({v: settings(d) for v, d in systems.items()})

    candidates = []
    for name in args.candidates:
        version = spec.prompts[name].version
        o = systems[version]["overall"]
        candidates.append(Candidate(name=name, version=version, macro_f1=o["macro_f1"],
                                    interval=tuple(o["bootstrap_95"]),
                                    failure_rate=o["coverage"]["failure_rate"],
                                    reviews=o["reviews"]))
    winner, reason = select(candidates)

    floor_o = floor["overall"]
    won = systems[winner.version]["overall"]
    overlaps = intervals_overlap(tuple(won["bootstrap_95"]), tuple(floor_o["bootstrap_95"]))
    margin = won["macro_f1"] - floor_o["macro_f1"]

    spark = build("select_prompt", cores="local[2]", driver_memory="2g")
    try:
        audit = audit_untouched(spark, args.scope)
    finally:
        spark.stop()
    if not audit["untouched"]:
        raise SystemExit(f"the audit set has been opened ({audit}); the prompt freeze is only "
                         "meaningful before it, so this selection is void")

    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True,
                          text=True, check=True).stdout.strip()
    out = {
        "kind": "prompt_selection", "decision": DECISION, "sample": args.sample,
        "scope": args.scope, "model_id": model, "taxonomy_version": tax.version,
        "taxonomy_hash": tax.file_hash, "selected_at": dt.datetime.now(dt.UTC).isoformat(),
        "selected_at_commit": head,
        "rule": ("highest development macro-F1 over supported themes; ties under "
                 "0.01 to the lower parse-failure rate, then to the earlier version; "
                 "parse failures scored as empty predictions, never netted off"),
        "winner": {"name": winner.name, "version": winner.version, "reason": reason},
        "systems": [
            {
                "version": version,
                "role": "candidate" if version in {spec.prompts[n].version for n in args.candidates}
                        else "baseline",
                "macro_f1": d["overall"]["macro_f1"],
                "bootstrap_95": d["overall"]["bootstrap_95"],
                "min_supported_recall": d["overall"]["min_supported_recall"],
                "supported_themes": len(d["overall"]["supported_themes"]),
                "reviews": d["overall"]["reviews"],
                "coverage": d["overall"]["coverage"],
                "per_theme": d["overall"]["per_theme"],
                "parse_census": (None if censuses[version] is None else
                                 {k: censuses[version][k] for k in
                                  ("parse_failed", "parse_failure_rate", "by_cause",
                                   "by_combination", "by_theme_slot", "diagnostic_ceiling")
                                  if k in censuses[version]}),
            }
            for version, d in systems.items()
        ],
        "star_only_floor": {
            "version": floor["prompt_version"], "macro_f1": floor_o["macro_f1"],
            "bootstrap_95": floor_o["bootstrap_95"], "margin": margin,
            "intervals_overlap": overlaps,
            "statement": (
                f"The winner's bootstrap interval {[round(x, 4) for x in won['bootstrap_95']]} "
                f"{'overlaps' if overlaps else 'does not overlap'} the star-only floor's "
                f"{[round(x, 4) for x in floor_o['bootstrap_95']]}. "
                + ("On 200 development rows the labeller is therefore not distinguishable from "
                   "predicting themes off the star rating; the freeze proceeds because one has "
                   "to, and the P6 verdict inherits this limitation."
                   if overlaps else
                   "The labeller is distinguishable from predicting themes off the star rating "
                   "at this sample size.")),
        },
        "audit_evidence": audit,
    }
    path = OUT_DIR / f"selection-{args.sample}.json"
    path.write_text(json.dumps(out, indent=1) + "\n")

    for version, d in systems.items():
        o = d["overall"]
        cen = censuses[version]
        lo, hi = o["bootstrap_95"]
        print(f"PROMPT_CANDIDATE version={version:<17} macro_f1={round(o['macro_f1'], 4):<7} "
              f"bootstrap95=[{round(lo, 4)},{round(hi, 4)}] "
              f"min_supported_recall={round(o['min_supported_recall'], 4)} "
              f"failure_rate={o['coverage']['failure_rate']} "
              f"parse_failed={o['coverage']['parse_failed']} "
              f"top_cause={next(iter(cen['by_cause']), 'none') if cen else 'n/a'}")
    print(f"PROMPT_SELECTION winner={winner.version} macro_f1={round(won['macro_f1'], 4)} "
          f"reason=\"{reason}\" star_only={round(floor_o['macro_f1'], 4)} "
          f"margin={round(margin, 4)} intervals_overlap={str(overlaps).lower()} "
          f"audit_system_rows={audit['audit_system_label_rows']} "
          f"out={path.relative_to(PROJECT_ROOT)}")

    if not args.freeze:
        print("PROMPT_FREEZE frozen=none (selection only; re-run with --freeze to record it)")
        return
    doc = json.loads(SPEC_PATH.read_text())
    doc["frozen_prompt"] = {
        "name": winner.name, "version": winner.version, "freeze_commit": head,
        "frozen_at": out["selected_at"], "decided_in": DECISION,
        "selected_on": {"sample": args.sample, "metric": "macro_f1",
                        "value": won["macro_f1"], "bootstrap_95": won["bootstrap_95"],
                        "reason": reason},
        "note": ("freeze_commit is the commit the selection was made at; the commit recording "
                 "this block is its child, so both are ancestors of any audit run."),
    }
    SPEC_PATH.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"PROMPT_FREEZE frozen={winner.version} freeze_commit={head[:8]} "
          f"out={SPEC_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

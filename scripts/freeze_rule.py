"""The protocol freeze: write the calibrated thresholds into the rule and seal it (ADR-0001).

One command, one commit, and after it `conf/decline_rule.toml` never moves again. It writes
the configuration `scripts/calibrate_gold.py` chose, sets `status = "frozen"`, and from that
point the gold job will evaluate points on and after `holdout_start` -- which is the moment
the 2020-2023 holdout opens and the project stops being able to un-see it.

Because that is irreversible in the only sense that matters, this refuses more than it
accepts. It will not freeze when:

* `eval/gold_calibration/gate.json` is missing, or was produced under a different protocol
  than the one currently in the file -- a freeze must seal the rule that was actually
  measured;
* the calibration's fast path disagreed with `src/gold/rule.py`, so the numbers describe a
  rule the project does not run;
* **no configuration in the grid held the placebo ceiling.** This is the refusal that cannot
  be waived. A rule whose noise rate is unknown or unacceptable is the thing ADR-0001 exists
  to prevent, and RR-09 round 5 is explicit that the ceiling is never weakened to let a rule
  through.

There is exactly one waiver, `--accept-power-shortfall`, and it exists because RR-09 round 5
pre-declared this specific contingency: *if no configuration reaches the power target under
an honest ceiling, do not weaken the ceiling -- report the trade-off and add two output
tiers.* So a power miss is a published limitation rather than a blocker, and the waiver makes
that a deliberate, recorded act instead of a silent one. It writes its own reason into the
config, where the design doc and the slides read it.

Run:  ./run.sh python scripts/freeze_rule.py [--accept-power-shortfall] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common import config as C
from src.common.console import line_buffered_stdout
from src.gold import calibration as K

line_buffered_stdout()

RULE_PATH = C.PROJECT_ROOT / "conf" / "decline_rule.toml"
ARTIFACT_PATH = C.PROJECT_ROOT / "eval" / "gold_calibration" / "gate.json"
SELECTED_PATH = C.PROJECT_ROOT / "eval" / "gold_calibration" / "selected.json"


def _rule_and_calibration(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    with path.open("rb") as f:
        doc = tomllib.load(f)
    return doc["rule"], doc.get("calibration", {})


def current_protocol_hash(path: Path = RULE_PATH) -> str:
    """The identity calibration stamped on its artefact, re-derived from the file as it is now."""
    import hashlib

    rule, cal = _rule_and_calibration(path)
    cal = {k: v for k, v in cal.items()}
    grid = cal.pop("grid", {})
    cal["grid"] = grid
    rule_hash = hashlib.sha256(json.dumps(rule, sort_keys=True).encode()).hexdigest()
    return K.protocol_hash(rule_hash, cal)


def rewrite(path: Path, chosen: dict[str, Any], *, waiver: str | None) -> str:
    """Set every chosen field and `status = "frozen"`, keeping the file's comments intact.

    A line-level rewrite, not a TOML round-trip: the comments in this file are the record of
    why each value is what it is, and a serialiser would drop them. Every field must be found
    exactly once, or nothing is written -- a freeze that silently missed a threshold would
    seal a rule nobody chose.
    """
    lines = path.read_text().splitlines(keepends=True)
    pending = {**chosen, "status": None}
    hits: dict[str, int] = {}
    in_rule = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("["):
            in_rule = stripped == "[rule]"
            continue
        if not in_rule or "=" not in stripped or stripped.startswith("#"):
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in pending:
            hits[key] = hits.get(key, 0) + 1
            if hits[key] > 1:
                raise ValueError(f"{path.name}: [rule] declares {key} more than once")
            comment = line.partition("#")[1:] if "#" in line else ("", "")
            tail = ("  # " + line.partition("#")[2].strip()) if "#" in line else ""
            del comment
            value = '"frozen"' if key == "status" else _toml(chosen[key])
            lines[i] = f"{key} = {value}{tail}\n"
    missing = [k for k in pending if k not in hits]
    if missing:
        raise ValueError(f"{path.name}: [rule] has no line for {', '.join(sorted(missing))}; "
                         f"nothing was written")
    stamp = [(f"\n# Frozen {datetime.now(UTC).date().isoformat()} by scripts/freeze_rule.py "
              f"from eval/gold_calibration/gate.json.\n"),
             ("# The values above are the configuration calibration chose; they do not move "
              "again (ADR-0001).\n")]
    if waiver:
        stamp.append(f"# Power shortfall accepted: {waiver}\n")
    return "".join(lines) + "".join(stamp)


def _toml(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return repr(v) if not isinstance(v, str) else f'"{v}"'


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--accept-power-shortfall", action="store_true",
                    help="freeze despite a missed power target, per RR-09 round 5: the ceiling "
                         "holds, the shortfall is published, and the investigate/watchlist "
                         "tiers carry the consequence")
    ap.add_argument("--dry-run", action="store_true", help="print what would change, write nothing")
    args = ap.parse_args()

    rule, _ = _rule_and_calibration(RULE_PATH)
    if rule["status"] == "frozen":
        sys.exit("FREEZE=REFUSED reason=already_frozen -- the protocol freeze happens once "
                 "and conf/decline_rule.toml has already had it")
    for p in (ARTIFACT_PATH, SELECTED_PATH):
        if not p.exists():
            sys.exit(f"FREEZE=REFUSED reason=no_calibration -- {p.relative_to(C.PROJECT_ROOT)} "
                     f"is missing; run scripts/calibrate_gold.py first")
    doc = json.loads(ARTIFACT_PATH.read_text())
    sel = json.loads(SELECTED_PATH.read_text())

    want = current_protocol_hash()
    if doc.get("protocol_hash") != want:
        sys.exit(f"FREEZE=REFUSED reason=stale_calibration -- the artefact was produced under "
                 f"protocol {str(doc.get('protocol_hash'))[:12]} and the file now hashes to "
                 f"{want[:12]}; recalibrate before freezing")

    constituents = " ".join(doc.get("constituents", []))
    if "fastpath_agrees=true" not in constituents:
        sys.exit("FREEZE=REFUSED reason=fastpath_disagrees -- the calibration's evaluator did "
                 "not agree with src/gold/rule.py, so its numbers are about a different rule")
    if "placebo_under_ceiling=true" not in constituents:
        sys.exit("FREEZE=REFUSED reason=ceiling_not_held -- no configuration in the grid kept "
                 "the placebo rate under the ceiling. RR-09 round 5 forbids weakening it, so "
                 "there is nothing here that may be frozen.")

    waiver = None
    if "power_target_met=true" not in constituents:
        if not args.accept_power_shortfall:
            sys.exit(f"FREEZE=REFUSED reason=power_target_missed -- power at the primary effect "
                     f"is {sel.get('power_at_primary')} against a target of "
                     f"{doc['metric']['threshold']}. RR-09 round 5 allows freezing anyway, with "
                     f"the ceiling intact and the shortfall published, but that is a decision "
                     f"and not a default: rerun with --accept-power-shortfall.")
        waiver = (f"power at the primary effect was {sel.get('power_at_primary')} against a "
                  f"target of {doc['metric']['threshold']}; the ceiling was held rather than "
                  f"weakened (RR-09 round 5), and the shortfall is published in the evaluation "
                  f"table's verdict column")

    chosen = dict(sel["chosen"])
    out = rewrite(RULE_PATH, chosen, waiver=waiver)
    print(f"FREEZE_CHOSEN key={sel['key']} "
          + " ".join(f"{k}={v}" for k, v in sorted(chosen.items())))
    print(f"FREEZE_EVIDENCE placebo_alerts_per_month={sel['placebo_alerts_per_month']} "
          f"power_at_primary={sel['power_at_primary']} calibration_verdict={sel['verdict']} "
          f"protocol_hash={want[:12]}")
    if args.dry_run:
        print("FREEZE=DRY_RUN nothing written")
        return
    RULE_PATH.write_text(out)
    head = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                          text=True, cwd=C.PROJECT_ROOT, check=False).stdout.strip()
    print(f"FREEZE=DONE file={RULE_PATH.relative_to(C.PROJECT_ROOT)} parent_commit={head} "
          f"waiver={'yes' if waiver else 'no'}")
    print("Commit this on its own, then `make gold` -- the holdout opens on that run.")


if __name__ == "__main__":
    main()

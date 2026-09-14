"""Calibrate the decline rule on pre-2020 data, and print the number the freeze waits on.

`make gold` currently reports 964 alerts. That number was produced by thresholds nobody had
justified, measured on the same data they were chosen against, so on its own it says almost
nothing -- an arbitrary rule fires an arbitrary number of times. This is the script that
turns it into a claim: run the same rule over series with the trend removed and count the
alerts it still finds (the placebo rate), and plant declines of a known size and count the
ones it catches (the power). Under a ceiling and a target both committed to
`conf/decline_rule.toml` before this ran.

It searches the committed grid rather than scoring the provisional rule alone, because the
provisional values were placeholders and the freeze has to choose *something*. Searching is
legitimate exactly here and nowhere later: every number below comes from evaluation points
before `holdout_start`, and the search ends at the freeze commit (ADR-0001).

Strictness the gold job does not apply, on purpose: a product's spine is truncated so that no
window of any calibration point reaches into the holdout. Gold stops at the first holdout
*point*, which still lets a 2019-12 point average 2020 ratings into its recent window --
tolerable for a development figure, not tolerable for the numbers that pick the thresholds.

Prints `GOLD_PLACEBO`, `GOLD_POWER` (one line per cell of the effect grid),
`GOLD_CALIBRATION_SELECT`, `GOLD_CALIBRATION_FASTPATH` and the terminal `GOLD_CALIBRATION`;
writes `eval/gold_calibration/gate.json`, and the chosen configuration to
`eval/gold_calibration/selected.json` for `scripts/freeze_rule.py` to read. Exit 0 on PASS.

Run:  ./run.sh python scripts/calibrate_gold.py [--scope full|sample]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import tomllib
from pathlib import Path
from typing import Any

import pandas as pd

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.console import line_buffered_stdout
from src.common.spark import build
from src.gates import calibration as verdicts
from src.gold import calibration as K
from src.gold.rule import Rule, evaluate, month_index, month_str

line_buffered_stdout()

RULE_PATH = C.PROJECT_ROOT / "conf" / "decline_rule.toml"
SELECTED_PATH = E.EVAL_ROOT / "gold_calibration" / "selected.json"

#: How many products the fast path is cross-checked against the rule module on. The check is
#: exact, so a handful of real spines is enough to catch drift; the cost is the reason it is
#: not every product.
FASTPATH_PRODUCTS = 400


# ------------------------------------------------------------------- the protocol ----
def load_calibration(path: Path = RULE_PATH) -> dict[str, Any]:
    """The `[calibration]` table, hard-failing on anything it cannot run under."""
    with path.open("rb") as f:
        doc = tomllib.load(f)
    if "calibration" not in doc:
        raise ValueError(f"{path.name} declares no [calibration] table, so there is no bar to "
                         f"measure against; the bar must precede the measurement")
    cal = dict(doc["calibration"])
    grid = dict(cal.pop("grid", {}))
    required = ("ceiling_alerts_per_month", "power_target", "primary_effect", "primary_mechanism",
                "effects", "mechanisms", "detection_window_points", "ramps",
                "primary_ramp_months",
                "placebo_replicates", "placebo_block_months", "injection_replicates", "seed")
    missing = [k for k in required if k not in cal]
    if missing:
        raise ValueError(f"[calibration] is missing {', '.join(missing)}")
    if cal["primary_effect"] not in cal["effects"]:
        raise ValueError("[calibration] primary_effect must be one of effects")
    if cal["primary_mechanism"] not in cal["mechanisms"]:
        raise ValueError("[calibration] primary_mechanism must be one of mechanisms")
    if cal["primary_ramp_months"] not in cal["ramps"]:
        raise ValueError("[calibration] primary_ramp_months must be one of ramps")
    if not grid:
        raise ValueError("[calibration.grid] declares no candidates")
    cal["grid"] = grid
    return cal


def build_grid(rule: dict[str, Any], grid: dict[str, list[Any]]) -> list[K.Config]:
    """Every combination the committed grid names, deduplicated, in a stable order."""
    import itertools

    fields = sorted(grid)
    out: dict[str, K.Config] = {}
    for combo in itertools.product(*(grid[f] for f in fields)):
        c = K.config_from(rule, **dict(zip(fields, combo, strict=True)))
        out.setdefault(c.key, c)
    return [out[k] for k in sorted(out)]


def rule_fields(path: Path = RULE_PATH) -> dict[str, Any]:
    import hashlib

    with path.open("rb") as f:
        doc = tomllib.load(f)
    r = doc["rule"]
    r = {**r, "config_hash": hashlib.sha256(
        json.dumps(r, sort_keys=True).encode()).hexdigest()}
    return r


# --------------------------------------------------------------------- the data ----
def read_silver(spark, src: dict[str, Any]) -> pd.DataFrame:
    df = (spark.read.option("snapshot-id", src["snapshot_id"]).table(src["table"])
          .select("parent_asin", "review_month", "rating"))
    pdf = df.toPandas()
    pdf["month"] = pd.to_datetime(pdf["review_month"]).dt.strftime("%Y-%m")
    return pdf[["parent_asin", "month", "rating"]]


def to_products(pdf: pd.DataFrame, *, holdout_start: str, min_reviews: int,
                min_months: int) -> tuple[list[K.Product], int]:
    """One `Product` per parent_asin over the months strictly before the holdout, and the
    count of products the prefilter dropped.

    The spine runs from the product's first development review to its last, empty months
    included, exactly as gold builds it -- but it stops short of `holdout_start` entirely, so
    no calibration window can reach a holdout rating.

    The prefilter is arithmetic, not judgement: a baseline window needs `min_reviews` reviews
    and the spine needs `B + R` months to carry a single point, so a product below either
    bound cannot be evaluable under *any* configuration in the grid and contributes nothing
    but padding to a 112,565-row array. Both bounds are taken as the grid's own minimum, so
    nothing admissible is excluded.
    """
    cut = month_index(holdout_start)
    pdf = pdf.copy()
    pdf["ix"] = [month_index(m) for m in pdf["month"]]
    pdf = pdf[pdf["ix"] < cut]
    out: list[K.Product] = []
    dropped = 0
    for asin, g in pdf.groupby("parent_asin", sort=True):
        ix = g["ix"].to_numpy()
        lo, hi = int(ix.min()), int(ix.max())
        if len(ix) < min_reviews or (hi - lo + 1) < min_months:
            dropped += 1
            continue
        out.append(K.Product(asin=str(asin), length=hi - lo + 1,
                             months=tuple(int(v) - lo for v in ix),
                             ratings=tuple(int(v) for v in g["rating"].to_numpy()),
                             start=lo))
    return out, dropped


# ------------------------------------------------------------------- the fast path ----
def fastpath_check(products: list[K.Product], rule: dict[str, Any],
                   c: K.Config) -> tuple[int, int, int, int]:
    """(points, alerts) from `src/gold/rule.py`, and the same from the array evaluator.

    Every number this script prints comes out of the array evaluator. If that has drifted
    from the rule the project actually ships, the whole calibration is about a rule nobody
    runs -- so the two are compared here, on real spines, before anything else is believed.
    """
    reference = Rule(status="frozen", holdout_start="9999-12",
                     baseline_months=c.baseline_months, recent_months=c.recent_months,
                     delta=c.delta, persistence=c.persistence, max_gap=c.max_gap,
                     recovery_points=c.recovery_points, min_reviews=c.min_reviews,
                     min_active_months=c.min_active_months,
                     unevaluable_resets_persistence=c.unevaluable_resets_persistence,
                     config_hash=rule["config_hash"], metric_tolerance=0.0)
    # Biggest first: a product with no evaluable point exercises nothing.
    chosen = sorted(products, key=lambda p: -len(p.ratings))[:FASTPATH_PRODUCTS]
    ref_points = ref_alerts = 0
    for p in chosen:
        counts, sums = K.spine_of(p)
        spine = [{"month": month_str(p.start + i), "review_count": int(counts[i]),
                  "rating_sum": sums[i], "neg_count": 0, "verified_count": 0,
                  "verified_rating_sum": 0.0, "nonempty_text_count": 0, "long_text_count": 0}
                 for i in range(p.length)]
        pts, _ = evaluate(spine, reference)
        ref_points += len(pts)
        ref_alerts += sum(1 for x in pts if x["alert"])
    book = K.Portfolio([K.spine_of(p) for p in chosen])
    r = book.scan(c)
    return ref_points, r["points"], ref_alerts, r["alerts"]


# ------------------------------------------------------------------------- main ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    rule = rule_fields()
    cal = load_calibration()
    if rule["status"] == "frozen":
        sys.exit("rule.status is already frozen: calibration chooses the thresholds and must "
                 "run before the freeze, never after it. GOLD_CALIBRATION=FAIL")

    row = runs.latest_success("gold", category=args.category, data_scope=args.scope)
    if row is None:
        sys.exit(f"no successful gold run for {args.category}/{args.scope}: "
                 f"GOLD_CALIBRATION=FAIL")
    src = row["inputs"]["silver"]

    spark = build("calibrate-gold", cores="local[4]", driver_memory="4g")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    try:
        pdf = read_silver(spark, src)
    finally:
        spark.stop()

    grid = cal["grid"]
    floor_reviews = min(grid.get("min_reviews", [rule["min_reviews"]]))
    floor_months = (min(grid.get("baseline_months", [rule["baseline_months"]]))
                    + min(grid.get("recent_months", [rule["recent_months"]])))
    products, dropped = to_products(pdf, holdout_start=rule["holdout_start"],
                                    min_reviews=floor_reviews, min_months=floor_months)
    del pdf
    if not products:
        sys.exit("no pre-holdout products: GOLD_CALIBRATION=FAIL")
    print(f"GOLD_CALIBRATION_INPUT products={len(products)} prefiltered_out={dropped} "
          f"prefilter=reviews>={floor_reviews},months>={floor_months} "
          f"product_months={sum(p.length for p in products)} "
          f"reviews={sum(len(p.ratings) for p in products)} "
          f"holdout_start={rule['holdout_start']} silver_snapshot={src['snapshot_id']}")
    main_run(products, rule=rule, cal=cal, row=row, src=src, scope=args.scope)


def main_run(products: list[K.Product], *, rule: dict[str, Any], cal: dict[str, Any],
             row: dict[str, Any], src: dict[str, Any], scope: str) -> None:
    reference = K.config_from(rule)
    configs = build_grid(rule, cal["grid"])
    spines = [K.spine_of(p) for p in products]
    book = K.Portfolio(spines)

    # ---- does the evaluator this script uses still agree with the rule the project ships ----
    rp, fp, ra, fa = fastpath_check(products, rule, reference)
    fastpath = verdicts.agreement_line(points_reference=rp, points_fast=fp,
                                       alerts_reference=ra, alerts_fast=fa)
    print(fastpath)
    fastpath_ok = (rp == fp and ra == fa)

    # ---- the denominator, fixed once under the committed rule so every candidate shares it ----
    watched = K.calendar_surveillance_months(products, book.evaluable_matrix(reference))
    observed = book.scan(reference)
    print(f"GOLD_CALIBRATION_OBSERVED config={reference.key} points={observed['points']} "
          f"evaluable={observed['evaluable']} alerts={observed['alerts']} "
          f"surveillance_months={watched} "
          f"alerts_per_month={observed['alerts'] / watched if watched else 0:.4f} "
          f"grid_configs={len(configs)}")

    # ---------------------------------------------------------------- the two measurements ----
    t0 = time.time()
    placebo = K.placebo(spines, configs, replicates=int(cal["placebo_replicates"]),
                        block=int(cal["placebo_block_months"]), seed=int(cal["seed"]),
                        surveillance_months=watched,
                        progress=lambda i, n: print(f"  placebo replicate {i}/{n} "
                                                    f"({time.time() - t0:.0f}s)", flush=True))
    t1 = time.time()
    power = K.power_grid(products, configs, reference=reference, effects=cal["effects"],
                         mechanisms=cal["mechanisms"],
                         ramps=[int(r) for r in cal["ramps"]],
                         window_points=int(cal["detection_window_points"]),
                         seed=int(cal["seed"]),
                         replicates=int(cal["injection_replicates"]),
                         progress=lambda i, n: print(f"  injection replicate {i}/{n} "
                                                     f"({time.time() - t1:.0f}s)", flush=True))

    # --------------------------------------------------------------------------- the choice ----
    primary_cell = (cal["primary_mechanism"], cal["primary_effect"],
                    int(cal["primary_ramp_months"]))
    primary = {c.key: power[(c.key, *primary_cell)] for c in configs}
    selection = K.select(configs, placebo, primary,
                         ceiling_alerts_per_month=float(cal["ceiling_alerts_per_month"]),
                         power_target=float(cal["power_target"]))

    # ------------------------------------------------------------------------------- output ----
    lines = [fastpath]
    chosen = selection.config
    ceiling = float(cal["ceiling_alerts_per_month"])
    target = float(cal["power_target"])
    # Every candidate's placebo rate is kept, so the grid is auditable and the choice is not
    # a claim the reader has to take on trust.
    for c in configs:
        line = verdicts.placebo_line(placebo[c.key], ceiling=ceiling,
                                     block=int(cal["placebo_block_months"]))
        if chosen and c.key == chosen.key:
            lines.append(line)
        print(("* " if chosen and c.key == chosen.key else "  ") + line)
    if chosen:
        for mech in cal["mechanisms"]:
            for ramp in (int(r) for r in cal["ramps"]):
                for eff in cal["effects"]:
                    p = power[(chosen.key, mech, eff, ramp)]
                    is_primary = (mech, eff, ramp) == primary_cell
                    line = verdicts.power_line(
                        p, target=target,
                        window_points=int(cal["detection_window_points"]),
                        primary=is_primary)
                    lines.append(line)
                    print(line)
    # RR-09 round 5: when nothing reaches power under an honest ceiling, the trade-off is
    # *reported* rather than resolved by moving the ceiling. So the whole grid is printed as a
    # frontier -- what each noise budget buys in detection -- and the best configuration at
    # each is named. Without this the FAIL below is an assertion; with it, it is a measurement.
    frontier = sorted(configs, key=lambda c: (-primary[c.key].power,
                                              placebo[c.key].alerts_per_month))
    print("GOLD_CALIBRATION_FRONTIER power_at_primary vs placebo_alerts_per_month, "
          "best-power first")
    seen: set[str] = set()
    for c in frontier:
        band = f"{min(placebo[c.key].alerts_per_month, 99.0):.0f}"
        if band in seen:
            continue
        seen.add(band)
        f_line = (f"GOLD_CALIBRATION_FRONTIER config={c.key} "
                  f"alerts_per_month={placebo[c.key].alerts_per_month:.4f} "
                  f"power={primary[c.key].power:.4f} "
                  f"under_ceiling={str(placebo[c.key].alerts_per_month <= ceiling).lower()}")
        lines.append(f_line)
        print(f_line)

    sel_line = verdicts.selection_line(selection, ceiling=ceiling, power_target=target)
    lines.append(sel_line)
    print(sel_line)

    v = verdicts.verdict(selection=selection, placebo_by_key=placebo,
                         primary_power=primary[chosen.key] if chosen else None,
                         constituents=lines, ceiling=ceiling, power_target=target,
                         fastpath_ok=fastpath_ok, scope=scope)
    print(v.terminal)

    protocol = K.protocol_hash(rule["config_hash"], cal)
    E.record(v, capability="gold_calibration", phase="P3 Gold", kind="quality",
             protocol_hash=protocol,
             population={"name": f"{C.CATEGORY} products with pre-{rule['holdout_start']} "
                                 f"evaluation points",
                         "n": len(products),
                         "surveillance_months": watched,
                         "silver_snapshot_id": src["snapshot_id"],
                         "grid_configs": len(configs),
                         "placebo_replicates": int(cal["placebo_replicates"]),
                         "injection_replicates": int(cal["injection_replicates"])},
             pipeline_run_id=row["run_id"], scope=scope,
             notes=[("thresholds were searched over the grid committed in "
                     "conf/decline_rule.toml before this ran; the search is confined to "
                     "evaluation points before holdout_start and ends at the freeze commit "
                     "(ADR-0001)"),
                    ("spines are truncated at holdout_start, so no calibration window reaches "
                     "a holdout rating -- stricter than the gold job, which stops at the "
                     "first holdout point and still averages later ratings into the recent "
                     "window of a 2019 point"),
                    ("the placebo null is a circular block permutation of each product's "
                     "monthly aggregates; it preserves volume, seasonality and dependence "
                     "shorter than a block, and removes trend"),
                    ("power is measured only on products that do not already alert inside "
                     "their own detection window, so a decline the rule would have caught "
                     "anyway is never counted as a detection")])

    if chosen:
        SELECTED_PATH.parent.mkdir(parents=True, exist_ok=True)
        SELECTED_PATH.write_text(json.dumps(
            {"chosen": chosen.as_rule_fields(), "key": chosen.key,
             "protocol_hash": protocol, "verdict": v.status,
             "placebo_alerts_per_month": round(placebo[chosen.key].alerts_per_month, 6),
             "power_at_primary": round(primary[chosen.key].power, 6),
             "reason": selection.reason}, indent=2, sort_keys=True) + "\n")
        print(f"GOLD_CALIBRATION_SELECTED {SELECTED_PATH.relative_to(C.PROJECT_ROOT)}")
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

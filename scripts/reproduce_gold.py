"""Independent reproduction of gold (ADR-0001, ADR-0008; ticket 03).

`GOLD_GATE` asserts gold's correctness using the code that produced gold: the spine is
complete, no review falls off it, every alert opened one episode. Those are real checks, but
a bug shared by the producer and the checker survives all three. This is the second claim,
the one silver already makes a layer down: re-derive the published tables by an independent
route and reconcile.

Deliberately *not* the Spark path. Starting from the silver snapshot the gold run pinned in
the ledger, this re-derives, in plain Python:

  * each product's monthly aggregates and its full calendar spine (empty months kept);
  * every evaluation point, from the words in `src/gold/rule.py`'s docstring and the values
    in `conf/decline_rule.toml` -- direct window sums, not prefix sums;
  * every decline episode, as a second pass over the finished point list rather than the
    single-pass state machine the reference runs.

It imports nothing from the gold job or the rule module. Spark is used only to *read* the
pinned Iceberg snapshots; `tests/test_reproduce_gold.py` is the one place the two
implementations meet, and it runs both over synthetic spines on every commit.

Three deliberate limits, written here because they bound what the gate can claim:

  * `persistence_run` is not compared. It is the reference's own loop counter, not a
    property of an evaluation point, and reproducing it would mean reproducing the loop.
  * `max_drop` is reconciled over the published span -- from the alert to the close -- and
    not over the span the rule's own docstring describes, which opens the episode at the
    first supporting point of the run. The two differ whenever a pre-alert point in the same
    run dropped further than anything after it. That is a divergence between gold's spec and
    gold's code, not a reproduction failure, so it is counted and printed on its own
    `MAX_DROP_SPAN` line rather than being allowed to fail the gate or to disappear.
  * `episode_key` is assigned to every point in an episode's span, from its first supporting
    point to its closing one. That coincides with the reference exactly while
    `unevaluable_resets_persistence = true`, which is what the rule config freezes. Flip
    that switch and the two readings of "which points belong to the episode" diverge -- and
    this gate is what would say so.

Prints one `GOLD_REPRO` line per reconciled table and a final `GOLD_REPRO_GATE`; exit 0 on
PASS. The verdict is pure (`src/gates/gold.py:repro`) and is published to
`eval/gold_repro/gate.json` for `make eval-table`.

Run:  ./run.sh python scripts/reproduce_gold.py [--scope full|sample]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import tomllib
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.console import line_buffered_stdout
from src.common.spark import build
from src.gates import gold as verdicts
from src.gates import lineage as L

line_buffered_stdout()

RULE_PATH = C.PROJECT_ROOT / "conf" / "decline_rule.toml"

#: The vector-cohort predicate, frozen in RR-06 and restated here rather than imported.
LONG_TEXT_WORDS = 20

#: What each reconciliation compares, per published table.
SPINE_FIELDS = ("review_count", "rating_sum", "mean_rating", "neg_count", "neg_share",
                "verified_count", "verified_rating_sum", "verified_mean",
                "nonempty_text_count", "long_text_count", "distinct_users")
POINT_FIELDS = ("in_holdout", "baseline_start", "baseline_end", "recent_start", "recent_end",
                "baseline_reviews", "baseline_active_months", "baseline_mean",
                "baseline_neg_share", "baseline_verified_reviews", "baseline_verified_mean",
                "baseline_nonempty_text", "baseline_long_text", "recent_reviews",
                "recent_active_months", "recent_mean", "recent_neg_share",
                "recent_verified_reviews", "recent_verified_mean", "recent_nonempty_text",
                "recent_long_text", "evaluable", "unevaluable_reason", "condition", "drop",
                "alert", "episode_key")
EPISODE_FIELDS = ("alert_triggered_at", "alert_complete_month", "last_supported_at",
                  "episode_closed_at", "closed_by", "in_holdout", "evaluable_points",
                  "supported_points", "baseline_mean_at_alert", "recent_mean_at_alert",
                  "drop_at_alert", "max_drop")

#: The window aggregates the rule sums over, and the unevaluable reasons in precedence order.
WINDOW_FIELDS = ("review_count", "rating_sum", "neg_count", "verified_count",
                 "verified_rating_sum", "nonempty_text_count", "long_text_count")


# --------------------------------------------------------------------- the spec ----
def load_rule_spec(path: Path = RULE_PATH) -> dict[str, Any]:
    """The decline rule as plain values, read from the config and not from the rule module.

    `config_hash` is the one thing here that is copied rather than re-derived: it is an
    *identifier* -- the SHA-256 of the `[rule]` table, as the config file's own comment
    describes it -- and matching it is how the reproduction proves it ran under the same
    frozen protocol as the published run, not a result being independently checked.
    """
    with path.open("rb") as f:
        doc = tomllib.load(f)
    r = doc["rule"]
    if r["status"] not in ("provisional", "frozen"):
        raise ValueError(f"rule.status must be provisional|frozen, got {r['status']!r}")
    if len(str(r["holdout_start"])) != 7:
        raise ValueError("rule.holdout_start must be YYYY-MM")
    return {
        "status": str(r["status"]),
        "holdout_start": str(r["holdout_start"]),
        "baseline_months": int(r["baseline_months"]),
        "recent_months": int(r["recent_months"]),
        "delta": float(r["delta"]),
        "persistence": int(r["persistence"]),
        "max_gap": int(r["max_gap"]),
        "recovery_points": int(r["recovery_points"]),
        "min_reviews": int(r["min_reviews"]),
        "min_active_months": int(r["min_active_months"]),
        "unevaluable_resets_persistence": bool(r["unevaluable_resets_persistence"]),
        "config_hash": hashlib.sha256(json.dumps(r, sort_keys=True).encode()).hexdigest(),
        "metric_tolerance": float(doc.get("reproduction", {}).get("metric_tolerance", 1e-9)),
    }


# ---------------------------------------------------------------------- months ----
def month_ix(month: str) -> int:
    y, m = str(month).split("-")[:2]
    return int(y) * 12 + int(m) - 1


def ix_month(index: int) -> str:
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


# ---------------------------------------------------------------------- the spine ----
def _empty(month: str) -> dict[str, Any]:
    return {"month": month, "review_count": 0, "rating_sum": 0.0, "neg_count": 0,
            "verified_count": 0, "verified_rating_sum": 0.0, "nonempty_text_count": 0,
            "long_text_count": 0, "_users": set()}


def fold(months: dict[str, dict[str, Any]], row) -> None:
    """Add one review to its month's running totals. The only place the aggregates are defined."""
    m = months.get(row["month"])
    if m is None:
        m = months[row["month"]] = _empty(row["month"])
    rating = float(row["rating"])
    verified = bool(row["verified_purchase"])
    words = int(row["text_word_count"] or 0)
    m["review_count"] += 1
    m["rating_sum"] += rating
    m["neg_count"] += 1 if rating <= 2 else 0
    m["verified_count"] += 1 if verified else 0
    m["verified_rating_sum"] += rating if verified else 0.0
    m["nonempty_text_count"] += 1 if words > 0 else 0
    m["long_text_count"] += 1 if words >= LONG_TEXT_WORDS else 0
    m["_users"].add(row["user_id"])


def accumulate(rows) -> dict[str, dict[str, Any]]:
    """One product's reviews folded into per-month totals, keyed by `YYYY-MM`."""
    months: dict[str, dict[str, Any]] = {}
    for row in rows:
        fold(months, row)
    return months


def spine_from_months(months: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Every month from the first review to the last, empty ones zeroed, with the shares."""
    lo, hi = min(map(month_ix, months)), max(map(month_ix, months))
    out = []
    for i in range(lo, hi + 1):
        row = dict(months.get(ix_month(i)) or _empty(ix_month(i)))
        n, v = row["review_count"], row["verified_count"]
        row["distinct_users"] = len(row.pop("_users"))
        # Null, not zero, where there is nothing to average over -- the shape the spine keeps.
        row["mean_rating"] = row["rating_sum"] / n if n else None
        row["neg_share"] = row["neg_count"] / n if n else None
        row["verified_mean"] = row["verified_rating_sum"] / v if v else None
        out.append(row)
    return out


def product_spine(rows) -> list[dict[str, Any]]:
    return spine_from_months(accumulate(rows))


# ------------------------------------------------------------------- the rule ----
def _window(spine: list[dict[str, Any]], lo: int, hi: int) -> dict[str, float]:
    """Direct sums over spine indices lo..hi inclusive, plus the count of active months."""
    part = spine[lo:hi + 1]
    w = {k: float(sum(row[k] for row in part)) for k in WINDOW_FIELDS}
    w["active"] = float(sum(1 for row in part if row["review_count"] > 0))
    return w


def _mean(total: float, n: float) -> float | None:
    return None if n == 0 else total / n


def _unevaluable_reason(b: dict[str, float], r: dict[str, float],
                        rule: dict[str, Any]) -> str | None:
    """The first requirement the point fails, in the precedence the rule states."""
    if b["review_count"] < rule["min_reviews"]:
        return "baseline_reviews"
    if b["active"] < rule["min_active_months"]:
        return "baseline_active_months"
    if r["review_count"] < rule["min_reviews"]:
        return "recent_reviews"
    if r["active"] < rule["min_active_months"]:
        return "recent_active_months"
    return None


def _point(spine, i: int, rule: dict[str, Any]) -> dict[str, Any]:
    B, R = rule["baseline_months"], rule["recent_months"]
    month = spine[i]["month"]
    idx = month_ix(month)
    b, r = _window(spine, i - B + 1, i), _window(spine, i + 1, i + R)
    reason = _unevaluable_reason(b, r, rule)
    evaluable = reason is None
    b_mean, r_mean = _mean(b["rating_sum"], b["review_count"]), _mean(r["rating_sum"], r["review_count"])
    return {
        "point_month": month, "in_holdout": idx >= month_ix(rule["holdout_start"]),
        "baseline_start": ix_month(idx - B + 1), "baseline_end": month,
        "recent_start": ix_month(idx + 1), "recent_end": ix_month(idx + R),
        "baseline_reviews": int(b["review_count"]), "baseline_active_months": int(b["active"]),
        "baseline_mean": b_mean, "baseline_neg_share": _mean(b["neg_count"], b["review_count"]),
        "baseline_verified_reviews": int(b["verified_count"]),
        "baseline_verified_mean": _mean(b["verified_rating_sum"], b["verified_count"]),
        "baseline_nonempty_text": int(b["nonempty_text_count"]),
        "baseline_long_text": int(b["long_text_count"]),
        "recent_reviews": int(r["review_count"]), "recent_active_months": int(r["active"]),
        "recent_mean": r_mean, "recent_neg_share": _mean(r["neg_count"], r["review_count"]),
        "recent_verified_reviews": int(r["verified_count"]),
        "recent_verified_mean": _mean(r["verified_rating_sum"], r["verified_count"]),
        "recent_nonempty_text": int(r["nonempty_text_count"]),
        "recent_long_text": int(r["long_text_count"]),
        "evaluable": evaluable, "unevaluable_reason": reason,
        "condition": (r_mean <= b_mean - rule["delta"]) if evaluable else None,
        "drop": (b_mean - r_mean) if evaluable else None,
        "alert": False, "episode_key": None,
    }


def _closes_at(points: list[dict[str, Any]], alert_i: int, rule: dict[str, Any]) -> tuple[int, str]:
    """Where the episode opened at `alert_i` ends, and which of the three reasons closed it."""
    false_run = gap_run = 0
    for j in range(alert_i + 1, len(points)):
        q = points[j]
        if not q["evaluable"]:
            gap_run += 1
            if gap_run > rule["max_gap"]:
                return j, "gap"
        else:
            gap_run = 0
            if q["condition"]:
                false_run = 0
            else:
                false_run += 1
                if false_run >= rule["recovery_points"]:
                    return j, "recovery"
    return len(points) - 1, "end_of_data"


def _episode(points, start_i: int, alert_i: int, close_i: int, closed_by: str,
             rule: dict[str, Any]) -> dict[str, Any]:
    span = points[start_i:close_i + 1]
    supported = [p for p in span if p["evaluable"] and p["condition"]]
    # `max_drop` is measured from the alert, not from the episode's first supporting point.
    # The two spans differ whenever a pre-alert point in the same run dropped further than
    # anything after it -- see MAX_DROP_SPAN below, which counts that rather than hiding it.
    from_alert = [p for p in points[alert_i:close_i + 1] if p["evaluable"] and p["condition"]]
    a = points[alert_i]
    return {
        "max_drop_from_episode_start": max(p["drop"] for p in supported),
        "condition_started_at": points[start_i]["point_month"],
        "alert_triggered_at": a["point_month"],
        "alert_complete_month": ix_month(month_ix(a["point_month"]) + rule["recent_months"]),
        "last_supported_at": supported[-1]["point_month"],
        "episode_closed_at": points[close_i]["point_month"],
        "closed_by": closed_by,
        "in_holdout": a["in_holdout"],
        "evaluable_points": sum(1 for p in span if p["evaluable"]),
        "supported_points": len(supported),
        "baseline_mean_at_alert": a["baseline_mean"],
        "recent_mean_at_alert": a["recent_mean"],
        "drop_at_alert": a["drop"],
        "max_drop": max(p["drop"] for p in from_alert),
    }


def _episodes(points: list[dict[str, Any]],
              rule: dict[str, Any]) -> list[tuple[dict[str, Any], tuple[int, int]]]:
    """Walk the finished point list; open an episode at the P-th supporting point, then close it.

    A second pass, not the reference's single pass: the closing scan runs forward from the
    alert independently of the run that produced it, and the episode's counts are read back
    off its span rather than accumulated on the way through.
    """
    P = rule["persistence"]
    resets = rule["unevaluable_resets_persistence"]
    out: list[tuple[dict[str, Any], tuple[int, int]]] = []
    run_len, start_i, i = 0, None, 0
    while i < len(points):
        p = points[i]
        if not p["evaluable"]:
            if resets:
                run_len, start_i = 0, None
        elif p["condition"]:
            run_len += 1
            start_i = i if start_i is None else start_i
            if run_len >= P:
                close_i, closed_by = _closes_at(points, i, rule)
                p["alert"] = True
                out.append((_episode(points, start_i, i, close_i, closed_by, rule),
                            (start_i, close_i)))
                # The closing point is spent: the reference was inside the episode when it
                # read it, so it can never also start the next run.
                run_len, start_i, i = 0, None, close_i + 1
                continue
        else:
            run_len, start_i = 0, None
        i += 1
    return out


def evaluate_product(spine: list[dict[str, Any]],
                     rule: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(evaluation points, episodes) for one product, from a complete sorted spine."""
    B, R = rule["baseline_months"], rule["recent_months"]
    frozen = rule["status"] == "frozen"
    holdout = month_ix(rule["holdout_start"])
    points: list[dict[str, Any]] = []
    for i in range(B - 1, len(spine) - R):
        # Before the freeze the holdout is not evaluated at all, so the spine stops here.
        if month_ix(spine[i]["month"]) >= holdout and not frozen:
            break
        points.append(_point(spine, i, rule))
    episodes = []
    for ep, (lo, hi) in _episodes(points, rule):
        for p in points[lo:hi + 1]:
            p["episode_key"] = ep["condition_started_at"]
        episodes.append(ep)
    return points, episodes


# ------------------------------------------------------------------ derivation ----
@dataclass(frozen=True)
class Derived:
    """The whole of gold, re-derived: the three tables keyed as the reconciliation reads them."""
    products_total: int
    products_materialised: int
    spine: dict[tuple[str, str], dict[str, Any]]
    points: dict[tuple[str, str], dict[str, Any]]
    episodes: dict[str, dict[str, Any]]


def derive(rows, rule: dict[str, Any]) -> Derived:
    """Fold silver's surviving reviews into the three gold tables, product by product."""
    monthly: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    totals: dict[str, int] = defaultdict(int)
    for row in rows:
        asin = row["parent_asin"]
        totals[asin] += 1
        fold(monthly[asin], row)

    spine_out: dict[tuple[str, str], dict[str, Any]] = {}
    points_out: dict[tuple[str, str], dict[str, Any]] = {}
    episodes_out: dict[str, dict[str, Any]] = {}
    materialised = 0
    for asin, months in monthly.items():
        # A product that can never fill a window is never materialised (ADR-0001).
        if totals[asin] < rule["min_reviews"]:
            continue
        materialised += 1
        spine = spine_from_months(months)
        for row in spine:
            spine_out[(asin, row["month"])] = row
        points, episodes = evaluate_product(spine, rule)
        for p in points:
            points_out[(asin, p["point_month"])] = p
        for ep in episodes:
            episodes_out[f"{asin}:{ep['condition_started_at']}"] = ep
    return Derived(products_total=len(totals), products_materialised=materialised,
                   spine=spine_out, points=points_out, episodes=episodes_out)


# -------------------------------------------------------------- reconciliation ----
def _norm(v: Any) -> Any:
    """Pandas nulls and NaN become None, so a missing value never compares equal to a number."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if v is pd.NaT or (not isinstance(v, (str, bytes)) and pd.api.types.is_scalar(v)
                       and pd.isna(v)):
        return None
    return v


def same(a: Any, b: Any, tol: float = 0.0) -> bool:
    """Equal, with floats allowed the configured absolute slack. Null matches only null."""
    a, b = _norm(a), _norm(b)
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b)
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) <= tol
    return a == b


@dataclass(frozen=True)
class Comparison:
    """One table reconciled: which keys are one-sided, and which fields disagree on the rest."""
    name: str
    compared: int
    n_only_mine: int
    n_only_theirs: int
    n_fields: int
    mismatched: tuple[tuple[str, int], ...]
    examples: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.n_only_mine == 0 and self.n_only_theirs == 0 and not self.mismatched

    @property
    def line(self) -> str:
        bad = ",".join(f"{f}:{n}" for f, n in self.mismatched) or "none"
        return (f"GOLD_REPRO compare={self.name} compared={self.compared} "
                f"only_mine={self.n_only_mine} only_theirs={self.n_only_theirs} "
                f"fields={self.n_fields} mismatched={bad}")


def reconcile(name: str, mine: dict, theirs: dict, fields, *, tol: float = 0.0,
              examples: int = 3) -> Comparison:
    """Compare two keyed tables: key sets first, then every field on the keys they share."""
    keys_mine, keys_theirs = set(mine), set(theirs)
    shared = keys_mine & keys_theirs
    counts: dict[str, int] = {}
    shown: list[str] = []
    for key in sorted(shared):
        m, t = mine[key], theirs[key]
        for f in fields:
            if not same(m.get(f), t.get(f), tol):
                counts[f] = counts.get(f, 0) + 1
                if len(shown) < examples:
                    shown.append(f"{name}[{key}].{f}: mine={m.get(f)!r} theirs={t.get(f)!r}")
    return Comparison(name=name, compared=len(shared),
                      n_only_mine=len(keys_mine - keys_theirs),
                      n_only_theirs=len(keys_theirs - keys_mine),
                      n_fields=len(tuple(fields)),
                      mismatched=tuple((f, counts[f]) for f in fields if f in counts),
                      examples=tuple(shown))


def max_drop_span_line(episodes: dict[str, dict[str, Any]], tol: float) -> str:
    """How many episodes would carry a different `max_drop` under the documented span.

    Reported, never blocking: it measures gold's spec against gold's code, and the
    reproduction has no standing to call either one wrong.
    """
    differing = sum(1 for e in episodes.values()
                    if not same(e["max_drop"], e["max_drop_from_episode_start"], tol))
    return (f"GOLD_REPRO MAX_DROP_SPAN episodes={len(episodes)} differing={differing} "
            f"verdict=REPORTED")


def comparison_checks(c: Comparison, *, require_non_empty: bool) -> list[tuple[str, bool]]:
    """A reconciliation's named constituents.

    `require_non_empty` guards the vacuous pass: a spine or a point set that compared nothing
    reproduced nothing, and must never read PASS. Episodes are exempt -- a product population
    with no decline is a legitimate result, not an empty comparison.
    """
    checks = [(f"{c.name}_keys", c.n_only_mine == 0 and c.n_only_theirs == 0),
              (f"{c.name}_fields", not c.mismatched)]
    if require_non_empty:
        checks.append((f"{c.name}_non_empty", c.compared > 0))
    return checks


# ------------------------------------------------------------------ spark reads ----
def read_silver(spark, src: dict[str, Any]) -> list[dict[str, Any]]:
    df = (spark.read.option("snapshot-id", src["snapshot_id"]).table(src["table"])
          .select("parent_asin", "review_month", "rating", "verified_purchase",
                  "text_word_count", "user_id"))
    pdf = df.toPandas()
    pdf["month"] = pd.to_datetime(pdf["review_month"]).dt.strftime("%Y-%m")
    return [{"parent_asin": t.parent_asin, "month": t.month, "rating": t.rating,
             "verified_purchase": t.verified_purchase, "text_word_count": t.text_word_count,
             "user_id": t.user_id}
            for t in pdf.itertuples(index=False)]


def _keyed(pdf: pd.DataFrame, key_cols: list[str], fields) -> dict:
    out = {}
    for rec in pdf.to_dict("records"):
        key = tuple(rec[c] for c in key_cols)
        out[key[0] if len(key) == 1 else key] = {f: _norm(rec.get(f)) for f in fields}
    return out


def read_published(spark, outputs: dict[str, Any]) -> tuple[dict, dict, dict]:
    def t(name: str, cols: list[str]):
        o = outputs[name]
        return (spark.read.option("snapshot-id", o["snapshot_id"]).table(o["table"])
                .select(*cols).toPandas())
    spine = _keyed(t("gold.product_month", ["parent_asin", "month", *SPINE_FIELDS]),
                   ["parent_asin", "month"], SPINE_FIELDS)
    points = _keyed(t("gold.evaluation_points", ["parent_asin", "point_month", *POINT_FIELDS]),
                    ["parent_asin", "point_month"], POINT_FIELDS)
    episodes = _keyed(t("gold.decline_episodes", ["episode_id", *EPISODE_FIELDS]),
                      ["episode_id"], EPISODE_FIELDS)
    return spine, points, episodes


# ------------------------------------------------------------------------ main ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()

    rule = load_rule_spec()
    row = runs.latest_success("gold", category=args.category, data_scope=args.scope)
    if row is None:
        sys.exit(f"no successful gold run for {args.category}/{args.scope}: GOLD_REPRO_GATE=FAIL")
    src = row["inputs"]["silver"]
    tol = rule["metric_tolerance"]

    spark = build("reproduce-gold", cores="local[4]", driver_memory="4g")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    try:
        reviews = read_silver(spark, src)
        mine = derive(reviews, rule)
        del reviews
        their_spine, their_points, their_episodes = read_published(spark, row["outputs"])
    finally:
        spark.stop()

    their_hash = str((row["inputs"].get("rule") or {}).get("config_hash") or "")
    # Every line the gate prints is a constituent of the artefact, in the order printed, so
    # a row in the table traces back to the exact output that produced it.
    lines = [
        (f"GOLD_REPRO run_id={row['run_id']} silver_run_id={src['run_id']} "
         f"silver_snapshot={src['snapshot_id']} rule_status={rule['status']} "
         f"rule_config_hash_mine={rule['config_hash'][:12]} "
         f"rule_config_hash_run={their_hash[:12] or 'none'} tolerance={tol:g}"),
        (f"GOLD_REPRO products total_mine={mine.products_total} "
         f"total_run={row['counts']['products_total']} "
         f"materialised_mine={mine.products_materialised} "
         f"materialised_run={row['counts']['products_materialised']}"),
    ]

    comparisons = [
        (reconcile("product_month", mine.spine, their_spine, SPINE_FIELDS, tol=tol), True),
        (reconcile("evaluation_points", mine.points, their_points, POINT_FIELDS, tol=tol), True),
        (reconcile("decline_episodes", mine.episodes, their_episodes, EPISODE_FIELDS, tol=tol),
         False),
    ]
    checks = [
        ("rule_config_hash", rule["config_hash"] == their_hash),
        ("products_materialised",
         mine.products_total == row["counts"]["products_total"]
         and mine.products_materialised == row["counts"]["products_materialised"]),
    ]
    for c, require_non_empty in comparisons:
        lines.append(c.line)
        checks += comparison_checks(c, require_non_empty=require_non_empty)
        for ex in c.examples:
            lines.append(f"GOLD_REPRO_DIFF {ex}")
    lines.append(max_drop_span_line(mine.episodes, tol))

    v = L.attest(verdicts.repro(scope=args.scope, checks=checks, constituents=lines),
                 "gold_repro")
    v.emit()

    E.record(v, capability="gold_repro", phase="P3 Gold", kind="reproducibility",
             protocol_hash=rule["config_hash"],
             population={"name": f"{args.category}/gold.product_month re-derived from "
                                 f"{src['table']}",
                         "n": len(mine.spine),
                         "silver_snapshot_id": src["snapshot_id"],
                         "points_compared": comparisons[1][0].compared,
                         "episodes_compared": comparisons[2][0].compared},
             pipeline_run_id=row["run_id"], scope=args.scope,
             notes=[("re-derived in plain Python from the pinned silver snapshot, importing "
                     "nothing from the gold job or the rule module (ADR-0001)"),
                    ("persistence_run is not compared: it is the reference's loop counter, "
                     "not a property of an evaluation point"),
                    ("max_drop is reconciled over the published span (alert to close); the "
                     "MAX_DROP_SPAN line counts where the documented span would differ")])
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

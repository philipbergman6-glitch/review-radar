"""The decline rule as a pure function over one product's calendar spine (ADR-0001, RR-09).

A *calendar spine* is every month from a product's first to its last review, empty months
included (`review_count = 0`, null statistics). At each month `t` (an *evaluation point*)
the baseline window is the B months ending at `t` and the recent window is the R months
after it. A point is *evaluable* only when both windows hold at least `min_reviews` reviews
over at least `min_active_months` active months. The condition is
`recent_mean <= baseline_mean - delta`; an *alert* fires at the P-th consecutive evaluable
point with the condition true. From the first point of that run an *episode* is open until
K consecutive evaluable false points (`recovery`), more than G consecutive unevaluable
points (`gap`), or the end of the spine (`end_of_data`).

No Spark and no pandas here: `evaluate(spine, rule)` takes plain rows and returns plain
rows, so the same function runs inside Spark (`applyInPandas`, src/spark/gold.py), in the
streaming `foreachBatch` (P8) and in tests. The independent pandas reproduction must
reimplement it from this docstring and the config, never import it.

Holdout guard: while `rule.status != "frozen"`, points on or after `holdout_start` are not
evaluated at all -- the pre-2020 development period is the only thing the code will show.
"""
from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

RULE_PATH = PROJECT_ROOT / "conf" / "decline_rule.toml"

SPINE_FIELDS = ("month", "review_count", "rating_sum", "neg_count", "verified_count",
                "verified_rating_sum", "nonempty_text_count", "long_text_count")
UNEVALUABLE_REASONS = ("baseline_reviews", "baseline_active_months",
                       "recent_reviews", "recent_active_months")
CLOSED_BY = ("recovery", "gap", "end_of_data")


@dataclass(frozen=True)
class Rule:
    status: str
    holdout_start: str
    baseline_months: int
    recent_months: int
    delta: float
    persistence: int
    max_gap: int
    recovery_points: int
    min_reviews: int
    min_active_months: int
    unevaluable_resets_persistence: bool
    config_hash: str
    metric_tolerance: float

    @property
    def frozen(self) -> bool:
        return self.status == "frozen"

    def as_params(self) -> dict[str, Any]:
        return asdict(self)


def load_rule(path: Path = RULE_PATH) -> Rule:
    with path.open("rb") as f:
        doc = tomllib.load(f)
    r = doc["rule"]
    if r["status"] not in ("provisional", "frozen"):
        raise ValueError(f"rule.status must be provisional|frozen, got {r['status']!r}")
    for k in ("baseline_months", "recent_months", "persistence", "max_gap", "recovery_points",
              "min_reviews", "min_active_months"):
        if int(r[k]) < 1 and k != "max_gap":
            raise ValueError(f"rule.{k} must be >= 1")
    if len(r["holdout_start"]) != 7:
        raise ValueError("rule.holdout_start must be YYYY-MM")
    config_hash = hashlib.sha256(json.dumps(r, sort_keys=True).encode()).hexdigest()
    return Rule(
        status=r["status"], holdout_start=r["holdout_start"],
        baseline_months=int(r["baseline_months"]), recent_months=int(r["recent_months"]),
        delta=float(r["delta"]), persistence=int(r["persistence"]), max_gap=int(r["max_gap"]),
        recovery_points=int(r["recovery_points"]), min_reviews=int(r["min_reviews"]),
        min_active_months=int(r["min_active_months"]),
        unevaluable_resets_persistence=bool(r["unevaluable_resets_persistence"]),
        config_hash=config_hash,
        metric_tolerance=float(doc.get("reproduction", {}).get("metric_tolerance", 1e-9)))


# ------------------------------------------------------------------ months ----
def month_index(month: str) -> int:
    y, m = month.split("-")
    return int(y) * 12 + int(m) - 1


def month_str(index: int) -> str:
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def spine_months(first: str, last: str) -> list[str]:
    return [month_str(i) for i in range(month_index(first), month_index(last) + 1)]


# ------------------------------------------------------------- the function ----
def _window(pref: dict[str, list[float]], lo: int, hi: int) -> dict[str, float]:
    """Sums over spine indices lo..hi inclusive, from prefix sums."""
    return {k: p[hi + 1] - p[lo] for k, p in pref.items()}


def _mean(total: float, n: float) -> float | None:
    return None if n == 0 else total / n


def evaluate(spine: list[dict[str, Any]], rule: Rule) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """(evaluation points, episodes) for one product. `spine` must be complete and sorted."""
    n = len(spine)
    for i in range(1, n):
        if month_index(spine[i]["month"]) != month_index(spine[i - 1]["month"]) + 1:
            raise ValueError(f"spine is not a complete calendar sequence at {spine[i]['month']}")
    keys = ("review_count", "rating_sum", "neg_count", "verified_count", "verified_rating_sum",
            "nonempty_text_count", "long_text_count", "active")
    pref: dict[str, list[float]] = {k: [0.0] for k in keys}
    for row in spine:
        for k in keys:
            v = 1.0 if k == "active" and row["review_count"] > 0 else (0.0 if k == "active" else float(row[k] or 0))
            pref[k].append(pref[k][-1] + v)

    B, R = rule.baseline_months, rule.recent_months
    holdout_idx = month_index(rule.holdout_start)
    points: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    run_len, run_start = 0, None
    ep: dict[str, Any] | None = None
    false_count = gap_count = 0

    def close(reason: str, month: str) -> None:
        nonlocal ep, run_len, run_start
        assert ep is not None
        ep["episode_closed_at"] = month
        ep["closed_by"] = reason
        episodes.append(ep)
        ep = None
        run_len, run_start = 0, None

    for i in range(B - 1, n - R):
        month = spine[i]["month"]
        idx = month_index(month)
        in_holdout = idx >= holdout_idx
        if in_holdout and not rule.frozen:
            break
        b, r = _window(pref, i - B + 1, i), _window(pref, i + 1, i + R)
        reason = None
        if b["review_count"] < rule.min_reviews:
            reason = "baseline_reviews"
        elif b["active"] < rule.min_active_months:
            reason = "baseline_active_months"
        elif r["review_count"] < rule.min_reviews:
            reason = "recent_reviews"
        elif r["active"] < rule.min_active_months:
            reason = "recent_active_months"
        evaluable = reason is None
        b_mean, r_mean = _mean(b["rating_sum"], b["review_count"]), _mean(r["rating_sum"], r["review_count"])
        drop = (b_mean - r_mean) if evaluable else None
        cond = (r_mean <= b_mean - rule.delta) if evaluable else None

        # ---- persistence / episode state machine ----
        alert = False
        if ep is None:
            if not evaluable:
                if rule.unevaluable_resets_persistence:
                    run_len, run_start = 0, None
            elif cond:
                run_len += 1
                run_start = run_start or month
                if run_len >= rule.persistence:
                    alert = True
                    ep = {"condition_started_at": run_start, "alert_triggered_at": month,
                          "alert_complete_month": month_str(idx + R),
                          "last_supported_at": month, "episode_closed_at": None, "closed_by": None,
                          "evaluable_points": run_len, "supported_points": run_len,
                          "baseline_mean_at_alert": b_mean, "recent_mean_at_alert": r_mean,
                          "drop_at_alert": drop, "max_drop": drop,
                          "in_holdout": in_holdout}
                    false_count = gap_count = 0
                    for p in points[-(run_len - 1):] if run_len > 1 else []:
                        p["episode_key"] = run_start
            else:
                run_len, run_start = 0, None
        else:
            if not evaluable:
                gap_count += 1
                if gap_count > rule.max_gap:
                    close("gap", month)
            else:
                gap_count = 0
                ep["evaluable_points"] += 1
                if cond:
                    false_count = 0
                    ep["supported_points"] += 1
                    ep["last_supported_at"] = month
                    ep["max_drop"] = max(ep["max_drop"], drop)
                else:
                    false_count += 1
                    if false_count >= rule.recovery_points:
                        close("recovery", month)

        points.append({
            "point_month": month, "in_holdout": in_holdout,
            "baseline_start": month_str(idx - B + 1), "baseline_end": month,
            "recent_start": month_str(idx + 1), "recent_end": month_str(idx + R),
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
            "condition": cond, "drop": drop,
            "persistence_run": run_len if ep is None else None,
            "alert": alert,
            "episode_key": (ep["condition_started_at"] if ep is not None
                            else (episodes[-1]["condition_started_at"]
                                  if episodes and episodes[-1]["episode_closed_at"] == month else None)),
        })
    if ep is not None:
        close("end_of_data", points[-1]["point_month"])
    return points, episodes

"""Matched controls and the seeded discovery draw, as pure functions (ADR-0003, RR-08).

A *matched control* (CONTEXT.md) is a product observed over the same calendar windows as a
decline candidate, similar in baseline rating and volume, and stable under the locked
decline rule. Nothing here touches Spark or Iceberg: `match_controls` takes plain rows and
returns plain rows, so the same code runs in the job (src/spark/theme_samples.py) and in
tests, and the ranking is fully determined by the data, never by partitioning.

*Text-characterisable* is checked here too: a candidate whose baseline and recent windows
each hold enough non-empty review text (and a sufficient long-text subset) to be narratable.
A candidate that fails it is dropped with a stated reason, never silently.
"""
from __future__ import annotations

import hashlib
import json
import math
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.common.config import PROJECT_ROOT

SAMPLING_PATH = PROJECT_ROOT / "conf" / "theme_sampling.toml"

DROP_REASONS = ("not_text_characterisable", "no_matching_control")


@dataclass(frozen=True)
class Matching:
    controls_per_candidate: int
    max_baseline_mean_diff: float
    min_volume_ratio: float
    max_volume_ratio: float
    require_evaluable: bool
    require_condition_false: bool
    min_nonempty_text_per_window: int
    min_long_text_per_window: int


@dataclass(frozen=True)
class Discovery:
    size: int
    low_rated_share: float
    low_rated_max_stars: int
    max_per_product: int
    min_text_words: int
    salt: str


@dataclass(frozen=True)
class SamplingProtocol:
    status: str
    holdout_start: str
    seed: int
    matching: Matching
    discovery: Discovery
    config_hash: str
    raw: dict[str, Any]

    @property
    def frozen(self) -> bool:
        return self.status == "frozen"


def load_protocol(path: Path = SAMPLING_PATH) -> SamplingProtocol:
    with path.open("rb") as f:
        doc = tomllib.load(f)
    p, m, d = doc["protocol"], doc["matching"], doc["discovery"]
    if p["status"] not in ("provisional", "frozen"):
        raise ValueError(f"protocol.status must be provisional|frozen, got {p['status']!r}")
    if len(p["holdout_start"]) != 7:
        raise ValueError("protocol.holdout_start must be YYYY-MM")
    if not 0.0 < d["low_rated_share"] < 1.0:
        raise ValueError("discovery.low_rated_share must be strictly between 0 and 1")
    if m["min_volume_ratio"] <= 0 or m["max_volume_ratio"] < m["min_volume_ratio"]:
        raise ValueError("matching volume ratios must satisfy 0 < min <= max")
    if int(m["controls_per_candidate"]) < 1:
        raise ValueError("matching.controls_per_candidate must be >= 1")
    config_hash = hashlib.sha256(
        json.dumps({"matching": m, "discovery": d, "protocol": p}, sort_keys=True).encode()).hexdigest()
    return SamplingProtocol(
        status=p["status"], holdout_start=p["holdout_start"], seed=int(p["seed"]),
        matching=Matching(
            controls_per_candidate=int(m["controls_per_candidate"]),
            max_baseline_mean_diff=float(m["max_baseline_mean_diff"]),
            min_volume_ratio=float(m["min_volume_ratio"]), max_volume_ratio=float(m["max_volume_ratio"]),
            require_evaluable=bool(m["require_evaluable"]),
            require_condition_false=bool(m["require_condition_false"]),
            min_nonempty_text_per_window=int(m["min_nonempty_text_per_window"]),
            min_long_text_per_window=int(m["min_long_text_per_window"])),
        discovery=Discovery(
            size=int(d["size"]), low_rated_share=float(d["low_rated_share"]),
            low_rated_max_stars=int(d["low_rated_max_stars"]), max_per_product=int(d["max_per_product"]),
            min_text_words=int(d["min_text_words"]), salt=str(d["salt"])),
        config_hash=config_hash, raw=doc)


# ------------------------------------------------------------- text characterisable ----
def text_characterisable(point: dict[str, Any], m: Matching) -> bool:
    """Both windows of this evaluation point carry enough text to be labelled."""
    return (int(point["baseline_nonempty_text"] or 0) >= m.min_nonempty_text_per_window
            and int(point["recent_nonempty_text"] or 0) >= m.min_nonempty_text_per_window
            and int(point["baseline_long_text"] or 0) >= m.min_long_text_per_window
            and int(point["recent_long_text"] or 0) >= m.min_long_text_per_window)


# -------------------------------------------------------------------- matching ----
def match_controls(candidate: dict[str, Any], pool: list[dict[str, Any]], m: Matching,
                   ) -> list[dict[str, Any]]:
    """Rank the eligible controls for one candidate point; return at most `controls_per_candidate`.

    `candidate` and every row of `pool` are evaluation-point rows at the *same* point_month.
    Pool rows must already exclude products that alert anywhere on their spine -- that is a
    whole-spine property the caller knows and this function cannot see.
    """
    c_mean, c_vol = float(candidate["baseline_mean"]), int(candidate["baseline_reviews"])
    if c_vol <= 0:
        raise ValueError(f"candidate {candidate['parent_asin']} has baseline_reviews {c_vol}")
    ranked: list[tuple[float, float, str, dict[str, Any]]] = []
    for q in pool:
        if q["parent_asin"] == candidate["parent_asin"]:
            continue
        if q["point_month"] != candidate["point_month"]:
            raise ValueError("pool rows must share the candidate's point_month")
        if m.require_evaluable and not q["evaluable"]:
            continue
        if m.require_condition_false and q["condition"]:
            continue
        if not text_characterisable(q, m):
            continue
        q_mean, q_vol = float(q["baseline_mean"]), int(q["baseline_reviews"])
        if q_vol <= 0:
            continue
        mean_diff = abs(q_mean - c_mean)
        if mean_diff > m.max_baseline_mean_diff:
            continue
        ratio = q_vol / c_vol
        if not (m.min_volume_ratio <= ratio <= m.max_volume_ratio):
            continue
        ranked.append((round(mean_diff, 12), round(abs(math.log2(ratio)), 12), q["parent_asin"], q))
    ranked.sort(key=lambda r: (r[0], r[1], r[2]))
    out = []
    for rank, (mean_diff, log_ratio, asin, q) in enumerate(ranked[:m.controls_per_candidate], start=1):
        out.append({"control_asin": asin, "control_rank": rank,
                    "baseline_mean_diff": mean_diff, "abs_log2_volume_ratio": log_ratio,
                    "control_baseline_mean": float(q["baseline_mean"]),
                    "control_baseline_reviews": int(q["baseline_reviews"]),
                    "control_recent_mean": float(q["recent_mean"]),
                    "control_recent_reviews": int(q["recent_reviews"])})
    return out


# ------------------------------------------------------------- deterministic draw ----
def draw_key(review_id: str, seed: int, salt: str) -> float:
    """A stable pseudo-random number in [0, 1) for one review under one named draw."""
    h = hashlib.sha256(f"{seed}\x1f{salt}\x1f{review_id}".encode()).digest()
    return int.from_bytes(h[:8], "big") / 2 ** 64


def draw_discovery(rows: list[dict[str, Any]], d: Discovery, seed: int) -> list[dict[str, Any]]:
    """Seeded stratified draw: `low_rated_share` of the rows rated <= `low_rated_max_stars`.

    `rows` are candidate/control window reviews that already pass `min_text_words`. Order is
    by draw key, so the sample is a function of (seed, salt, review ids) alone. Per-product
    capping is applied inside each stratum. A stratum that cannot fill its quota is reported
    by returning fewer rows; the caller hard-fails or records the shortfall.
    """
    low_target = round(d.size * d.low_rated_share)
    high_target = d.size - low_target
    picked: list[dict[str, Any]] = []
    for stratum, target in (("low", low_target), ("high", high_target)):
        pool = [r for r in rows
                if (int(r["rating"]) <= d.low_rated_max_stars) == (stratum == "low")]
        pool.sort(key=lambda r: (draw_key(r["review_id"], seed, d.salt), r["review_id"]))
        per_product: dict[str, int] = {}
        taken = 0
        for r in pool:
            if taken >= target:
                break
            asin = r["parent_asin"]
            if per_product.get(asin, 0) >= d.max_per_product:
                continue
            per_product[asin] = per_product.get(asin, 0) + 1
            picked.append({**r, "stratum": stratum})
            taken += 1
    return picked

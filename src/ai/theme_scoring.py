"""Scoring a theme labeller against the frozen reference labels (ADR-0003, RR-21).

Pure functions, no Spark: the same code scores in the job, in a test and in the gate, and the
numbers are a function of the labels alone.

Two decisions are baked in here because they change what the headline number means:

* **A `parse_failed` or `api_failed` row scores as an empty prediction, not as an exclusion.**
  Dropping the rows the model could not answer for would measure "how good is the model when
  it answers", which is not what a pipeline consuming these labels experiences. Every
  reference positive on such a review is a false negative, and the failure coverage is
  reported beside the score so the two can be read together.
* **A supported theme has at least `min_support` positives in the reference labels of the
  evaluation set** (ADR-0003: ten in the audit set). Macro-F1 averages over supported themes
  only, so a theme with three positives cannot swing the headline by a third of a point.
  Unsupported themes are still printed with their counts.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ThemeScore:
    theme_id: str
    tp: int
    fp: int
    fn: int
    support: int          # reference positives
    predicted: int        # system positives
    supported: bool

    @property
    def precision(self) -> float | None:
        return self.tp / (self.tp + self.fp) if self.tp + self.fp else None

    @property
    def recall(self) -> float | None:
        return self.tp / self.support if self.support else None

    @property
    def f1(self) -> float | None:
        p, r = self.precision, self.recall
        if p is None or r is None or p + r == 0:
            return 0.0 if self.support else None
        return 2 * p * r / (p + r)


def score_themes(reference: dict[str, set[str]], system: dict[str, set[str]],
                 theme_ids: list[str], *, min_support: int) -> list[ThemeScore]:
    """Per-theme confusion over the reviews present in `reference`.

    `system` may be missing a review (a terminal failure produced no labels); it is then
    treated as an empty prediction, never skipped.
    """
    out: list[ThemeScore] = []
    for theme in theme_ids:
        tp = fp = fn = support = predicted = 0
        for review_id, ref in reference.items():
            sys_labels = system.get(review_id, set())
            in_ref, in_sys = theme in ref, theme in sys_labels
            support += in_ref
            predicted += in_sys
            tp += in_ref and in_sys
            fp += in_sys and not in_ref
            fn += in_ref and not in_sys
        out.append(ThemeScore(theme, tp, fp, fn, support, predicted, support >= min_support))
    return out


def macro_f1(scores: list[ThemeScore]) -> float | None:
    """Unweighted mean F1 over supported themes. None when nothing is supported."""
    f1s = [s.f1 for s in scores if s.supported and s.f1 is not None]
    return sum(f1s) / len(f1s) if f1s else None


def _draw_key(seed: int, salt: str, key: str) -> float:
    h = hashlib.sha256(f"{seed}\x1f{salt}\x1f{key}".encode()).digest()
    return int.from_bytes(h[:8], "big") / 2 ** 64


def bootstrap_macro_f1(reference: dict[str, set[str]], system: dict[str, set[str]],
                       theme_ids: list[str], product_of: dict[str, str], *, min_support: int,
                       seed: int, draws: int = 1000, alpha: float = 0.05,
                       ) -> tuple[float | None, float | None]:
    """A product-clustered percentile interval, seeded, reported as context not as a gate.

    Reviews of one product are not independent -- they share a formulation, a listing and a
    manufacturing batch -- so the resampling unit is the product, not the review. The interval
    is context for the point estimate: ADR-0003's pass rule is on the point estimate, and
    nothing here may re-decide it.
    """
    products = sorted({product_of[r] for r in reference if r in product_of})
    if not products or draws <= 0:
        return (None, None)
    by_product: dict[str, list[str]] = {}
    for review_id in sorted(reference):
        by_product.setdefault(product_of.get(review_id, review_id), []).append(review_id)
    n = len(products)
    values: list[float] = []
    for d in range(draws):
        picked: dict[str, set[str]] = {}
        sys_picked: dict[str, set[str]] = {}
        for i in range(n):
            # A seeded index into the product list: reproducible without a global RNG.
            idx = int(_draw_key(seed, f"boot{d}", str(i)) * n)
            for j, review_id in enumerate(by_product[products[min(idx, n - 1)]]):
                alias = f"{d}:{i}:{j}"
                picked[alias] = reference[review_id]
                sys_picked[alias] = system.get(review_id, set())
        m = macro_f1(score_themes(picked, sys_picked, theme_ids, min_support=min_support))
        if m is not None:
            values.append(m)
    if not values:
        return (None, None)
    values.sort()
    lo = values[int(alpha / 2 * (len(values) - 1))]
    hi = values[int((1 - alpha / 2) * (len(values) - 1))]
    return (lo, hi)


def failure_coverage(statuses: dict[str, str], selected: list[str]) -> dict[str, Any]:
    """How many of the evaluated reviews the system could not answer for, by named status."""
    tally = {"succeeded": 0, "model_abstained": 0, "parse_failed": 0, "api_failed": 0, "absent": 0}
    for review_id in selected:
        tally[statuses.get(review_id, "absent")] += 1
    n = max(len(selected), 1)
    tally["failure_rate"] = round((tally["parse_failed"] + tally["api_failed"] + tally["absent"]) / n, 4)
    tally["abstention_rate"] = round(tally["model_abstained"] / n, 4)
    return tally


def system_report(*, subset: str, reference: dict[str, set[str]], system: dict[str, set[str]],
                  theme_ids: list[str], product_of: dict[str, str], min_support: int, seed: int,
                  draws: int, statuses: dict[str, str] | None = None,
                  reference_other: dict[str, bool] | None = None,
                  system_other: dict[str, bool | None] | None = None) -> dict[str, Any]:
    """One system's numbers on one set of reviews, in the shape every reader expects.

    Three systems are compared on the audit set (ADR-0002) and a fourth reader -- the themes
    gate -- reads whichever artefact is published. They must be the *same* shape: a gate that
    reaches for `overall.min_supported_recall` cannot tell a differently-built dict from a
    system that scored badly. So the shape is written once, here, rather than assembled at
    each writer.

    `statuses` is absent for a system that cannot fail to answer -- the star-only floor is a
    function of a rating it always has -- and every review then counts as `succeeded`. That is
    a claim about the system, not a default: a labeller with no statuses would be silently
    credited with perfect coverage, so the callers that have them always pass them.

    `reference_other` / `system_other` are likewise absent for a system that does not predict
    `other` at all, and the key is then omitted rather than reported as zero agreement.
    """
    ids = sorted(reference)
    scores = score_themes(reference, system, theme_ids, min_support=min_support)
    lo, hi = bootstrap_macro_f1(reference, system, theme_ids, product_of,
                                min_support=min_support, seed=seed, draws=draws)
    out: dict[str, Any] = {
        "subset": subset, "reviews": len(ids), "macro_f1": macro_f1(scores),
        "bootstrap_95": [lo, hi],
        "supported_themes": [s.theme_id for s in scores if s.supported],
        "min_supported_recall": min([s.recall for s in scores
                                     if s.supported and s.recall is not None], default=None),
        "per_theme": [{"theme_id": s.theme_id, "support": s.support, "predicted": s.predicted,
                       "tp": s.tp, "fp": s.fp, "fn": s.fn, "precision": s.precision,
                       "recall": s.recall, "f1": s.f1, "supported": s.supported}
                      for s in scores],
    }
    if reference_other is not None:
        out["other"] = other_agreement(reference_other, system_other or {}, ids)
    out["coverage"] = failure_coverage(statuses if statuses is not None
                                       else dict.fromkeys(ids, "succeeded"), ids)
    return out


def other_agreement(reference_other: dict[str, bool], system_other: dict[str, bool | None],
                    selected: list[str]) -> dict[str, Any]:
    """`other` is a boolean per review; a row the system failed on counts as False, not absent."""
    tp = fp = fn = tn = 0
    for review_id in selected:
        ref = bool(reference_other.get(review_id))
        sys_ = bool(system_other.get(review_id))
        tp += ref and sys_
        fp += sys_ and not ref
        fn += ref and not sys_
        tn += not ref and not sys_
    total = max(tp + fp + fn + tn, 1)
    return {"tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "agreement": round((tp + tn) / total, 4),
            "reference_present": tp + fn, "system_present": tp + fp}

"""Selecting the prompt that gets frozen (docs/decisions/theme-prompt-freeze.md, ADR-0003).

The rule lives here as pure functions over already-computed scores, so it can be read, tested
and re-run without Spark, a model or the labels. It was committed before the numbers existed;
`tests/test_prompt_selection.py` is what keeps it from drifting toward an outcome afterwards.

Nothing here computes a score. `scripts/score_themes.py` and `scripts/baseline_star_only.py`
write the artefacts; this module only compares them, and it refuses to compare artefacts that
were not produced under the same settings rather than quietly ranking incomparable numbers.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Rule 5(a): macro-F1 differences below this are not a difference. 200 rows and a bootstrap
# interval of roughly +/- 0.06 cannot resolve a hundredth; the tie-breaks decide instead.
TIE_EPSILON = 0.01

# Rule 2: every setting that moves a macro-F1. Two artefacts disagreeing on any of these are
# not measuring the same thing, and comparing them is the mistake this list exists to prevent.
COMPARABLE_FIELDS = ("reviews", "min_support", "seed", "bootstrap_draws", "taxonomy_hash",
                     "reference_source")

_VERSION_ORDINAL = re.compile(r"(\d+)\s*$")


@dataclass(frozen=True)
class Candidate:
    """One scored system, reduced to what the selection rule is allowed to look at."""

    name: str                              # the spec's prompt key, e.g. label_v5
    version: str                           # the frozen version string, e.g. label-v5
    macro_f1: float | None
    interval: tuple[float | None, float | None]
    failure_rate: float
    reviews: int

    @property
    def ordinal(self) -> int:
        """Version order for rule 5(c). An unnumbered version sorts last, never first."""
        m = _VERSION_ORDINAL.search(self.version)
        return int(m.group(1)) if m else 10**6


def intervals_overlap(a: tuple[float | None, float | None],
                      b: tuple[float | None, float | None]) -> bool:
    """Do two bootstrap intervals overlap? An unbounded interval is never called separated.

    Touching at an endpoint counts as overlap: the claim being made is separation, and two
    intervals that meet have not established it.
    """
    if any(x is None for x in (*a, *b)):
        return True
    return a[0] <= b[1] and b[0] <= a[1]


def require_comparable(artefacts: dict[str, dict[str, Any]]) -> None:
    """Hard-fail unless every artefact declares the same score-moving settings (rule 2).

    A missing field is a mismatch, not a default: an artefact that does not say what it did
    cannot be shown to have done the same thing.
    """
    names = list(artefacts)
    if len(names) < 2:
        return
    first = names[0]
    for field in COMPARABLE_FIELDS:
        seen = {name: artefacts[name].get(field, _MISSING) for name in names}
        if len(set(seen.values())) > 1:
            detail = ", ".join(f"{n}={'<missing>' if v is _MISSING else v!r}"
                               for n, v in seen.items())
            raise ValueError(f"{field} differs across the scored systems ({detail}); "
                             "they were not measured the same way and are not comparable")
        if seen[first] is _MISSING:
            raise ValueError(f"{field} is missing from every artefact; a setting that is not "
                             "declared cannot be shown to match")


class _Missing:
    def __repr__(self) -> str:
        return "<missing>"


_MISSING = _Missing()


def select(candidates: list[Candidate]) -> tuple[Candidate, str]:
    """Apply rules 4 and 5 and return the winner with the reason it won, in words.

    The reason is written for the freeze record: whoever reads it later should be able to see
    which clause of the rule decided, without re-deriving it from the numbers.
    """
    if not candidates:
        raise ValueError("no candidates to select from")
    unscored = [c.name for c in candidates if c.macro_f1 is None]
    if unscored:
        raise ValueError(f"candidate(s) {', '.join(unscored)} not scored; a system without a "
                         "development macro-F1 is re-scored, never ranked last")

    ranked = sorted(candidates, key=lambda c: (-c.macro_f1, c.ordinal))
    best, *rest = ranked
    if not rest:
        return best, f"the only candidate, macro-F1 {best.macro_f1:.4f}"

    tied = [c for c in ranked if abs(c.macro_f1 - best.macro_f1) < TIE_EPSILON]
    if len(tied) == 1:
        margin = best.macro_f1 - rest[0].macro_f1
        return best, (f"highest development macro-F1 {best.macro_f1:.4f}, "
                      f"{margin:.4f} above {rest[0].version} ({rest[0].macro_f1:.4f}); "
                      f"the gap exceeds the {TIE_EPSILON} tie epsilon")

    cheapest = min(c.failure_rate for c in tied)
    still_tied = [c for c in tied if c.failure_rate == cheapest]
    spread = max(c.macro_f1 for c in tied) - min(c.macro_f1 for c in tied)
    if len(still_tied) == 1:
        winner = still_tied[0]
        return winner, (f"tie: {len(tied)} candidates within {TIE_EPSILON} macro-F1 "
                        f"(spread {spread:.4f}); broken on parse-failure rate "
                        f"{winner.failure_rate} against "
                        f"{sorted(c.failure_rate for c in tied if c is not winner)}")
    winner = min(still_tied, key=lambda c: c.ordinal)
    return winner, (f"tie: {len(tied)} candidates within {TIE_EPSILON} macro-F1 "
                    f"(spread {spread:.4f}) and identical parse-failure rate "
                    f"{cheapest}; broken on the earlier version")

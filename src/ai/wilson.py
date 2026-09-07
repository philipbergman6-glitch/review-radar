"""The Wilson score interval for a binomial proportion.

Used wherever this project reports a rate on a small denominator -- the sentiment weak-label
check (ADR-0003) and the agent-vs-Philip agreement (RR-21). The normal approximation is wrong
at the ends of the range, which is exactly where agreement rates tend to land, so Wilson is
the default rather than an upgrade.
"""
from __future__ import annotations

import math

Z95 = 1.959963984540054


def wilson(successes: int, n: int, z: float = Z95) -> tuple[float, float]:
    """A two-sided interval for successes/n. n == 0 gives the whole range, not a crash."""
    if successes < 0 or n < 0 or successes > n:
        raise ValueError(f"wilson: {successes} successes out of {n} is not a proportion")
    if n == 0:
        return (0.0, 1.0)
    p = successes / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def cohens_kappa(a: list[bool], b: list[bool]) -> float | None:
    """Chance-corrected agreement on a binary decision. None when chance agreement is 1.

    Kappa is undefined when both annotators always say the same thing: expected agreement is
    then 1 and the denominator vanishes. Returning None says so instead of returning 0, which
    would read as "no better than chance" for a pair that agreed perfectly.
    """
    if len(a) != len(b):
        raise ValueError("cohens_kappa needs two equally long label lists")
    n = len(a)
    if n == 0:
        return None
    observed = sum(1 for x, y in zip(a, b, strict=True) if x == y) / n
    pa, pb = sum(a) / n, sum(b) / n
    expected = pa * pb + (1 - pa) * (1 - pb)
    if abs(1 - expected) < 1e-12:
        return None
    return (observed - expected) / (1 - expected)

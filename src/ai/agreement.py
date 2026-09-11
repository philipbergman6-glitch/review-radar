"""Agent ground truth against Philip's blind 50 (RR-21, ticket 10).

P6's ground truth is machine-made: the in-session agent labelled all 400 development and audit
reviews blind, and every macro-F1 in the evaluation table is scored against those labels. That
buys speed and costs independence -- the reference annotator and the system under test are both
language models, and their errors may be correlated. RR-21's answer is not a caveat but a
number: Philip hand-labels a stratified 50 of the audit set, drawn by seeded key before he sees
anything, and **the agreement is published**.

What "agreement" means here is a decision, so it is stated rather than implied:

* **The unit is one (review, theme) decision**, not one review. Each of the ten themes is an
  independent yes/no about each review, which is exactly how the labels are used downstream and
  exactly what the per-theme F1s are built from. Fifty reviews over ten themes is 500 decisions.
* **Both raw agreement and Cohen's kappa are reported.** Theme presence is rare -- most themes
  sit under 15% prevalence -- so raw agreement is inflated by the many easy shared negatives and
  would read as a high number that means little. Kappa corrects for chance and is the honest
  half; raw agreement is kept beside it because it is what a reader expects to see and hiding it
  invites the suspicion that it was chosen after the fact.
* **The interval is Wilson, not normal.** Agreement rates land near 1, where the normal
  approximation is worst (`src/ai/wilson.py`).
* **Kappa is `None`, never 0.0, when it is undefined** -- when both annotators say the same
  thing about every review of a theme, expected agreement is 1 and the correction divides by
  zero. A theme neither annotator ever labels is perfect agreement about nothing.

Pure: no Spark, no filesystem. The caller loads two label sets and passes them in.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.ai.wilson import cohens_kappa, wilson


def theme_agreement(theme_id: str, agent: Mapping[str, set[str]], human: Mapping[str, set[str]],
                    review_ids: Sequence[str]) -> dict[str, Any]:
    """One theme's agreement over the adjudicated reviews, with its own Wilson interval.

    `both` / `agent_only` / `human_only` are printed beside the rate because the rate alone
    cannot distinguish "they agreed this theme is absent 48 times" from "they agreed it is
    present 48 times", and only the second is evidence the ground truth is sound.
    """
    a_flags = [theme_id in agent.get(r, set()) for r in review_ids]
    h_flags = [theme_id in human.get(r, set()) for r in review_ids]
    both = sum(1 for a, h in zip(a_flags, h_flags, strict=True) if a and h)
    agent_only = sum(1 for a, h in zip(a_flags, h_flags, strict=True) if a and not h)
    human_only = sum(1 for a, h in zip(a_flags, h_flags, strict=True) if h and not a)
    n = len(review_ids)
    agree = n - agent_only - human_only
    lo, hi = wilson(agree, n) if n else (None, None)
    return {"theme_id": theme_id, "n": n, "agree": agree,
            "agreement": agree / n if n else None, "wilson_95": [lo, hi],
            "both": both, "agent_only": agent_only, "human_only": human_only,
            "agent_positives": both + agent_only, "human_positives": both + human_only,
            "kappa": cohens_kappa(a_flags, h_flags)}


def agreement_report(*, agent: Mapping[str, set[str]], human: Mapping[str, set[str]],
                     theme_ids: Sequence[str], review_ids: Sequence[str]) -> dict[str, Any]:
    """Per-theme and pooled agreement over the adjudicated subset.

    A review Philip labelled that the agent did not (or vice versa) is not silently dropped:
    `review_ids` is the caller's declared subset, and a missing side is read as "no themes",
    which is what an absent label means everywhere else in P6's scoring (`src/ai/theme_scoring`).
    That keeps the denominator fixed at the drawn 50 -- an agreement number measured over
    whichever rows both happened to cover would be exactly the moving denominator the protocol
    forbids.
    """
    ids = sorted(review_ids)
    per_theme = [theme_agreement(t, agent, human, ids) for t in theme_ids]
    decisions = len(ids) * len(theme_ids)
    agreed = sum(t["agree"] for t in per_theme)
    lo, hi = wilson(agreed, decisions) if decisions else (None, None)
    pooled_a = [t in agent.get(r, set()) for r in ids for t in theme_ids]
    pooled_h = [t in human.get(r, set()) for r in ids for t in theme_ids]
    exact = sum(1 for r in ids if agent.get(r, set()) == human.get(r, set()))
    return {
        "reviews": len(ids), "themes": len(theme_ids), "n": decisions, "agreed": agreed,
        "agreement": agreed / decisions if decisions else None, "wilson_95": [lo, hi],
        "kappa": cohens_kappa(pooled_a, pooled_h),
        "exact_set_match": exact,
        "exact_set_match_rate": exact / len(ids) if ids else None,
        "per_theme": per_theme,
    }

"""Why a labelling configuration failed to parse, by named cause (ADR-0003, RR-21).

Pure functions over the validator's own messages, so the census is the same number in the
diagnosis script, in a test and in anything that later reads the artefact.

The census answers the question a reader asks of a low score -- "is that disagreement, or is
it plumbing?" -- and it answers it in the validator's vocabulary rather than in a total. A
row that failed for two reasons is counted under both causes, because each cause is one thing
a prompt could fix; it is counted once under its combination, because that is what the row
actually looked like.

Nothing here scores anything. `scripts/score_themes.py` is the scorer, and it reads a
`parse_failed` row as an empty prediction: the model was asked and produced nothing usable.
"""
from __future__ import annotations

import collections
import re
from collections.abc import Iterable
from typing import Any

# The validator's messages, bucketed. Each bucket is one thing a prompt version could fix.
# The needles are substrings of what `validate_label` writes; changing that wording without
# changing these turns real causes into `other`, which the census makes visible rather than
# hiding.
FAILURE_CAUSES = (
    ("repeats theme_id", "repeated_theme"),
    ("words, limit", "quote_too_long"),
    ("not present in the review", "quote_not_verbatim"),
)

_SLOT = re.compile(r"themes\[(\d+)\]")


def classify(error: str | None) -> list[str]:
    """The distinct named causes in one validator message. An unknown cause is `other`."""
    kinds: set[str] = set()
    for part in (error or "").split("; "):
        if not part.strip():
            continue
        for needle, kind in FAILURE_CAUSES:
            if needle in part:
                kinds.add(kind)
                break
        else:
            kinds.add("other")
    return sorted(kinds)


def census(errors: Iterable[str | None]) -> dict[str, Any]:
    """Count failed rows by cause, by combination of causes, and by the theme slot they hit."""
    by_cause: collections.Counter[str] = collections.Counter()
    by_combination: collections.Counter[str] = collections.Counter()
    by_slot: collections.Counter[str] = collections.Counter()
    rows = 0
    for error in errors:
        rows += 1
        kinds = classify(error)
        by_cause.update(kinds)
        by_combination[" + ".join(kinds)] += 1
        for match in _SLOT.finditer(error or ""):
            by_slot[match.group(1)] += 1
    return {
        "rows": rows,
        "by_cause": dict(by_cause.most_common()),
        "by_combination": dict(by_combination.most_common()),
        "by_theme_slot": dict(sorted(by_slot.items(), key=lambda kv: int(kv[0]))),
    }

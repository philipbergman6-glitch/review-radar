"""What one label row means to a trainer and to a scorer -- deliberately not the same thing.

A row of `gold.review_theme_labels` carries a terminal status and, when the model answered, a
theme array. Two consumers read that row and they must read it differently:

* **Training** (`for_training`) drops a row the model never answered for. A `parse_failed` row
  has no label; training on it as `themes = []` would teach the classifier that long,
  complaint-dense reviews are empty, because those are exactly the reviews the labeller fails
  on. The count dropped is reported (`training_census`) rather than absorbed, so the size of
  the teacher is never overstated.
* **Evaluation** (`for_evaluation`) scores the same row as an empty prediction. Excluding it
  would measure "how good is the model when it answers", which is not what a pipeline
  consuming these labels experiences; every reference positive on such a review is a false
  negative, and the failure coverage is reported beside the score (ADR-0003, RR-23).

The asymmetry is the point, so it lives in one module with both halves side by side and
`tests/test_label_usage.py` asserting both on the same row. Written as pure functions over
plain rows: the same code runs in the Spark job, in the scorer and in the test.

A drop is `None`, never an empty set. `set()` is a real answer -- "the model saw no complaint"
-- and a consumer writing `if labels:` would merge the two readings back together.
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from src.ai.labels import LABEL_STATUSES

# The model applied the contract and produced an answer. An abstention is one: the validator
# requires it to be empty and low-confidence, so it states "no themes" rather than leaving a gap.
TRAINABLE_STATUSES = ("succeeded", "model_abstained")
# Nothing usable came back. The row records that the model was asked, not what it said.
UNANSWERED_STATUSES = ("parse_failed", "api_failed")


def _status(row: Any) -> str:
    """Both a dict and a Spark `Row` subscript by column name, so one accessor covers both."""
    status = row["label_status"]
    if status not in LABEL_STATUSES:
        raise ValueError(f"label_status {status!r} is not one of {list(LABEL_STATUSES)}")
    return status


def theme_ids(themes: Any) -> set[str]:
    """The theme ids of a themes array, whether its elements are dicts or Spark Rows."""
    out: set[str] = set()
    for t in themes or []:
        out.add(t["theme_id"] if isinstance(t, dict) else t.theme_id)
    return out


def for_training(row: Any) -> set[str] | None:
    """The theme set this row teaches, or `None` when training must drop it.

    `None` and `set()` are different answers and callers must keep them apart: the first is a
    review with no label, the second a review the model read and found no complaint in.
    """
    status = _status(row)
    if status in UNANSWERED_STATUSES:
        return None
    themes = row["themes"]
    if themes is None:
        raise ValueError(f"a {status!r} row has no themes array; that combination is a "
                         "contradiction in the label contract, not an empty label")
    return theme_ids(themes)


def for_evaluation(row: Any) -> set[str]:
    """The theme set this row predicted. A row the model never answered predicted nothing."""
    status = _status(row)
    if status in UNANSWERED_STATUSES:
        return set()
    return theme_ids(row["themes"])


def training_census(statuses: Iterable[str]) -> dict[str, Any]:
    """How big the teacher actually is, and what was left out of it, by named status."""
    by_status: dict[str, int] = {s: 0 for s in LABEL_STATUSES}
    for status in statuses:
        if status not in by_status:
            raise ValueError(f"label_status {status!r} is not one of {list(LABEL_STATUSES)}")
        by_status[status] += 1
    kept = sum(by_status[s] for s in TRAINABLE_STATUSES)
    dropped = sum(by_status[s] for s in UNANSWERED_STATUSES)
    total = kept + dropped
    return {
        "pool_rows": total,
        "training_rows": kept,
        "dropped_rows": dropped,
        "by_status": {s: by_status[s] for s in LABEL_STATUSES if by_status[s]},
        "dropped_by_status": {s: by_status[s] for s in UNANSWERED_STATUSES if by_status[s]},
        "drop_rate": round(dropped / total, 4) if total else 0.0,
    }

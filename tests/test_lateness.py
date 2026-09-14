"""Injected lateness is planned, and predicted, before a record is sent (ticket 16).

Pure -- no Kafka, no Spark. The Spark-side check that the watermark model here agrees with
Spark's own `numRowsDroppedByWatermark` lives in tests/test_stream_spark.py.
"""
from __future__ import annotations

import pytest

from src.ingest.lateness import (
    DAY_MS,
    InjectionPlanError,
    InjectionSpec,
    plan_injection,
    simulate_watermark,
)
from src.ingest.replay_config import load_replay_config

BASE = 1_600_000_000_000

SPEC = InjectionSpec(seed=1, near_salt="n", near_rows=2, near_lag_days=7,
                     far_salt="f", far_rows=2, far_lag_days=730, watermark_days=30,
                     batch_size=10)


def _file(n: int, *, step_days: int = 5, start: int = BASE) -> tuple[list[int], list[str]]:
    """`n` unique reviews, one every `step_days`, in event-time order."""
    return [start + i * step_days * DAY_MS for i in range(n)], [f"r{i:05d}" for i in range(n)]


def _sparse_then_dense(sparse: int, dense: int) -> tuple[list[int], list[str]]:
    """The real file's shape in miniature: years between early rows, days between late ones."""
    ts = [BASE + i * 400 * DAY_MS for i in range(sparse)]
    ts += [ts[-1] + (i + 1) * 5 * DAY_MS for i in range(dense)]
    return ts, [f"r{i:05d}" for i in range(len(ts))]


# ------------------------------------------------------------------ the watermark ----

def test_nothing_is_dropped_in_the_first_batch_or_from_a_sorted_sequence():
    ts, _ = _file(50)
    assert simulate_watermark(ts, batch_size=10, delay_ms=30 * DAY_MS) == []


def test_a_row_below_the_watermark_in_force_is_dropped_and_one_above_it_is_not():
    # Batch 0 reaches day 45, so the watermark computed after it is day 15 -- and that is the
    # one late rows in batch 2 are judged by. A row at day 10 is late, a row at day 20 is not.
    days = (0, 15, 30, 45, 50, 55, 60, 65, 10, 20, 70, 75)
    ts = [BASE + d * DAY_MS for d in days]
    assert simulate_watermark(ts, batch_size=4, delay_ms=30 * DAY_MS) == [8]


def test_the_late_event_watermark_lags_one_batch_behind():
    """Spark judges batch N's late rows by the watermark in force for batch N-1.

    Day 0 sits in batch 1, right after a batch that reached day 100: the watermark computed
    from batch 0 is day 70, but it is not the one in force for batch 1's late rows, so day 0
    survives. In batch 2 the same row would be dropped.
    """
    ts = [BASE + 100 * DAY_MS, BASE]
    assert simulate_watermark(ts, batch_size=1, delay_ms=30 * DAY_MS) == []
    ts = [BASE + 100 * DAY_MS, BASE + 50 * DAY_MS, BASE]
    assert simulate_watermark(ts, batch_size=1, delay_ms=30 * DAY_MS) == [2]


def test_the_watermark_never_moves_backwards():
    ts = [BASE + d * DAY_MS for d in (100, 50, 60, 65)]
    # After batch 0 the watermark is day 70; batch 1's max (day 50) must not lower it, so
    # both day 60 and day 65 are judged late against day 70.
    assert simulate_watermark(ts, batch_size=1, delay_ms=30 * DAY_MS) == [2, 3]


# ------------------------------------------------------------------------ the plan ----

def test_a_plan_draws_exactly_the_frozen_sizes_from_unique_ids_and_is_deterministic():
    ts, ids = _file(400)
    a = plan_injection(ts, ids, spec=SPEC)
    b = plan_injection(ts, ids, spec=SPEC)
    assert len(a.near) == 2 and len(a.far) == 2
    assert a.held_back == b.held_back
    assert len({h.review_id for h in a.held_back}) == 4


def test_the_send_order_is_a_permutation_of_the_file():
    ts, ids = _file(400)
    plan = plan_injection(ts, ids, spec=SPEC)
    order = [i for i, _, _ in plan.sequence()]
    assert sorted(order) == list(range(400))
    moved = {h.natural_index for h in plan.held_back}
    for h in plan.held_back:
        pos, release = order.index(h.natural_index), order.index(h.release_before)
        # After every natural row that precedes its release point, before the release row,
        # and with nothing but other releases between the two.
        assert pos > max(order.index(j) for j in range(h.release_before) if j not in moved)
        assert pos < release
        assert set(order[pos + 1:release]) <= moved


def test_a_held_back_row_is_released_before_the_first_row_at_or_after_its_lag():
    ts, ids = _file(400)
    plan = plan_injection(ts, ids, spec=SPEC)
    for h in plan.held_back:
        assert h.release_ms == h.event_ms + (7 if h.slice == "near" else 730) * DAY_MS
        assert ts[h.release_before] >= h.release_ms
        assert ts[h.release_before - 1] < h.release_ms


def test_the_prediction_is_the_frozen_outcome():
    """The whole point: far dropped, near accepted, nothing natural touched, before any run."""
    ts, ids = _file(400)
    plan = plan_injection(ts, ids, spec=SPEC)
    assert plan.predicted == {"dropped": 2, "far_dropped": 2, "near_dropped": 0,
                              "natural_dropped": 0}
    c = plan.counts()
    assert (c["expected_far_dropped"], c["expected_near_accepted"], c["expected_drops"]) == (2, 2, 2)


def test_key_collisions_are_never_held_back():
    ts, ids = _file(400)
    ids = [("dup" if i % 2 else rid) for i, rid in enumerate(ids)]   # half the file collides
    plan = plan_injection(ts, ids, spec=SPEC)
    assert all(h.review_id != "dup" for h in plan.held_back)
    assert plan.pool["key_collision_ids"] == 1
    assert plan.pool["far_ineligible"]["key_collision"] == 200


def test_a_far_row_whose_fate_a_batch_boundary_could_decide_is_not_eligible():
    """The sparse-years case: the batch before the release has not passed T + watermark."""
    ts, ids = _sparse_then_dense(12, 400)
    plan = plan_injection(ts, ids, spec=SPEC)
    reasons = plan.pool["far_ineligible"]
    # Rows 0..9 release inside the sparse era, where no earlier batch has passed T + 30 days;
    # rows 10 and 11 release into the dense era and are decidable.
    assert reasons["watermark_undecidable"] == 10
    assert reasons["release_past_end"] > 0
    assert all(h.natural_index >= 10 for h in plan.far)
    # For every drawn row a guard row two batches and two slacks before the release is already
    # past T + watermark -- the property that makes the drop independent of batch boundaries.
    for h in plan.far:
        guard = h.release_before - 2 * SPEC.batch_size - 2 * SPEC.slack
        assert ts[guard] - 30 * DAY_MS > h.event_ms


def test_a_pool_too_small_for_the_frozen_size_is_a_hard_failure():
    ts, ids = _file(40)                     # 730 days never lands inside 200 days of file
    with pytest.raises(InjectionPlanError, match="eligible for the far slice"):
        plan_injection(ts, ids, spec=SPEC)


def test_an_unsorted_input_is_refused():
    ts, ids = _file(400)
    ts[10], ts[11] = ts[11], ts[10]
    with pytest.raises(InjectionPlanError, match="not in event-time order"):
        plan_injection(ts, ids, spec=SPEC)


def test_a_zero_watermark_cannot_be_planned_because_it_would_drop_the_near_slice():
    """The trip case at the spec: a watermark below the near lag is refused on construction."""
    with pytest.raises(ValueError, match="strictly between"):
        InjectionSpec(seed=1, near_salt="n", near_rows=2, near_lag_days=7, far_salt="f",
                      far_rows=2, far_lag_days=730, watermark_days=0, batch_size=10)


def test_a_zero_watermark_drops_the_near_slice_in_the_model():
    """Same trip case at the mechanism: released 7 days late, dropped by a 0-day watermark.

    A row a day, one record per batch: six later rows go by before the near row is released,
    so even the one-batch lag of the late-event watermark has seen past it.
    """
    ts, ids = _file(2000, step_days=1)
    plan = plan_injection(ts, ids, spec=SPEC)
    order = list(plan.sequence())
    dropped = simulate_watermark((t for _, t, _ in order), batch_size=1, delay_ms=0)
    slices = [order[p][2].slice if order[p][2] else "natural" for p in dropped]
    assert slices.count("near") == 2 and slices.count("far") == 2 and "natural" not in slices


def test_the_committed_config_builds_a_spec():
    spec = InjectionSpec.from_config(load_replay_config())
    assert (spec.near_rows, spec.far_rows) == (2000, 2000)
    assert spec.batch_size == 50_000
    assert spec.near_lag_days < spec.watermark_days < spec.far_lag_days

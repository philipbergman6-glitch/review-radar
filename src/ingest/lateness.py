"""Injected lateness: which rows are held back, where they are released, and what the
watermark will do to them -- decided before a single record is produced (ADR-0010, ticket 16).

Lateness does not occur in this data: the sorted file is in event-time order, and the control
run proves the watermark drops nothing from it. So the demo run *injects* it, in two slices
frozen in `conf/stream_replay.toml`: a **near** slice released 7 days late in event time,
which the 30-day watermark must accept, and a **far** slice released 730 days late, which it
must drop. The gate's claim is that exactly those counts land on each side, and that claim is
only evidence if the counts were known before the run. This module is where they are known.

**Release.** A held-back row with event time `T` and lag `L` is taken out of its natural
position and sent immediately before the first natural row whose event time is at or after
`T + L`: it is released once every review timestamped before `T + L` has gone by. Both slices
are released the same way; only the lag differs.

**What the watermark does, and why it is predictable.** Spark (3.5) advances the watermark
at the end of each micro-batch to the largest event time seen so far minus the delay. A row is
rejected as late against the watermark that was in force for the *previous* batch -- Spark
keeps a separate, one-batch-older watermark for late events than the one it evicts state by
-- so the value that decides a row in batch N was computed from the batches up to N-2.
`tests/test_stream_spark.py` holds the model here to that behaviour. Two consequences decide
the two slices:

* A near row is always accepted. Everything sent before it carries an event time below
  `T + 7d`, so any watermark in force is below `T + 7d - 30d`, which is below `T`. This holds
  for any near lag shorter than the watermark, which the config loader enforces.
* A far row is dropped when a micro-batch two before its own already reached past `T + 30d`.
  That is not guaranteed everywhere: the watermark only moves between micro-batches, and in
  the sparse early years one 50,000-row batch spans a decade -- a far row released inside the
  first two batches, or inside a batch whose predecessors had not reached `T + 30d`, would be
  accepted. So the far slice draws only from rows whose fate no micro-batch boundary can
  change: rows for which a natural row at least two full batches (plus the positional shift
  the injection itself causes) before the release point already lies past `T + watermark`.
  Rows that fail that test are counted as `watermark_undecidable` and never drawn, which is
  what makes "exactly `far_rows` dropped" a prediction and not a hope.

**The simulation.** After the slices are drawn and the send order is fixed, the whole
sequence is run through a pure model of the watermark with the frozen batch size. The
expected drops are read off that model, and the plan hard-fails if the far slice is not
dropped whole, if any near row is dropped, or if any natural row is. `tests/test_stream_spark.py`
checks the model against Spark itself on small inputs, so the prediction rests on a tested
account of the mechanism rather than on the prose above.

Eligibility for either slice also excludes rows whose review id is not unique in the file.
Silver's dedupe exists for those key collisions, and a held-back row that shares its id with
a row still in place would be removed as a duplicate rather than dropped as late -- or kept
in place of it -- and the count would no longer say what it claims. The rows are also
assumed valid under silver's validation; the control run measured zero rejects over the
whole file, and the gate's re-read of the topic reports the count, so a held-back row silver
would have rejected shows up as a drop count that does not match rather than as a quiet miss.

Pure: no Kafka, no Spark, no filesystem. The producer feeds it the sorted file's index and
sends what it returns.
"""
from __future__ import annotations

import heapq
from bisect import bisect_left
from collections import Counter
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from src.ingest.replay_config import ReplayConfig, slice_rank

DAY_MS = 86_400_000

#: Why a row cannot be drawn into the far slice, in the order the reasons are tested.
FAR_INELIGIBLE = ("key_collision", "release_past_end", "watermark_undecidable")
NEAR_INELIGIBLE = ("key_collision", "release_past_end", "drawn_into_far")


class InjectionPlanError(RuntimeError):
    """The frozen slices cannot be injected with a predictable outcome on this file."""


@dataclass(frozen=True)
class InjectionSpec:
    """The frozen numbers the plan depends on, lifted out of `ReplayConfig`.

    A separate dataclass so a test can plan over a ten-row file with a two-row batch without
    forging a whole replay config; `from_config` is the only way a real run builds one.
    """
    seed: int
    near_salt: str
    near_rows: int
    near_lag_days: int
    far_salt: str
    far_rows: int
    far_lag_days: int
    watermark_days: int
    batch_size: int

    def __post_init__(self) -> None:
        if not self.near_lag_days < self.watermark_days < self.far_lag_days:
            raise ValueError(
                f"the watermark ({self.watermark_days} days) must sit strictly between the near "
                f"lag ({self.near_lag_days}) and the far lag ({self.far_lag_days}); otherwise the "
                "two slices are not separable and their counts cannot be predicted")
        for name in ("near_rows", "far_rows", "batch_size"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be >= 1, got {getattr(self, name)}")
        if self.near_salt == self.far_salt:
            raise ValueError("the two slices must draw under different salts")

    @classmethod
    def from_config(cls, cfg: ReplayConfig) -> InjectionSpec:
        return cls(seed=cfg.seed, near_salt=cfg.near_salt, near_rows=cfg.near_rows,
                   near_lag_days=cfg.near_lag_days, far_salt=cfg.far_salt,
                   far_rows=cfg.far_rows, far_lag_days=cfg.far_lag_days,
                   watermark_days=cfg.watermark_days, batch_size=cfg.max_offsets_per_trigger)

    @property
    def slack(self) -> int:
        """The most any natural row can shift position because rows were moved."""
        return self.near_rows + self.far_rows


@dataclass(frozen=True)
class HeldBack:
    """One row taken out of its place and released late."""
    slice: str
    review_id: str
    natural_index: int
    event_ms: int
    release_ms: int
    #: The natural row this one is sent immediately before.
    release_before: int

    def as_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Plan:
    spec: InjectionSpec
    #: The sorted file's event times, in file order. Shared with the caller, never copied.
    timestamps: Sequence[int]
    held_back: tuple[HeldBack, ...]
    pool: dict[str, Any]
    #: What the watermark model predicts for this exact send order.
    predicted: dict[str, int]

    @property
    def rows(self) -> int:
        return len(self.timestamps)

    @property
    def near(self) -> tuple[HeldBack, ...]:
        return tuple(h for h in self.held_back if h.slice == "near")

    @property
    def far(self) -> tuple[HeldBack, ...]:
        return tuple(h for h in self.held_back if h.slice == "far")

    def sequence(self) -> Iterator[tuple[int, int, HeldBack | None]]:
        """The send order: (natural index, event time, the HeldBack it is if it is one).

        Every row of the file is yielded exactly once. Held-back rows are skipped at their
        natural index and yielded at their release point, before the natural row they are
        released ahead of; several released at the same point go in a fixed order.
        """
        skipped = {h.natural_index for h in self.held_back}
        before: dict[int, list[HeldBack]] = {}
        for h in self.held_back:
            before.setdefault(h.release_before, []).append(h)
        for group in before.values():
            group.sort(key=lambda h: (h.release_ms, h.slice, h.review_id))
        for i in range(self.rows):
            for h in before.get(i, ()):
                yield h.natural_index, h.event_ms, h
            if i not in skipped:
                yield i, self.timestamps[i], None

    def counts(self) -> dict[str, Any]:
        """What the producer records about the plan, before it sends anything."""
        return {
            "near_rows": len(self.near), "far_rows": len(self.far),
            "near_lag_days": self.spec.near_lag_days, "far_lag_days": self.spec.far_lag_days,
            "expected_near_accepted": len(self.near) - self.predicted["near_dropped"],
            "expected_far_dropped": self.predicted["far_dropped"],
            "expected_drops": self.predicted["dropped"],
            "pool": self.pool,
        }


# --------------------------------------------------------------------- the watermark ----
def simulate_watermark(event_ms: Iterable[int], *, batch_size: int, delay_ms: int) -> list[int]:
    """Positions the watermark drops, for a sequence processed `batch_size` records at a time.

    Spark's rule, stated so a test can hold this to it. After every batch the watermark moves
    to the largest event time seen so far minus the delay, and never backwards. A record is
    dropped as late when its event time is below the watermark that was in force for the
    *previous* batch -- the one computed from the batches before that -- so nothing is dropped
    in the first two batches, and a record in batch N is judged by the maxima of batches up to
    N-2 (`eventTimeWatermarkForLateEvents` in Spark 3.5's `IncrementalExecution`).
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")
    dropped: list[int] = []
    in_force: int | None = None          # the watermark computed after the previous batch
    for_late: int | None = None          # the one in force for the previous batch
    seen_max: int | None = None
    for pos, ts in enumerate(event_ms):
        if pos % batch_size == 0 and seen_max is not None:
            for_late = in_force
            candidate = seen_max - delay_ms
            in_force = candidate if in_force is None else max(in_force, candidate)
        if for_late is not None and ts < for_late:
            dropped.append(pos)
        seen_max = ts if seen_max is None else max(seen_max, ts)
    return dropped


# ------------------------------------------------------------------------ the draw ----
def _release_point(timestamps: Sequence[int], event_ms: int, lag_ms: int) -> tuple[int, int]:
    release = event_ms + lag_ms
    return release, bisect_left(timestamps, release)


def _far_reason(timestamps: Sequence[int], i: int, spec: InjectionSpec,
                collisions: set[str], review_id: str) -> str | None:
    """Why row `i` may not be drawn into the far slice, or None when it may."""
    if review_id in collisions:
        return "key_collision"
    _, q = _release_point(timestamps, timestamps[i], spec.far_lag_days * DAY_MS)
    if q >= len(timestamps):
        return "release_past_end"
    # The row is dropped when a batch two before its own already passed T + watermark (the
    # late-event watermark lags one batch; see `simulate_watermark`). That batch ends at least
    # 2*batch_size records earlier in the send order, and a natural row's position in that
    # order is within `slack` of its natural index in either direction (rows removed before it
    # shift it left, rows released before it shift it right). So a natural row at index
    # q - 2*batch_size - 2*slack is certainly in a batch the late-event watermark has already
    # absorbed, and if *it* lies past T + watermark, the watermark in force does.
    guard = q - 2 * spec.batch_size - 2 * spec.slack
    if guard < 0 or timestamps[guard] - spec.watermark_days * DAY_MS <= timestamps[i]:
        return "watermark_undecidable"
    return None


def _near_reason(timestamps: Sequence[int], i: int, spec: InjectionSpec,
                 collisions: set[str], review_id: str, drawn_far: set[int]) -> str | None:
    if review_id in collisions:
        return "key_collision"
    if i in drawn_far:
        return "drawn_into_far"
    _, q = _release_point(timestamps, timestamps[i], spec.near_lag_days * DAY_MS)
    if q >= len(timestamps):
        return "release_past_end"
    return None


def _draw(candidates: Iterable[int], review_ids: Sequence[str], *, n: int, seed: int,
          salt: str) -> list[int]:
    """The `n` candidates whose slice rank is lowest -- the frozen, file-determined draw."""
    return heapq.nsmallest(n, candidates,
                           key=lambda i: (slice_rank(seed, salt, review_ids[i]), review_ids[i]))


def plan_injection(timestamps: Sequence[int], review_ids: Sequence[str], *,
                   spec: InjectionSpec) -> Plan:
    """Draw both slices, fix the send order, and predict what the watermark does to it.

    `timestamps` is the sorted file's event times in file order; `review_ids` the ids beside
    them. Hard-fails on an unsorted input, on a pool too small for the frozen sizes, and on a
    plan the watermark model does not predict to land exactly as frozen.
    """
    n = len(timestamps)
    if n != len(review_ids):
        raise ValueError(f"{n} timestamps but {len(review_ids)} review ids")
    if any(timestamps[i] < timestamps[i - 1] for i in range(1, n)):
        raise InjectionPlanError("the input is not in event-time order; the plan is built on "
                                 "the sort job's output and nothing else")

    ids = Counter(review_ids)
    collisions = {rid for rid, c in ids.items() if c > 1}

    # Seeded with every reason at zero so a refusal message names the ones that did not
    # fire as well as the ones that did: "0 undecidable" is a fact about the file.
    far_reasons: Counter = Counter(dict.fromkeys(FAR_INELIGIBLE, 0))
    far_pool: list[int] = []
    for i in range(n):
        reason = _far_reason(timestamps, i, spec, collisions, review_ids[i])
        if reason is None:
            far_pool.append(i)
        else:
            far_reasons[reason] += 1
    if len(far_pool) < spec.far_rows:
        raise InjectionPlanError(
            f"only {len(far_pool)} rows are eligible for the far slice and the frozen size is "
            f"{spec.far_rows}; ineligible by reason: {dict(far_reasons)}")
    far = _draw(far_pool, review_ids, n=spec.far_rows, seed=spec.seed, salt=spec.far_salt)
    drawn_far = set(far)

    near_reasons: Counter = Counter(dict.fromkeys(NEAR_INELIGIBLE, 0))
    near_pool: list[int] = []
    for i in range(n):
        reason = _near_reason(timestamps, i, spec, collisions, review_ids[i], drawn_far)
        if reason is None:
            near_pool.append(i)
        else:
            near_reasons[reason] += 1
    if len(near_pool) < spec.near_rows:
        raise InjectionPlanError(
            f"only {len(near_pool)} rows are eligible for the near slice and the frozen size is "
            f"{spec.near_rows}; ineligible by reason: {dict(near_reasons)}")
    near = _draw(near_pool, review_ids, n=spec.near_rows, seed=spec.seed, salt=spec.near_salt)

    held: list[HeldBack] = []
    for name, drawn, lag_days in (("near", near, spec.near_lag_days),
                                  ("far", far, spec.far_lag_days)):
        for i in drawn:
            release, q = _release_point(timestamps, timestamps[i], lag_days * DAY_MS)
            held.append(HeldBack(slice=name, review_id=review_ids[i], natural_index=i,
                                 event_ms=timestamps[i], release_ms=release, release_before=q))
    held.sort(key=lambda h: h.natural_index)

    plan = Plan(spec=spec, timestamps=timestamps, held_back=tuple(held),
                pool={"rows": n, "key_collision_ids": len(collisions),
                      "far_eligible": len(far_pool), "far_ineligible": dict(far_reasons),
                      "near_eligible": len(near_pool), "near_ineligible": dict(near_reasons)},
                predicted={})
    plan = Plan(spec=spec, timestamps=timestamps, held_back=plan.held_back,
                pool=plan.pool, predicted=predict(plan))
    _require_clean(plan)
    return plan


def predict(plan: Plan) -> dict[str, int]:
    """Run the send order through the watermark model; count drops by what was dropped."""
    order = list(plan.sequence())
    if len(order) != plan.rows:
        raise InjectionPlanError(f"the send order has {len(order)} records for {plan.rows} rows")
    dropped = simulate_watermark((ts for _, ts, _ in order),
                                 batch_size=plan.spec.batch_size,
                                 delay_ms=plan.spec.watermark_days * DAY_MS)
    counts = {"dropped": len(dropped), "near_dropped": 0, "far_dropped": 0, "natural_dropped": 0}
    for pos in dropped:
        _, _, h = order[pos]
        counts[f"{h.slice}_dropped" if h else "natural_dropped"] += 1
    return counts


def _require_clean(plan: Plan) -> None:
    p = plan.predicted
    spec = plan.spec
    problems = []
    if p["far_dropped"] != len(plan.far):
        problems.append(f"the watermark model drops {p['far_dropped']} of the {len(plan.far)} "
                        "far rows, not all of them")
    if p["near_dropped"]:
        problems.append(f"the watermark model drops {p['near_dropped']} near row(s); a "
                        f"{spec.near_lag_days}-day lag under a {spec.watermark_days}-day "
                        "watermark must be accepted")
    if p["natural_dropped"]:
        problems.append(f"the watermark model drops {p['natural_dropped']} row(s) that were "
                        "never held back")
    if problems:
        raise InjectionPlanError("the plan does not land as frozen: " + "; ".join(problems))

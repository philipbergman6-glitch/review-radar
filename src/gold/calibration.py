"""What the decline rule does to noise, and to a decline of known size (ADR-0001, RR-09).

The protocol freeze is the moment `conf/decline_rule.toml` stops being editable, and
`GOLD_CALIBRATION` is what must print a number before it happens (ADR-0011, RR-13). Two
numbers, answering two different objections to "964 products declined":

* **Placebo trigger rate.** Run the same rule over series that have the same volumes, the
  same seasonality and the same short-run dependence, but no trend -- a *blocked circular
  permutation* of each product's monthly aggregates. Every alert there is a false one. The
  ceiling comes from the persona: a category manager with capacity for four investigations a
  month, of which at most a quarter may be noise, so **one placebo alert per month** across
  the whole eligible portfolio (RR-09 round 5).
* **Detection power.** Relabel reviews inside a product's own history so its mean falls by a
  known amount at a known month, and ask whether the rule notices. The bar, fixed before any
  of this ran: **80% of injected 0.3-star steps detected within 6 evaluable points**.

Both are computed on evaluation points **before `holdout_start` only**, and more strictly
than the gold job computes them: here a product's spine is truncated so that no window of any
calibration point reaches into the holdout at all. The gold job stops at the first holdout
*point*, which still lets a 2019-12 point average 2020 ratings into its recent window. That
is tolerable for a development figure and not tolerable for the numbers that choose the
thresholds, so calibration pays the extra caution and says so.

Nothing here reads a file or a database. `scripts/calibrate_gold.py` loads the spines and
owns the printing; this module owns the arithmetic, so `tests/test_calibration.py` can run
the whole protocol over synthetic products in milliseconds.

**The fast path.** The loops below evaluate one rule over hundreds of thousands of
product-months, hundreds of times. `src/gold/rule.py:evaluate` builds a full evaluation-point
record per month, which is the right shape for gold and the wrong shape for this. So
`alerts_and_points` reimplements just the state machine over two arrays -- and the script
asserts on every run that it agrees with the reference on the observed data, which is the
one constituent here that can fail for a reason other than the rule being bad.
"""
from __future__ import annotations

import hashlib
import math
import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

#: The rule fields calibration varies. Everything else in `[rule]` is held at its committed
#: value, so a chosen configuration differs from the provisional one in named ways only.
GRID_FIELDS = ("baseline_months", "recent_months", "delta", "persistence", "min_reviews",
               "min_active_months", "recovery_points", "max_gap")

#: Injection mechanisms (RR-09 round 5), as the target star a relabelled 5-star review takes.
MECHANISMS = {"severe": (1, 2), "moderate": (3,)}


# --------------------------------------------------------------------- the rule ----
@dataclass(frozen=True)
class Config:
    """One candidate rule, as the grid varies it. Ordered so it can key a dict and print."""
    baseline_months: int
    recent_months: int
    delta: float
    persistence: int
    min_reviews: int
    min_active_months: int
    recovery_points: int
    max_gap: int
    unevaluable_resets_persistence: bool = True

    @property
    def key(self) -> str:
        return (f"B{self.baseline_months}R{self.recent_months}d{self.delta:g}"
                f"P{self.persistence}n{self.min_reviews}a{self.min_active_months}"
                f"K{self.recovery_points}G{self.max_gap}")

    def as_rule_fields(self) -> dict[str, Any]:
        """Every `[rule]` field this configuration determines, as the freeze writes them back.

        Wider than `GRID_FIELDS`: calibration never *varies* `unevaluable_resets_persistence`,
        but the configuration still carries it, and a freeze that wrote back only the varied
        fields would leave the rule half-specified.
        """
        return {**{f: getattr(self, f) for f in GRID_FIELDS},
                "unevaluable_resets_persistence": self.unevaluable_resets_persistence}


def config_from(rule: Mapping[str, Any], **overrides: Any) -> Config:
    """A candidate built from the committed rule, with the named fields replaced."""
    base = {f: rule[f] for f in GRID_FIELDS}
    base["unevaluable_resets_persistence"] = bool(rule["unevaluable_resets_persistence"])
    unknown = set(overrides) - set(base)
    if unknown:
        raise ValueError(f"calibration may only vary {sorted(base)}, got {sorted(unknown)}")
    base.update(overrides)
    return Config(**base)


def alerts_and_points(counts: Sequence[float], sums: Sequence[float],
                      c: Config) -> tuple[int, int, list[int]]:
    """(points, evaluable points, alert indices) for one product's spine.

    `counts[i]` and `sums[i]` are month `i`'s review count and rating sum over a complete
    calendar spine. Returns spine indices, not months, because every caller here works in
    index space. The state machine is `src/gold/rule.py`'s, stripped to the three things
    calibration counts.
    """
    n = len(counts)
    B, R = c.baseline_months, c.recent_months
    if n < B + R:
        return 0, 0, []
    pc = [0.0] * (n + 1)
    ps = [0.0] * (n + 1)
    pa = [0.0] * (n + 1)
    for i in range(n):
        pc[i + 1] = pc[i] + counts[i]
        ps[i + 1] = ps[i] + sums[i]
        pa[i + 1] = pa[i] + (1.0 if counts[i] > 0 else 0.0)

    points = evaluable = 0
    alerts: list[int] = []
    run_len = 0
    in_episode = False
    false_count = gap_count = 0
    for i in range(B - 1, n - R):
        points += 1
        blo, bhi = i - B + 1, i + 1
        rlo, rhi = i + 1, i + R + 1
        bc, rc = pc[bhi] - pc[blo], pc[rhi] - pc[rlo]
        ok = (bc >= c.min_reviews and (pa[bhi] - pa[blo]) >= c.min_active_months
              and rc >= c.min_reviews and (pa[rhi] - pa[rlo]) >= c.min_active_months)
        if ok:
            evaluable += 1
            cond = ((ps[rhi] - ps[rlo]) / rc) <= ((ps[bhi] - ps[blo]) / bc) - c.delta
        else:
            cond = None

        if not in_episode:
            if not ok:
                if c.unevaluable_resets_persistence:
                    run_len = 0
            elif cond:
                run_len += 1
                if run_len >= c.persistence:
                    alerts.append(i)
                    in_episode = True
                    false_count = gap_count = 0
            else:
                run_len = 0
        elif not ok:
            gap_count += 1
            if gap_count > c.max_gap:
                in_episode, run_len = False, 0
        else:
            gap_count = 0
            if cond:
                false_count = 0
            else:
                false_count += 1
                if false_count >= c.recovery_points:
                    in_episode, run_len = False, 0
    return points, evaluable, alerts


# ------------------------------------------------------------- the whole portfolio ----
class Portfolio:
    """Every product's spine in two padded arrays, so one configuration costs one pass.

    The scalar `alerts_and_points` above is the readable statement of the state machine, and
    it is far too slow for what calibration asks: a grid of configurations, each re-scored on
    every permutation replicate, over 473,268 product-months. So the same machine is written
    once more across products instead of along them -- the loop runs over the *calendar*, at
    most a few hundred steps, and every step is a handful of array operations over all 13,122
    products at once. `tests/test_calibration.py` asserts the two agree on random spines, and
    `scripts/calibrate_gold.py` asserts the array form agrees with `src/gold/rule.py` on the
    observed data before it believes any number below.

    Padding is zeros and `lengths` is authoritative: a product is only asked for points at
    indices its own spine actually reaches.
    """

    def __init__(self, spines: Sequence[tuple[Sequence[float], Sequence[float]]]):
        import numpy as np

        if not spines:
            raise ValueError("a portfolio with no products calibrates nothing")
        self.np = np
        self.lengths = np.array([len(c) for c, _ in spines], dtype=np.int64)
        self.width = int(self.lengths.max())
        p = len(spines)
        self.counts = np.zeros((p, self.width), dtype=np.float64)
        self.sums = np.zeros((p, self.width), dtype=np.float64)
        for i, (c, s) in enumerate(spines):
            n = len(c)
            self.counts[i, :n] = c
            self.sums[i, :n] = s
        self._refresh()

    def _refresh(self) -> None:
        np = self.np
        z = np.zeros((self.counts.shape[0], 1))
        self.cum_c = np.hstack([z, np.cumsum(self.counts, axis=1)])
        self.cum_s = np.hstack([z, np.cumsum(self.sums, axis=1)])
        self.cum_a = np.hstack([z, np.cumsum((self.counts > 0).astype(np.float64), axis=1)])

    def replace(self, spines: Sequence[tuple[Sequence[float], Sequence[float]]]) -> None:
        """Overwrite the ratings in place -- what a permutation replicate does each round."""
        self.counts[:] = 0.0
        self.sums[:] = 0.0
        for i, (c, s) in enumerate(spines):
            n = len(c)
            self.counts[i, :n] = c
            self.sums[i, :n] = s
        self._refresh()

    def scan(self, c: Config, *, step: Any = None, deadline_from: Any = None,
             window_points: int = 0) -> dict[str, Any]:
        """Run one configuration over the whole portfolio.

        Returns the three counts calibration needs, and -- when `step` is given, one spine
        index per product -- whether an alert landed inside the detection window, and how long
        it took.

        The window has two edges, because an evaluation point looks *forward*. Point `i`
        compares a baseline ending at `i` against the recent window `i+1 .. i+R`, so a decline
        beginning at month `s` first touches a point's recent window at `i = s - R`, but is
        only fully inside one at `i = s - 1`.

        * `step` is where the window **opens** (`s - R`): an alert there is a genuine early
          catch off a partly-declined recent window, and refusing to count it would understate
          the rule.
        * `deadline_from` is where the clock **starts** (`s - 1`): `window_points` is counted
          from the first point that can see the whole decline. Counting the deadline from the
          opening edge instead would demand detection from points whose recent windows are
          mostly pre-decline -- with `R = 12` and a 6-point window, every point inside it
          would carry at most half the drop, and the bar would be unreachable by construction
          rather than by the rule being weak.
        """
        np = self.np
        B, R = c.baseline_months, c.recent_months
        n_products = self.counts.shape[0]
        last_point = self.lengths - R - 1          # the final index that can carry a point
        run_len = np.zeros(n_products, dtype=np.int64)
        in_ep = np.zeros(n_products, dtype=bool)
        false_count = np.zeros(n_products, dtype=np.int64)
        gap_count = np.zeros(n_products, dtype=np.int64)
        points = evaluable = alerts = 0

        tracking = step is not None
        if tracking:
            step_ix = np.asarray(step, dtype=np.int64)
            dead_ix = np.asarray(step if deadline_from is None else deadline_from,
                                 dtype=np.int64)
            ev_after = np.zeros(n_products, dtype=np.int64)
            delay = np.full(n_products, -1, dtype=np.int64)
            in_time = np.zeros(n_products, dtype=bool)

        for i in range(B - 1, self.width - R):
            live = (i <= last_point)
            if not live.any():
                continue
            blo, bhi, rlo, rhi = i - B + 1, i + 1, i + 1, i + R + 1
            bc = self.cum_c[:, bhi] - self.cum_c[:, blo]
            rc = self.cum_c[:, rhi] - self.cum_c[:, rlo]
            ok = (live & (bc >= c.min_reviews) & (rc >= c.min_reviews)
                  & ((self.cum_a[:, bhi] - self.cum_a[:, blo]) >= c.min_active_months)
                  & ((self.cum_a[:, rhi] - self.cum_a[:, rlo]) >= c.min_active_months))
            with np.errstate(invalid="ignore", divide="ignore"):
                b_mean = (self.cum_s[:, bhi] - self.cum_s[:, blo]) / np.where(bc > 0, bc, 1.0)
                r_mean = (self.cum_s[:, rhi] - self.cum_s[:, rlo]) / np.where(rc > 0, rc, 1.0)
            cond = ok & (r_mean <= b_mean - c.delta)

            points += int(live.sum())
            evaluable += int(ok.sum())
            if tracking:
                # The clock ticks on evaluable points from the deadline edge, so the budget is
                # spent on points that can actually see the decline.
                ev_after += (ok & (i >= dead_ix)).astype(np.int64)

            # --- outside an episode: build, break or fire the persistence run ---
            free = live & ~in_ep
            if c.unevaluable_resets_persistence:
                run_len = np.where(free & ~ok, 0, run_len)
            run_len = np.where(free & cond, run_len + 1, run_len)
            run_len = np.where(free & ok & ~cond, 0, run_len)
            fire = free & cond & (run_len >= c.persistence)
            alerts += int(fire.sum())
            if tracking:
                first = fire & (i >= step_ix) & (delay < 0)
                # An alert before the deadline edge has spent none of the budget; one after it
                # has spent `ev_after`. Either way the first alert inside the open window is
                # the one that counts, and a later one cannot rescue a missed deadline.
                spent = np.maximum(ev_after - 1, 0)
                delay = np.where(first, spent, delay)
                in_time = np.where(first, spent < window_points, in_time)
            in_ep = in_ep | fire
            false_count = np.where(fire, 0, false_count)
            gap_count = np.where(fire, 0, gap_count)

            # --- inside one: count the gap or the recovery that will close it ---
            ins = live & in_ep & ~fire
            gap_count = np.where(ins & ~ok, gap_count + 1, np.where(ins, 0, gap_count))
            false_count = np.where(ins & ok & ~cond, false_count + 1,
                                   np.where(ins & ok, 0, false_count))
            closing = ((ins & ~ok & (gap_count > c.max_gap))
                       | (ins & ok & ~cond & (false_count >= c.recovery_points)))
            in_ep = in_ep & ~closing
            run_len = np.where(closing, 0, run_len)

        out: dict[str, Any] = {"points": points, "evaluable": evaluable, "alerts": alerts}
        if tracking:
            out["delay"] = delay
            out["detected"] = (delay >= 0) & in_time
        return out

    def evaluable_matrix(self, c: Config) -> Any:
        """`(product, spine index)` -> is there an evaluable point here under `c`.

        Only the eligibility half of the rule, so it needs no state machine. Used to pick
        injection months: a step is only meaningful where the product is already being
        watched.
        """
        np = self.np
        B, R = c.baseline_months, c.recent_months
        ev = np.zeros(self.counts.shape, dtype=bool)
        last_point = self.lengths - R - 1
        for i in range(B - 1, self.width - R):
            live = i <= last_point
            if not live.any():
                continue
            blo, bhi, rlo, rhi = i - B + 1, i + 1, i + 1, i + R + 1
            ev[:, i] = (live
                        & ((self.cum_c[:, bhi] - self.cum_c[:, blo]) >= c.min_reviews)
                        & ((self.cum_c[:, rhi] - self.cum_c[:, rlo]) >= c.min_reviews)
                        & ((self.cum_a[:, bhi] - self.cum_a[:, blo]) >= c.min_active_months)
                        & ((self.cum_a[:, rhi] - self.cum_a[:, rlo]) >= c.min_active_months))
        return ev


# --------------------------------------------------------------- the placebo null ----
def block_permute(counts: Sequence[float], sums: Sequence[float], *, block: int,
                  rng: random.Random) -> tuple[list[float], list[float]]:
    """A circular block permutation of one product's spine, counts and sums moving together.

    Cut the spine into consecutive blocks of `block` months, rotate it by a random offset so
    block boundaries are not fixed to the calendar, then shuffle the block order. Volume,
    verification and text move with the ratings they belong to, and dependence shorter than a
    block survives; what does not survive is any trend longer than one block -- which is
    exactly what the decline rule is built to find. So an alert here is a placebo trigger.

    The spine keeps its length, so the permuted product is under surveillance for the same
    number of months as the real one, and the two rates are comparable.
    """
    n = len(counts)
    if block < 1:
        raise ValueError("placebo block length must be >= 1")
    if n <= block:
        return list(counts), list(sums)
    off = rng.randrange(n)
    rc = [counts[(i + off) % n] for i in range(n)]
    rs = [sums[(i + off) % n] for i in range(n)]
    blocks = [(rc[i:i + block], rs[i:i + block]) for i in range(0, n, block)]
    rng.shuffle(blocks)
    out_c: list[float] = []
    out_s: list[float] = []
    for bc, bs in blocks:
        out_c.extend(bc)
        out_s.extend(bs)
    return out_c, out_s


@dataclass(frozen=True)
class PlaceboResult:
    """What one configuration did to noise, summed over replicates."""
    config: Config
    replicates: int
    alerts: int
    points: int
    evaluable: int
    surveillance_months: int

    @property
    def alerts_per_month(self) -> float:
        """Expected placebo alerts landing on the desk in one calendar month.

        The denominator is the number of distinct calendar months the portfolio was under
        surveillance in, per replicate -- the unit the capacity argument is stated in.
        """
        d = self.replicates * self.surveillance_months
        return self.alerts / d if d else 0.0

    @property
    def alerts_per_eligible_product_year(self) -> float:
        """The second unit RR-09 asks for: one evaluable point is one product-month."""
        return self.alerts / (self.evaluable / 12.0) if self.evaluable else 0.0


def placebo(spines: Sequence[tuple[Sequence[float], Sequence[float]]], configs: Sequence[Config],
            *, replicates: int, block: int, seed: int, surveillance_months: int,
            progress: Any = None) -> dict[str, PlaceboResult]:
    """Every configuration's placebo rate, over the same permuted series.

    The permutation is drawn once per replicate and every configuration is scored on it, so
    two configurations differ because the rule differs and never because the noise did -- a
    grid compared across independent noise draws would rank configurations partly on which
    draw they happened to get.
    """
    totals = {c.key: [0, 0, 0] for c in configs}
    book = Portfolio(spines)
    for rep in range(replicates):
        rng = random.Random(f"{seed}\x1fplacebo\x1f{rep}")
        book.replace([block_permute(cs, ss, block=block, rng=rng) for cs, ss in spines])
        for c in configs:
            r = book.scan(c)
            t = totals[c.key]
            t[0] += r["alerts"]
            t[1] += r["points"]
            t[2] += r["evaluable"]
        if progress:
            progress(rep + 1, replicates)
    return {c.key: PlaceboResult(config=c, replicates=replicates, alerts=totals[c.key][0],
                                 points=totals[c.key][1], evaluable=totals[c.key][2],
                                 surveillance_months=surveillance_months)
            for c in configs}


# ------------------------------------------------------------------- injection ----
def inject(months: Sequence[int], ratings: Sequence[int], *, step_index: int, target_drop: float,
           mechanism: str, ramp_months: int, rng: random.Random) -> list[int]:
    """Relabel reviews so the product's mean falls by `target_drop` from `step_index` on.

    Row-level, as RR-09 round 5 requires: timestamps, volume, verification and identity are
    untouched and only the star changes, so nothing about the product's observability moves.
    Only 5-star reviews are relabelled -- lowering an already-low review would model a
    different thing and cannot reach the same drop.

    The ramp is linear in *effect*: a review in the k-th month after the step is relabelled
    with probability scaled by `min(1, (k + 1) / ramp_months)`, so the mean falls to its
    terminal size over `ramp_months` calendar months and then persists. `ramp_months = 1` is
    the pure step.

    Returns the full relabelled rating list. A product with too few 5-star reviews to reach
    the drop gets as far as it can; the caller counts the shortfall rather than discarding it
    silently -- an injection that could not be delivered is not a detection failure.
    """
    if mechanism not in MECHANISMS:
        raise ValueError(f"unknown injection mechanism {mechanism!r}")
    if ramp_months < 1:
        raise ValueError("ramp_months must be >= 1")
    targets = MECHANISMS[mechanism]
    out = list(ratings)
    # Sized per month, not over the post-step period as a whole. Sizing it globally would make
    # `target_drop` the *average* drop across the period, which for a step is also its
    # terminal level but for a ramp is not: hitting an average of 0.3 while climbing from 0
    # forces the tail well past 0.3, and the ramp would then look easier to detect than the
    # step it is supposed to be a harder version of.
    per_relabel = 5.0 - sum(targets) / len(targets)
    by_month: dict[int, list[int]] = {}
    for i, m in enumerate(months):
        if m >= step_index:
            by_month.setdefault(m, []).append(i)
    taken = 0
    # A month holds a whole number of reviews, so the drop it can deliver is quantised: with
    # twelve reviews and a 3.5-star relabel, one row is worth 0.29 stars and the nearest whole
    # number is rarely the target. Rounding each month independently would round the same way
    # every time and inject systematically harder than asked -- which would show up as power
    # the rule does not have. The carry keeps the fractional remainder and spends it later, so
    # the delivered effect tracks the target instead of drifting above it.
    carry = 0.0
    for m in sorted(by_month):
        rows = by_month[m]
        scale = min(1.0, (m - step_index + 1) / ramp_months)
        want = target_drop * scale * len(rows) / per_relabel + carry
        needed = int(want)
        carry = want - needed
        if needed <= 0:
            continue
        fives = sorted((i for i in rows if ratings[i] == 5), key=lambda i: (rng.random(), i))
        for i in fives[:needed]:
            out[i] = targets[taken % len(targets)]
            taken += 1
        # A month short of five-star rows cannot deliver its share; the shortfall rolls
        # forward rather than vanishing, so the period still reaches the level it claims.
        carry += needed - len(fives[:needed])
    return out


@dataclass(frozen=True)
class Product:
    """One product as injection needs it: a spine length, and every review's month and star.

    `months[k]` is a spine *index*, not a calendar month, because every other array here is in
    index space. Reviews are kept individually rather than pre-aggregated because RR-09 round
    5 requires the injection to be a row-level relabelling: aggregate arithmetic that merely
    subtracts from a monthly mean would invent reviews that never existed and would silently
    allow a mean below one star.
    """
    asin: str
    length: int
    months: tuple[int, ...]
    ratings: tuple[int, ...]
    #: Calendar month index (`src/gold/rule.py:month_index`) of spine index 0, so a product's
    #: own timeline can be put back on the shared calendar -- which the placebo denominator
    #: needs, since one desk's month is one month however many products alert in it.
    start: int = 0


def spine_of(p: Product, ratings: Sequence[int] | None = None) -> tuple[list[float], list[float]]:
    """(counts, rating sums) over the product's complete calendar spine."""
    stars = p.ratings if ratings is None else ratings
    counts = [0.0] * p.length
    sums = [0.0] * p.length
    for m, r in zip(p.months, stars, strict=True):
        counts[m] += 1.0
        sums[m] += float(r)
    return counts, sums


def choose_steps(book: Portfolio, reference: Config, *, window_points: int, seed: int,
                 replicate: int) -> Any:
    """One injection month per product, drawn from the months it is already under surveillance.

    The step is chosen under the **reference** configuration -- the rule as committed -- and
    then held fixed while every candidate in the grid is scored on the same injected series.
    Letting each candidate pick its own step would compare detectors on different experiments
    and would quietly reward a rule for declaring fewer months eligible.

    A product needs `window_points` evaluable points at or after the step for the detection
    window to fit inside its own history; one that does not gets `-1` and is excluded, not
    counted as a miss.
    """
    import numpy as np

    ev = book.evaluable_matrix(reference)
    steps = np.full(ev.shape[0], -1, dtype=np.int64)
    rng = random.Random(f"{seed}\x1fstep\x1f{replicate}")
    for i in range(ev.shape[0]):
        idx = np.flatnonzero(ev[i])
        if len(idx) < window_points:
            continue
        # Any of these leaves a full detection horizon inside the product's own spine.
        steps[i] = int(idx[rng.randrange(len(idx) - window_points + 1)])
    return steps


@dataclass(frozen=True)
class PowerResult:
    """One (configuration, mechanism, effect size) cell of the injection grid."""
    config: Config
    mechanism: str
    effect: float
    ramp_months: int
    eligible: int
    delivered: int
    detected: int
    delays: tuple[int, ...]

    @property
    def power(self) -> float:
        return self.detected / self.delivered if self.delivered else 0.0

    @property
    def median_delay(self) -> float | None:
        if not self.delays:
            return None
        s = sorted(self.delays)
        mid = len(s) // 2
        return float(s[mid]) if len(s) % 2 else (s[mid - 1] + s[mid]) / 2.0


def detected_within(alerts: Sequence[int], *, from_index: int, window_points: int,
                    evaluable_at: Sequence[int]) -> int | None:
    """The delay, in evaluable points, of the first alert inside the detection window.

    `from_index` is where the window *opens*, which is `step - R` and not the step month
    itself: an evaluation point looks forward over its recent window, so the first point that
    can see a decline beginning at month `s` sits R months before it. `evaluable_at` is the
    ascending spine indices of this product's evaluable points, which is the clock RR-09
    states the bar in -- *within 6 evaluable points*, not within 6 calendar months. Returns
    None when nothing fired in time.
    """
    after = [i for i in evaluable_at if i >= from_index]
    if not after:
        return None
    horizon = after[:window_points]
    if not horizon:
        return None
    last = horizon[-1]
    for a in alerts:
        if from_index <= a <= last:
            return sum(1 for i in after if i <= a) - 1
    return None


def power_grid(products: Sequence[Product], configs: Sequence[Config], *, reference: Config,
               effects: Sequence[float], mechanisms: Sequence[str], ramps: Sequence[int],
               window_points: int, seed: int, replicates: int,
               progress: Any = None) -> dict[tuple[str, str, float, int], PowerResult]:
    """Power for every (configuration, mechanism, effect, shape), pooled over replicates.

    `ramps` is the injection *shape*: `1` is the pure step the power bar is stated at, and a
    larger value is a linear ramp to the same terminal size. RR-09 round 5 keeps them apart
    deliberately -- the target is "80% of the 0.3-star step", with the ramp "reported, not
    required to match" -- because a ramp is a strictly harder thing to catch inside a fixed
    horizon and scoring the bar against it would be measuring a different claim.

    Each replicate redraws the injection month and the choice of which 5-star reviews take the
    hit, so the number is not an artefact of one lucky set of months. Within a replicate every
    configuration sees the *same* injected series, for the same reason the placebo replicates
    are shared.

    Two exclusions, both counted rather than hidden:

    * a product with no room for the detection window is **not eligible** -- it never enters
      the denominator, because failing to detect inside a horizon that does not exist is not a
      miss;
    * a product that already alerts inside its own detection window **before** the injection
      is eligible but **not delivered** -- it was declining anyway, and crediting the detector
      for noticing a decline it would have caught regardless is how power gets overstated.
    """
    import numpy as np

    observed = [spine_of(p) for p in products]
    book = Portfolio(observed)
    tallies: dict[tuple[str, str, float, int], list[Any]] = {
        (c.key, m, e, r): [0, 0, 0, []]
        for c in configs for m in mechanisms for e in effects for r in ramps}

    for rep in range(replicates):
        steps = choose_steps(book, reference, window_points=window_points, seed=seed,
                             replicate=rep)
        has_room = steps >= 0
        safe_steps = np.where(has_room, steps, 0)

        # An evaluation point at month `i` decides a boundary: its baseline ends at `i` and
        # its recent window is `i+1 .. i+R`. So a decline beginning at month `s` first becomes
        # visible at point `s - R`, and the earliest honest alert lands *before* the step
        # month, not after it. Counting the detection window from `s` would score every
        # on-time alert as a miss -- and would make a larger injection look less detectable
        # than a smaller one, because it is caught earlier. Each configuration gets its own
        # start, since each has its own R.
        detect_from = {c.key: np.maximum(0, safe_steps - c.recent_months) for c in configs}
        deadline_from = np.maximum(0, safe_steps - 1)

        # Who was already declining there, per configuration, with nothing injected.
        book.replace(observed)
        prior = {c.key: book.scan(c, step=detect_from[c.key], deadline_from=deadline_from,
                                  window_points=window_points)["detected"]
                 for c in configs}

        for mech in mechanisms:
            for eff in effects:
                for ramp in ramps:
                    rng = random.Random(f"{seed}\x1finject\x1f{rep}\x1f{mech}\x1f{eff}\x1f{ramp}")
                    # Only the products that actually take an injection are re-aggregated; the
                    # rest keep the spine already built, which is most of the portfolio.
                    injected = list(observed)
                    for i, (p, s, room) in enumerate(zip(products, steps, has_room,
                                                         strict=True)):
                        if not room:
                            continue
                        stars = inject(p.months, p.ratings, step_index=int(s), target_drop=eff,
                                       mechanism=mech, ramp_months=ramp, rng=rng)
                        injected[i] = spine_of(p, stars)
                    book.replace(injected)
                    for c in configs:
                        r = book.scan(c, step=detect_from[c.key], deadline_from=deadline_from,
                                      window_points=window_points)
                        delivered = has_room & ~prior[c.key]
                        hit = delivered & r["detected"]
                        t = tallies[(c.key, mech, eff, ramp)]
                        t[0] += int(has_room.sum())
                        t[1] += int(delivered.sum())
                        t[2] += int(hit.sum())
                        t[3].extend(int(d) for d in r["delay"][hit])
        if progress:
            progress(rep + 1, replicates)

    by_key = {c.key: c for c in configs}
    return {k: PowerResult(config=by_key[k[0]], mechanism=k[1], effect=k[2], ramp_months=k[3],
                           eligible=v[0], delivered=v[1], detected=v[2], delays=tuple(v[3]))
            for k, v in tallies.items()}


def calendar_surveillance_months(products: Sequence[Product], evaluable: Any) -> int:
    """Distinct calendar months in which *anything* in the portfolio was under watch.

    The placebo ceiling is stated as alerts arriving on one desk in one month, so the
    denominator counts calendar months and not product-months: ten products alerting in the
    same month is ten items of work in that month, not ten months of work. Months in which
    nothing was eligible are not counted -- no alert could have landed then, and including
    them would dilute the rate with years the portfolio did not yet exist.
    """
    import numpy as np

    months: set[int] = set()
    for i, p in enumerate(products):
        for j in np.flatnonzero(evaluable[i]):
            months.add(p.start + int(j))
    return len(months)


# ------------------------------------------------------------------- selection ----
def wilson(successes: int, n: int, z: float = 1.959963984540054) -> tuple[float, float]:
    """Wilson score interval -- the one the agreement number already uses, so both match."""
    if n <= 0:
        return (0.0, 0.0)
    p = successes / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (max(0.0, centre - half), min(1.0, centre + half))


@dataclass(frozen=True)
class Selection:
    """The chosen configuration and why it won, under a rule fixed before the run."""
    config: Config | None
    reason: str
    considered: int
    admissible: int


def select(configs: Sequence[Config], placebo_by_key: Mapping[str, PlaceboResult],
           power_by_key: Mapping[str, PowerResult], *, ceiling_alerts_per_month: float,
           power_target: float) -> Selection:
    """Admissible = under the placebo ceiling. Winner = most power, then fastest, then strictest.

    The tie-breaks are declared here and committed before the grid runs, because a selection
    rule invented after seeing the grid is the same discretion the holdout exists to bound.
    Ties after power and delay go to the *larger* delta and the *longer* persistence -- the
    more conservative rule, which is the direction a tie should fall in.
    """
    admissible = [c for c in configs
                  if placebo_by_key[c.key].alerts_per_month <= ceiling_alerts_per_month]
    if not admissible:
        return Selection(config=None,
                         reason=(f"no configuration in the grid holds the placebo ceiling of "
                                 f"{ceiling_alerts_per_month:g} alerts/month; RR-09 round 5 "
                                 f"forbids weakening it, so nothing is frozen"),
                         considered=len(configs), admissible=0)

    def rank(c: Config) -> tuple:
        p = power_by_key[c.key]
        delay = p.median_delay
        return (-p.power, delay if delay is not None else math.inf, -c.delta, -c.persistence,
                c.key)

    best = min(admissible, key=rank)
    p = power_by_key[best.key]
    met = p.power >= power_target
    return Selection(
        config=best,
        reason=(f"highest power ({p.power:.3f}) among {len(admissible)} configurations under "
                f"the ceiling" + ("" if met else
                                  f"; the {power_target:.2f} power target is not met and is "
                                  f"reported as missed rather than bought by a weaker ceiling")),
        considered=len(configs), admissible=len(admissible))


# ---------------------------------------------------------------- protocol hash ----
def protocol_hash(rule_hash: str, calibration: Mapping[str, Any]) -> str:
    """Identity of what calibration ran under: the rule config plus the calibration protocol."""
    import json
    payload = json.dumps({"rule": rule_hash, "calibration": calibration}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()

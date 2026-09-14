"""`GOLD_CALIBRATION`: what the rule does to noise and to a decline of known size.

Quality, not reproducibility -- it measures the rule rather than the pipeline -- and so by
ADR-0011 it never blocks a *phase*. It blocks something else instead, which is the one
exception on the map: **a rule that fails calibration must not be frozen** (RR-13 §5). So the
verdict here is read by `scripts/freeze_rule.py`, not by `make eval-table`, and the two bars
it carries are both pre-registered:

* the placebo trigger rate must sit at or under the ceiling the capacity argument fixes;
* detection power at the primary effect size must reach its target.

Missing the power target is a **FAIL** and not a REPORTED result, because the freeze is a
decision and this is its precondition. Missing it does not license a lower ceiling: RR-09
round 5 settled that a rule which cannot reach power under an honest ceiling is reported as
such and routed through the investigate/watchlist split, never bought a pass by moving the
noise bar. `select()` enforces that upstream; this module only says so out loud.

Pure over the results `src/gold/calibration.py` computed: no Spark, no Postgres, no clock.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.common.evaluation import Verdict
from src.gold.calibration import PlaceboResult, PowerResult, Selection, wilson


def placebo_line(p: PlaceboResult, *, ceiling: float, block: int) -> str:
    ok = p.alerts_per_month <= ceiling
    return (f"GOLD_PLACEBO config={p.config.key} replicates={p.replicates} block_months={block} "
            f"alerts={p.alerts} evaluable_points={p.evaluable} "
            f"surveillance_months={p.surveillance_months} "
            f"alerts_per_month={p.alerts_per_month:.4f} ceiling={ceiling:g} "
            f"alerts_per_eligible_product_year={p.alerts_per_eligible_product_year:.5f} "
            f"verdict={'PASS' if ok else 'FAIL'}")


def power_line(p: PowerResult, *, target: float, window_points: int, primary: bool) -> str:
    lo, hi = wilson(p.detected, p.delivered)
    delay = p.median_delay
    return (f"GOLD_POWER config={p.config.key} mechanism={p.mechanism} effect={p.effect:g} "
            f"ramp_months={p.ramp_months} eligible={p.eligible} delivered={p.delivered} "
            f"detected={p.detected} power={p.power:.4f} ci=[{lo:.4f},{hi:.4f}] "
            f"median_delay_points={'-' if delay is None else f'{delay:g}'} "
            f"window_points={window_points} "
            + (f"target={target:g} verdict={'PASS' if p.power >= target else 'FAIL'}"
               if primary else "target=- verdict=REPORTED"))


def selection_line(s: Selection, *, ceiling: float, power_target: float) -> str:
    return (f"GOLD_CALIBRATION_SELECT considered={s.considered} admissible={s.admissible} "
            f"ceiling_alerts_per_month={ceiling:g} power_target={power_target:g} "
            f"chosen={s.config.key if s.config else 'none'} reason={s.reason!r}")


def agreement_line(*, points_reference: int, points_fast: int, alerts_reference: int,
                   alerts_fast: int) -> str:
    """The fast path agreed with `src/gold/rule.py` on the observed data, or it did not.

    Everything else here is measured with the lean evaluator. If it has drifted from the rule
    the project actually runs, every number above is about a rule nobody ships -- so this is a
    blocking constituent, and the only one that fails for a reason other than the rule's own
    behaviour.
    """
    ok = points_reference == points_fast and alerts_reference == alerts_fast
    return (f"GOLD_CALIBRATION_FASTPATH points_reference={points_reference} "
            f"points_fast={points_fast} alerts_reference={alerts_reference} "
            f"alerts_fast={alerts_fast} agrees={str(ok).lower()}")


def terminal_line(*, chosen: str, placebo_ok: bool, power: float | None, power_ok: bool,
                  fastpath_ok: bool, target: float, scope: str) -> str:
    ok = placebo_ok and power_ok and fastpath_ok
    return (f"GOLD_CALIBRATION scope={scope} chosen={chosen} "
            f"placebo_under_ceiling={str(placebo_ok).lower()} "
            f"power={'-' if power is None else f'{power:.4f}'} target={target:g} "
            f"power_target_met={str(power_ok).lower()} "
            f"fastpath_agrees={str(fastpath_ok).lower()} "
            f"verdict={'PASS' if ok else 'FAIL'}")


def verdict(*, selection: Selection, placebo_by_key: Mapping[str, PlaceboResult],
            primary_power: PowerResult | None, constituents: Sequence[str],
            ceiling: float, power_target: float, fastpath_ok: bool, scope: str) -> Verdict:
    """`GOLD_CALIBRATION` over one chosen configuration, or over the fact that none qualified."""
    chosen = selection.config
    placebo_ok = bool(chosen) and placebo_by_key[chosen.key].alerts_per_month <= ceiling
    power = primary_power.power if primary_power else None
    power_ok = power is not None and power >= power_target
    checks = (("a_configuration_holds_the_placebo_ceiling", placebo_ok),
              ("power_target_met_at_primary_effect", power_ok),
              ("fastpath_agrees_with_the_rule_module", fastpath_ok))
    ok = all(v for _, v in checks)
    terminal = terminal_line(chosen=chosen.key if chosen else "none", placebo_ok=placebo_ok,
                             power=power, power_ok=power_ok, fastpath_ok=fastpath_ok,
                             target=power_target, scope=scope)
    metric: dict[str, Any] = {
        "name": "detection_power_at_primary_effect",
        "value": round(power, 4) if power is not None else 0.0,
        "threshold": power_target, "direction": "gte"}
    if primary_power and primary_power.delivered:
        lo, hi = wilson(primary_power.detected, primary_power.delivered)
        metric["interval"] = [round(lo, 4), round(hi, 4)]
    return Verdict(gate_name="GOLD_CALIBRATION", status="PASS" if ok else "FAIL",
                   constituents=tuple(constituents), terminal=terminal, metric=metric,
                   checks=checks)

"""P3 Gold's two verdicts: the identity gate, and the analytical result it reports beside it.

`GOLD_GATE` is reproducibility -- the spine is complete, no review lands off it, and every
alert opened exactly one episode. It blocks.

`GOLD_ANALYTICAL` is quality -- how much of the eligible product population the frozen
decline rule actually alerts on. It has no bar to clear here: the rule's thresholds were
frozen in `conf/decline_rule.toml` before the holdout was opened, so the share is an outcome
to publish, not a target to hit. That is why it prints `verdict=REPORTED` and why the two
never share a field (ADR-0011).

Pure over the counts the gold job already computed: no Spark, no Postgres.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.common.evaluation import Verdict, repro_verdict

UNEVALUABLE_REASONS = ("baseline_reviews", "baseline_active_months", "recent_reviews",
                       "recent_active_months")


def constituent_lines(c: Mapping[str, Any]) -> list[str]:
    """The three counted lines the gate summarises, in the order gold prints them."""
    r = c["unevaluable_reasons"]
    e = c["episodes_by_closure"]
    return [
        (f"GOLD_SPINE products_total={c['products_total']} "
         f"products_materialised={c['products_materialised']} "
         f"below_min_reviews={c['products_below_min_reviews']} product_months={c['product_months']} "
         f"active_product_months={c['active_product_months']} reviews_on_spine={c['reviews_on_spine']}"),
        (f"GOLD_POINTS points={c['points']} evaluable={c['evaluable']} "
         f"condition_true={c['condition_true']} "
         f"alerts={c['alerts']} evaluable_products={c['evaluable_products']} "
         + " ".join(f"unevaluable_{k}={r.get(k, 0)}" for k in UNEVALUABLE_REASONS)),
        (f"GOLD_EPISODES episodes={c['episodes']} recovery={e['recovery']} gap={e['gap']} "
         f"end_of_data={e['end_of_data']}"),
    ]


def gate_checks(c: Mapping[str, Any]) -> list[tuple[str, bool]]:
    return [
        ("spine_complete", c["product_months"] == c["product_months_expected"]),
        ("reviews_on_spine", c["reviews_on_spine"] <= c["silver_rows"]),
        ("alerts_equal_episodes", c["alerts"] == c["episodes"]),
    ]


def gate_line(c: Mapping[str, Any], *, run_id: str, scope: str, rule_status: str,
              checks: list[tuple[str, bool]]) -> str:
    named = dict(checks)
    ok = all(named.values())
    return (f"GOLD_GATE run_id={run_id} scope={scope} "
            f"spine_complete={str(named['spine_complete']).lower()} "
            f"reviews_on_spine={c['reviews_on_spine']} silver_rows={c['silver_rows']} "
            f"alerts_equal_episodes={str(named['alerts_equal_episodes']).lower()} "
            f"rule_status={rule_status} "
            f"GOLD_GATE={'PASS' if ok else 'FAIL'}")


def verdict(c: Mapping[str, Any], *, run_id: str, scope: str, rule_status: str) -> Verdict:
    """`GOLD_GATE` over the counts the job computed. The analytical line is a separate verdict."""
    checks = gate_checks(c)
    return repro_verdict("GOLD_GATE", checks,
                         gate_line(c, run_id=run_id, scope=scope, rule_status=rule_status,
                                   checks=checks),
                         constituents=constituent_lines(c))


def analytical(c: Mapping[str, Any], *, rule_status: str, holdout_start: str,
               config_hash: str) -> Verdict:
    """`GOLD_ANALYTICAL`: the alert rate, reported against a rule frozen before it was measured.

    Before the freeze the number is a development figure over the pre-holdout period and says
    so; after it, it is the holdout share of eligible products. Either way the status is
    REPORTED -- there is no bar, so there is nothing here that could block a phase.
    """
    if rule_status == "frozen":
        eligible = c["holdout_eligible_products"]
        share = c["holdout_alerted_products"] / eligible if eligible else 0.0
        line = (f"GOLD_ANALYTICAL period=holdout eligible_products={eligible} "
                f"alerted_products={c['holdout_alerted_products']} share_of_eligible={share:.4f} "
                f"holdout_alerts={c['holdout_alerts']} holdout_episodes={c['holdout_episodes']} "
                f"rule_status=frozen rule_config_hash={config_hash[:12]} verdict=REPORTED")
        metric = {"name": "holdout_alerted_share_of_eligible", "value": round(share, 4),
                  "threshold": None, "direction": "none"}
        population = eligible
    else:
        line = (f"GOLD_ANALYTICAL period=development(before {holdout_start}) "
                f"evaluable_products={c['evaluable_products']} alerts={c['alerts']} "
                f"episodes={c['episodes']} rule_status=provisional "
                f"rule_config_hash={config_hash[:12]} verdict=REPORTED")
        metric = {"name": "development_alerts", "value": c["alerts"], "threshold": None,
                  "direction": "none"}
        population = c["evaluable_products"]
    # An empty population has no share to report. Printing 0.0000 would read as "the rule
    # fires on nothing" when the truth is "nothing was eligible" -- so it says that instead.
    if population <= 0:
        return Verdict(gate_name="GOLD_ANALYTICAL", status="NOT_RUN", constituents=(),
                       terminal=line, metric=metric,
                       checks=(("population_non_empty", False),),
                       cut_reason=("no eligible products in this population, so there is no "
                                   "alert rate to report"))
    return Verdict(gate_name="GOLD_ANALYTICAL", status="REPORTED", constituents=(),
                   terminal=line, metric=metric,
                   checks=(("population_non_empty", True),))


def analytical_population(c: Mapping[str, Any], *, rule_status: str) -> dict[str, Any]:
    """What the analytical number was measured over -- named, so the table can say."""
    if rule_status == "frozen":
        return {"name": "holdout eligible products", "n": c["holdout_eligible_products"],
                "holdout_points": c["holdout_points"], "holdout_evaluable": c["holdout_evaluable"]}
    return {"name": "development evaluable products", "n": c["evaluable_products"],
            "points": c["points"], "evaluable": c["evaluable"]}

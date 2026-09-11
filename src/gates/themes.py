"""P6's two verdicts: `THEMES_GATE` blocks, `THEMES_QUALITY` reports (ADR-0011, ticket 10).

The split is the whole point of this module. Until ticket 10 the themes gate printed one
`THEMES_GATE=PASS|FAIL` over seven constituents, six of which were reproducibility claims and
one of which was the macro-F1 against ADR-0003's 0.70 bar. That made a disappointing quality
number look like a broken phase -- and a broken phase is something you reopen. Reopening P6
after seeing the audit number is precisely the act RR-21, ADR-0001 and the audit seal exist to
refuse, so the bar moves out of the blocking gate and keeps its own line instead:

  THEMES_GATE      reproducibility -- the taxonomy, the frames, the frozen prompt, the
                   reference labels, the audit labelling run and the seal. Every one is a
                   claim that the number *means what it says*, and every one blocks.
  THEMES_QUALITY   the number itself, against the bar that was frozen before it existed.
                   It prints `verdict=PASS|FAIL` and blocks nothing. A miss sets P6 to
                   `built, evaluated, below target`.
  THEMES_AGREEMENT the published agreement between the machine-made ground truth and
                   Philip's blind 50 (RR-21), with Wilson intervals. REPORTED, never barred.

Neither bar moves here: `MACRO_F1_BAR` and `MIN_RECALL_BAR` are imported from
`src.ai.audit_seal`, where they sit *inside* the audit fingerprint, so lowering one after the
fact is as visible as refitting a threshold.

Three reporting decisions are made here rather than at the writer, because each of them is a
statement about what may honestly be published:

* **The representative stratum publishes `NOT_RUN`, not an interval.** No theme reaches
  `min_support` in its 80 rows, so `macro_f1` is `None` there for all three systems --
  correctly. The sealed artefacts nonetheless carry a `bootstrap_95` beside that `None`,
  because the resampler averaged over whichever draws happened to contain a supported theme;
  star-only reads `[0.483, 0.800]` in that stratum, *above* its own overall score. An interval
  around an undefined statistic is not a weak number, it is not a number, so it is withheld
  and the reason is printed. The artefacts themselves are **not** regenerated: their sha256s
  are in the seal, and rewriting a scored artefact after the set is opened is the exact act
  the seal refuses. This is a reporting fix, and it is the only kind available.
* **The three-way comparison is a finding with its own line.** Ticket 10 was drafted expecting
  the labeller's interval to overlap the star-only floor's, as it did on development. On the
  held-out set it does not. The comparison prints both intervals and says which pairs separate,
  so the finding is read off the gate rather than assembled by hand in a slide.
* **Repeat-kappa is `NOT_RUN` with a written reason** and lives in `conf/lineage_chain.toml`
  as a cut capability. A deterministic labeller re-run gives back its own labels; the kappa
  that follows is 1.0 and means nothing.

Pure over already-loaded facts: no Spark, no Postgres, no filesystem.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.ai.audit_seal import MACRO_F1_BAR, MIN_RECALL_BAR
from src.common.evaluation import Verdict, repro_verdict

TAXONOMY_CEILING = 10

#: Why the representative stratum publishes no number. Printed verbatim and stored as a note.
REPRESENTATIVE_NOT_RUN = ("no theme reaches min_support in the 80 prevalence-representative "
                          "rows, so macro-F1 is undefined there and the interval beside it in "
                          "the sealed artefact is an artefact of the resampler, not a result")

#: Why no repeat-run kappa is published (RR-21). Mirrors `conf/lineage_chain.toml`.
REPEAT_KAPPA_NOT_RUN = ("the labeller is deterministic (temperature 0, fixed seed), so a "
                        "repeat-run kappa measures nothing; the published agreement number is "
                        "THEMES_AGREEMENT (ADR-0011, RR-21)")


def b(x: Any) -> str:
    return str(bool(x)).lower()


def _f(v: float | None, places: int = 4) -> str:
    return "none" if v is None else str(round(v, places))


def _interval(pair: Sequence[float | None] | None) -> str:
    """`[lo,hi]`, or `none` when either end is missing. Never half an interval."""
    if not pair or pair[0] is None or pair[1] is None:
        return "none"
    return f"[{round(pair[0], 4)},{round(pair[1], 4)}]"


# ============================================================ THEMES_GATE (blocks) ====
def taxonomy_line(f: Mapping[str, Any]) -> str:
    return (f"THEMES_TAXONOMY version={f['version']} themes={f['themes']} "
            f"ceiling={TAXONOMY_CEILING} hash={f['file_hash'][:12]} "
            f"recorded={(f['recorded_hash'] or 'none')[:12]} "
            f"unchanged_since_audit={b(f['unchanged'])} ok={b(f['ok'])}")


def protocol_line(f: Mapping[str, Any]) -> str:
    sizes = " ".join(f"{k}={f['sizes'].get(k, 0)}/{v}" for k, v in sorted(f["expected"].items()))
    return (f"THEMES_PROTOCOL status={f['status']} hash={f['config_hash'][:12]} {sizes} "
            f"reviews_in_two_samples={f['overlap']} ok={b(f['ok'])}")


def prompt_line(f: Mapping[str, Any]) -> str:
    if not f.get("frozen"):
        return ("THEMES_PROMPT frozen=none ok=false "
                "(conf/theme-label-spec.json has no frozen_prompt block)")
    return (f"THEMES_PROMPT frozen={f['version']} freeze_commit={f['freeze_commit'][:8] or 'none'} "
            f"in_spec={b(f['in_spec'])} audit_used={f['audit_used'] or 'none'} "
            f"frozen_before_audit={b(f['frozen_before_audit'])} ok={b(f['ok'])}")


def reference_line(f: Mapping[str, Any]) -> str:
    if not f.get("table_present", True):
        return "THEMES_REFERENCE table=missing ok=false"
    return (f"THEMES_REFERENCE source=agent_reference rows={f['rows']} distinct={f['distinct']} "
            f"expected={f['expected']} human_rows={f['human_rows']} ok={b(f['ok'])}")


def audit_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "THEMES_AUDIT run=none ok=false"
    return (f"THEMES_AUDIT run_id={f['run_id']} model={f['model_id']} config={f['config'][:12]} "
            f"rows={f['rows']}/{f['expected']} terminal={f['terminal']} "
            f"ok_labels={f['succeeded']} abstained={f['model_abstained']} "
            f"parse_failed={f['parse_failed']} api_failed={f['api_failed']} ok={b(f['ok'])}")


def seal_lines(f: Mapping[str, Any]) -> list[str]:
    """The seal's own line, then one line per moved freeze, changed artefact and system.

    A seal that cannot be read is a failure with a named cause, never a silent absence: the
    three unreadable cases each print their own reason, because "the audit has not been
    opened" and "the freezes no longer parse" have different fixes.
    """
    if not f.get("present"):
        return [("THEMES_SEAL seal=missing opened=false ok=false "
                 "(the audit set has not been opened; run `make audit-once`, ticket 09)")]
    if not f.get("freezes_readable", True):
        return [(f"THEMES_SEAL seal={f['sealed'][:12]} freezes_unreadable=true ok=false "
                 f"({f['unreadable_reason']})")]
    lines = [(f"THEMES_SEAL opened_at={f['opened_at']} commit={f['commit'][:8]} "
              f"sealed={f['sealed'][:12]} today={f['today'][:12]} systems={len(f['systems'])} "
              f"freezes_moved={len(f['moved'])} artefacts_changed={len(f['artefacts_changed'])} "
              f"verdict={'PASS' if f['verdict_passed'] else 'FAIL'} ok={b(f['ok'])}")]
    lines += [f"THEMES_SEAL_MOVED {d}" for d in f["moved"]]
    lines += [f"THEMES_SEAL_ARTEFACT {d}" for d in f["artefacts_changed"]]
    for s in f["systems"]:
        lines.append(f"THEMES_SEAL_SYSTEM {s['system']:<11} macro_f1={_f(s['macro_f1'])} "
                     f"min_supported_recall={s['min_supported_recall']} "
                     f"reviews={s['reviews']} runs={len(s['run_ids'])} artefact={s['artefact']}")
    return lines


def gate_checks(facts: Mapping[str, Any]) -> tuple[tuple[str, bool], ...]:
    """The six reproducibility constituents, in the order the gate prints them.

    `THEMES_SCORE` is deliberately absent: it is the quality number, and a quality number that
    can fail a phase gate is a standing invitation to retune a frozen protocol (ADR-0011).
    """
    return (("taxonomy", bool(facts["taxonomy"]["ok"])),
            ("protocol", bool(facts["protocol"]["ok"])),
            ("prompt", bool(facts["prompt"]["ok"])),
            ("reference", bool(facts["reference"]["ok"])),
            ("audit_run", bool(facts["audit"]["ok"])),
            ("seal", bool(facts["seal"]["ok"])))


def gate_line(*, scope: str, checks: Sequence[tuple[str, bool]]) -> str:
    failed = [name for name, ok in checks if not ok]
    return (f"THEMES_GATE scope={scope} constituents_ok={sum(1 for _, ok in checks if ok)}/"
            f"{len(checks)} failed={','.join(failed) or 'none'} kind=reproducibility "
            f"THEMES_GATE={'PASS' if not failed else 'FAIL'}")


def verdict(facts: Mapping[str, Any], *, scope: str) -> Verdict:
    """`THEMES_GATE` over the six re-derived reproducibility claims."""
    checks = gate_checks(facts)
    constituents = [taxonomy_line(facts["taxonomy"]), protocol_line(facts["protocol"]),
                    prompt_line(facts["prompt"]), reference_line(facts["reference"]),
                    audit_line(facts["audit"]), *seal_lines(facts["seal"])]
    return repro_verdict("THEMES_GATE", checks, gate_line(scope=scope, checks=checks),
                         constituents=constituents)


# ======================================================== THEMES_QUALITY (reports) ====
def theme_lines(per_theme: Sequence[Mapping[str, Any]]) -> list[str]:
    def n(v: float | None) -> str:
        return "  -  " if v is None else f"{v:.3f}"
    return [(f"THEMES_THEME {t['theme_id']:<20} support={t['support']:>3} tp={t['tp']:>3} "
             f"fp={t['fp']:>3} fn={t['fn']:>3} p={n(t['precision'])} r={n(t['recall'])} "
             f"f1={n(t['f1'])} supported={b(t['supported'])}") for t in per_theme]


def stratum_lines(score: Mapping[str, Any]) -> list[str]:
    """One line per stratum. A stratum with no supported theme publishes no interval.

    The enriched stratum has supported themes and reports normally. The representative one
    does not, and the sealed artefact's interval beside its `None` point estimate is the
    resampler averaging over the draws that happened to contain a supported theme. Publishing
    it would put a number above the system's own overall score into the record, so the line
    says `NOT_RUN` and carries the reason instead.
    """
    lines = []
    for key in ("enriched", "representative"):
        s = score.get(key)
        if s is None:
            continue
        if s["macro_f1"] is None:
            lines.append(f"THEMES_SUBSET {key} reviews={s['reviews']} macro_f1=NOT_RUN "
                         f"bootstrap95=withheld supported={len(s['supported_themes'])} "
                         f"failure_rate={s['coverage']['failure_rate']} "
                         f"reason=\"{REPRESENTATIVE_NOT_RUN}\"")
        else:
            lines.append(f"THEMES_SUBSET {key} reviews={s['reviews']} "
                         f"macro_f1={_f(s['macro_f1'])} bootstrap95={_interval(s['bootstrap_95'])} "
                         f"supported={len(s['supported_themes'])} "
                         f"failure_rate={s['coverage']['failure_rate']} (reported, not gated)")
    return lines


def disjoint(a: Sequence[float | None] | None, b_: Sequence[float | None] | None) -> bool | None:
    """Do two percentile intervals fail to overlap? `None` when either is missing.

    Non-overlapping 95% intervals are a conservative way to say two systems separated -- it is
    a stricter claim than a test of the difference, not a weaker one -- and conservatism is the
    right direction for a number that goes in a submission.
    """
    if not a or not b_ or None in tuple(a) + tuple(b_):
        return None
    return a[1] < b_[0] or b_[1] < a[0]


def comparison_line(systems: Mapping[str, Mapping[str, Any] | None], *, sample: str,
                    development: Mapping[str, Any] | None = None) -> str:
    """`THEMES_COMPARISON`: all three systems on one line, with the separation stated.

    ADR-0002's pass rule for the MLlib arm is a comparison, not a bar, and RR-19 made the
    star-only floor the thing the labeller has to beat to have earned its place. Both readings
    need the same three intervals side by side, so they are printed once, here, rather than
    recovered from three artefacts by whoever writes the slide.
    """
    llm = (systems.get("llm") or {}).get("overall") or {}
    clf = (systems.get("classifier") or {}).get("overall") or {}
    star = (systems.get("star_only") or {}).get("overall") or {}
    parts = [f"THEMES_COMPARISON set={sample}"]
    for name, s in (("llm", llm), ("classifier", clf), ("star_only", star)):
        parts.append(f"{name}={_f(s.get('macro_f1'))} {_interval(s.get('bootstrap_95'))}")
    for name, s in (("llm", llm), ("classifier", clf)):
        d = disjoint(s.get("bootstrap_95"), star.get("bootstrap_95"))
        parts.append(f"{name}_vs_star={'none' if d is None else ('disjoint' if d else 'overlapping')}")
    if development is not None:
        d = disjoint((development.get("llm") or {}).get("bootstrap_95"),
                     (development.get("star_only") or {}).get("bootstrap_95"))
        parts.append("development_llm_vs_star="
                     + ("none" if d is None else ("disjoint" if d else "overlapping")))
    return " ".join(parts)


def comparison_finding(systems: Mapping[str, Mapping[str, Any] | None], *,
                       development: Mapping[str, Any] | None = None) -> str:
    """The finding in one sentence, so the artefact carries the claim and not only the numbers."""
    llm = (systems.get("llm") or {}).get("overall") or {}
    star = (systems.get("star_only") or {}).get("overall") or {}
    clf = (systems.get("classifier") or {}).get("overall") or {}
    sep = disjoint(llm.get("bootstrap_95"), star.get("bootstrap_95"))
    clf_sep = disjoint(clf.get("bootstrap_95"), star.get("bootstrap_95"))
    dev_sep = None if development is None else disjoint(
        (development.get("llm") or {}).get("bootstrap_95"),
        (development.get("star_only") or {}).get("bootstrap_95"))
    if sep is None:
        return "the three-way comparison is incomplete: an interval is missing"
    if sep:
        head = (f"on the held-out audit set the labeller's interval {_interval(llm['bootstrap_95'])} "
                f"and the star-only floor's {_interval(star['bootstrap_95'])} are disjoint, so the "
                "labeller is distinguishably better than predicting themes off the star rating")
        if dev_sep is False:
            head += ("; on development the two overlapped, and the holdout reverses that earlier, "
                     "smaller-sample reading rather than confirming it")
    else:
        head = (f"the labeller's interval {_interval(llm['bootstrap_95'])} still overlaps the "
                f"star-only floor's {_interval(star['bootstrap_95'])}: a weak local model did not "
                "separate from predicting themes off the star rating on a J-shaped corpus")
    if clf_sep is False:
        head += (f"; the MLlib classifier's {_interval(clf['bootstrap_95'])} does overlap the floor "
                 "and is not distinguishable from it")
    return head


def quality_line(o: Mapping[str, Any], *, scope: str, passed: bool) -> str:
    return (f"THEMES_QUALITY macro_f1={_f(o['macro_f1'])} bar={MACRO_F1_BAR} "
            f"bootstrap95={_interval(o['bootstrap_95'])} "
            f"min_supported_recall={_f(o['min_supported_recall'])} recall_bar={MIN_RECALL_BAR} "
            f"supported={len(o['supported_themes'])}/{TAXONOMY_CEILING} reviews={o['reviews']} "
            f"failure_rate={o['coverage']['failure_rate']} scope={scope} blocks=false "
            f"verdict={'PASS' if passed else 'FAIL'}")


def quality(score: Mapping[str, Any] | None, *, scope: str, sample: str = "audit",
            systems: Mapping[str, Mapping[str, Any] | None] | None = None,
            development: Mapping[str, Any] | None = None) -> Verdict:
    """`THEMES_QUALITY`: the published number against the bar frozen before it existed.

    Blocks nothing. A miss makes P6 `built, evaluated, below target` (ADR-0011) -- the phase is
    built and measured, and the measurement is the deliverable whichever way it reads.
    """
    if score is None:
        return Verdict(gate_name="THEMES_QUALITY", status="NOT_RUN", constituents=(),
                       terminal=(f"THEMES_QUALITY macro_f1=none bar={MACRO_F1_BAR} scope={scope} "
                                 f"blocks=false verdict=NOT_RUN"),
                       metric={"name": "macro_f1", "value": 0.0, "threshold": MACRO_F1_BAR,
                               "direction": "gte"},
                       checks=(("audit_scored", False),),
                       cut_reason=("the audit set has not been scored, so P6 has no published "
                                   "number; run `make audit-once` (ticket 09)"))
    o = score["overall"]
    m, r = o["macro_f1"], o["min_supported_recall"]
    checks = (("macro_f1_at_or_above_bar", m is not None and m >= MACRO_F1_BAR),
              ("min_supported_recall_at_or_above_bar", r is not None and r >= MIN_RECALL_BAR))
    passed = all(ok for _, ok in checks)
    constituents = [*theme_lines(o["per_theme"]), *stratum_lines(score)]
    if systems:
        constituents.append(comparison_line(systems, sample=sample, development=development))
    metric: dict[str, Any] = {"name": "macro_f1", "value": round(m, 4) if m is not None else 0.0,
                              "threshold": MACRO_F1_BAR, "direction": "gte"}
    if o["bootstrap_95"] and None not in tuple(o["bootstrap_95"]):
        metric["interval"] = [round(x, 4) for x in o["bootstrap_95"]]
    return Verdict(gate_name="THEMES_QUALITY", status="PASS" if passed else "FAIL",
                   constituents=tuple(constituents),
                   terminal=quality_line(o, scope=scope, passed=passed), metric=metric,
                   checks=checks)


def quality_notes(score: Mapping[str, Any], *,
                  systems: Mapping[str, Mapping[str, Any] | None] | None = None,
                  development: Mapping[str, Any] | None = None) -> list[str]:
    """What belongs beside the number in the table: the finding, the plumbing, the strata."""
    o = score["overall"]
    cov = o["coverage"]
    notes = []
    if systems:
        notes.append(comparison_finding(systems, development=development))
    notes.append(f"parse-failure census: failure_rate={cov['failure_rate']} "
                 f"(parse_failed={cov['parse_failed']} api_failed={cov['api_failed']} "
                 f"absent={cov['absent']} abstained={cov['model_abstained']} of {o['reviews']}); "
                 "each failure scores as an empty prediction, so it is a recall loss that "
                 "precedes any judgement about theme quality")
    unsupported = [t["theme_id"] for t in o["per_theme"] if not t["supported"]]
    if unsupported:
        verb = "falls" if len(unsupported) == 1 else "fall"
        notes.append(f"macro-F1 averages over {len(o['supported_themes'])} supported themes; "
                     f"{', '.join(unsupported)} {verb} below min_support "
                     f"{score['min_support']} in the reference labels and {'is' if len(unsupported) == 1 else 'are'} "
                     "excluded from the headline, with counts printed")
    if (score.get("representative") or {}).get("macro_f1") is None:
        notes.append("representative stratum: NOT_RUN -- " + REPRESENTATIVE_NOT_RUN)
    return notes


# ===================================================== THEMES_AGREEMENT (reports) ====
def agreement_line(a: Mapping[str, Any] | None, *, scope: str) -> str:
    if a is None:
        return (f"THEMES_AGREEMENT n=0 expected={AGREEMENT_ROWS} scope={scope} blocks=false "
                f"verdict=NOT_RUN")
    return (f"THEMES_AGREEMENT n={a['n']} reviews={a['reviews']} "
            f"agreement={_f(a['agreement'])} wilson95={_interval(a['wilson_95'])} "
            f"kappa={_f(a['kappa'])} themes={a['themes']} scope={scope} blocks=false "
            f"verdict=REPORTED")


#: RR-21 / conf/theme_sampling.toml `audit.adjudication_rows`. Repeated here only for the
#: NOT_RUN line, which must say what is missing before the config that defines it is loaded.
AGREEMENT_ROWS = 50


def agreement_theme_lines(per_theme: Sequence[Mapping[str, Any]]) -> list[str]:
    return [(f"THEMES_AGREEMENT_THEME {t['theme_id']:<20} n={t['n']:>3} agree={t['agree']:>3} "
             f"rate={_f(t['agreement'], 3)} wilson95={_interval(t['wilson_95'])} "
             f"both={t['both']:>3} agent_only={t['agent_only']:>3} human_only={t['human_only']:>3} "
             f"kappa={_f(t['kappa'], 3)}") for t in per_theme]


def agreement(a: Mapping[str, Any] | None, *, scope: str, cut_reason: str | None = None) -> Verdict:
    """`THEMES_AGREEMENT`: agent ground truth against Philip's blind 50 (RR-21).

    RR-21 reversed the ground-truth owner -- the agent labels all 400, Philip hand-labels a
    stratified 50 of the audit set -- explicitly so the correlated-error risk of machine-made
    ground truth is **priced rather than caveated**. The price is this number. It is REPORTED
    and never barred: there was no agreement threshold frozen before it was measured, and
    inventing one now is what the whole protocol refuses.

    Until the 50 are labelled the capability publishes `NOT_RUN` with the reason, which is the
    contract's way of saying "this is owed and named" rather than leaving it silent.
    """
    if a is None:
        return Verdict(gate_name="THEMES_AGREEMENT", status="NOT_RUN", constituents=(),
                       terminal=agreement_line(None, scope=scope),
                       metric={"name": "agreement", "value": 0.0, "threshold": None,
                               "direction": "none"},
                       checks=(("human_subset_labelled", False),),
                       cut_reason=cut_reason or (
                           f"Philip's blind stratified {AGREEMENT_ROWS} of the audit set are drawn "
                           "and exported but not yet hand-labelled, so no agreement number exists "
                           "(RR-21); the draw is frozen and cannot be re-tuned once it is"))
    metric: dict[str, Any] = {"name": "agreement", "value": round(a["agreement"], 4),
                              "threshold": None, "direction": "none"}
    if a["wilson_95"] and None not in tuple(a["wilson_95"]):
        metric["interval"] = [round(x, 4) for x in a["wilson_95"]]
    return Verdict(gate_name="THEMES_AGREEMENT", status="REPORTED",
                   constituents=tuple(agreement_theme_lines(a["per_theme"])),
                   terminal=agreement_line(a, scope=scope), metric=metric,
                   checks=(("human_subset_labelled", True),))

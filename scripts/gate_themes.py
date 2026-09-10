"""Themes gate: re-derive every P6 constituent, then print `THEMES_GATE=PASS|FAIL`.

Constituents, each on its own line:

  THEMES_TAXONOMY   conf/theme-taxonomy.json is v1 with ten themes, and the taxonomy hash the
                    audit run recorded equals the file's hash today
  THEMES_PROTOCOL   conf/theme_sampling.toml is frozen; discovery, development, training_pool
                    and audit are drawn, disjoint, and at their protocol sizes
  THEMES_PROMPT     conf/theme-label-spec.json names a frozen prompt, the freeze commit is an
                    ancestor of the audit run's commit, and the audit run used that prompt
  THEMES_REFERENCE  every audit review carries exactly one agent_reference label; provenance is
                    agent_reference, never human
  THEMES_AUDIT      the audit labelling run is present, terminal for every review, and its
                    inference_config_hash matches the frozen configuration
  THEMES_SEAL       eval/themes/audit-seal.json exists, every frozen thing the audit numbers
                    depend on is byte-for-byte what it was when the set was opened, and the
                    score artefacts the seal names are the ones on disk
  THEMES_SCORE      macro-F1 and the minimum supported-theme recall, against the rule frozen in
                    ADR-0003 *before* any of these numbers existed

The pass rule does not move: `audit macro-F1 >= 0.70` and `no supported-theme recall < 0.50`.
A miss is reported FAIL with the per-theme table; nothing is re-tuned after seeing a result.

Exit 0 on PASS, 1 otherwise.  Run:  ./run.sh python scripts/gate_themes.py [--scope full]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any

from pyspark.sql import functions as F

from src.ai.audit_seal import (
    MACRO_F1_BAR,
    MIN_RECALL_BAR,
    collect_freezes,
    fingerprint,
    freezes_moved,
    load_seal,
    seal_matches_artefacts,
)
from src.ai.classifier_spec import llm_artefact_stem, score_artefact_name
from src.ai.labels import load_spec, load_taxonomy
from src.ai.theme_labels import table_name
from src.common import config as C
from src.common import runs
from src.common.spark import build
from src.gold.controls import load_protocol
from src.spark.theme_samples import table_names as sample_tables

# The bars are ADR-0003's, frozen 2026-09-06 and unmoved through RR-19, RR-21 and ticket 06.
# They live in src/ai/audit_seal.py because they are *inside* the audit fingerprint: a bar
# lowered after the audit invalidates the measurement exactly as a refitted cut does, and a
# rule the seal cannot see is a rule the seal cannot protect.
TAXONOMY_CEILING = 10
SCORES = C.PROJECT_ROOT / "eval" / "themes"


def b(x: Any) -> str:
    return str(bool(x)).lower()


def check_taxonomy(audit_run: dict[str, Any] | None) -> bool:
    tax = load_taxonomy()
    n = len(tax.themes)
    recorded = (audit_run or {}).get("params", {}).get("taxonomy_hash")
    unchanged = recorded == tax.file_hash if recorded else False
    ok = tax.version == "1" and n == TAXONOMY_CEILING and unchanged
    print(f"THEMES_TAXONOMY version={tax.version} themes={n} ceiling={TAXONOMY_CEILING} "
          f"hash={tax.file_hash[:12]} recorded={(recorded or 'none')[:12]} "
          f"unchanged_since_audit={b(unchanged)} ok={b(ok)}")
    return ok


def check_protocol(spark, scope: str, category: str) -> bool:
    protocol = load_protocol()
    names = sample_tables(scope)
    if not spark.catalog.tableExists(names["assignments"]):
        print(f"THEMES_PROTOCOL status={protocol.status} assignments=missing ok=false")
        return False
    asg = spark.table(names["assignments"])
    sizes = {r["sample_name"]: r["n"] for r in
             asg.groupBy("sample_name").agg(F.count("*").alias("n")).collect()}
    expected = {"discovery": protocol.discovery.size,
                **{k: f.size for k, f in protocol.frames.items()}}
    right = all(sizes.get(k) == v for k, v in expected.items())
    overlap = (asg.groupBy("review_id").agg(F.countDistinct("sample_name").alias("s"))
               .filter(F.col("s") > 1).count())
    ok = protocol.frozen and right and overlap == 0
    print(f"THEMES_PROTOCOL status={protocol.status} hash={protocol.config_hash[:12]} "
          + " ".join(f"{k}={sizes.get(k, 0)}/{v}" for k, v in sorted(expected.items()))
          + f" reviews_in_two_samples={overlap} ok={b(ok)}")
    return ok


def _commit_is_ancestor(older: str, newer: str) -> bool:
    if not older or not newer:
        return False
    r = subprocess.run(["git", "merge-base", "--is-ancestor", older, newer],
                       cwd=C.PROJECT_ROOT, capture_output=True, check=False)
    return r.returncode == 0


def check_prompt(spec, audit_run: dict[str, Any] | None) -> tuple[bool, str | None]:
    frozen = spec.raw.get("frozen_prompt")
    if not frozen:
        print("THEMES_PROMPT frozen=none ok=false "
              "(conf/theme-label-spec.json has no frozen_prompt block)")
        return False, None
    name, version, commit = frozen["name"], frozen["version"], frozen.get("freeze_commit", "")
    matches_spec = spec.prompts.get(name) is not None and spec.prompts[name].version == version
    used = (audit_run or {}).get("params", {}).get("prompt_version")
    used_frozen = used == version
    before = _commit_is_ancestor(commit, (audit_run or {}).get("git_commit_sha", ""))
    ok = matches_spec and used_frozen and before
    print(f"THEMES_PROMPT frozen={version} freeze_commit={commit[:8] or 'none'} "
          f"in_spec={b(matches_spec)} audit_used={used or 'none'} "
          f"frozen_before_audit={b(before)} ok={b(ok)}")
    return ok, name


def check_reference(spark, scope: str, audit_size: int) -> bool:
    table = table_name(scope)
    if not spark.catalog.tableExists(table):
        print("THEMES_REFERENCE table=missing ok=false")
        return False
    ref = spark.table(table).filter((F.col("budget_line") == "audit")
                                    & (F.col("label_source") == "agent_reference"))
    rows, distinct = ref.count(), ref.select("source_review_id").distinct().count()
    human = spark.table(table).filter(F.col("label_source") == "human").count()
    ok = rows == audit_size and distinct == audit_size
    print(f"THEMES_REFERENCE source=agent_reference rows={rows} distinct={distinct} "
          f"expected={audit_size} human_rows={human} ok={b(ok)}")
    return ok


def check_audit_run(spark, scope: str, audit_run: dict[str, Any] | None, audit_size: int) -> bool:
    if audit_run is None:
        print("THEMES_AUDIT run=none ok=false")
        return False
    c = audit_run["counts"]
    table = table_name(scope)
    config = audit_run["inputs"]["spec"]["config_hash"]
    got = spark.table(table).filter((F.col("budget_line") == "audit")
                                    & (F.col("label_source") == "local_llm")
                                    & (F.col("inference_config_hash") == config))
    rows = got.count()
    terminal = got.filter(F.col("label_status").isin("succeeded", "model_abstained",
                                                     "parse_failed", "api_failed")).count()
    ok = rows == audit_size and terminal == rows
    print(f"THEMES_AUDIT run_id={audit_run['run_id']} model={audit_run['params'].get('model_id')} "
          f"config={config[:12]} rows={rows}/{audit_size} terminal={terminal} "
          f"ok_labels={c.get('succeeded')} abstained={c.get('model_abstained')} "
          f"parse_failed={c.get('parse_failed')} api_failed={c.get('api_failed')} ok={b(ok)}")
    return ok


def check_seal() -> bool:
    """The audit was opened once, under freezes that have not moved since (ticket 09).

    THEMES_SCORE reads a number off a file. This is what says the number still means what it
    meant when it was measured: the prompt, the cuts, the star thresholds, the taxonomy and
    the pass rule are re-derived from today's config files and compared against the material
    the seal recorded. Differences are printed by name, because "the fingerprint differs" only
    tells a reader that something moved, not what.
    """
    seal = load_seal()
    if seal is None:
        print("THEMES_SEAL seal=missing opened=false ok=false "
              "(the audit set has not been opened; run `make audit-once`, ticket 09)")
        return False
    try:
        current = collect_freezes()
    except ValueError as exc:
        print(f"THEMES_SEAL seal={seal['freeze_fingerprint'][:12]} freezes_unreadable=true "
              f"ok=false ({exc})")
        return False
    moved = freezes_moved(seal["freezes"], current)
    artefacts = seal_matches_artefacts(seal, scores_dir=SCORES)
    now = fingerprint(current)
    ok = not moved and not artefacts and now == seal["freeze_fingerprint"]
    print(f"THEMES_SEAL opened_at={seal['opened_at']} commit={seal.get('git_commit_sha', '')[:8]} "
          f"sealed={seal['freeze_fingerprint'][:12]} today={now[:12]} "
          f"systems={len(seal['systems'])} freezes_moved={len(moved)} "
          f"artefacts_changed={len(artefacts)} verdict={'PASS' if seal['verdict']['passed'] else 'FAIL'} "
          f"ok={b(ok)}")
    for diff in moved:
        print(f"THEMES_SEAL_MOVED {diff}")
    for fail in artefacts:
        print(f"THEMES_SEAL_ARTEFACT {fail}")
    for sysrow in seal["systems"]:
        sm = sysrow["macro_f1"]
        print(f"THEMES_SEAL_SYSTEM {sysrow['system']:<11} "
              f"macro_f1={'none' if sm is None else round(sm, 4)} "
              f"min_supported_recall={sysrow['min_supported_recall']} "
              f"reviews={sysrow['reviews']} runs={len(sysrow['run_ids'])} "
              f"artefact={sysrow['artefact']}")
    return ok


def check_score(score: dict[str, Any] | None) -> bool:
    if score is None:
        print(f"THEMES_SCORE artefact=missing macro_f1=none bar={MACRO_F1_BAR} ok=false")
        return False
    o = score["overall"]
    m, r = o["macro_f1"], o["min_supported_recall"]
    lo, hi = o["bootstrap_95"]
    ok = m is not None and m >= MACRO_F1_BAR and r is not None and r >= MIN_RECALL_BAR
    worst = min((t for t in o["per_theme"] if t["supported"]),
                key=lambda t: (t["recall"] if t["recall"] is not None else 1.0), default=None)
    print(f"THEMES_SCORE macro_f1={None if m is None else round(m, 4)} bar={MACRO_F1_BAR} "
          f"bootstrap95=[{'' if lo is None else round(lo, 4)},{'' if hi is None else round(hi, 4)}] "
          f"min_supported_recall={None if r is None else round(r, 4)} recall_bar={MIN_RECALL_BAR} "
          f"worst_theme={worst['theme_id'] if worst else 'none'} "
          f"supported={len(o['supported_themes'])}/10 reviews={o['reviews']} "
          f"failure_rate={o['coverage']['failure_rate']} ok={b(ok)}")
    for t in o["per_theme"]:
        f = lambda v: "  -  " if v is None else f"{v:.3f}"
        print(f"THEMES_THEME {t['theme_id']:<20} support={t['support']:>3} tp={t['tp']:>3} "
              f"fp={t['fp']:>3} fn={t['fn']:>3} p={f(t['precision'])} r={f(t['recall'])} "
              f"f1={f(t['f1'])} supported={b(t['supported'])}")
    for key in ("enriched", "representative"):
        if key in score:
            s = score[key]
            sm = s["macro_f1"]
            print(f"THEMES_SUBSET {key} reviews={s['reviews']} "
                  f"macro_f1={None if sm is None else round(sm, 4)} "
                  f"supported={len(s['supported_themes'])} "
                  f"failure_rate={s['coverage']['failure_rate']} (reported, not gated)")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    spec = load_spec()
    protocol = load_protocol()
    audit_size = protocol.frames["audit"].size
    audit_run = runs.latest_success("theme_labels_llm", category=args.category,
                                    data_scope=args.scope, params_match={"budget_line": "audit"})
    frozen = spec.raw.get("frozen_prompt") or {}
    score_path = SCORES / score_artefact_name(
        sample="audit", stem=llm_artefact_stem(prompt_version=frozen.get("version", "none"),
                                               model_id=spec.model_id))
    score = json.loads(score_path.read_text()) if score_path.exists() else None

    spark = build("gate_themes", cores="local[2]", driver_memory="2g")
    try:
        results = [
            check_taxonomy(audit_run),
            check_protocol(spark, args.scope, args.category),
            check_prompt(spec, audit_run)[0],
            check_reference(spark, args.scope, audit_size),
            check_audit_run(spark, args.scope, audit_run, audit_size),
            check_seal(),
            check_score(score),
        ]
    finally:
        spark.stop()
    passed = all(results)
    print(f"THEMES_GATE={'PASS' if passed else 'FAIL'} constituents_ok={sum(results)}/{len(results)} "
          f"scope={args.scope} bar_macro_f1={MACRO_F1_BAR} bar_min_recall={MIN_RECALL_BAR}")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

"""P6's gates: re-derive every reproducibility claim, then publish the number beside it.

Two verdicts, printed in this order and written to two artefacts (ADR-0011, ticket 10):

  THEMES_GATE      **blocks.** Six constituents, each of them a claim that the published
                   number means what it says:

                     THEMES_TAXONOMY   conf/theme-taxonomy.json is v1 with ten themes, and the
                                       taxonomy hash the audit run recorded equals today's
                     THEMES_PROTOCOL   conf/theme_sampling.toml is frozen; discovery,
                                       development, training_pool and audit are drawn, disjoint
                                       and at their protocol sizes
                     THEMES_PROMPT     a frozen prompt, frozen at a commit that is an ancestor
                                       of the audit run's, and used by that run
                     THEMES_REFERENCE  every audit review carries exactly one agent_reference
                                       label; provenance is agent_reference, never human
                     THEMES_AUDIT      the audit labelling run is present, terminal for every
                                       review, under the frozen configuration
                     THEMES_SEAL       the set was opened once, and nothing it depended on has
                                       moved since -- freezes re-derived, artefact sha256s
                                       re-checked

  THEMES_QUALITY   **reports.** macro-F1 and the minimum supported-theme recall against
                   ADR-0003's 0.70 / 0.50 bars, which were frozen before any of these numbers
                   existed and do not move. A miss prints `verdict=FAIL` and sets P6 to
                   `built, evaluated, below target`; it never fails the phase, because
                   reopening a phase on a quality result is what tuning against a holdout
                   looks like. Beside it: the three-way comparison with the star-only floor,
                   the parse-failure census, and the per-stratum lines.

  THEMES_AGREEMENT **reports.** Philip's blind 50 against the machine-made ground truth
                   (RR-21), or `NOT_RUN` with its reason until they are labelled.

Exit 0 when `THEMES_GATE` passes, whatever the quality verdict reads.

Run:  ./run.sh python scripts/gate_themes.py [--scope full]   (or `make gate-themes`)
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any

from pyspark.sql import functions as F

from src.ai.audit_seal import (
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
from src.common import evaluation as E
from src.common import runs
from src.common.spark import build
from src.gates import lineage as L
from src.gates import themes as gate
from src.gold.controls import load_protocol
from src.spark.theme_samples import table_names as sample_tables

SCORES = C.PROJECT_ROOT / "eval" / "themes"


# --------------------------------------------------------------------------- facts ----
def taxonomy_facts(audit_run: dict[str, Any] | None) -> dict[str, Any]:
    tax = load_taxonomy()
    recorded = (audit_run or {}).get("params", {}).get("taxonomy_hash")
    unchanged = recorded == tax.file_hash if recorded else False
    return {"version": tax.version, "themes": len(tax.themes), "file_hash": tax.file_hash,
            "recorded_hash": recorded, "unchanged": unchanged,
            "ok": tax.version == "1" and len(tax.themes) == gate.TAXONOMY_CEILING and unchanged}


def protocol_facts(spark, scope: str) -> dict[str, Any]:
    protocol = load_protocol()
    names = sample_tables(scope)
    expected = {"discovery": protocol.discovery.size,
                **{k: f.size for k, f in protocol.frames.items()}}
    if not spark.catalog.tableExists(names["assignments"]):
        return {"status": protocol.status, "config_hash": protocol.config_hash, "sizes": {},
                "expected": expected, "overlap": 0, "ok": False, "assignments": "missing"}
    asg = spark.table(names["assignments"])
    sizes = {r["sample_name"]: r["n"] for r in
             asg.groupBy("sample_name").agg(F.count("*").alias("n")).collect()}
    overlap = (asg.groupBy("review_id").agg(F.countDistinct("sample_name").alias("s"))
               .filter(F.col("s") > 1).count())
    right = all(sizes.get(k) == v for k, v in expected.items())
    return {"status": protocol.status, "config_hash": protocol.config_hash, "sizes": sizes,
            "expected": expected, "overlap": overlap,
            "ok": protocol.frozen and right and overlap == 0}


def _commit_is_ancestor(older: str, newer: str) -> bool:
    if not older or not newer:
        return False
    r = subprocess.run(["git", "merge-base", "--is-ancestor", older, newer],
                       cwd=C.PROJECT_ROOT, capture_output=True, check=False)
    return r.returncode == 0


def prompt_facts(spec, audit_run: dict[str, Any] | None) -> dict[str, Any]:
    frozen = spec.raw.get("frozen_prompt")
    if not frozen:
        return {"frozen": False, "ok": False, "name": None, "version": None}
    name, version, commit = frozen["name"], frozen["version"], frozen.get("freeze_commit", "")
    in_spec = spec.prompts.get(name) is not None and spec.prompts[name].version == version
    used = (audit_run or {}).get("params", {}).get("prompt_version")
    before = _commit_is_ancestor(commit, (audit_run or {}).get("git_commit_sha", ""))
    return {"frozen": True, "name": name, "version": version, "freeze_commit": commit,
            "in_spec": in_spec, "audit_used": used, "frozen_before_audit": before,
            "ok": in_spec and used == version and before}


def reference_facts(spark, scope: str, audit_size: int) -> dict[str, Any]:
    table = table_name(scope)
    if not spark.catalog.tableExists(table):
        return {"table_present": False, "ok": False}
    ref = spark.table(table).filter((F.col("budget_line") == "audit")
                                    & (F.col("label_source") == "agent_reference"))
    rows, distinct = ref.count(), ref.select("source_review_id").distinct().count()
    human = spark.table(table).filter(F.col("label_source") == "human").count()
    return {"table_present": True, "rows": rows, "distinct": distinct, "expected": audit_size,
            "human_rows": human, "ok": rows == audit_size and distinct == audit_size}


def audit_facts(spark, scope: str, audit_run: dict[str, Any] | None,
                audit_size: int) -> dict[str, Any]:
    if audit_run is None:
        return {"present": False, "ok": False}
    c = audit_run["counts"]
    config = audit_run["inputs"]["spec"]["config_hash"]
    got = spark.table(table_name(scope)).filter((F.col("budget_line") == "audit")
                                                & (F.col("label_source") == "local_llm")
                                                & (F.col("inference_config_hash") == config))
    rows = got.count()
    terminal = got.filter(F.col("label_status").isin("succeeded", "model_abstained",
                                                     "parse_failed", "api_failed")).count()
    return {"present": True, "run_id": audit_run["run_id"],
            "model_id": audit_run["params"].get("model_id"), "config": config,
            "rows": rows, "expected": audit_size, "terminal": terminal,
            "succeeded": c.get("succeeded"), "model_abstained": c.get("model_abstained"),
            "parse_failed": c.get("parse_failed"), "api_failed": c.get("api_failed"),
            "ok": rows == audit_size and terminal == rows}


def seal_facts() -> dict[str, Any]:
    seal = load_seal()
    if seal is None:
        return {"present": False, "ok": False, "seal": None}
    try:
        current = collect_freezes()
    except ValueError as exc:
        return {"present": True, "freezes_readable": False, "unreadable_reason": str(exc),
                "sealed": seal["freeze_fingerprint"], "ok": False, "seal": seal}
    moved = freezes_moved(seal["freezes"], current)
    artefacts = seal_matches_artefacts(seal, scores_dir=SCORES)
    now = fingerprint(current)
    return {"present": True, "freezes_readable": True, "opened_at": seal["opened_at"],
            "commit": seal.get("git_commit_sha", ""), "sealed": seal["freeze_fingerprint"],
            "today": now, "systems": seal["systems"], "moved": moved,
            "artefacts_changed": artefacts,
            "verdict_passed": seal["verdict"]["passed"],
            "ok": not moved and not artefacts and now == seal["freeze_fingerprint"],
            "seal": seal}


# ------------------------------------------------------------------------ artefacts ----
def _load(path) -> dict[str, Any] | None:
    return json.loads(path.read_text()) if path.exists() else None


def sealed_systems(seal: dict[str, Any] | None) -> dict[str, dict[str, Any] | None]:
    """The three audit artefacts, found through the seal rather than by rebuilding names.

    The seal already names each system's artefact and carries its sha256, so reading them from
    there is the one path that cannot drift from what was actually measured. Rebuilding the
    filename from today's config would silently pick up a different file if a config moved --
    and the seal exists precisely to notice that, not to be worked around.
    """
    if seal is None:
        return {}
    return {s["system"]: _load(SCORES / s["artefact"]) for s in seal["systems"]}


def development_systems(spec, frozen: dict[str, Any]) -> dict[str, dict[str, Any] | None]:
    """The labeller and the floor on development -- the weaker, earlier evidence, for contrast."""
    stem = llm_artefact_stem(prompt_version=frozen.get("version", "none"), model_id=spec.model_id)
    return {"llm": (_load(SCORES / score_artefact_name(sample="development", stem=stem)) or {}
                    ).get("overall"),
            "star_only": (_load(SCORES / score_artefact_name(sample="development",
                                                             stem="star_only")) or {}
                          ).get("overall")}


# ----------------------------------------------------------------------------- main ----
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    spec = load_spec()
    protocol = load_protocol()
    audit_size = protocol.frames["audit"].size
    audit_run = runs.latest_success("theme_labels_llm", category=args.category,
                                    data_scope=args.scope, params_match={"budget_line": "audit"})
    frozen = spec.raw.get("frozen_prompt") or {}

    spark = build("gate_themes", cores="local[2]", driver_memory="2g")
    try:
        facts = {
            "taxonomy": taxonomy_facts(audit_run),
            "protocol": protocol_facts(spark, args.scope),
            "prompt": prompt_facts(spec, audit_run),
            "reference": reference_facts(spark, args.scope, audit_size),
            "audit": audit_facts(spark, args.scope, audit_run, audit_size),
            "seal": seal_facts(),
        }
    finally:
        spark.stop()

    systems = sealed_systems(facts["seal"]["seal"])
    score = systems.get("llm")
    development = development_systems(spec, frozen)
    agreement_report = _load(SCORES / "agreement-audit.json")

    repro = L.attest(gate.verdict(facts, scope=args.scope), "themes")
    q = gate.quality(score, scope=args.scope, sample="audit", systems=systems,
                     development=development)
    agree = gate.agreement((agreement_report or {}).get("overall"), scope=args.scope)
    for v in (repro, q, agree):
        v.emit()

    # The artefacts are published whatever the verdicts read -- a phase that publishes only its
    # good numbers is the failure mode ADR-0011 exists to make impossible.
    protocol_hash = facts["protocol"]["config_hash"]
    run_id = facts["audit"].get("run_id")
    population = {"name": "audit (held-out, post-2020 candidate and control products)",
                  "n": audit_size,
                  "enriched_rows": protocol.frames["audit"].enriched_rows,
                  "representative_rows": protocol.frames["audit"].representative_rows,
                  "reference_source": "agent_reference"}
    if run_id:
        E.record(repro, capability="themes", phase="P6 Themes", kind="reproducibility",
                 protocol_hash=protocol_hash, model=facts["audit"].get("model_id"),
                 prompt=frozen.get("version"), population=population, pipeline_run_id=run_id,
                 scope=args.scope,
                 notes=[(f"audit set opened once at {facts['seal'].get('opened_at', 'n/a')}, "
                         f"freeze fingerprint {facts['seal'].get('sealed', 'none')[:12]}; "
                         "inference may repeat, measurement may not (ticket 09)")])
        if score is not None:
            E.record(q, capability="themes_quality", phase="P6 Themes", kind="quality",
                     protocol_hash=protocol_hash, model=facts["audit"].get("model_id"),
                     prompt=frozen.get("version"), population=population, pipeline_run_id=run_id,
                     scope=args.scope,
                     notes=gate.quality_notes(score, systems=systems, development=development))
        else:
            E.record(q, capability="themes_quality", phase="P6 Themes", kind="quality",
                     protocol_hash=protocol_hash, model=facts["audit"].get("model_id"),
                     prompt=frozen.get("version"), population=population, pipeline_run_id=run_id,
                     scope=args.scope)
        E.record(agree, capability="themes_agreement", phase="P6 Themes", kind="quality",
                 protocol_hash=protocol_hash,
                 model=(agreement_report or {}).get("human_annotator"),
                 prompt=None,
                 population={"name": "audit adjudication subset (Philip, blind)",
                             "n": (agreement_report or {}).get("reviews",
                                                               gate.AGREEMENT_ROWS)},
                 pipeline_run_id=(agreement_report or {}).get("run_id") or run_id,
                 scope=args.scope,
                 notes=[(agreement_report or {}).get("note")] if (agreement_report or {}).get("note")
                 else ())

    sys.exit(0 if repro.passed else 1)


if __name__ == "__main__":
    main()

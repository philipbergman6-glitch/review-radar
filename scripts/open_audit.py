"""Open the audit set. Once, for all three systems, in one pass (ADR-0002, ticket 09).

This is the measurement the whole P6 protocol exists to protect. The LLM labeller, the MLlib
classifier and the star-only floor are scored against the blind `agent_reference` labels on the
held-out audit frame, together, so that no system is measured after another's result is known.

What happens, in order, and nothing is read from the audit frame until every refusal has passed:

  1. `require_audit_unopened`  -- the seal does not exist yet; the set is opened once.
  2. `collect_freezes`         -- the frozen prompt, the classifier's cuts, the star thresholds,
                                  the taxonomy and the pass rule, gathered and fingerprinted.
  3. development first         -- each system's development score is on disk, so the claim
                                  "one fitting pass, one scoring pass" is checkable afterwards.
  4. inference is complete     -- every audit review has a terminal row for the LLM and for the
                                  classifier. Inference may be re-run; measurement may not.
  5. one Spark session         -- reference labels, assignments and both systems' rows are read
                                  once, and the three systems are scored from them.
  6. the seal                  -- three score artefacts and `eval/themes/audit-seal.json`, which
                                  records the fingerprint, each artefact's sha256, the run ids
                                  and the verdict.

**A macro-F1 below the 0.70 bar is a reported FAIL, not a blocker.** The bar was set before any
of these numbers existed and does not move. P6's status becomes `built, evaluated, below target`
and P7 is not held up. Reopening P6 on a quality result is exactly what tuning against a holdout
looks like, and `require_audit_unopened` is what makes doing it accidentally impossible.

Run:  ./run.sh python scripts/open_audit.py [--scope full]     (or: make audit-once)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime

from pyspark.sql import functions as F

from src.ai.audit_seal import (
    MACRO_F1_BAR,
    MIN_RECALL_BAR,
    SAMPLE,
    SEAL_PATH,
    STAR_LABEL_SOURCE,
    artefact_sha256,
    build_seal,
    collect_freezes,
    fingerprint,
    identities,
    require_audit_unopened,
)
from src.ai.classifier_spec import (
    SCORES_DIR,
    SystemIdentity,
    require_development_first,
    score_artefact_name,
)
from src.ai.label_usage import for_evaluation, theme_ids
from src.ai.labels import load_taxonomy
from src.ai.theme_labels import table_name
from src.ai.theme_scoring import system_report
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build
from src.gold.controls import load_protocol

REFERENCE_SOURCE = "agent_reference"
TERMINAL = ("succeeded", "model_abstained", "parse_failed", "api_failed")


def head_commit() -> str:
    import subprocess
    r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True,
                       text=True, check=False)
    return r.stdout.strip()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--min-support", type=int, default=10,
                    help="ADR-0003: a supported theme has >= 10 positives in the audit set")
    ap.add_argument("--bootstrap-draws", type=int, default=1000)
    ap.add_argument("--seed", type=int, default=20260907)
    args = ap.parse_args()

    # ---- refusals, all of them, before a single audit row is read ----
    try:
        require_audit_unopened()
        freezes = collect_freezes()
        ids = identities()
        for name, identity in ids.items():
            require_development_first(SAMPLE, identity=identity)
    except ValueError as exc:
        raise SystemExit(f"AUDIT_REFUSED {exc}") from exc

    tax = load_taxonomy()
    protocol = load_protocol()
    audit_size = protocol.frames[SAMPLE].size
    sample_run = runs.latest_success("theme_samples", category=args.category,
                                     data_scope=args.scope, params_match={"sample": SAMPLE})
    if sample_run is None:
        raise SystemExit(f"AUDIT_REFUSED no theme_samples run drew the {SAMPLE!r} frame")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]

    print(f"AUDIT_FREEZES fingerprint={fingerprint(freezes)[:12]} "
          f"prompt={freezes['llm']['prompt_version']} "
          f"classifier={freezes['classifier']['spec_hash'][:12]} "
          f"star=v{freezes['star_only']['version']} taxonomy={tax.file_hash[:12]} "
          f"bar={MACRO_F1_BAR}/{MIN_RECALL_BAR}")

    spark = build("open_audit", cores="local[2]", driver_memory="2g")
    try:
        labels = spark.table(table_name(args.scope)).filter(F.col("budget_line") == SAMPLE)
        ref_rows = [r.asDict(recursive=True) for r in
                    labels.filter(F.col("label_source") == REFERENCE_SOURCE).collect()]
        sys_rows = {}
        for name in ("llm", "classifier"):
            identity = ids[name]
            sys_rows[name] = [
                r.asDict(recursive=True) for r in
                labels.filter((F.col("label_source") == identity.label_source)
                              & (F.col("inference_config_hash")
                                 == identity.inference_config_hash)).collect()]
        asg = {r["review_id"]: r.asDict() for r in
               (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
                .filter(F.col("sample_name") == SAMPLE)
                .select("review_id", "parent_asin", "stratum", "rating").collect())}
    finally:
        spark.stop()

    # ---- inference must be complete for both inferring systems ----
    if len(ref_rows) != audit_size:
        raise SystemExit(f"AUDIT_REFUSED the {SAMPLE} frame has {len(ref_rows)} "
                         f"{REFERENCE_SOURCE} labels, expected {audit_size}; the reference is "
                         "what the systems are scored against, so a partial one would score "
                         "three systems on a set nobody chose")
    for name in ("llm", "classifier"):
        rows = sys_rows[name]
        terminal = [r for r in rows if r["label_status"] in TERMINAL]
        if len(rows) != audit_size or len(terminal) != len(rows):
            raise SystemExit(
                f"AUDIT_REFUSED {name} has {len(rows)} audit rows ({len(terminal)} terminal), "
                f"expected {audit_size} terminal rows under config "
                f"{ids[name].inference_config_hash[:12] or '(none)'}. Inference over the audit "
                "reviews is not sealed and may be re-run: finish it, then open the set. Scoring "
                "a system on the reviews it happened to answer for measures something else.")

    reference = {r["source_review_id"]: theme_ids(r["themes"]) for r in ref_rows}
    reference_other = {r["source_review_id"]: bool(r["other_present"]) for r in ref_rows}
    selected = sorted(reference)
    product_of = {rid: asg[rid]["parent_asin"] for rid in selected if rid in asg}
    missing_asg = [r for r in selected if r not in asg]
    if missing_asg:
        raise SystemExit(f"AUDIT_REFUSED {len(missing_asg)} audit review(s) carry a reference "
                         "label but no assignment row, so their product and rating are unknown; "
                         "the star floor needs the rating and the bootstrap needs the product")

    star_cuts = {t: int(k) for t, k in freezes[STAR_LABEL_SOURCE]["thresholds"].items()}
    predictions: dict[str, dict] = {
        "llm": {"labels": {r["source_review_id"]: for_evaluation(r) for r in sys_rows["llm"]},
                "other": {r["source_review_id"]: r["other_present"] for r in sys_rows["llm"]},
                "statuses": {r["source_review_id"]: r["label_status"] for r in sys_rows["llm"]},
                "run_ids": sorted({r["run_id"] for r in sys_rows["llm"] if r["run_id"]})},
        "classifier": {
            "labels": {r["source_review_id"]: for_evaluation(r) for r in sys_rows["classifier"]},
            "other": {r["source_review_id"]: r["other_present"] for r in sys_rows["classifier"]},
            "statuses": {r["source_review_id"]: r["label_status"] for r in sys_rows["classifier"]},
            "run_ids": sorted({r["run_id"] for r in sys_rows["classifier"] if r["run_id"]})},
        # The floor predicts from a rating and a frozen cut. It runs no inference, so it has no
        # ledger run and no `other`; both are recorded as absent rather than as zero.
        STAR_LABEL_SOURCE: {
            "labels": {rid: {t for t, k in star_cuts.items()
                             if k and (asg[rid]["rating"] or 0) <= k} for rid in selected},
            "other": None, "statuses": None, "run_ids": []},
    }

    enriched = [r for r in selected if (asg[r].get("stratum") or "").startswith("enriched")]
    representative = [r for r in selected if asg[r].get("stratum") == "representative"]
    opened_at = datetime.now(UTC).isoformat()
    commit = head_commit()
    sealed_systems = []

    for name in ("llm", "classifier", STAR_LABEL_SOURCE):
        identity: SystemIdentity = ids[name]
        p = predictions[name]

        def report(subset_ids: list[str], subset: str, p=p) -> dict:
            return system_report(
                subset=subset, reference={k: reference[k] for k in subset_ids},
                system=p["labels"], theme_ids=tax.ids, product_of=product_of,
                min_support=args.min_support, seed=args.seed, draws=args.bootstrap_draws,
                statuses=p["statuses"],
                reference_other=reference_other if p["other"] is not None else None,
                system_other=p["other"])

        out = {
            "sample": SAMPLE, "scope": args.scope, "system": name,
            "label_source": identity.label_source, "model_id": identity.model_id,
            "prompt_version": identity.prompt_version,
            "inference_config_hash": identity.inference_config_hash,
            "taxonomy_version": tax.version, "taxonomy_hash": tax.file_hash,
            "min_support": args.min_support, "reference_source": REFERENCE_SOURCE,
            "reference_rows": len(ref_rows), "system_rows": len(p["labels"]),
            "seed": args.seed, "bootstrap_draws": args.bootstrap_draws,
            # The rows this number was computed from name the run that wrote them (ADR-0008).
            # The floor has none: it inferred nothing, and an invented run id would claim it did.
            "run_ids": p["run_ids"],
            "audit_freeze_fingerprint": fingerprint(freezes),
            "opened_at": opened_at, "git_commit_sha": commit,
            "overall": report(selected, "all"),
        }
        if representative:
            out["enriched"] = report(enriched, "enriched")
            out["representative"] = report(representative, "representative")
        if name == STAR_LABEL_SOURCE:
            out["thresholds"] = star_cuts

        path = SCORES_DIR / score_artefact_name(sample=SAMPLE, stem=identity.artefact_stem)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(out, indent=1) + "\n")

        o = out["overall"]
        lo, hi = o["bootstrap_95"]
        m, r = o["macro_f1"], o["min_supported_recall"]
        sealed_systems.append({
            "system": name, "label_source": identity.label_source,
            "model_id": identity.model_id, "prompt_version": identity.prompt_version,
            "inference_config_hash": identity.inference_config_hash,
            "artefact": path.name, "artefact_sha256": artefact_sha256(path),
            "run_ids": p["run_ids"], "reviews": o["reviews"], "macro_f1": m,
            "bootstrap_95": [lo, hi], "min_supported_recall": r,
            "supported_themes": len(o["supported_themes"]),
            "failure_rate": o["coverage"]["failure_rate"]})
        print(f"AUDIT_SYSTEM system={name} prompt={identity.prompt_version} "
              f"config={identity.inference_config_hash[:12] or 'none'} reviews={o['reviews']} "
              f"macro_f1={'none' if m is None else round(m, 4)} "
              f"bootstrap95=[{'' if lo is None else round(lo, 4)},"
              f"{'' if hi is None else round(hi, 4)}] "
              f"min_supported_recall={'none' if r is None else round(r, 4)} "
              f"supported={len(o['supported_themes'])}/{len(tax.ids)} "
              f"failure_rate={o['coverage']['failure_rate']} runs={len(p['run_ids'])} "
              f"out={path.relative_to(PROJECT_ROOT)}")

    seal = build_seal(freezes=freezes, systems=sealed_systems, scope=args.scope,
                      reference_rows=len(ref_rows), git_commit_sha=commit, opened_at=opened_at)
    SEAL_PATH.write_text(json.dumps(seal, indent=1) + "\n")
    v = seal["verdict"]
    print(f"AUDIT_SEAL fingerprint={seal['freeze_fingerprint'][:12]} systems={len(sealed_systems)} "
          f"opened_at={opened_at} commit={commit[:8]} out={SEAL_PATH.relative_to(PROJECT_ROOT)}")
    print(f"AUDIT_ONCE published={seal['pass_rule']['published_system']} "
          f"macro_f1={'none' if v['macro_f1'] is None else round(v['macro_f1'], 4)} "
          f"bar={MACRO_F1_BAR} "
          f"min_supported_recall={'none' if v['min_supported_recall'] is None else round(v['min_supported_recall'], 4)} "
          f"recall_bar={MIN_RECALL_BAR} verdict={'PASS' if v['passed'] else 'FAIL'}")
    if not v["passed"]:
        print("AUDIT_ONCE_NOTE the bar was set before these numbers existed and does not move. "
              "P6 is `built, evaluated, below target`; P7 is not held up and nothing is refitted.")
    # The exit code reports the pass, but the seal is written either way: the measurement
    # happened, and a non-zero exit must not read as "the audit set is still unopened".
    sys.exit(0 if v["passed"] else 1)


if __name__ == "__main__":
    main()

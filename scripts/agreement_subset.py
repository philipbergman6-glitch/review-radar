"""Philip's blind stratified 50, and the agreement number they buy (RR-21, ticket 10).

P6's ground truth is machine-made. The agent labelled all 200 audit reviews blind, every
macro-F1 in the evaluation table is scored against those labels, and the reference annotator
and the system under test are both language models -- so their errors may be correlated. RR-21
refuses to leave that as a caveat and requires a **number**: Philip hand-labels a stratified 50
of the audit set, and the agreement is published per-theme and overall with Wilson intervals.

Four phases, because only one of them is the agent's to do:

  --draw     picks the 50 by a seeded key over the audit frame's own strata and writes
             eval/themes/agreement-subset-audit.json. **The draw is frozen on first write.**
             A second --draw against a different result is refused, not overwritten: a subset
             redrawn after anyone has seen a label is a subset chosen for its answer.
  --export   writes eval/themes/blind-agreement-audit.jsonl -- the 50 rows of the existing
             blind audit export, title and text only. Philip opens this and nothing else.
  --import   reads eval/themes/human-agreement-audit.jsonl, validates every label exactly as
             the agent's and the model's are validated, and writes rows with
             `label_source="human"` -- the one value reserved for this subset.
  --score    computes the agreement into eval/themes/agreement-audit.json, which
             `scripts/gate_themes.py` publishes as THEMES_AGREEMENT.

Why the 50 are drawn from the frame's strata rather than at random: the audit frame is 120
term-enriched rows (twelve per theme) plus 80 prevalence-representative ones, and a flat random
50 would be ~60% enriched by construction and would price the correlated-error risk only where
themes are dense. Three per theme stratum (30) plus 20 representative keeps both halves of the
frame represented in the proportion the frame itself has, and gives every theme a guaranteed
floor of hand-labelled reviews rather than whatever the draw happened to produce.

What this never does: change a label, re-score a system, or move a threshold. The agreement is
a property of the ground truth, published beside the numbers that rest on it. It has no bar --
none was frozen before it was measured, and inventing one now is the failure the whole protocol
refuses -- so it publishes `verdict=REPORTED`.

Run:  ./run.sh python scripts/agreement_subset.py --draw    (or `make agreement-draw`)
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from typing import Any

from pyspark.sql import functions as F

from src.ai.agreement import agreement_report
from src.ai.labels import (
    decoding_schema,
    idempotency_key,
    load_spec,
    load_taxonomy,
    validate_label,
)
from src.ai.theme_labels import ensure_table, label_row, merge_chunk, table_name, terminal_status
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build
from src.gold.controls import draw_key, load_protocol

sys.stdout.reconfigure(line_buffering=True)

OUT_DIR = PROJECT_ROOT / "eval" / "themes"
SAMPLE = "audit"

LABEL_SOURCE = "human"
ANNOTATOR = "philip"               # the annotator's identity, recorded like any other
HUMAN_PROTOCOL_VERSION = "human-blind-v1"

#: Its own salt, so the subset is not the first 50 of the frame's own ordering. The frame's
#: draw_key already decided which reviews are *in* the audit set; reusing it here would make
#: the adjudicated 50 a deterministic prefix of that, which correlates the two draws for no gain.
SUBSET_SALT = "audit-adjudication"

SUBSET_PATH = OUT_DIR / f"agreement-subset-{SAMPLE}.json"
BLIND_SUBSET_PATH = OUT_DIR / f"blind-agreement-{SAMPLE}.jsonl"
HUMAN_LABELS_PATH = OUT_DIR / f"human-agreement-{SAMPLE}.jsonl"
REPORT_PATH = OUT_DIR / f"agreement-{SAMPLE}.json"
BLIND_PATH = OUT_DIR / f"blind-{SAMPLE}.jsonl"
MAP_PATH = OUT_DIR / f"blind-{SAMPLE}.map.json"


def _load(path) -> Any:
    return json.loads(path.read_text())


def _blind_rows() -> dict[str, dict[str, Any]]:
    return {json.loads(x)["blind_id"]: json.loads(x)
            for x in BLIND_PATH.read_text().splitlines() if x.strip()}


# ------------------------------------------------------------------------- the draw ----
def quotas(theme_ids: list[str], *, enriched: int, representative: int, total: int
           ) -> dict[str, int]:
    """How many rows each stratum owes, proportional to the frame and even across themes.

    Rounding is resolved in favour of the enriched half, because that is where the per-theme
    agreement numbers come from; the representative half absorbs the remainder. The caller
    prints any stratum that could not fill its quota, and the shortfall is released to a final
    pass over the rest of the frame rather than topped up from a neighbouring theme (RR-22).
    """
    if total <= 0 or enriched + representative <= 0:
        raise ValueError("the adjudication subset needs a positive size and a non-empty frame")
    enriched_total = round(total * enriched / (enriched + representative))
    per_theme = enriched_total // len(theme_ids)
    q = {f"enriched_{t}": per_theme for t in theme_ids}
    q["representative"] = total - per_theme * len(theme_ids)
    return q


def pick(rows: list[dict[str, Any]], *, q: dict[str, int], seed: int, total: int
         ) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Fill each stratum's quota by seeded key, then release any shortfall to a final pass."""
    by_stratum: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by_stratum.setdefault(r["stratum"], []).append(r)
    for group in by_stratum.values():
        group.sort(key=lambda r: (draw_key(r["review_id"], seed, SUBSET_SALT), r["review_id"]))
    picked: list[dict[str, Any]] = []
    shortfall: dict[str, int] = {}
    for stratum, want in sorted(q.items()):
        have = by_stratum.get(stratum, [])
        picked.extend(have[:want])
        if len(have) < want:
            shortfall[stratum] = want - len(have)
    if len(picked) < total:
        chosen = {r["review_id"] for r in picked}
        rest = sorted((r for r in rows if r["review_id"] not in chosen),
                      key=lambda r: (draw_key(r["review_id"], seed, SUBSET_SALT), r["review_id"]))
        picked.extend(rest[:total - len(picked)])
    return picked, shortfall


def draw(args) -> None:
    protocol = load_protocol()
    tax = load_taxonomy()
    frame = protocol.frames[SAMPLE]
    total = int(frame.extras["adjudication_rows"])
    q = quotas(tax.ids, enriched=frame.enriched_rows,
               representative=frame.representative_rows, total=total)

    sample_run = runs.latest_success("theme_samples", category=args.category,
                                     data_scope=args.scope, params_match={"sample": SAMPLE})
    if sample_run is None:
        raise SystemExit(f"no successful theme_samples run for sample {SAMPLE!r}")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    spark = build("agreement_subset", cores="local[2]", driver_memory="2g")
    try:
        rows = [r.asDict() for r in
                (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
                 .filter(F.col("sample_name") == SAMPLE)
                 .select("review_id", "stratum").collect())]
    finally:
        spark.stop()

    picked, shortfall = pick(rows, q=q, seed=protocol.seed, total=total)
    mapping = _load(MAP_PATH)
    to_blind = {v: k for k, v in mapping["blind_ids"].items()}
    missing = [r["review_id"] for r in picked if r["review_id"] not in to_blind]
    if missing:
        raise SystemExit(f"{len(missing)} drawn review(s) are absent from the blind audit export; "
                         "the subset must be expressible in blind ids or it is not blind")
    picked.sort(key=lambda r: to_blind[r["review_id"]])
    doc = {
        "sample": SAMPLE, "scope": args.scope, "seed": protocol.seed, "salt": SUBSET_SALT,
        "protocol_config_hash": frame.config_hash, "theme_samples_run_id": sample_run["run_id"],
        "assignments_snapshot_id": asg_out["snapshot_id"],
        "blind_export_sha256": hashlib.sha256(BLIND_PATH.read_bytes()).hexdigest(),
        "size": total, "quotas": q, "shortfall": shortfall,
        "annotator": ANNOTATOR, "label_source": LABEL_SOURCE,
        "rows": [{"blind_id": to_blind[r["review_id"]], "stratum": r["stratum"]} for r in picked],
    }
    # Frozen on first write. RR-21: the 50 are drawn before Philip sees anything and are never
    # re-tuned -- so a second draw that disagrees is a crash, not an overwrite.
    if SUBSET_PATH.exists():
        existing = _load(SUBSET_PATH)
        if existing["rows"] != doc["rows"]:
            raise SystemExit(f"{SUBSET_PATH.relative_to(PROJECT_ROOT)} already names a different "
                             "50; the adjudication draw is frozen once written (RR-21)")
        print(f"AGREEMENT_DRAW unchanged=true rows={len(doc['rows'])} "
              f"out={SUBSET_PATH.relative_to(PROJECT_ROOT)}")
        return
    SUBSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUBSET_PATH.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"AGREEMENT_DRAW sample={SAMPLE} scope={args.scope} rows={len(doc['rows'])}/{total} "
          f"seed={protocol.seed} salt={SUBSET_SALT} "
          + " ".join(f"{k}={sum(1 for r in doc['rows'] if r['stratum'] == k)}/{v}"
                     for k, v in sorted(q.items()))
          + f" shortfall={shortfall or 'none'} out={SUBSET_PATH.relative_to(PROJECT_ROOT)}")


# ------------------------------------------------------------------------ the export ----
def export(args) -> None:
    """The 50 blind rows, carved out of the export the agent already worked from.

    Nothing new is read out of silver here. Taking the subset from `blind-audit.jsonl` means
    Philip and the agent see byte-identical text for the same review, so a disagreement is a
    disagreement about the label and never about what was on the page.
    """
    subset = _load(SUBSET_PATH)
    if hashlib.sha256(BLIND_PATH.read_bytes()).hexdigest() != subset["blind_export_sha256"]:
        raise SystemExit("the blind audit export has changed since the 50 were drawn; the subset "
                         "no longer names the rows it was drawn from")
    blind = _blind_rows()
    with BLIND_SUBSET_PATH.open("w") as f:
        for r in subset["rows"]:
            row = blind[r["blind_id"]]
            f.write(json.dumps({"blind_id": row["blind_id"], "title": row["title"],
                                "text": row["text"]}, ensure_ascii=False) + "\n")
    print(f"AGREEMENT_EXPORT sample={SAMPLE} rows={len(subset['rows'])} "
          f"out={BLIND_SUBSET_PATH.relative_to(PROJECT_ROOT)} "
          f"labels_expected_at={HUMAN_LABELS_PATH.relative_to(PROJECT_ROOT)} "
          f"annotator={ANNOTATOR} taxonomy=conf/theme-taxonomy.json")


# ------------------------------------------------------------------------ the import ----
def import_labels(args) -> None:
    t0 = time.time()
    for p in (SUBSET_PATH, BLIND_SUBSET_PATH, HUMAN_LABELS_PATH, MAP_PATH):
        if not p.exists():
            raise SystemExit(f"missing {p.relative_to(PROJECT_ROOT)}")
    subset, mapping = _load(SUBSET_PATH), _load(MAP_PATH)
    blind = _blind_rows()
    wanted = {r["blind_id"] for r in subset["rows"]}
    submitted = [json.loads(x) for x in HUMAN_LABELS_PATH.read_text().splitlines() if x.strip()]

    spec, tax = load_spec(), load_taxonomy()
    config_hash = spec.config_hash("label_v1", decoding_schema(tax.ids),
                                   extra={"taxonomy": tax.file_hash, "annotator": ANNOTATOR,
                                          "protocol": HUMAN_PROTOCOL_VERSION},
                                   model_id=ANNOTATOR)
    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    sample_run = runs.latest_success("theme_samples", category=args.category,
                                     data_scope=args.scope, params_match={"sample": SAMPLE})
    if silver_run is None or sample_run is None:
        raise SystemExit("the adjudication import needs a successful silver run and the frame's own run")
    if sample_run["run_id"] != subset["theme_samples_run_id"]:
        raise SystemExit(f"the 50 were drawn from theme_samples run {subset['theme_samples_run_id']}, "
                         f"but the latest run for {SAMPLE!r} is {sample_run['run_id']}")

    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    table = table_name(args.scope)
    run = runs.start("theme_labels_human", runs.THEME_HUMAN_SPEC_VERSION,
                     category=args.category, data_scope=args.scope,
                     inputs={"samples": {"run_id": sample_run["run_id"], "table": asg_out["table"],
                                         "snapshot_id": asg_out["snapshot_id"],
                                         "sample_name": SAMPLE},
                             "silver": {"run_id": silver_run["run_id"],
                                        "table": silver_run["outputs"]["silver.reviews"]["table"],
                                        "snapshot_id": silver_run["outputs"]["silver.reviews"]["snapshot_id"]},
                             "blind_export": {"path": str(BLIND_SUBSET_PATH.relative_to(PROJECT_ROOT)),
                                              "map_path": str(MAP_PATH.relative_to(PROJECT_ROOT)),
                                              "rows": len(wanted),
                                              "sha256": hashlib.sha256(
                                                  BLIND_SUBSET_PATH.read_bytes()).hexdigest()},
                             "taxonomy": {"path": "conf/theme-taxonomy.json",
                                          "version": tax.version, "file_hash": tax.file_hash}},
                     params={"budget_line": SAMPLE, "label_source": LABEL_SOURCE,
                             "annotator": ANNOTATOR, "protocol": HUMAN_PROTOCOL_VERSION,
                             "api_mode": "manual", "config_hash": config_hash})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"inference_config_hash": config_hash[:12],
                              "taxonomy_hash": tax.file_hash[:12],
                              "reviews_selected": len(wanted), "labels_submitted": len(submitted)}
    try:
        spark = build("theme_labels_human", cores="local[2]", driver_memory="2g")
        try:
            ensure_table(spark, table)
            seen: set[str] = set()
            rejected: list[str] = []
            rows: list[dict[str, Any]] = []
            theme_hits = {t: 0 for t in tax.ids}
            abstained = other_present = no_theme = 0
            for entry in submitted:
                bid = entry.get("blind_id")
                if bid not in wanted:
                    rejected.append(f"{bid}: not in the drawn 50")
                    continue
                if bid in seen:
                    rejected.append(f"{bid}: submitted twice")
                    continue
                seen.add(bid)
                label = {k: v for k, v in entry.items() if k != "blind_id"}
                fails = validate_label(label, title=blind[bid]["title"], text=blind[bid]["text"],
                                       theme_ids=tax.ids, limits=spec.limits)
                if fails:
                    rejected.append(f"{bid}: {'; '.join(fails)}")
                    continue
                review_id = mapping["blind_ids"][bid]
                for t in label["themes"]:
                    theme_hits[t["theme_id"]] += 1
                abstained += 1 if label["abstain"] else 0
                other_present += 1 if label["other"]["present"] else 0
                no_theme += 1 if not label["themes"] and not label["abstain"] else 0
                rows.append(label_row(
                    idempotency_key=idempotency_key(
                        source_review_id=review_id, label_source=LABEL_SOURCE, model_id=ANNOTATOR,
                        label_spec_version=spec.label_spec_version,
                        prompt_version=HUMAN_PROTOCOL_VERSION, inference_config_hash=config_hash),
                    source_review_id=review_id, budget_line=SAMPLE, label_source=LABEL_SOURCE,
                    model_id=ANNOTATOR, label_spec_version=spec.label_spec_version,
                    prompt_version=HUMAN_PROTOCOL_VERSION, inference_config_hash=config_hash,
                    api_mode="manual", status=terminal_status("succeeded", label), parsed=label,
                    attempts=[{"attempt_no": 1, "provider_request_id": None,
                               "raw_response": json.dumps(label, ensure_ascii=False),
                               "validation_error": None, "input_tokens": None,
                               "output_tokens": None, "estimated_cost_usd": None,
                               "duration_s": None, "completed_at": None}],
                    source_silver_run_id=silver_run["run_id"], run_id=run.run_id))
            missing = sorted(wanted - seen)
            counts.update({"accepted": len(rows), "rejected": len(rejected), "abstained": abstained,
                           "theme_hits": theme_hits, "other_present": other_present,
                           "no_theme_labels": no_theme})
            if rejected or missing:
                for r in rejected[:20]:
                    print(f"[adjudication] REJECTED {r}")
                if missing:
                    print(f"[adjudication] MISSING {len(missing)} blind id(s): {missing[:10]}")
                raise RuntimeError(f"{len(rejected)} rejected, {len(missing)} missing; the "
                                   "adjudication subset is never partially imported -- a moving "
                                   "denominator is the thing the frozen draw exists to prevent")
            for i in range(0, len(rows), args.chunk):
                merge_chunk(spark, table, rows[i:i + args.chunk])
            snap = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots "
                             "ORDER BY committed_at DESC LIMIT 1").first()
            outputs["gold.review_theme_labels"] = {"table": table, "budget_line": SAMPLE,
                                                   "label_source": LABEL_SOURCE,
                                                   "snapshot_id": int(snap["snapshot_id"])}
            landed = spark.table(table).filter((F.col("inference_config_hash") == config_hash)
                                               & (F.col("label_source") == LABEL_SOURCE)
                                               & (F.col("budget_line") == SAMPLE))
            counts.update({"table_rows_for_config": landed.count(),
                           "distinct_keys_for_config": landed.select("idempotency_key").distinct().count(),
                           "elapsed_s": round(time.time() - t0, 1)})
        finally:
            spark.stop()
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["reviews_selected"], records_out=counts["accepted"],
                 records_rejected=counts["rejected"], outputs=outputs, counts=counts)
    print(f"HUMAN_LABELS run_id={run.run_id} sample={SAMPLE} scope={args.scope} "
          f"annotator={ANNOTATOR} source={LABEL_SOURCE} config={config_hash[:12]} "
          f"selected={counts['reviews_selected']} accepted={counts['accepted']} "
          f"rejected={counts['rejected']} abstained={counts['abstained']} "
          f"no_theme={counts['no_theme_labels']} other={counts['other_present']} "
          f"rows={counts['table_rows_for_config']} elapsed_s={counts['elapsed_s']}")


# ------------------------------------------------------------------------- the score ----
def score(args) -> None:
    subset, mapping = _load(SUBSET_PATH), _load(MAP_PATH)
    tax = load_taxonomy()
    review_ids = [mapping["blind_ids"][r["blind_id"]] for r in subset["rows"]]
    human_run = runs.latest_success("theme_labels_human", category=args.category,
                                    data_scope=args.scope)
    if human_run is None:
        raise SystemExit("no successful theme_labels_human run: the 50 have not been imported")

    spark = build("agreement_score", cores="local[2]", driver_memory="2g")
    try:
        labels = (spark.table(table_name(args.scope))
                  .filter((F.col("budget_line") == SAMPLE)
                          & F.col("source_review_id").isin(review_ids))
                  .select("source_review_id", "label_source", "themes", "label_status"))
        collected = [r.asDict(recursive=True) for r in labels.collect()]
    finally:
        spark.stop()

    def themes_by_source(source: str) -> dict[str, set[str]]:
        return {r["source_review_id"]: {t["theme_id"] for t in (r["themes"] or [])}
                for r in collected if r["label_source"] == source}

    agent, human = themes_by_source("agent_reference"), themes_by_source("human")
    absent = sorted(set(review_ids) - set(human))
    if absent:
        raise SystemExit(f"{len(absent)} of the drawn 50 carry no human label; the agreement "
                         "denominator is the frozen draw, never whatever happened to be labelled")
    report = {
        "sample": SAMPLE, "scope": args.scope, "reviews": len(review_ids),
        "human_annotator": ANNOTATOR, "human_run_id": human_run["run_id"],
        "run_id": human_run["run_id"],
        "reference_source": "agent_reference", "taxonomy_version": tax.version,
        "taxonomy_hash": tax.file_hash, "subset_seed": subset["seed"], "subset_salt": subset["salt"],
        "note": ("agreement between the machine-made ground truth every P6 macro-F1 is scored "
                 "against and Philip's blind stratified 50 (RR-21); the unit is one "
                 "(review, theme) decision, kappa is reported beside the raw rate because theme "
                 "presence is rare, and there is no bar -- none was frozen before it was measured"),
        "overall": agreement_report(agent=agent, human=human, theme_ids=tax.ids,
                                    review_ids=review_ids),
    }
    REPORT_PATH.write_text(json.dumps(report, indent=1) + "\n")
    o = report["overall"]
    for t in o["per_theme"]:
        print(f"AGREEMENT_THEME {t['theme_id']:<20} n={t['n']:>3} agree={t['agree']:>3} "
              f"rate={t['agreement']:.3f} wilson95=[{t['wilson_95'][0]:.3f},{t['wilson_95'][1]:.3f}] "
              f"both={t['both']:>3} agent_only={t['agent_only']:>3} human_only={t['human_only']:>3} "
              f"kappa={'none' if t['kappa'] is None else round(t['kappa'], 3)}")
    print(f"AGREEMENT sample={SAMPLE} reviews={o['reviews']} themes={o['themes']} "
          f"decisions={o['n']} agreement={round(o['agreement'], 4)} "
          f"wilson95=[{round(o['wilson_95'][0], 4)},{round(o['wilson_95'][1], 4)}] "
          f"kappa={'none' if o['kappa'] is None else round(o['kappa'], 4)} "
          f"exact_set_match={o['exact_set_match']}/{o['reviews']} "
          f"out={REPORT_PATH.relative_to(PROJECT_ROOT)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--chunk", type=int, default=50)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--draw", action="store_true")
    g.add_argument("--export", action="store_true")
    g.add_argument("--import", dest="do_import", action="store_true")
    g.add_argument("--score", action="store_true")
    args = ap.parse_args()
    if args.draw:
        draw(args)
    elif args.export:
        export(args)
    elif args.do_import:
        import_labels(args)
    else:
        score(args)


if __name__ == "__main__":
    main()

"""Adjudicate every reference/system disagreement once, without changing a label (ADR-0003).

Two phases, because adjudication is a judgement the agent makes and a script cannot:

  --export   writes eval/themes/adjudication-<sample>.todo.jsonl -- one row per disagreement,
             carrying the review text, both labels, the evidence quotes and the star rating,
             which is needed only here: `star_misleading` cannot be identified without it.
  --import   reads eval/themes/adjudication-<sample>.jsonl (the same rows with a `cause`),
             validates every cause against ADR-0003's fixed list, and writes
             docs/theme-taxonomy/adjudication-<sample>.csv plus the cause counts.

Nothing here may change a label, the taxonomy or the spec. ADR-0003: "adjudicate each
disagreement once without changing the labels or spec". The adjudication is a description of
the errors, not a correction of them, and the score is never recomputed from it.

`model_missed` on a row whose `system_status` is `parse_failed` is still `model_missed` -- the
model did not produce the label -- but the export carries the status so the table can say how
many misses were failures to answer rather than failures to see.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from typing import Any

from pyspark.sql import functions as F

from src.ai.label_themes import PROMPT_MAX_THEMES
from src.ai.labels import decoding_schema, load_spec, load_taxonomy
from src.ai.theme_labels import table_name
from src.common import config as C
from src.common import runs
from src.common.config import PROJECT_ROOT
from src.common.spark import build

CAUSES = ("model_missed", "model_invented", "definition_boundary", "human_error", "star_misleading")
OUT_DIR = PROJECT_ROOT / "eval" / "themes"
DOCS = PROJECT_ROOT / "docs" / "theme-taxonomy"


def config_for(spec, tax, prompt_name: str, model: str) -> tuple[str, str]:
    prompt = spec.prompts[prompt_name]
    schema = decoding_schema(tax.ids, max_themes=PROMPT_MAX_THEMES.get(prompt.version))
    return spec.config_hash(prompt_name, schema, extra={"taxonomy": tax.file_hash},
                            model_id=model), prompt.version


def export(args, spec, tax) -> None:
    model = args.model or spec.model_id
    config_hash, version = config_for(spec, tax, args.prompt, model)
    sample_run = runs.latest_success("theme_samples", category=args.category, data_scope=args.scope,
                                     params_match={"sample": args.sample})
    silver_run = runs.latest_success("silver", category=args.category, data_scope=args.scope)
    if sample_run is None or silver_run is None:
        raise SystemExit("adjudication needs the frame's theme_samples run and a silver run")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    slv = silver_run["outputs"]["silver.reviews"]

    spark = build("adjudicate_themes", cores="local[2]", driver_memory="2g")
    try:
        labels = spark.table(table_name(args.scope)).filter(F.col("budget_line") == args.sample)
        ref = {r["source_review_id"]: r.asDict(recursive=True) for r in
               labels.filter(F.col("label_source") == "agent_reference").collect()}
        sysrows = {r["source_review_id"]: r.asDict(recursive=True) for r in
                   labels.filter((F.col("label_source") == "local_llm")
                                 & (F.col("inference_config_hash") == config_hash)).collect()}
        asg = {r["review_id"]: r.asDict() for r in
               (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
                .filter(F.col("sample_name") == args.sample)
                .select("review_id", "parent_asin", "rating", "stratum", "role").collect())}
        text = {r["review_id"]: r.asDict() for r in
                (spark.read.option("snapshot-id", slv["snapshot_id"]).table(slv["table"])
                 .select("review_id", "title", "text")
                 .join(spark.createDataFrame([(k,) for k in sorted(ref)], ["review_id"]),
                       on="review_id", how="inner").collect())}
    finally:
        spark.stop()

    rows: list[dict[str, Any]] = []
    for review_id in sorted(ref):
        r_themes = {t["theme_id"]: t["evidence_quote"] for t in (ref[review_id]["themes"] or [])}
        s_row = sysrows.get(review_id)
        s_themes = {t["theme_id"]: t["evidence_quote"] for t in ((s_row or {}).get("themes") or [])}
        status = (s_row or {}).get("label_status", "absent")
        for theme in sorted(set(r_themes) | set(s_themes)):
            if theme in r_themes and theme in s_themes:
                continue
            rows.append({
                "review_id": review_id, "theme_id": theme,
                "direction": "reference_only" if theme in r_themes else "system_only",
                "system_status": status,
                "reference_quote": r_themes.get(theme), "system_quote": s_themes.get(theme),
                "reference_themes": sorted(r_themes), "system_themes": sorted(s_themes),
                "rating": asg.get(review_id, {}).get("rating"),
                "stratum": asg.get(review_id, {}).get("stratum"),
                "title": text.get(review_id, {}).get("title"),
                "text": text.get(review_id, {}).get("text"),
                "cause": None,
            })
    path = OUT_DIR / f"adjudication-{args.sample}.todo.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"ADJUDICATION_EXPORT sample={args.sample} model={model} prompt={version} "
          f"config={config_hash[:12]} reviews={len(ref)} disagreements={len(rows)} "
          f"reference_only={sum(1 for r in rows if r['direction'] == 'reference_only')} "
          f"system_only={sum(1 for r in rows if r['direction'] == 'system_only')} "
          f"out={path.relative_to(PROJECT_ROOT)}")


def load_rows(path):
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


def import_causes(args) -> None:
    todo = OUT_DIR / f"adjudication-{args.sample}.todo.jsonl"
    done = OUT_DIR / f"adjudication-{args.sample}.jsonl"
    if not done.exists():
        raise SystemExit(f"missing {done.relative_to(PROJECT_ROOT)}")
    expected = {(r["review_id"], r["theme_id"]) for r in load_rows(todo)} if todo.exists() else None
    rows = load_rows(done)
    seen, bad = set(), []
    for r in rows:
        key = (r["review_id"], r["theme_id"])
        if key in seen:
            bad.append(f"{key}: adjudicated twice")
        seen.add(key)
        if r.get("cause") not in CAUSES:
            bad.append(f"{key}: cause {r.get('cause')!r} is not one of {list(CAUSES)}")
    missing = sorted(expected - seen) if expected is not None else []
    if bad or missing:
        for x in bad[:20]:
            print(f"[adjudication] REJECTED {x}")
        if missing:
            print(f"[adjudication] MISSING {len(missing)}: {missing[:5]}")
        raise SystemExit(f"{len(bad)} invalid, {len(missing)} unadjudicated; ADR-0003 requires "
                         "every disagreement adjudicated exactly once")

    DOCS.mkdir(parents=True, exist_ok=True)
    out = DOCS / f"adjudication-{args.sample}.csv"
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["review_id", "theme_id", "direction", "system_status", "rating", "stratum",
                    "cause", "reference_quote", "system_quote", "note"])
        for r in sorted(rows, key=lambda r: (r["review_id"], r["theme_id"])):
            w.writerow([r["review_id"], r["theme_id"], r["direction"], r["system_status"],
                        r["rating"], r["stratum"], r["cause"],
                        (r.get("reference_quote") or "").replace("\n", " "),
                        (r.get("system_quote") or "").replace("\n", " "),
                        (r.get("note") or "").replace("\n", " ")])
    counts = Counter(r["cause"] for r in rows)
    by_status = Counter((r["cause"], r["system_status"]) for r in rows)
    print(f"ADJUDICATION sample={args.sample} disagreements={len(rows)} "
          + " ".join(f"{c}={counts.get(c, 0)}" for c in CAUSES)
          + f" model_missed_from_parse_failed={by_status.get(('model_missed', 'parse_failed'), 0)}"
          + f" star_misleading_model_right={sum(1 for r in rows if r['cause'] == 'star_misleading')}"
          + f" out={out.relative_to(PROJECT_ROOT)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit"])
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--prompt", default="label_v4")
    ap.add_argument("--model", default=None)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--export", action="store_true")
    g.add_argument("--import", dest="do_import", action="store_true")
    args = ap.parse_args()
    if args.export:
        export(args, load_spec(), load_taxonomy())
    else:
        import_causes(args)


if __name__ == "__main__":
    main()

"""Why a labelling configuration failed to parse, and what its score would be without that.

Nothing here scores anything that counts. `scripts/score_themes.py` is the scorer, and it
treats a `parse_failed` row as an empty prediction, which is the honest reading: the model
was asked and produced nothing usable. This script exists to answer the *separate* question
a reader will ask when they see a low number -- "is that disagreement, or is it plumbing?" --
by censusing the validator's rejection reasons and reporting a ceiling over the answered
rows alone.

The ceiling is a diagnostic and is labelled as one wherever it is printed. It is not a score:
it is computed on a subset the model chose for itself, so it is optimistic by construction.

Run:  ./run.sh python scripts/diagnose_label_failures.py --sample development --prompt label_v4
"""
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from src.ai.label_failures import census
from src.ai.labels import load_spec, load_taxonomy, prompt_schema
from src.ai.theme_labels import table_name
from src.ai.theme_scoring import macro_f1, score_themes
from src.common.config import PROJECT_ROOT
from src.common.spark import build

REFERENCE_SOURCE = "agent_reference"
OUT_DIR = PROJECT_ROOT / "eval" / "themes"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", required=True)
    ap.add_argument("--prompt", required=True, help="spec prompt key, e.g. label_v4")
    ap.add_argument("--model", default="qwen3:8b")
    ap.add_argument("--scope", default="full")
    ap.add_argument("--out", default=None,
                    help="artefact path; defaults to eval/themes/parse-census-<sample>-<prompt>-<model>.json")
    args = ap.parse_args()

    spec, tax = load_spec(), load_taxonomy()
    prompt = spec.prompts[args.prompt]
    prompt_version = prompt.version
    # The same prompt version can have run under more than one configuration; censusing across
    # them would mix a superseded run's failures into this one's (src/ai/theme_labels.py).
    schema = prompt_schema(prompt_version, tax.ids)
    config_hash = spec.config_hash(args.prompt, schema, extra={"taxonomy": tax.file_hash},
                                   model_id=args.model)

    spark = build("theme-label-diagnosis")
    table = table_name(args.scope)
    pred = spark.sql(f"""
        SELECT source_review_id, label_status, themes, attempts FROM {table}
        WHERE label_source = 'local_llm' AND prompt_version = '{prompt_version}'
          AND model_id = '{args.model}' AND inference_config_hash = '{config_hash}'
          AND budget_line = '{args.sample}'""").collect()
    if not pred:
        raise SystemExit(f"no local_llm rows for prompt {prompt_version} model {args.model} "
                         f"config {config_hash[:12]} budget line {args.sample}")
    ref = spark.sql(f"""
        SELECT source_review_id, themes FROM {table}
        WHERE label_source = '{REFERENCE_SOURCE}' AND label_status = 'succeeded'
          AND budget_line = '{args.sample}'""").collect()
    reference = {r["source_review_id"]: r["themes"] for r in ref}

    failed = [r for r in pred if r["label_status"] == "parse_failed"]
    errors = [row["attempts"][0]["validation_error"] if row["attempts"] else None for row in failed]
    counts = census(errors)

    print(f"prompt={prompt_version} model={args.model} rows={len(pred)} parse_failed={len(failed)}")
    print(f"\nrejection causes ({len(failed)} failed rows, first attempt) -- a row with two "
          "causes counts under both:")
    for cause, count in counts["by_cause"].items():
        print(f"  {count:4d}  {cause}")
    print(f"\nrejection combinations ({len(failed)} failed rows, each counted once):")
    for combination, count in counts["by_combination"].items():
        print(f"  {count:4d}  {combination or '(no recorded validation error)'}")
    if counts["by_theme_slot"]:
        # A prompt that pads to its theme limit fails in the slots it padded. If the
        # violations cluster away from slot 0, the model's first answer was fine.
        print("\nviolations by theme slot:", counts["by_theme_slot"])

    def themes_of(themes: object) -> set[str]:
        return {t["theme_id"] for t in (themes or [])}

    answered = {r["source_review_id"] for r in pred if r["label_status"] == "succeeded"}
    paired = {r["source_review_id"]: themes_of(r["themes"]) for r in pred
              if r["source_review_id"] in reference and r["source_review_id"] in answered}
    truth = {rid: themes_of(reference[rid]) for rid in paired}
    per_theme = score_themes(truth, paired, tax.ids, min_support=10)
    ceiling = macro_f1(per_theme)
    supported = [s for s in per_theme if s.supported]
    print(f"\nDIAGNOSTIC CEILING over the {len(paired)} answered rows only -- not a score, and "
          f"optimistic by\nconstruction, because the model selected the subset: "
          f"macro_f1={ceiling:.4f} over {len(supported)} supported themes")

    # The census is committed, not just printed: the score artefact carries the failure rate,
    # and this is the file that says what those failures were. It is a diagnosis, so it is not
    # an evaluation artefact under conf/eval-artifact.schema.json and never enters the table.
    statuses = {"succeeded": 0, "model_abstained": 0, "parse_failed": 0, "api_failed": 0}
    for row in pred:
        statuses[row["label_status"]] = statuses.get(row["label_status"], 0) + 1
    out = {
        "kind": "parse_failure_census", "sample": args.sample, "scope": args.scope,
        "label_source": "local_llm", "model_id": args.model, "prompt_version": prompt_version,
        "inference_config_hash": config_hash, "rows": len(pred), "by_status": statuses,
        "parse_failed": len(failed),
        "parse_failure_rate": round(len(failed) / len(pred), 4),
        **{k: counts[k] for k in ("by_cause", "by_combination", "by_theme_slot")},
        "diagnostic_ceiling": {
            "macro_f1": ceiling, "answered_rows": len(paired), "supported_themes": len(supported),
            "note": "Not a score. Computed over the rows the model chose to answer, so it is "
                    "optimistic by construction; the scored number reads every parse failure as "
                    "an empty prediction.",
        },
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    path = OUT_DIR / (args.out or
                      f"parse-census-{args.sample}-{prompt_version}-{args.model.replace(':', '_')}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=1) + "\n")
    print(f"\nPARSE_CENSUS sample={args.sample} prompt={prompt_version} model={args.model} "
          f"rows={len(pred)} parse_failed={len(failed)} rate={out['parse_failure_rate']} "
          f"causes={len(counts['by_cause'])} out={path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

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
import collections
import re

from src.ai.labels import load_spec, load_taxonomy
from src.ai.theme_labels import table_name
from src.ai.theme_scoring import macro_f1, score_themes
from src.common.spark import build

REFERENCE_SOURCE = "agent_reference"

# The validator's messages, bucketed. Each bucket is one thing a prompt could fix.
FAILURE_KINDS = (
    ("repeats theme_id", "repeated_theme"),
    ("words, limit", "quote_too_long"),
    ("not present in the review", "quote_not_verbatim"),
)


def classify(error: str) -> list[str]:
    """One validator message can carry several violations; every distinct kind counts once."""
    kinds: set[str] = set()
    for part in (error or "").split("; "):
        if not part.strip():
            continue
        for needle, kind in FAILURE_KINDS:
            if needle in part:
                kinds.add(kind)
                break
        else:
            kinds.add("other")
    return sorted(kinds)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", required=True)
    ap.add_argument("--prompt", required=True, help="spec prompt key, e.g. label_v4")
    ap.add_argument("--model", default="qwen3:8b")
    ap.add_argument("--scope", default="full")
    args = ap.parse_args()

    spec, tax = load_spec(), load_taxonomy()
    prompt_version = spec.prompts[args.prompt].version

    spark = build("theme-label-diagnosis")
    table = table_name(args.scope)
    pred = spark.sql(f"""
        SELECT source_review_id, label_status, themes, attempts FROM {table}
        WHERE label_source = 'local_llm' AND prompt_version = '{prompt_version}'
          AND model_id = '{args.model}'""").collect()
    if not pred:
        raise SystemExit(f"no local_llm rows for prompt {prompt_version} model {args.model}")
    ref = spark.sql(f"""
        SELECT source_review_id, themes FROM {table}
        WHERE label_source = '{REFERENCE_SOURCE}' AND label_status = 'succeeded'""").collect()
    reference = {r["source_review_id"]: r["themes"] for r in ref}

    failed = [r for r in pred if r["label_status"] == "parse_failed"]
    census: collections.Counter[str] = collections.Counter()
    slots: collections.Counter[int] = collections.Counter()
    for row in failed:
        error = row["attempts"][0]["validation_error"] if row["attempts"] else ""
        census[" + ".join(classify(error))] += 1
        for match in re.finditer(r"themes\[(\d+)\]", error or ""):
            slots[int(match.group(1))] += 1

    print(f"prompt={prompt_version} model={args.model} rows={len(pred)} parse_failed={len(failed)}")
    print(f"\nrejection reasons ({len(failed)} failed rows, first attempt):")
    for kind, count in census.most_common():
        print(f"  {count:4d}  {kind}")
    if slots:
        # A prompt that pads to its theme limit fails in the slots it padded. If the
        # violations cluster away from slot 0, the model's first answer was fine.
        print("\nviolations by theme slot:", dict(sorted(slots.items())))

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


if __name__ == "__main__":
    main()

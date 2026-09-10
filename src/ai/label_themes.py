"""Job `theme_labels_llm`: label a named frame against the frozen taxonomy (ADR-0003, RR-19).

This is the **system under test**. It sends one review's `title` and `text` to a local model
with the frozen taxonomy rendered into the system prompt, constrains decoding to the output
schema, validates the answer semantically, and writes one row per logical inference to
`gold.review_theme_labels` with `label_source="local_llm"`.

Data minimisation is unchanged and strictly stronger than ADR-0003's hosted plan: only title
and text are assembled into the request, and the model runs on this machine, so no review
text leaves it. No rating, product, window, identifier or reference label is ever in scope.

Prompt development: at most five versions (ADR-0003). Each is a separate `prompt_version`
with its own `inference_config_hash`, so its rows never mix with another's, and the selected
prompt is frozen in its own commit **before** the audit frame is opened.

Run:  ./run.sh python -m src.ai.label_themes --sample development [--prompt label] [--model ...]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai import ollama
from src.ai.labels import (
    decoding_schema,
    idempotency_key,
    load_spec,
    load_taxonomy,
    render_taxonomy,
    validate_label,
)
from src.ai.theme_labels import (
    cached_keys,
    ensure_table,
    label_row,
    merge_chunk,
    table_name,
    terminal_status,
)
from src.common import config as C
from src.common import runs
from src.common.spark import build

sys.stdout.reconfigure(line_buffering=True)

LABEL_SOURCE = "local_llm"

# The taxonomy is ~830 words and sits between the rules and the review. From label-v4 the two
# limits that actually fail (theme count, quote length) are repeated after it, so they are the
# last thing read before the answer. Earlier prompt versions get no tail, so their frozen
# config hashes stay exactly what they were when they ran.
PROMPT_TAILS = {
    "label-v4": ("Remember, before you answer: at most THREE themes, each theme id at most "
                 "once, and every evidence_quote copied exactly from the review and at most "
                 "15 words -- count them."),
    "label-v5": ("Remember, before you answer: at most THREE themes and never the same theme "
                 "id twice. Every evidence_quote is copied exactly from the review and is "
                 "SHORT -- 3 to 6 words, never more than 12. Count the words before you "
                 "write them."),
}

# The array cap is structural, so it belongs to the schema rather than to hope. Only the
# version whose prompt asks for it declares it; the others keep their original hash.
PROMPT_MAX_THEMES = {"label-v5": 3}


def prompt_tail(prompt_version: str) -> str:
    return PROMPT_TAILS.get(prompt_version, "")


def select_reviews(spark: SparkSession, *, sample: str, sample_run: dict[str, Any],
                   silver_run: dict[str, Any]) -> list[dict[str, Any]]:
    """The assigned reviews of one frame, with the two fields the model is allowed to see."""
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    silver_out = silver_run["outputs"]["silver.reviews"]
    asg = (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
           .filter(F.col("sample_name") == sample).select("review_id", "draw_key"))
    silver = (spark.read.option("snapshot-id", silver_out["snapshot_id"]).table(silver_out["table"])
              .select("review_id", "title", "text"))
    rows = [r.asDict() for r in asg.join(silver, on="review_id", how="inner").collect()]
    missing = asg.count() - len(rows)
    if missing:
        raise RuntimeError(f"{missing} assigned {sample} review(s) are absent from the pinned silver snapshot")
    rows.sort(key=lambda r: (r["draw_key"], r["review_id"]))
    return rows


def run_label_themes(spark: SparkSession, *, sample: str, scope: str, category: str,
                     prompt_name: str, model_id: str | None, limit: int | None,
                     chunk: int) -> dict[str, Any]:
    t0 = time.time()
    spec = load_spec()
    tax = load_taxonomy()
    model = model_id or spec.model_id
    if sample == "training_pool":
        # The classifier's teacher (ticket 07, ADR-0002). Any other prompt's rows would land in
        # the same table under a different hash and read as a pool; refuse before spending hours.
        spec.require_frozen(purpose="labelling the training pool", prompt_name=prompt_name)
    prompt = spec.prompts[prompt_name]
    schema = decoding_schema(tax.ids, max_themes=PROMPT_MAX_THEMES.get(prompt.version))
    config_hash = spec.config_hash(prompt_name, schema, extra={"taxonomy": tax.file_hash},
                                   model_id=model)
    system = f"{prompt.text.rstrip()}\n\n{render_taxonomy(tax)}\n\n{prompt_tail(prompt.version)}"
    sample_run = runs.latest_success("theme_samples", category=category, data_scope=scope,
                                     params_match={"sample": sample})
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if sample_run is None or silver_run is None:
        raise RuntimeError(f"labelling {sample} needs a successful theme_samples run for that "
                           "sample and one successful silver run")
    table = table_name(scope)
    ensure_table(spark, table)

    run = runs.start("theme_labels_llm", runs.THEME_LABELS_SPEC_VERSION, category=category,
                     data_scope=scope,
                     inputs={"samples": {"run_id": sample_run["run_id"],
                                         "table": sample_run["outputs"]["gold.theme_sample_assignments"]["table"],
                                         "snapshot_id": sample_run["outputs"]["gold.theme_sample_assignments"]["snapshot_id"],
                                         "sample_name": sample},
                             "silver": {"run_id": silver_run["run_id"],
                                        "table": silver_run["outputs"]["silver.reviews"]["table"],
                                        "snapshot_id": silver_run["outputs"]["silver.reviews"]["snapshot_id"]},
                             "spec": {"path": "conf/theme-label-spec.json",
                                      "version": spec.label_spec_version, "model_id": model,
                                      "prompt_version": prompt.version, "config_hash": config_hash}},
                     params={"budget_line": sample, "label_source": LABEL_SOURCE,
                             "api_mode": spec.api_mode, "inference": spec.inference,
                             "model_id": model, "prompt_version": prompt.version,
                             "taxonomy_version": tax.version, "taxonomy_hash": tax.file_hash,
                             "chunk": chunk})
    outputs: dict[str, Any] = {}
    counts: dict[str, Any] = {"inference_config_hash": config_hash[:12],
                              "taxonomy_hash": tax.file_hash[:12]}
    try:
        selected = select_reviews(spark, sample=sample, sample_run=sample_run, silver_run=silver_run)
        if limit:
            selected = selected[:limit]
        cached = cached_keys(spark, table, config_hash, LABEL_SOURCE)
        pending: list[tuple[str, dict[str, Any]]] = []
        hits = 0
        for r in selected:
            key = idempotency_key(source_review_id=r["review_id"], label_source=LABEL_SOURCE,
                                  model_id=model, label_spec_version=spec.label_spec_version,
                                  prompt_version=prompt.version, inference_config_hash=config_hash)
            if key in cached:
                hits += 1
            else:
                pending.append((key, r))
        counts.update({"reviews_selected": len(selected), "cache_hits": hits,
                       "inferences_run": len(pending)})
        print(f"[label] {sample}: {len(selected)} assigned, {hits} cached, {len(pending)} to infer "
              f"with {model} prompt={prompt.version} config={config_hash[:12]}")

        tally = {"succeeded": 0, "model_abstained": 0, "parse_failed": 0, "api_failed": 0}
        theme_hits: dict[str, int] = {t: 0 for t in tax.ids}
        retried = other_present = empty_labels = 0
        buffer: list[dict[str, Any]] = []
        started = time.time()
        for i, (key, r) in enumerate(pending, start=1):
            payload = json.dumps({"title": r["title"], "text": r["text"]}, ensure_ascii=False)
            res = ollama.infer(spec, system=system, prompt=payload, schema=schema,
                               model_id=model,
                               validate=lambda obj, rr=r: validate_label(
                                   obj, title=rr["title"], text=rr["text"], theme_ids=tax.ids,
                                   limits=spec.limits))
            status = terminal_status(res.status, res.parsed)
            tally[status] += 1
            retried += 1 if res.attempt_count > 1 else 0
            if res.parsed is not None:
                for t in res.parsed["themes"]:
                    theme_hits[t["theme_id"]] += 1
                other_present += 1 if res.parsed["other"]["present"] else 0
                empty_labels += 1 if not res.parsed["themes"] and not res.parsed["abstain"] else 0
            buffer.append(label_row(
                idempotency_key=key, source_review_id=r["review_id"], budget_line=sample,
                label_source=LABEL_SOURCE, model_id=model,
                label_spec_version=spec.label_spec_version, prompt_version=prompt.version,
                inference_config_hash=config_hash, api_mode=spec.api_mode, status=status,
                parsed=res.parsed,
                attempts=[{"attempt_no": a.attempt_no, "provider_request_id": None,
                           "raw_response": a.raw_response, "validation_error": a.validation_error,
                           "input_tokens": a.input_tokens, "output_tokens": a.output_tokens,
                           "estimated_cost_usd": None, "duration_s": a.duration_s,
                           "completed_at": datetime.fromtimestamp(a.completed_at, tz=UTC)}
                          for a in res.attempts],
                source_silver_run_id=silver_run["run_id"], run_id=run.run_id))
            if len(buffer) >= chunk:
                merge_chunk(spark, table, buffer)
                buffer = []
                rate = (time.time() - started) / i
                print(f"[label] {i}/{len(pending)} done, {rate:.1f}s/review, ok={tally['succeeded']} "
                      f"abstained={tally['model_abstained']} parse_failed={tally['parse_failed']} "
                      f"api_failed={tally['api_failed']}, eta {(len(pending) - i) * rate / 60:.0f} min")
        merge_chunk(spark, table, buffer)

        snap = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        outputs["gold.review_theme_labels"] = {"table": table, "budget_line": sample,
                                               "label_source": LABEL_SOURCE,
                                               "snapshot_id": int(snap["snapshot_id"]) if snap else None}
        landed = spark.table(table).filter((F.col("inference_config_hash") == config_hash)
                                           & (F.col("label_source") == LABEL_SOURCE))
        counts.update({
            "succeeded": tally["succeeded"], "model_abstained": tally["model_abstained"],
            "parse_failed": tally["parse_failed"], "api_failed": tally["api_failed"],
            "retried_inferences": retried, "theme_hits": theme_hits,
            "other_present": other_present, "no_theme_labels": empty_labels,
            "table_rows_for_config": landed.count(),
            "distinct_keys_for_config": landed.select("idempotency_key").distinct().count(),
            "elapsed_s": round(time.time() - t0, 1),
            "seconds_per_review": round((time.time() - started) / len(pending), 2) if pending else None})
        if counts["table_rows_for_config"] != counts["distinct_keys_for_config"]:
            raise RuntimeError(f"{table}: {counts['table_rows_for_config']} rows but "
                               f"{counts['distinct_keys_for_config']} distinct idempotency keys")
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise

    runs.success(run, records_in=counts["reviews_selected"],
                 records_out=counts["succeeded"] + counts["model_abstained"],
                 records_rejected=counts["parse_failed"] + counts["api_failed"],
                 outputs=outputs, counts=counts)
    print(f"THEME_LABELS run_id={run.run_id} sample={sample} scope={scope} model={model} "
          f"prompt={prompt.version} config={config_hash[:12]} taxonomy={tax.file_hash[:12]} "
          f"selected={counts['reviews_selected']} cached={counts['cache_hits']} "
          f"inferred={counts['inferences_run']} ok={counts['succeeded']} "
          f"abstained={counts['model_abstained']} parse_failed={counts['parse_failed']} "
          f"api_failed={counts['api_failed']} retried={counts['retried_inferences']} "
          f"no_theme={counts['no_theme_labels']} other={counts['other_present']} "
          f"rows={counts['table_rows_for_config']} s_per_review={counts['seconds_per_review']} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "outputs": outputs, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", required=True, choices=["development", "audit", "training_pool"])
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    frozen = load_spec().frozen
    ap.add_argument("--prompt", default=frozen.name if frozen else "label",
                    help=f"prompt name in conf/theme-label-spec.json (default: the frozen "
                         f"{frozen.version if frozen else 'none'})")
    ap.add_argument("--model", default=None, help="override the primary model (the comparison row)")
    ap.add_argument("--limit", type=int, default=None, help="execution setting: infer at most N pending")
    ap.add_argument("--chunk", type=int, default=25, help="execution setting: rows per MERGE")
    args = ap.parse_args()
    spark = build("theme_labels_llm", cores="local[4]", driver_memory="3g")
    try:
        run_label_themes(spark, sample=args.sample, scope=args.scope, category=args.category,
                         prompt_name=args.prompt, model_id=args.model, limit=args.limit,
                         chunk=args.chunk)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

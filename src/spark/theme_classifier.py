"""Jobs `theme_classifier_train` / `theme_classifier_score`: the MLlib baseline (ADR-0002, RR-23).

One `spark.ml` text pipeline -- RegexTokenizer -> StopWordsRemover -> CountVectorizer -> IDF --
shared by ten binary L2 logistic regressions, one per frozen theme. It trains on the 3,000-review
pre-2020 `training_pool` labelled by the **frozen** prompt, and is scored on the same audit set
as the labeller, beside the star-only baseline.

It is an evaluation row and an in-Spark scale path, never a fourth capability and never a
predictor. Its rows land in `gold.review_theme_labels` with `label_source="classifier"`, so no
aggregation over the primary labels can pick them up by accident.

Two things it deliberately does not learn: `other`, which stays an LLM-only output (ADR-0002),
and the abstention. It emits a theme set and nothing else.

Training drops pool rows that ended `parse_failed` or `api_failed` (RR-23): a row with no label
is not a row with no themes, and training on it teaches the model that long, complaint-dense
reviews are empty. The count dropped is reported. On the *evaluation* sets a failure still
scores as an empty prediction for every system -- that is a different question.

Run:  ./run.sh python -m src.spark.theme_classifier --train
      ./run.sh python -m src.spark.theme_classifier --score --sample audit
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import time
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.feature import IDF, CountVectorizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.ai.labels import idempotency_key, load_taxonomy
from src.ai.theme_labels import ensure_table, label_row, merge_chunk, table_name
from src.common import config as C
from src.common import runs
from src.common.spark import build

sys.stdout.reconfigure(line_buffering=True)

LABEL_SOURCE = "classifier"
SPEC_PATH = C.PROJECT_ROOT / "conf" / "theme-classifier.json"
MODEL_DIR = C.PROJECT_ROOT / "data" / "models" / "theme_classifier"
SPEC_VERSION = runs.THEME_CLASSIFIER_SPEC_VERSION

# Identity settings only, exactly as the embedding spec separates identity from execution
# (ADR-0005): what the model *is*, never how many cores fitted it.
HYPERPARAMS = {
    "tokenizer_pattern": r"[^a-z0-9']+",
    "min_token_length": 2,
    "remove_stopwords": True,
    "vocab_size": 20000,
    "min_df": 3,
    "idf_min_doc_freq": 3,
    "reg_param": 0.05,
    "elastic_net_param": 0.0,     # pure L2 (ADR-0002)
    "max_iter": 100,
    "threshold_grid_step": 0.05,
}


def spec_hash(taxonomy_hash: str, source_config_hash: str) -> str:
    payload = {"hyperparams": HYPERPARAMS, "taxonomy": taxonomy_hash,
               "training_labels_config": source_config_hash, "spec_version": SPEC_VERSION}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def features_pipeline() -> Pipeline:
    tok = RegexTokenizer(inputCol="doc", outputCol="tokens", pattern=HYPERPARAMS["tokenizer_pattern"],
                         minTokenLength=HYPERPARAMS["min_token_length"], toLowercase=True)
    stop = StopWordsRemover(inputCol="tokens", outputCol="kept")
    cv = CountVectorizer(inputCol="kept", outputCol="tf", vocabSize=HYPERPARAMS["vocab_size"],
                         minDF=HYPERPARAMS["min_df"])
    idf = IDF(inputCol="tf", outputCol="features", minDocFreq=HYPERPARAMS["idf_min_doc_freq"])
    return Pipeline(stages=[tok, stop, cv, idf])


def labelled_frame(spark: SparkSession, *, sample: str, scope: str, category: str,
                   source: str, config_hash: str | None):
    """Reviews of one frame with their label sets, as (review_id, doc, themes, rating)."""
    sample_run = runs.latest_success("theme_samples", category=category, data_scope=scope,
                                     params_match={"sample": sample})
    silver_run = runs.latest_success("silver", category=category, data_scope=scope)
    if sample_run is None or silver_run is None:
        raise RuntimeError(f"the classifier needs a theme_samples run for {sample!r} and a silver run")
    asg_out = sample_run["outputs"]["gold.theme_sample_assignments"]
    slv = silver_run["outputs"]["silver.reviews"]
    labels = spark.table(table_name(scope)).filter((F.col("budget_line") == sample)
                                                   & (F.col("label_source") == source))
    if config_hash:
        labels = labels.filter(F.col("inference_config_hash") == config_hash)
    asg = (spark.read.option("snapshot-id", asg_out["snapshot_id"]).table(asg_out["table"])
           .filter(F.col("sample_name") == sample).select("review_id", "parent_asin", "rating"))
    text = (spark.read.option("snapshot-id", slv["snapshot_id"]).table(slv["table"])
            .select("review_id", "title", "text"))
    joined = (labels.select(F.col("source_review_id").alias("review_id"), "label_status", "themes")
              .join(asg, on="review_id", how="inner").join(text, on="review_id", how="inner")
              .withColumn("doc", F.concat_ws(" ", F.coalesce("title", F.lit("")),
                                             F.coalesce("text", F.lit("")))))
    return joined, sample_run, silver_run


def train(spark: SparkSession, *, scope: str, category: str, source_config_hash: str,
          force: bool) -> dict[str, Any]:
    t0 = time.time()
    tax = load_taxonomy()
    if SPEC_PATH.exists() and not force:
        raise SystemExit(f"{SPEC_PATH.name} is already frozen; refitting after an audit number "
                         "exists is what ADR-0002 forbids (pass --force only before the freeze)")
    shash = spec_hash(tax.file_hash, source_config_hash)
    run = runs.start("theme_classifier_train", SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"labels": {"table": table_name(scope), "budget_line": "training_pool",
                                        "label_source": "local_llm",
                                        "inference_config_hash": source_config_hash},
                             "taxonomy": {"path": "conf/theme-taxonomy.json",
                                          "version": tax.version, "file_hash": tax.file_hash}},
                     params={"hyperparams": HYPERPARAMS, "spec_hash": shash,
                             "themes": tax.ids})
    counts: dict[str, Any] = {"spec_hash": shash[:12]}
    outputs: dict[str, Any] = {}
    try:
        pool, _, _ = labelled_frame(spark, sample="training_pool", scope=scope, category=category,
                                    source="local_llm", config_hash=source_config_hash)
        total = pool.count()
        usable = pool.filter(F.col("label_status").isin("succeeded", "model_abstained")).cache()
        n = usable.count()
        counts.update({"pool_rows": total, "training_rows": n, "dropped_failed_rows": total - n})
        if n < 100:
            raise RuntimeError(f"only {n} usable training rows; the pool must be labelled first")
        for theme in tax.ids:
            usable = usable.withColumn(
                f"y_{theme}",
                F.when(F.array_contains(F.transform("themes", lambda t: t.theme_id), theme), 1.0)
                 .otherwise(0.0))
        feats = features_pipeline().fit(usable)
        featured = feats.transform(usable).select("review_id", "features",
                                                  *[f"y_{t}" for t in tax.ids]).cache()
        target = MODEL_DIR / shash[:12]
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        feats.write().overwrite().save(str(target / "features"))
        positives = {}
        for theme in tax.ids:
            pos = featured.filter(F.col(f"y_{theme}") == 1.0).count()
            positives[theme] = pos
            lr = LogisticRegression(featuresCol="features", labelCol=f"y_{theme}",
                                    regParam=HYPERPARAMS["reg_param"],
                                    elasticNetParam=HYPERPARAMS["elastic_net_param"],
                                    maxIter=HYPERPARAMS["max_iter"])
            lr.fit(featured).write().overwrite().save(str(target / f"lr_{theme}"))
            print(f"[classifier] {theme}: {pos} positives of {n}")
        counts["theme_positives"] = positives
        vocab = len(feats.stages[2].vocabulary)
        counts["vocabulary"] = vocab
        outputs["theme_classifier_model"] = {"path": str(target.relative_to(C.PROJECT_ROOT)),
                                             "spec_hash": shash, "themes": len(tax.ids),
                                             "vocabulary": vocab}
        counts["elapsed_s"] = round(time.time() - t0, 1)
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise
    runs.success(run, records_in=counts["pool_rows"], records_out=counts["training_rows"],
                 records_rejected=counts["dropped_failed_rows"], outputs=outputs, counts=counts)
    SPEC_PATH.write_text(json.dumps(
        {"classifier": "mllib_theme_baseline", "spec_version": SPEC_VERSION, "decided_in": "RR-23",
         "spec_hash": shash, "model_path": str(MODEL_DIR.relative_to(C.PROJECT_ROOT) / shash[:12]),
         "taxonomy_version": tax.version, "taxonomy_hash": tax.file_hash,
         "training_labels_config": source_config_hash, "hyperparams": HYPERPARAMS,
         "training_rows": counts["training_rows"], "vocabulary": counts["vocabulary"],
         "thresholds": None, "thresholds_fitted_on": None,
         "note": "thresholds are fitted on development by --fit-thresholds and frozen with the prompt"},
        indent=1) + "\n")
    print(f"CLASSIFIER_TRAIN run_id={run.run_id} spec={shash[:12]} pool={counts['pool_rows']} "
          f"trained_on={counts['training_rows']} dropped_failed={counts['dropped_failed_rows']} "
          f"vocab={counts['vocabulary']} elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "spec_hash": shash}


def _predict(spark: SparkSession, frame, tax, model_path) -> dict[str, dict[str, float]]:
    """Per-review, per-theme probability of the positive class."""
    feats = PipelineModel.load(str(model_path / "features"))
    featured = feats.transform(frame).select("review_id", "features").cache()
    scores: dict[str, dict[str, float]] = {}
    for theme in tax.ids:
        lr = LogisticRegressionModel.load(str(model_path / f"lr_{theme}"))
        for r in lr.transform(featured).select("review_id", "probability").collect():
            scores.setdefault(r["review_id"], {})[theme] = float(r["probability"][1])
    return scores


def fit_thresholds(spark: SparkSession, *, scope: str, category: str, force: bool) -> None:
    """Sweep each theme's probability cut on the development set (RR-23), then freeze."""
    from src.ai.theme_scoring import score_themes
    tax = load_taxonomy()
    spec = json.loads(SPEC_PATH.read_text())
    if spec.get("thresholds") and not force:
        raise SystemExit("thresholds are already frozen; refitting them after an audit number "
                         "exists is what ADR-0002 forbids")
    frame, _, _ = labelled_frame(spark, sample="development", scope=scope, category=category,
                                 source="agent_reference", config_hash=None)
    reference = {r["review_id"]: {t["theme_id"] for t in (r["themes"] or [])}
                 for r in frame.select("review_id", "themes").collect()}
    scores = _predict(spark, frame, tax, C.PROJECT_ROOT / spec["model_path"])
    step = HYPERPARAMS["threshold_grid_step"]
    grid = [round(step * i, 2) for i in range(1, int(1 / step))]
    thresholds, fit = {}, []
    for theme in tax.ids:
        best = (0.0, 0.5)
        for cut in grid:
            pred = {r: ({theme} if scores.get(r, {}).get(theme, 0.0) >= cut else set())
                    for r in reference}
            s = next(x for x in score_themes(reference, pred, [theme], min_support=1))
            if (s.f1 or 0.0) > best[0]:
                best = (s.f1 or 0.0, cut)
        thresholds[theme] = best[1]
        fit.append({"theme_id": theme, "cut": best[1], "development_f1": round(best[0], 4)})
    spec["thresholds"] = thresholds
    spec["thresholds_fitted_on"] = "development"
    spec["threshold_fit"] = fit
    SPEC_PATH.write_text(json.dumps(spec, indent=1) + "\n")
    print("CLASSIFIER_THRESHOLDS " + " ".join(f"{t}={thresholds[t]}" for t in tax.ids)
          + f" out={SPEC_PATH.relative_to(C.PROJECT_ROOT)}")


def score(spark: SparkSession, *, sample: str, scope: str, category: str, chunk: int) -> dict[str, Any]:
    t0 = time.time()
    tax = load_taxonomy()
    spec = json.loads(SPEC_PATH.read_text())
    if not spec.get("thresholds"):
        raise SystemExit("the classifier has no frozen thresholds; run --fit-thresholds first")
    if spec["taxonomy_hash"] != tax.file_hash:
        raise SystemExit("the classifier was trained against a different taxonomy")
    table = table_name(scope)
    ensure_table(spark, table)
    run = runs.start("theme_classifier_score", SPEC_VERSION, category=category, data_scope=scope,
                     inputs={"model": {"path": spec["model_path"], "spec_hash": spec["spec_hash"]},
                             "taxonomy": {"path": "conf/theme-taxonomy.json",
                                          "version": tax.version, "file_hash": tax.file_hash}},
                     params={"budget_line": sample, "label_source": LABEL_SOURCE,
                             "thresholds": spec["thresholds"], "spec_hash": spec["spec_hash"]})
    counts: dict[str, Any] = {"spec_hash": spec["spec_hash"][:12]}
    outputs: dict[str, Any] = {}
    try:
        frame, _, silver_run = labelled_frame(spark, sample=sample, scope=scope, category=category,
                                              source="agent_reference", config_hash=None)
        probs = _predict(spark, frame, tax, C.PROJECT_ROOT / spec["model_path"])
        thresholds = spec["thresholds"]
        rows, hits, empty = [], {t: 0 for t in tax.ids}, 0
        for review_id, per_theme in sorted(probs.items()):
            themes = [{"theme_id": t, "evidence_quote": ""} for t in tax.ids
                      if per_theme.get(t, 0.0) >= thresholds[t]]
            for t in themes:
                hits[t["theme_id"]] += 1
            empty += 1 if not themes else 0
            parsed = {"themes": themes, "other": {"present": False, "phrase": None},
                      "abstain": False, "overall_sentiment": "none", "label_confidence": "low"}
            rows.append(label_row(
                idempotency_key=idempotency_key(
                    source_review_id=review_id, label_source=LABEL_SOURCE,
                    model_id="mllib_logreg", label_spec_version=SPEC_VERSION,
                    prompt_version=f"classifier-v{SPEC_VERSION}",
                    inference_config_hash=spec["spec_hash"]),
                source_review_id=review_id, budget_line=sample, label_source=LABEL_SOURCE,
                model_id="mllib_logreg", label_spec_version=SPEC_VERSION,
                prompt_version=f"classifier-v{SPEC_VERSION}",
                inference_config_hash=spec["spec_hash"], api_mode="local", status="succeeded",
                parsed=parsed,
                attempts=[{"attempt_no": 1, "provider_request_id": None, "raw_response": None,
                           "validation_error": None, "input_tokens": None, "output_tokens": None,
                           "estimated_cost_usd": None, "duration_s": None, "completed_at": None}],
                source_silver_run_id=silver_run["run_id"], run_id=run.run_id))
        for i in range(0, len(rows), chunk):
            merge_chunk(spark, table, rows[i:i + chunk])
        snap = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").first()
        outputs["gold.review_theme_labels"] = {"table": table, "budget_line": sample,
                                               "label_source": LABEL_SOURCE,
                                               "snapshot_id": int(snap["snapshot_id"])}
        counts.update({"reviews_scored": len(rows), "theme_hits": hits,
                       "no_predicted_theme": empty, "elapsed_s": round(time.time() - t0, 1)})
    except Exception as exc:
        runs.failed(run, notes=f"{type(exc).__name__}: {exc}", outputs=outputs, counts=counts)
        raise
    runs.success(run, records_in=counts["reviews_scored"], records_out=counts["reviews_scored"],
                 records_rejected=0, outputs=outputs, counts=counts)
    print(f"CLASSIFIER_SCORE run_id={run.run_id} sample={sample} spec={spec['spec_hash'][:12]} "
          f"reviews={counts['reviews_scored']} no_predicted_theme={counts['no_predicted_theme']} "
          f"elapsed_s={counts['elapsed_s']}")
    return {"run_id": run.run_id, "counts": counts}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    ap.add_argument("--sample", default="audit", choices=["development", "audit"])
    ap.add_argument("--source-config-hash", default=None,
                    help="inference_config_hash of the frozen prompt that labelled the pool")
    ap.add_argument("--chunk", type=int, default=100)
    ap.add_argument("--force", action="store_true")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--train", action="store_true")
    g.add_argument("--fit-thresholds", dest="fit_thresholds", action="store_true")
    g.add_argument("--score", action="store_true")
    args = ap.parse_args()
    spark = build("theme_classifier", cores="local[4]", driver_memory="3g")
    try:
        if args.train:
            if not args.source_config_hash:
                raise SystemExit("--train needs --source-config-hash (the frozen prompt's hash)")
            train(spark, scope=args.scope, category=args.category,
                  source_config_hash=args.source_config_hash, force=args.force)
        elif args.fit_thresholds:
            fit_thresholds(spark, scope=args.scope, category=args.category, force=args.force)
        else:
            score(spark, sample=args.sample, scope=args.scope, category=args.category,
                  chunk=args.chunk)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

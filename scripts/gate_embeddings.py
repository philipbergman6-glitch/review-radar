"""Embeddings gate: re-derive every constituent from the spec, the ledger, Iceberg, ES and the judgements.

Constituents, each on its own line, then `EMBED_GATE=PASS|FAIL`:

  EMBED_SPEC        conf/embedding-spec.json hash, model and revision equal what the latest
                    successful embeddings run recorded; the run reads the latest silver snapshot
  EMBED_TABLE       gold.review_embeddings at the pinned snapshot, rows of the active spec:
                    ID set == silver cohort ID set (text_word_count >= min_words, both ways),
                    rows == distinct ids == ledger records_out, every vector 384-dim unit length
  EMBED_INDEX       latest reviews index run is spec version 2 built from that embeddings run;
                    alias -> ledger index; vector-bearing ES ID set == table ID set
  EMBED_RECALL      ANN recall artefact computed on the live generation under the active spec
                    (recall values are outcomes, printed, not gated)
  EMBED_JUDGEMENTS  pool judged complete through rank 10 for every Embeddings system
  EMBED_HYPOTHESES  comparison recorded on the current set and spec, decision file present
                    (verdicts are outcomes, printed, not gated)

Every `check_*` below is I/O that returns facts; the lines and the verdict are formatted by
`src/gates/embeddings.py`, which is pure and unit-tested with nothing running. On the way out
the gate writes `eval/embeddings/gate.json` for `make eval-table`.

Exit 0 on PASS, 1 otherwise.  Run:  ./run.sh python scripts/gate_embeddings.py [--scope full]
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from pyspark.sql import functions as F

from src.ai.embed import NORM_TOL, table_name
from src.ai.spec import load_spec
from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.common.spark import build
from src.gates import embeddings as gate
from src.serving import judgements as J
from src.serving import projection as P
from src.serving import search as S

RECALL = C.PROJECT_ROOT / "eval" / "embeddings" / "ann_recall.json"
COMPARISON = C.PROJECT_ROOT / "eval" / "embeddings" / "retrieval_comparison.json"
DECISION = C.PROJECT_ROOT / "docs" / "decisions" / "embeddings-retrieval.md"
REQUIRED_QUERIES = 20


def check_spec(scope: str, category: str) -> tuple[dict[str, Any], Any, dict | None]:
    spec = load_spec()
    emb = runs.latest_success("embeddings", category=category, data_scope=scope)
    silver = runs.latest_success("silver", category=category, data_scope=scope)
    if emb is None or silver is None:
        return {"present": False, "ok": False,
                "embeddings_run": "none" if emb is None else "present",
                "silver_run": "none" if silver is None else "present"}, spec, None
    rec = emb["inputs"]["spec"]
    latest_silver = silver["outputs"]["silver.reviews"]["snapshot_id"]
    same = rec["hash"] == spec.hash and rec["model"] == spec.model and rec["revision"] == spec.identity["revision"]
    fresh = int(emb["inputs"]["silver"]["snapshot_id"]) == int(latest_silver)
    return {
        "present": True, "run_id": emb["run_id"], "path": spec.rel_path(), "hash": spec.hash,
        "ledger_hash": rec["hash"], "model": spec.model, "revision": spec.identity["revision"],
        "unchanged": same, "silver_snapshot": emb["inputs"]["silver"]["snapshot_id"],
        "latest_silver_snapshot": latest_silver, "fresh": fresh, "ok": same and fresh,
    }, spec, emb


def check_table(spark, spec, emb: dict, scope: str) -> tuple[dict[str, Any], set[str]]:
    out = emb["outputs"]["gold.review_embeddings"]
    table = table_name(scope)
    silver_in = emb["inputs"]["silver"]
    rows = (spark.read.option("snapshot-id", int(out["snapshot_id"])).table(table)
            .filter(F.col("embedding_spec_hash") == spec.hash))
    cohort = (spark.read.option("snapshot-id", int(silver_in["snapshot_id"])).table(silver_in["table"])
              .filter(F.col("text_word_count") >= spec.min_words).select("review_id"))
    agg = rows.agg(
        F.count("*").alias("rows"), F.countDistinct("review_id").alias("distinct"),
        F.sum(F.when(F.size("vector") == spec.dims, 1).otherwise(0)).alias("dims_ok"),
        F.sum(F.when(F.abs(F.sqrt(F.aggregate("vector", F.lit(0.0),
                                               lambda acc, x: acc + x.cast("double") * x.cast("double"))) - 1.0)
                     < NORM_TOL, 1).otherwise(0)).alias("unit_norm")).first()
    ids = rows.select("review_id")
    missing = cohort.join(ids, "review_id", "left_anti").count()      # in cohort, not embedded
    extra = ids.join(cohort, "review_id", "left_anti").count()        # embedded, not in cohort
    cohort_n = cohort.count()
    n = int(agg["rows"])
    ok = (missing == 0 and extra == 0 and n == int(agg["distinct"]) == emb["records_out"] == cohort_n
          and n == int(agg["dims_ok"]) == int(agg["unit_norm"]))
    facts = {
        "present": True, "table": out["table"], "snapshot": out["snapshot_id"],
        "spec_hash": spec.hash, "rows": n, "distinct": agg["distinct"],
        "records_out": emb["records_out"], "cohort": cohort_n, "missing": missing,
        "extra": extra, "dims_ok": agg["dims_ok"], "unit_norm": agg["unit_norm"], "ok": ok,
    }
    return facts, ({r[0] for r in ids.collect()} if ok else set())


def es_vector_ids(es, index: str) -> set[str]:
    ids: set[str] = set()
    after = None
    while True:
        resp = es.search(index=index, size=10_000, query=S.COHORT_FILTER, _source=False,
                         sort=[{"review_id": "asc"}], search_after=after)
        hits = resp["hits"]["hits"]
        if not hits:
            return ids
        ids.update(h["_id"] for h in hits)
        after = hits[-1]["sort"]


def check_index(es, emb: dict, table_ids: set[str], scope: str, category: str) -> dict[str, Any]:
    row = runs.latest_success("search_index_reviews", category=category, data_scope=scope)
    if row is None:
        return {"present": False, "ok": False}
    out = row["outputs"]["es.reviews"]
    alias, index = out["alias"], out["index"]
    targets = P.alias_targets(es, alias)
    v2 = row["spec_version"] == "2" and row["inputs"].get("embeddings", {}).get("run_id") == emb["run_id"]
    es_ids = es_vector_ids(es, alias) if targets == [index] and v2 else set()
    same = es_ids == table_ids and bool(table_ids)
    matches = targets == [index]
    return {
        "present": True, "run_id": row["run_id"], "spec_version": row["spec_version"],
        "alias": alias, "alias_targets": targets, "index": index,
        "alias_matches_ledger": matches, "built_from_embeddings_run": v2,
        "es_docs": len(es_ids), "table_rows": len(table_ids),
        "es_only": len(es_ids - table_ids), "table_only": len(table_ids - es_ids),
        "id_sets_equal": same, "ok": matches and v2 and same,
    }


def check_recall(es, spec) -> dict[str, Any]:
    if not RECALL.exists():
        return {"present": False, "ok": False}
    r = json.loads(RECALL.read_text())
    targets = P.alias_targets(es, "reviews")
    d, fq = r["document_queries"], r["frozen_queries"]
    return {
        "present": True, "index": r["index"], "on_live": r["index"] in targets,
        "spec_matches": r["embedding_spec_hash"] == spec.hash, "k": r["knn"]["k"],
        "num_candidates": r["knn"]["num_candidates"], "doc_queries": d["n"],
        "doc_mean": d["mean"], "doc_min": d["min"],
        "frozen_mean": fq["mean"], "frozen_min": fq["min"],
        "ok": bool(r["index"] in targets and r["embedding_spec_hash"] == spec.hash and d["n"] > 0),
    }


def check_judgements() -> tuple[dict[str, Any], J.JudgementSet]:
    qs = J.load_queries()
    systems = S.systems_in("embeddings")
    comp = J.completeness(J.load_pool(), J.load_judgements(), systems=systems)
    complete = [q.id for q in qs.queries if comp.get(q.id, {}).get("complete")]
    return {
        "set_version": qs.version, "hash": qs.hash, "systems": systems,
        "pooled": sum(v["pooled"] for v in comp.values()),
        "judged": sum(v["judged"] for v in comp.values()), "complete": len(complete),
        "required": REQUIRED_QUERIES, "ok": len(complete) == REQUIRED_QUERIES,
    }, qs


def check_hypotheses(qs: J.JudgementSet, spec) -> dict[str, Any]:
    if not COMPARISON.exists():
        return {"present": False, "ok": False}
    r = json.loads(COMPARISON.read_text())
    cells = " ".join(f"{h.split()[0]}{'' if '(' not in h else h[h.index('(') + 1:-1][:4]}={v['verdict']}"
                     for h, v in r["hypotheses"].items())
    set_ok, spec_ok = r.get("judgement_set_hash") == qs.hash, r.get("embedding_spec_hash") == spec.hash
    return {
        "present": True, "complete": bool(r.get("complete")), "set_hash_matches": set_ok,
        "spec_matches": spec_ok, "decision_file": DECISION.exists(), "cells": cells,
        "judges": r.get("judges"),
        "ok": bool(r.get("complete") and set_ok and spec_ok and DECISION.exists()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    es = P.client()
    spec_facts, spec, emb = check_spec(args.scope, args.category)
    table_facts: dict[str, Any] = {"present": False, "ok": False}
    index_facts: dict[str, Any] = {"present": False, "ok": False}
    if emb is not None:
        spark = build("gate-embeddings", cores="local[4]", driver_memory="2g")
        try:
            table_facts, table_ids = check_table(spark, spec, emb, args.scope)
        finally:
            spark.stop()
        index_facts = check_index(es, emb, table_ids, args.scope, args.category)
    judgements, qs = check_judgements()
    facts = {
        "spec": spec_facts, "table": table_facts, "index": index_facts,
        "recall": check_recall(es, spec), "judgements": judgements,
        "hypotheses": check_hypotheses(qs, spec),
    }
    v = gate.verdict(facts, scope=args.scope)
    v.emit()
    if emb is None:
        sys.exit(1)   # no ledger row: nothing to attribute the result to, so nothing to publish
    E.record(v, capability="embeddings", phase="P5 Embeddings", kind="reproducibility",
             protocol_hash=spec.hash, model=spec.model,
             population={"name": table_facts.get("table", "gold.review_embeddings"),
                         "n": table_facts.get("rows", 0),
                         "silver_cohort": table_facts.get("cohort"),
                         "embeddings_snapshot_id": table_facts.get("snapshot")},
             pipeline_run_id=emb["run_id"], scope=args.scope,
             notes=[(f"spec revision {spec.identity['revision']}; judgement set "
                     f"{judgements['set_version']} ({judgements['hash'][:12]})")])
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

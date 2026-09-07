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

Exit 0 on PASS, 1 otherwise.  Run:  ./run.sh python scripts/gate_embeddings.py [--scope full]
"""
from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import functions as F

from src.ai.embed import NORM_TOL, table_name
from src.ai.spec import load_spec
from src.common import config as C
from src.common import runs
from src.common.spark import build
from src.serving import judgements as J
from src.serving import projection as P
from src.serving import search as S

RECALL = C.PROJECT_ROOT / "eval" / "embeddings" / "ann_recall.json"
COMPARISON = C.PROJECT_ROOT / "eval" / "embeddings" / "retrieval_comparison.json"
DECISION = C.PROJECT_ROOT / "docs" / "decisions" / "embeddings-retrieval.md"


def b(x) -> str:
    return str(bool(x)).lower()


def check_spec(scope: str, category: str):
    spec = load_spec()
    emb = runs.latest_success("embeddings", category=category, data_scope=scope)
    silver = runs.latest_success("silver", category=category, data_scope=scope)
    if emb is None or silver is None:
        print(f"EMBED_SPEC embeddings_run={'none' if emb is None else 'present'} "
              f"silver_run={'none' if silver is None else 'present'} ok=false")
        return False, spec, None
    rec = emb["inputs"]["spec"]
    same = rec["hash"] == spec.hash and rec["model"] == spec.model and rec["revision"] == spec.identity["revision"]
    fresh = int(emb["inputs"]["silver"]["snapshot_id"]) == int(silver["outputs"]["silver.reviews"]["snapshot_id"])
    ok = same and fresh
    print(f"EMBED_SPEC run_id={emb['run_id']} path={spec.rel_path()} hash={spec.hash[:12]} "
          f"ledger_hash={rec['hash'][:12]} model={spec.model} revision={spec.identity['revision'][:12]} "
          f"unchanged_since_run={b(same)} silver_snapshot={emb['inputs']['silver']['snapshot_id']} "
          f"latest_silver_snapshot={silver['outputs']['silver.reviews']['snapshot_id']} reads_latest_silver={b(fresh)} "
          f"ok={b(ok)}")
    return ok, spec, emb


def check_table(spark, spec, emb: dict, scope: str) -> tuple[bool, set[str]]:
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
    print(f"EMBED_TABLE table={out['table']} snapshot={out['snapshot_id']} spec_hash={spec.hash[:12]} rows={n} "
          f"distinct={agg['distinct']} records_out={emb['records_out']} silver_cohort={cohort_n} "
          f"cohort_not_embedded={missing} embedded_not_in_cohort={extra} dims_ok={agg['dims_ok']} "
          f"unit_norm={agg['unit_norm']} ok={b(ok)}")
    table_ids = {r[0] for r in ids.collect()} if ok else set()
    return ok, table_ids


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


def check_index(es, emb: dict, table_ids: set[str], scope: str, category: str) -> bool:
    row = runs.latest_success("search_index_reviews", category=category, data_scope=scope)
    if row is None:
        print("EMBED_INDEX ledger_row=none ok=false")
        return False
    out = row["outputs"]["es.reviews"]
    alias, index = out["alias"], out["index"]
    targets = P.alias_targets(es, alias)
    v2 = row["spec_version"] == "2" and row["inputs"].get("embeddings", {}).get("run_id") == emb["run_id"]
    es_ids = es_vector_ids(es, alias) if targets == [index] and v2 else set()
    same = es_ids == table_ids and bool(table_ids)
    ok = targets == [index] and v2 and same
    print(f"EMBED_INDEX run_id={row['run_id']} spec_version={row['spec_version']} alias={alias} "
          f"alias_targets={','.join(targets) or 'none'} ledger_index={index} alias_matches_ledger={b(targets == [index])} "
          f"built_from_embeddings_run={b(v2)} es_vector_docs={len(es_ids)} table_rows={len(table_ids)} "
          f"es_only={len(es_ids - table_ids)} table_only={len(table_ids - es_ids)} id_sets_equal={b(same)} ok={b(ok)}")
    return ok


def check_recall(es, spec) -> bool:
    if not RECALL.exists():
        print("EMBED_RECALL artefact=missing ok=false")
        return False
    r = json.loads(RECALL.read_text())
    targets = P.alias_targets(es, "reviews")
    ok = r["index"] in targets and r["embedding_spec_hash"] == spec.hash and r["document_queries"]["n"] > 0
    d, fq = r["document_queries"], r["frozen_queries"]
    print(f"EMBED_RECALL index={r['index']} on_live_generation={b(r['index'] in targets)} "
          f"spec_matches={b(r['embedding_spec_hash'] == spec.hash)} k={r['knn']['k']} "
          f"num_candidates={r['knn']['num_candidates']} doc_queries={d['n']} doc_recall10_mean={d['mean']} "
          f"doc_recall10_min={d['min']} frozen_recall10_mean={fq['mean']} frozen_recall10_min={fq['min']} ok={b(ok)}")
    return bool(ok)


def check_judgements() -> tuple[bool, J.JudgementSet]:
    qs = J.load_queries()
    systems = S.systems_in("embeddings")
    comp = J.completeness(J.load_pool(), J.load_judgements(), systems=systems)
    complete = [q.id for q in qs.queries if comp.get(q.id, {}).get("complete")]
    pooled = sum(v["pooled"] for v in comp.values())
    judged = sum(v["judged"] for v in comp.values())
    ok = len(complete) == 20
    print(f"EMBED_JUDGEMENTS set_version={qs.version} hash={qs.hash[:12]} systems={','.join(systems)} "
          f"pooled={pooled} judged={judged} complete_queries={len(complete)}/20 ok={b(ok)}")
    return ok, qs


def check_hypotheses(qs: J.JudgementSet, spec) -> bool:
    if not COMPARISON.exists():
        print("EMBED_HYPOTHESES comparison=missing ok=false")
        return False
    r = json.loads(COMPARISON.read_text())
    ok = (r.get("complete") and r.get("judgement_set_hash") == qs.hash
          and r.get("embedding_spec_hash") == spec.hash and DECISION.exists())
    cells = " ".join(f"{h.split()[0]}{'' if '(' not in h else h[h.index('(') + 1:-1][:4]}={v['verdict']}"
                     for h, v in r["hypotheses"].items())
    print(f"EMBED_HYPOTHESES complete={b(r.get('complete'))} set_hash_matches={b(r.get('judgement_set_hash') == qs.hash)} "
          f"spec_matches={b(r.get('embedding_spec_hash') == spec.hash)} decision_file={b(DECISION.exists())} "
          f"{cells} judges={r.get('judges')} ok={b(ok)}")
    return bool(ok)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    es = P.client()
    s_ok, spec, emb = check_spec(args.scope, args.category)
    t_ok, i_ok = False, False
    if emb is not None:
        spark = build("gate-embeddings", cores="local[4]", driver_memory="2g")
        try:
            t_ok, table_ids = check_table(spark, spec, emb, args.scope)
        finally:
            spark.stop()
        i_ok = check_index(es, emb, table_ids, args.scope, args.category)
    r_ok = check_recall(es, spec)
    j_ok, qs = check_judgements()
    h_ok = check_hypotheses(qs, spec)
    ok = all((s_ok, t_ok, i_ok, r_ok, j_ok, h_ok))
    print(f"EMBED_GATE scope={args.scope} spec={b(s_ok)} table={b(t_ok)} index={b(i_ok)} recall={b(r_ok)} "
          f"judgements={b(j_ok)} hypotheses={b(h_ok)} EMBED_GATE={'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

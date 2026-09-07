"""ANN recall@10 of the HNSW `knn` system against exact cosine search on the live alias.

Two query sets, both seeded: (a) `--n` vector-cohort reviews drawn at random from the alias,
each queried by its own stored vector with itself excluded from both result lists, and (b) the
20 frozen judgement queries encoded by the spec model. For each query the approximate top-10
(`k=50, num_candidates=200`, the production setting) is compared with the brute-force top-10
over every vector-bearing document; recall@10 = |approx ∩ exact| / 10. Numbers are outcomes,
written to eval/embeddings/ann_recall.json and reported by the gate, never a pass line.

Run:  ./run.sh python scripts/ann_recall.py [--n 200] [--seed 20260907]
"""
from __future__ import annotations

import argparse
import json
import statistics
from datetime import UTC, datetime

from src.ai.embedder import encode_query
from src.ai.spec import load_spec
from src.common import config as C
from src.serving import judgements as J
from src.serving import projection as P
from src.serving import search as S

OUT = C.PROJECT_ROOT / "eval" / "embeddings" / "ann_recall.json"
ALIAS = "reviews"
DEPTH = 10


def sample_cohort(es, n: int, seed: int) -> list[dict]:
    resp = es.search(index=ALIAS, size=n, _source=["review_id", S.VECTOR_FIELD],
                     query={"function_score": {"query": S.COHORT_FILTER,
                                               "random_score": {"seed": seed, "field": "review_id"},
                                               "boost_mode": "replace"}})
    hits = resp["hits"]["hits"]
    if len(hits) != n:
        raise SystemExit(f"asked for {n} cohort documents, alias returned {len(hits)}")
    return [h["_source"] for h in hits]


def recall(es, qv: list[float], exclude: list[str] | None) -> tuple[float, list[str], list[str]]:
    approx = [h.review_id for h in S.knn(es, ALIAS, qv, size=DEPTH, exclude_ids=exclude)]
    exact = [h.review_id for h in S.exact_knn(es, ALIAS, qv, size=DEPTH, exclude_ids=exclude)]
    if len(exact) != DEPTH or len(approx) != DEPTH:
        raise SystemExit(f"short result list: approx {len(approx)} exact {len(exact)}")
    return len(set(approx) & set(exact)) / DEPTH, approx, exact


def summarise(values: list[float]) -> dict:
    return {"n": len(values), "mean": round(statistics.mean(values), 4), "min": min(values),
            "median": statistics.median(values), "perfect": sum(v == 1.0 for v in values),
            "below_0_8": sum(v < 0.8 for v in values)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=200)
    ap.add_argument("--seed", type=int, default=20260907)
    args = ap.parse_args()
    es = P.client()
    spec = load_spec()
    targets = P.alias_targets(es, ALIAS)
    if len(targets) != 1:
        raise SystemExit(f"alias {ALIAS} must point at exactly one generation, got {targets}")
    vector_docs = P.count_with_vector(es, ALIAS)
    if vector_docs == 0:
        raise SystemExit(f"alias {ALIAS} carries no vectors; run make index-reviews-vectors first")

    doc_recalls = []
    for d in sample_cohort(es, args.n, args.seed):
        r, _, _ = recall(es, d[S.VECTOR_FIELD], exclude=[d["review_id"]])
        doc_recalls.append(r)
    query_rows = []
    for q in J.load_queries().queries:
        r, approx, exact = recall(es, encode_query(q.query, spec), exclude=None)
        query_rows.append({"query_id": q.id, "recall_at_10": r, "approx": approx, "exact": exact})
    q_recalls = [row["recall_at_10"] for row in query_rows]

    result = {"computed_at": datetime.now(UTC).isoformat(), "alias": ALIAS, "index": targets[0],
              "embedding_spec_hash": spec.hash, "vector_docs": vector_docs, "depth": DEPTH,
              "knn": {"k": S.KNN_K, "num_candidates": S.KNN_NUM_CANDIDATES},
              "seed": args.seed, "document_queries": summarise(doc_recalls),
              "frozen_queries": summarise(q_recalls), "frozen_query_rows": query_rows}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True))
    d, fq = result["document_queries"], result["frozen_queries"]
    print(f"ANN_RECALL index={targets[0]} spec_hash={spec.hash[:12]} vector_docs={vector_docs} "
          f"k={S.KNN_K} num_candidates={S.KNN_NUM_CANDIDATES} doc_queries={d['n']} doc_recall10_mean={d['mean']} "
          f"doc_recall10_min={d['min']} doc_below_0.8={d['below_0_8']} frozen_queries={fq['n']} "
          f"frozen_recall10_mean={fq['mean']} frozen_recall10_min={fq['min']} out={OUT.relative_to(C.PROJECT_ROOT)}")


if __name__ == "__main__":
    main()

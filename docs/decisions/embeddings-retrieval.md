# Embeddings retrieval — kNN and hybrid against BM25

Measured 2026-09-07 on judgement set `570f31717e1b` (20 queries, 10 lexical + 10 descriptive), pool depth 10, embedding spec `b1908a97527f` (sentence-transformers/all-MiniLM-L6-v2). Judgements complete for every system. Numbers are outcomes, not thresholds; the two tables are never merged (ADR-0005).

## controlled table

| system | stratum | macro P@5 | MRR@10 |
|---|---|---|---|
| bm25_cohort | lexical | 0.840 | 0.950 |
| bm25_cohort | descriptive | 0.660 | 0.792 |
| bm25_cohort | overall | 0.750 | 0.871 |
| knn | lexical | 0.920 | 1.000 |
| knn | descriptive | 0.780 | 0.900 |
| knn | overall | 0.850 | 0.950 |
| hybrid_cohort | lexical | 0.780 | 1.000 |
| hybrid_cohort | descriptive | 0.800 | 0.933 |
| hybrid_cohort | overall | 0.790 | 0.967 |

## production table

| system | stratum | macro P@5 | MRR@10 |
|---|---|---|---|
| bm25_stemmed | lexical | 0.860 | 1.000 |
| bm25_stemmed | descriptive | 0.760 | 0.861 |
| bm25_stemmed | overall | 0.810 | 0.931 |
| hybrid | lexical | 0.780 | 0.950 |
| hybrid | descriptive | 0.860 | 0.933 |
| hybrid | overall | 0.820 | 0.942 |
| knn | lexical | 0.920 | 1.000 |
| knn | descriptive | 0.780 | 0.900 |
| knn | overall | 0.850 | 0.950 |

Hypotheses (frozen in conf/search/queries.json before any kNN result was inspected; the reading of which table and systems each one compares was fixed in scripts/eval_embeddings.py before pooling):

- H-E1 BM25 >= kNN on lexical: **not held** (bm25_cohort 0.840 vs knn 0.920)
- H-E2 kNN >= BM25 on descriptive: **held** (knn 0.780 vs bm25_cohort 0.660)
- H-E3 hybrid >= BM25 overall (controlled): **held** (hybrid_cohort 0.790 vs bm25_cohort 0.750)
- H-E3 hybrid >= BM25 overall (production): **held** (hybrid 0.820 vs bm25_stemmed 0.810)

Reading the verdicts: with 10 queries per stratum one relevant document moves macro P@5 by 0.020, so a margin of a few hundredths is within a single query's noise; verdicts are the sign of the difference on this set, not a significance claim, and no confidence interval is computed on n=20. The serving default stays `bm25_stemmed` (frozen in P4); `knn` and `hybrid` are additional systems exposed by `src/serving/search.py`, and any promotion would need its own frozen hypothesis and a fresh judgement round.

Judges (labels per judge name in judgements.jsonl): `claude` 575, `philip` 4. Labels under `claude` are model-generated against the frozen relevance rules, not human judgements; treat every number above as model-judged relevance. Human labels under `philip` are the audit set.

Source: `eval/embeddings/retrieval_comparison.json`, `eval/search/judgements.jsonl`, `eval/embeddings/ann_recall.json` (ANN recall against exact search).

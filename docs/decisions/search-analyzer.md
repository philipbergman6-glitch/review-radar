# Search analyzer decision — frozen BM25 default: `bm25_stemmed`

Measured 2026-09-07 on judgement set `570f31717e1b` (20 queries, 10 lexical + 10 descriptive), pool depth 10, judgements complete for every system. Numbers are outcomes, not thresholds.

| system | stratum | macro P@5 | MRR@10 |
|---|---|---|---|
| bm25_plain | lexical | 0.820 | 0.950 |
| bm25_plain | descriptive | 0.700 | 0.800 |
| bm25_plain | overall | 0.760 | 0.875 |
| bm25_stemmed | lexical | 0.860 | 1.000 |
| bm25_stemmed | descriptive | 0.760 | 0.861 |
| bm25_stemmed | overall | 0.810 | 0.931 |

Hypotheses (frozen in conf/search/queries.json before judging):

- H-A1 stemmed >= plain on descriptive: **held**
- H-A2 |stemmed - plain| <= 0.1 on lexical: **held**

Rule: higher overall macro P@5 wins, tie -> bm25_plain. Result: `bm25_stemmed` is the analyzer family the production BM25 leg uses from here on.

Judges (labels per judge name in judgements.jsonl): `claude` 269, `philip` 4. Labels under `claude` are model-generated against the frozen relevance rules, not human judgements; treat every number above as model-judged relevance. Human labels under `philip` are the audit set.

Source: `eval/search/analyzer_comparison.json`, `eval/search/judgements.jsonl`.

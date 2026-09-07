"""Score the Embeddings systems on the complete judgements: two tables, never merged (ADR-0005).

  controlled  (vector cohort only)   bm25_cohort · knn · hybrid_cohort
  production  (every review)         bm25_stemmed · hybrid · knn (cohort kNN, for reference)

Frozen hypotheses (conf/search/queries.json, `hypotheses.embeddings`), read here as:
  H-E1  BM25 >= kNN on the lexical stratum         -> controlled: bm25_cohort vs knn
  H-E2  kNN  >= BM25 on the descriptive stratum    -> controlled: knn vs bm25_cohort
  H-E3  hybrid >= BM25 overall                     -> controlled: hybrid_cohort vs bm25_cohort
                                                      production: hybrid vs bm25_stemmed
This reading was fixed in this file before any kNN or hybrid result was pooled or judged
(commit history is the evidence). Verdicts are held / not held; none is a pass line. Scoring
is scripts/eval_search.score (P@5 primary, MRR@10 secondary), recomputed for every system on
the complete judgements. Writes eval/embeddings/retrieval_comparison.json and
docs/decisions/embeddings-retrieval.md.

Run:  ./run.sh python scripts/eval_embeddings.py
"""
from __future__ import annotations

import json
from datetime import UTC, datetime

from scripts.eval_search import DEPTH, fmt, judge_counts, score
from src.ai.spec import load_spec
from src.common import config as C
from src.serving import judgements as J

OUT_JSON = C.PROJECT_ROOT / "eval" / "embeddings" / "retrieval_comparison.json"
DECISION = C.PROJECT_ROOT / "docs" / "decisions" / "embeddings-retrieval.md"
TABLES = {"controlled": ["bm25_cohort", "knn", "hybrid_cohort"],
          "production": ["bm25_stemmed", "hybrid", "knn"]}
HYPOTHESES = [  # (label, table, stratum, better-or-equal system, baseline system)
    ("H-E1 BM25 >= kNN on lexical", "controlled", "lexical", "bm25_cohort", "knn"),
    ("H-E2 kNN >= BM25 on descriptive", "controlled", "descriptive", "knn", "bm25_cohort"),
    ("H-E3 hybrid >= BM25 overall (controlled)", "controlled", "overall", "hybrid_cohort", "bm25_cohort"),
    ("H-E3 hybrid >= BM25 overall (production)", "production", "overall", "hybrid", "bm25_stemmed"),
]


def main() -> None:
    systems = sorted({s for ss in TABLES.values() for s in ss})
    qs, per_query, summary = score(systems)
    judged = J.load_judgements()
    spec = load_spec()

    for table, ss in TABLES.items():
        print(f"EMBED_EVAL_QUERY table={table} {'id':4} {'stratum':11} " + " ".join(f"{s:>22}" for s in ss))
        for q in qs.queries:
            cells = []
            for s in ss:
                r = per_query[q.id][s]
                cells.append(f"P@5={fmt(r['p5'])} RR={fmt(r['mrr10'])}" if r["complete"]
                             else f"incomplete(u{r['unjudged']},c{r['cannot_judge']})")
            print(f"EMBED_EVAL_QUERY table={table} {q.id:4} {q.stratum:11} " + " ".join(f"{c:>22}" for c in cells))
        for s in ss:
            for stratum, m in summary[s].items():
                print(f"EMBED_EVAL table={table} system={s} stratum={stratum} queries={m['queries']} "
                      f"complete={m['complete']} macro_p5={fmt(m['macro_p5'])} mrr10={fmt(m['mrr10'])}")

    all_complete = all(m["complete"] == m["queries"] for s in systems for m in summary[s].values())
    verdicts: dict[str, dict] = {}
    for label, table, stratum, a, b in HYPOTHESES:
        pa, pb = summary[a][stratum]["macro_p5"], summary[b][stratum]["macro_p5"]
        v = "undecided" if not all_complete else ("held" if pa >= pb else "not held")
        verdicts[label] = {"table": table, "stratum": stratum, "system": a, "baseline": b,
                           "macro_p5": pa, "baseline_macro_p5": pb, "verdict": v}
        print(f"EMBED_HYPOTHESIS {label}: {v} ({a}={fmt(pa)} vs {b}={fmt(pb)})")
    print(f"EMBED_EVAL_SUMMARY complete={str(all_complete).lower()} judgement_set_hash={qs.hash[:12]} "
          f"spec_hash={spec.hash[:12]} judges={judge_counts(judged)}")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps({
        "computed_at": datetime.now(UTC).isoformat(), "judgement_set_hash": qs.hash,
        "embedding_spec_hash": spec.hash, "tables": TABLES, "systems": systems, "complete": all_complete,
        "summary": summary, "per_query": per_query, "hypotheses": verdicts,
        "judges": judge_counts(judged)}, indent=2, sort_keys=True))

    lines = ["# Embeddings retrieval — kNN and hybrid against BM25", "",
             (f"Measured {datetime.now(UTC):%Y-%m-%d} on judgement set `{qs.hash[:12]}` (20 queries, "
             f"10 lexical + 10 descriptive), pool depth {DEPTH}, embedding spec `{spec.hash[:12]}` "
             f"({spec.model}). Judgements {'complete' if all_complete else 'INCOMPLETE'} for every system. "
             "Numbers are outcomes, not thresholds; the two tables are never merged (ADR-0005)."), ""]
    for table, ss in TABLES.items():
        lines += [f"## {table} table", "", "| system | stratum | macro P@5 | MRR@10 |", "|---|---|---|---|"]
        for s in ss:
            for stratum, m in summary[s].items():
                lines.append(f"| {s} | {stratum} | {fmt(m['macro_p5'])} | {fmt(m['mrr10'])} |")
        lines.append("")
    lines += [("Hypotheses (frozen in conf/search/queries.json before any kNN result was inspected; the reading "
              "of which table and systems each one compares was fixed in scripts/eval_embeddings.py before "
              "pooling):"), ""]
    lines += [f"- {h}: **{v['verdict']}** ({v['system']} {fmt(v['macro_p5'])} vs {v['baseline']} "
              f"{fmt(v['baseline_macro_p5'])})" for h, v in verdicts.items()]
    judges = ", ".join(f"`{j}` {n}" for j, n in judge_counts(judged).items())
    lines += ["", (f"Judges (labels per judge name in judgements.jsonl): {judges}. Labels under `claude` are "
              "model-generated against the frozen relevance rules, not human judgements; treat every number "
              "above as model-judged relevance. Human labels under `philip` are the audit set."),
              "", ("Source: `eval/embeddings/retrieval_comparison.json`, `eval/search/judgements.jsonl`, "
              "`eval/embeddings/ann_recall.json` (ANN recall against exact search).")]
    DECISION.parent.mkdir(parents=True, exist_ok=True)
    DECISION.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()

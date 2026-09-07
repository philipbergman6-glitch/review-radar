"""Score every system on the complete judgements: P@5 (primary), MRR@10 (secondary).

A query counts for a system only when every top-10 document of that system is judged
relevant or not_relevant; an unjudged or cannot_judge document leaves the query incomplete
and it is reported, never scored (RR-06 item 7). Prints one SEARCH_EVAL line per system and
stratum, the per-query table, whether the frozen hypotheses held, and writes
eval/search/analyzer_comparison.json plus docs/decisions/search-analyzer.md with the frozen
BM25 default. Outcomes are reported, not gated.

Run:  ./run.sh python scripts/eval_search.py [--systems bm25_plain,bm25_stemmed]
"""
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from src.common import config as C
from src.serving import judgements as J
from src.serving.search import SYSTEMS

OUT_JSON = J.EVAL_DIR / "analyzer_comparison.json"
DECISION = C.PROJECT_ROOT / "docs" / "decisions" / "search-analyzer.md"
DEPTH = 10


def ranked(pool, qid: str, system: str) -> list[str]:
    rows = [(r["ranks"][system], rid) for (q, rid), r in pool.items() if q == qid and system in r["ranks"]]
    return [rid for _, rid in sorted(rows)]


def score(systems: list[str]):
    qs = J.load_queries()
    pool, judged = J.load_pool(), J.load_judgements()
    per_query: dict[str, dict[str, dict]] = {}
    for q in qs.queries:
        per_query[q.id] = {}
        for s in systems:
            docs = ranked(pool, q.id, s)[:DEPTH]
            states = [judged.get((q.id, d), {}).get("state") for d in docs]
            complete = len(docs) == DEPTH and all(st in ("relevant", "not_relevant") for st in states)
            rel = [st == "relevant" for st in states]
            p5 = sum(rel[:5]) / 5 if complete else None
            rr = next((1 / (i + 1) for i, r in enumerate(rel) if r), 0.0) if complete else None
            per_query[q.id][s] = {"complete": complete, "p5": p5, "mrr10": rr, "hits": len(docs),
                                  "unjudged": sum(st is None for st in states),
                                  "cannot_judge": sum(st == "cannot_judge" for st in states)}
    summary = {}
    for s in systems:
        summary[s] = {}
        for stratum in (*J.STRATA, "overall"):
            rows = [per_query[q.id][s] for q in qs.queries if stratum in ("overall", q.stratum)]
            done = [r for r in rows if r["complete"]]
            summary[s][stratum] = {
                "queries": len(rows), "complete": len(done),
                "macro_p5": sum(r["p5"] for r in done) / len(done) if done else None,
                "mrr10": sum(r["mrr10"] for r in done) / len(done) if done else None,
            }
    return qs, per_query, summary


def fmt(x) -> str:
    return "n/a" if x is None else f"{x:.3f}"


def judge_counts(judged: dict) -> dict[str, int]:
    """Judgement rows per judge name, most first; the decision doc discloses these."""
    counts: dict[str, int] = {}
    for row in judged.values():
        counts[row["judge"]] = counts.get(row["judge"], 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: -kv[1]))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--systems", default="bm25_plain,bm25_stemmed")
    args = ap.parse_args()
    systems = args.systems.split(",")
    for s in systems:
        if s not in SYSTEMS:
            raise SystemExit(f"unknown system {s}")
    qs, per_query, summary = score(systems)
    judged = J.load_judgements()

    print(f"SEARCH_EVAL_QUERY {'id':4} {'stratum':11} " + " ".join(f"{s:>22}" for s in systems))
    for q in qs.queries:
        cells = []
        for s in systems:
            r = per_query[q.id][s]
            cells.append(f"P@5={fmt(r['p5'])} RR={fmt(r['mrr10'])}" if r["complete"]
                         else f"incomplete(u{r['unjudged']},c{r['cannot_judge']})")
        print(f"SEARCH_EVAL_QUERY {q.id:4} {q.stratum:11} " + " ".join(f"{c:>22}" for c in cells))
    for s in systems:
        for stratum, m in summary[s].items():
            print(f"SEARCH_EVAL system={s} stratum={stratum} queries={m['queries']} complete={m['complete']} "
                  f"macro_p5={fmt(m['macro_p5'])} mrr10={fmt(m['mrr10'])}")

    all_complete = all(m["complete"] == m["queries"] for s in systems for m in summary[s].values())
    plain, stem = summary.get("bm25_plain"), summary.get("bm25_stemmed")
    verdicts = {}
    default = None
    if plain and stem and all_complete:
        d_p, d_s = plain["descriptive"]["macro_p5"], stem["descriptive"]["macro_p5"]
        l_p, l_s = plain["lexical"]["macro_p5"], stem["lexical"]["macro_p5"]
        verdicts["H-A1 stemmed >= plain on descriptive"] = "held" if d_s >= d_p else "not held"
        verdicts["H-A2 |stemmed - plain| <= 0.1 on lexical"] = "held" if abs(l_s - l_p) <= 0.1 else "not held"
        o_p, o_s = plain["overall"]["macro_p5"], stem["overall"]["macro_p5"]
        default = "bm25_stemmed" if o_s > o_p else "bm25_plain"
    for h, v in verdicts.items():
        print(f"SEARCH_HYPOTHESIS {h}: {v}")
    print(f"SEARCH_ANALYZER_DECISION complete={str(all_complete).lower()} "
          f"frozen_default={default or 'undecided'} judgement_set_hash={qs.hash[:12]}")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps({
        "computed_at": datetime.now(UTC).isoformat(), "judgement_set_hash": qs.hash,
        "systems": systems, "complete": all_complete, "summary": summary, "per_query": per_query,
        "hypotheses": verdicts, "frozen_default": default,
        "judges": judge_counts(judged)}, indent=2, sort_keys=True))
    if all_complete and default:
        DECISION.parent.mkdir(parents=True, exist_ok=True)
        measured = (f"Measured {datetime.now(UTC):%Y-%m-%d} on judgement set `{qs.hash[:12]}` "
                    f"(20 queries, 10 lexical + 10 descriptive), pool depth {DEPTH}, "
                    "judgements complete for every system. Numbers are outcomes, not thresholds.")
        lines = [f"# Search analyzer decision — frozen BM25 default: `{default}`", "", measured, "",
                 "| system | stratum | macro P@5 | MRR@10 |", "|---|---|---|---|"]
        for s in systems:
            for stratum, m in summary[s].items():
                lines.append(f"| {s} | {stratum} | {fmt(m['macro_p5'])} | {fmt(m['mrr10'])} |")
        lines += ["", "Hypotheses (frozen in conf/search/queries.json before judging):", ""]
        lines += [f"- {h}: **{v}**" for h, v in verdicts.items()]
        rule = (f"Rule: higher overall macro P@5 wins, tie -> bm25_plain. Result: `{default}` "
                "is the analyzer family the production BM25 leg uses from here on.")
        judges = ", ".join(f"`{j}` {n}" for j, n in judge_counts(judged).items())
        provenance = (f"Judges (labels per judge name in judgements.jsonl): {judges}. Labels under `claude` are "
                      "model-generated against the frozen relevance rules, not human judgements; treat every "
                      "number above as model-judged relevance. Human labels under `philip` are the audit set.")
        lines += ["", rule, "", provenance,
                  "", "Source: `eval/search/analyzer_comparison.json`, `eval/search/judgements.jsonl`."]
        DECISION.write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()

"""Pool and judge the frozen query set, retriever hidden, order randomised (RR-01 addendum).

`--pool` runs every system in `--systems` (default: the Search analyzers) against the alias,
takes the top 10 per query, and appends any new (query, review) pairs to eval/search/pool.jsonl
with per-system ranks and the pooling round. `--judge` then walks the unjudged pool documents
query by query in a seeded random order, showing the query, its information need and
relevance rule, and the document (title, text, rating, product) -- never which system
ranked it. Keys: r = relevant, n = not relevant, c = cannot judge, s = skip for now,
q = quit. Judgements append to eval/search/judgements.jsonl; rerun to resume.

Run:  ./run.sh python scripts/judge_search.py --pool [--round search]
      ./run.sh python scripts/judge_search.py --judge --judge-name philip
"""
from __future__ import annotations

import argparse
import random
import sys
import textwrap

from src.serving import judgements as J
from src.serving import projection as P
from src.serving.search import SYSTEMS, run_system

ALIAS = "reviews"
DEPTH = 10
SEED = 20260907


def pool(systems: list[str], round_name: str) -> None:
    qs = J.load_queries()
    es = P.client()
    rows = []
    for q in qs.queries:
        ranks: dict[str, dict[str, int]] = {}
        for s in systems:
            hits = run_system(es, ALIAS, s, q.query, size=DEPTH)
            if len(hits) < DEPTH:
                print(f"[pool] {q.id} {s}: only {len(hits)} hits", file=sys.stderr)
            for h in hits:
                ranks.setdefault(h.review_id, {})[s] = h.rank
        rows += [{"query_id": q.id, "review_id": rid, "ranks": r} for rid, r in ranks.items()]
    added = J.add_to_pool(rows, round_name=round_name, set_hash=qs.hash)
    total = len(J.load_pool())
    print(f"SEARCH_POOL round={round_name} systems={','.join(systems)} depth={DEPTH} "
          f"candidates={len(rows)} added={added} pool_total={total} judgement_set_hash={qs.hash[:12]}")


def _fetch(es, review_ids: list[str]) -> dict[str, dict]:
    resp = es.mget(index=ALIAS, ids=review_ids, source_includes=["title", "text", "rating", "product_title",
                                                                 "store", "review_month"])
    return {d["_id"]: d["_source"] for d in resp["docs"] if d.get("found")}


def judge(judge_name: str) -> None:
    qs = J.load_queries()
    pool_rows = J.load_pool()
    done = J.load_judgements()
    es = P.client()
    todo_by_q: dict[str, list[str]] = {}
    for qid, rid in pool_rows:
        j = done.get((qid, rid))
        if j is None or j["state"] == "cannot_judge":
            todo_by_q.setdefault(qid, []).append(rid)
    if not todo_by_q:
        print("nothing to judge: pool fully judged")
        return
    total = sum(len(v) for v in todo_by_q.values())
    n = 0
    for q in qs.queries:
        rids = todo_by_q.get(q.id)
        if not rids:
            continue
        random.Random(f"{SEED}:{q.id}").shuffle(rids)
        docs = _fetch(es, rids)
        for rid in rids:
            d = docs.get(rid)
            if d is None:
                print(f"[judge] {q.id}/{rid} not in alias; skipped", file=sys.stderr)
                continue
            n += 1
            print("\n" + "=" * 100)
            print(f"[{n}/{total}] QUERY {q.id} ({q.stratum}): {q.query}")
            print(f"  need: {q.information_need}")
            print(f"  rule: {q.relevance_rule}")
            print("-" * 100)
            print(f"  product: {d.get('product_title') or '?'}  [{d.get('store') or '?'}]   "
                  f"rating {d.get('rating')}   {d.get('review_month')}")
            print(f"  title:   {d.get('title') or ''}")
            print(textwrap.fill(d.get("text") or "", width=98, initial_indent="  ", subsequent_indent="  "))
            while True:
                key = input("  [r]elevant  [n]ot relevant  [c]annot judge  [s]kip  [q]uit > ").strip().lower()
                if key in ("r", "n", "c", "s", "q"):
                    break
            if key == "q":
                print("stopped; rerun --judge to resume")
                return
            if key == "s":
                continue
            state = {"r": "relevant", "n": "not_relevant", "c": "cannot_judge"}[key]
            pr = pool_rows[(q.id, rid)]["pool_round"]
            J.record_judgement(q.id, rid, state, judge=judge_name, pool_round=pr, set_hash=qs.hash)
    print("\npool fully judged")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pool", action="store_true")
    ap.add_argument("--judge", action="store_true")
    ap.add_argument("--round", default="search")
    ap.add_argument("--systems", default=",".join(s for s, v in SYSTEMS.items() if v["phase"] == "search"))
    ap.add_argument("--judge-name", default="philip")
    args = ap.parse_args()
    if not (args.pool or args.judge):
        ap.error("pass --pool and/or --judge")
    if args.pool:
        pool(args.systems.split(","), args.round)
    if args.judge:
        judge(args.judge_name)


if __name__ == "__main__":
    main()

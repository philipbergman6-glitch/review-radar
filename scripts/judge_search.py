"""Pool and judge the frozen query set, retriever hidden, order randomised (RR-01 addendum).

`--pool` runs every system in `--systems` (default: the Search analyzers) against the alias,
takes the top 10 per query, and appends any new (query, review) pairs to eval/search/pool.jsonl
with per-system ranks and the pooling round. `--judge` then walks the unjudged pool documents
query by query in a seeded random order, showing the query, its information need and
relevance rule, and the document (title, text, rating, product) -- never which system
ranked it. Keys: r = relevant, n = not relevant, c = cannot judge, s = skip for now,
q = quit. Judgements append to eval/search/judgements.jsonl; rerun to resume.

`--export FILE` writes the unjudged pool documents (same fields the interactive judge
shows, retriever hidden) as JSONL for judging outside the terminal; `--import FILE` appends
the labels from such a file (`{query_id, review_id, state[, note]}`) under `--judge-name`.
Every row is validated against the pool and the frozen set hash; a bad row aborts the import.

Run:  ./run.sh python scripts/judge_search.py --pool [--round search|embeddings]
      ./run.sh python scripts/judge_search.py --judge --judge-name philip
      ./run.sh python scripts/judge_search.py --export /path/unjudged.jsonl
      ./run.sh python scripts/judge_search.py --import /path/labels.jsonl --judge-name claude
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import textwrap
from pathlib import Path

from src.serving import judgements as J
from src.serving import projection as P
from src.serving.search import SYSTEMS, run_system, systems_in

ALIAS = "reviews"
DEPTH = 10
SEED = 20260907


def pool(systems: list[str], round_name: str) -> None:
    qs = J.load_queries()
    es = P.client()
    rows = []
    needs_vector = any(SYSTEMS[s]["kind"] != "bm25" for s in systems)
    for q in qs.queries:
        ranks: dict[str, dict[str, int]] = {}
        qv = None
        if needs_vector:
            from src.ai.embedder import encode_query
            qv = encode_query(q.query)
        for s in systems:
            hits = run_system(es, ALIAS, s, q.query, size=DEPTH, query_vector=qv)
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


def _todo(pool_rows, done) -> dict[str, list[str]]:
    todo_by_q: dict[str, list[str]] = {}
    for qid, rid in pool_rows:
        j = done.get((qid, rid))
        if j is None or j["state"] == "cannot_judge":
            todo_by_q.setdefault(qid, []).append(rid)
    return todo_by_q


def export(path: Path) -> None:
    qs = J.load_queries()
    pool_rows, done = J.load_pool(), J.load_judgements()
    todo_by_q = _todo(pool_rows, done)
    es = P.client()
    n = 0
    with path.open("w") as f:
        for q in qs.queries:
            rids = todo_by_q.get(q.id, [])
            random.Random(f"{SEED}:{q.id}").shuffle(rids)
            docs = _fetch(es, rids)
            for rid in rids:
                d = docs.get(rid)
                if d is None:
                    print(f"[export] {q.id}/{rid} not in alias; skipped", file=sys.stderr)
                    continue
                f.write(json.dumps({"query_id": q.id, "stratum": q.stratum, "query": q.query,
                                    "information_need": q.information_need, "relevance_rule": q.relevance_rule,
                                    "review_id": rid, "pool_round": pool_rows[(q.id, rid)]["pool_round"],
                                    **{k: d.get(k) for k in ("product_title", "store", "rating", "review_month",
                                                             "title", "text")}}, sort_keys=True) + "\n")
                n += 1
    print(f"SEARCH_EXPORT unjudged={n} queries={len(todo_by_q)} path={path}")


def import_labels(path: Path, judge_name: str) -> None:
    qs = J.load_queries()
    pool_rows = J.load_pool()
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    seen: set[tuple[str, str]] = set()
    for r in rows:  # validate everything before appending anything
        key = (r["query_id"], r["review_id"])
        if key not in pool_rows:
            raise SystemExit(f"{path}: {key} is not in the pool")
        if r["state"] not in J.JUDGEMENT_STATES:
            raise SystemExit(f"{path}: bad state {r['state']!r} for {key}")
        if key in seen:
            raise SystemExit(f"{path}: duplicate row for {key}")
        seen.add(key)
    for r in rows:
        pr = pool_rows[(r["query_id"], r["review_id"])]["pool_round"]
        J.record_judgement(r["query_id"], r["review_id"], r["state"], judge=judge_name, pool_round=pr,
                           set_hash=qs.hash, note=r.get("note", ""))
    comp = J.completeness(pool_rows, J.load_judgements(), systems=list(SYSTEMS))
    complete = sum(v["complete"] for v in comp.values())
    print(f"SEARCH_IMPORT labels={len(rows)} judge={judge_name} complete_queries={complete}/{len(qs.queries)}")


def judge(judge_name: str) -> None:
    qs = J.load_queries()
    pool_rows = J.load_pool()
    done = J.load_judgements()
    es = P.client()
    todo_by_q = _todo(pool_rows, done)
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
    ap.add_argument("--export", type=Path, default=None, metavar="FILE")
    ap.add_argument("--import", dest="import_", type=Path, default=None, metavar="FILE")
    ap.add_argument("--round", default="search", choices=["search", "embeddings"])
    ap.add_argument("--systems", default=None, help="default: every system of the round's phase")
    ap.add_argument("--judge-name", default="philip")
    args = ap.parse_args()
    if not (args.pool or args.judge or args.export or args.import_):
        ap.error("pass --pool, --judge, --export FILE or --import FILE")
    if args.pool:
        pool(args.systems.split(",") if args.systems else systems_in(args.round), args.round)
    if args.export:
        export(args.export)
    if args.import_:
        import_labels(args.import_, args.judge_name)
    if args.judge:
        judge(args.judge_name)


if __name__ == "__main__":
    main()

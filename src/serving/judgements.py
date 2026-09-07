"""The frozen judgement set, the pool and the judgements on disk (RR-01 addendum, RR-06 item 7).

conf/search/queries.json is frozen: its hash is recorded in every pool and judgement row.
eval/search/pool.jsonl holds one row per (query, review) with which systems ranked it where
and in which pooling round it entered. eval/search/judgements.jsonl is append-only; the
latest row per (query, review) wins. Nothing here talks to Elasticsearch.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common import config as C

QUERIES_PATH = C.PROJECT_ROOT / "conf" / "search" / "queries.json"
EVAL_DIR = C.PROJECT_ROOT / "eval" / "search"
POOL_PATH = EVAL_DIR / "pool.jsonl"
JUDGEMENTS_PATH = EVAL_DIR / "judgements.jsonl"

JUDGEMENT_STATES = ("relevant", "not_relevant", "cannot_judge")
STRATA = ("lexical", "descriptive")


@dataclass(frozen=True)
class Query:
    id: str
    stratum: str
    query: str
    information_need: str
    relevance_rule: str


@dataclass(frozen=True)
class JudgementSet:
    version: str
    hash: str
    queries: tuple[Query, ...]
    raw: dict[str, Any]

    def by_id(self) -> dict[str, Query]:
        return {q.id: q for q in self.queries}


def load_queries(path: Path = QUERIES_PATH) -> JudgementSet:
    raw = json.loads(path.read_text())
    digest = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    queries = tuple(Query(q["id"], q["stratum"], q["query"], q["information_need"], q["relevance_rule"])
                    for q in raw["queries"])
    ids = [q.id for q in queries]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate query ids in the judgement set")
    if any(q.stratum not in STRATA for q in queries):
        raise ValueError(f"stratum must be one of {STRATA}")
    if len(queries) != 20 or sum(q.stratum == "lexical" for q in queries) != 10:
        raise ValueError("the judgement set is frozen at 10 lexical + 10 descriptive queries")
    return JudgementSet(str(raw["judgement_set_version"]), digest, queries, raw)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for r in rows:
            f.write(json.dumps(r, sort_keys=True) + "\n")


def load_pool(path: Path = POOL_PATH) -> dict[tuple[str, str], dict[str, Any]]:
    """(query_id, review_id) -> row; later rows merge their system ranks into earlier ones."""
    pool: dict[tuple[str, str], dict[str, Any]] = {}
    for r in _read_jsonl(path):
        key = (r["query_id"], r["review_id"])
        if key in pool:
            pool[key]["ranks"].update(r["ranks"])
        else:
            pool[key] = {**r, "ranks": dict(r["ranks"])}
    return pool


def add_to_pool(rows: list[dict[str, Any]], *, round_name: str, set_hash: str,
                path: Path = POOL_PATH) -> int:
    """Append pool rows `{query_id, review_id, ranks:{system:rank}}` not yet in the pool."""
    existing = load_pool(path)
    new = []
    for r in rows:
        key = (r["query_id"], r["review_id"])
        if key in existing:
            missing = {s: k for s, k in r["ranks"].items() if s not in existing[key]["ranks"]}
            if not missing:
                continue
            r = {**r, "ranks": missing}
        new.append({**r, "pool_round": round_name, "judgement_set_hash": set_hash,
                    "added_at": datetime.now(UTC).isoformat()})
    _append_jsonl(path, new)
    return len(new)


def load_judgements(path: Path = JUDGEMENTS_PATH) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for r in _read_jsonl(path):
        if r["state"] not in JUDGEMENT_STATES:
            raise ValueError(f"bad judgement state {r['state']!r} in {path}")
        out[(r["query_id"], r["review_id"])] = r
    return out


def record_judgement(query_id: str, review_id: str, state: str, *, judge: str, pool_round: str,
                     set_hash: str, note: str = "", path: Path = JUDGEMENTS_PATH) -> None:
    if state not in JUDGEMENT_STATES:
        raise ValueError(state)
    _append_jsonl(path, [{"query_id": query_id, "review_id": review_id, "state": state,
                          "judge": judge, "pool_round": pool_round, "judgement_set_hash": set_hash,
                          "note": note, "judged_at": datetime.now(UTC).isoformat()}])


def completeness(pool: dict[tuple[str, str], dict[str, Any]],
                 judgements: dict[tuple[str, str], dict[str, Any]], *, systems: list[str],
                 depth: int = 10) -> dict[str, dict[str, Any]]:
    """Per query: is every top-`depth` document of every `system` judged relevant/not_relevant?"""
    per_query: dict[str, dict[str, Any]] = defaultdict(lambda: {"pooled": 0, "judged": 0,
                                                                 "cannot_judge": 0, "unjudged": 0})
    for (qid, rid), row in pool.items():
        if not any(s in row["ranks"] and row["ranks"][s] <= depth for s in systems):
            continue
        q = per_query[qid]
        q["pooled"] += 1
        j = judgements.get((qid, rid))
        if j is None:
            q["unjudged"] += 1
        elif j["state"] == "cannot_judge":
            q["cannot_judge"] += 1
        else:
            q["judged"] += 1
    for q in per_query.values():
        q["complete"] = q["unjudged"] == 0 and q["cannot_judge"] == 0
    return dict(per_query)

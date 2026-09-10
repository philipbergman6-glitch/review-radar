"""Search gate: re-derive every constituent from ES, the ledger, Kibana and the judgements.

Constituents (RR-01 log item 10), each printed on its own line, then `SEARCH_GATE=PASS|FAIL`:

  SEARCH_REVIEWS         alias -> ledger index, ES count == records_out, silver identity
  SEARCH_PRODUCT_MONTH   alias -> ledger index, ES count == gold rows, alias snapshot ==
                         the latest gold run's snapshot (lineage, RR-16)
  SEARCH_CONTRACT        contract hash unchanged since the index run; analyzer cases N/N on
                         the live alias; required-field validation rejects; strict mapping
                         rejects an unknown field on the live alias (nothing is written)
  SEARCH_KIBANA          the dashboard saved object exists (Kibana up)
  SEARCH_JUDGEMENTS      judgement set frozen (hash), 20 queries, judgements complete
                         through rank 10 for every Search system
  SEARCH_ANALYZER        comparison recorded and a default frozen (scores are outcomes)

Every `check_*` below is I/O that returns facts; the lines and the verdict are formatted by
`src/gates/search.py`, which is pure and unit-tested with nothing running. On the way out the
gate writes `eval/search/gate.json` for `make eval-table`.

Exit 0 on PASS, 1 otherwise.  Run:  ./run.sh python scripts/gate_search.py [--scope full]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from typing import Any

from elasticsearch import BadRequestError

from src.common import config as C
from src.common import evaluation as E
from src.common import runs
from src.gates import search as gate
from src.serving import judgements as J
from src.serving import projection as P
from src.serving.contract import ContractViolation, load_contract, run_analyzer_tests, validate_doc
from src.serving.search import SYSTEMS

KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")
DASHBOARD_ID = "pm-dashboard"
REQUIRED_QUERIES = 20


def check_reviews(es, scope: str, category: str) -> tuple[dict[str, Any], dict]:
    row = runs.latest_success("search_index_reviews", category=category, data_scope=scope)
    if row is None:
        return {"present": False, "ok": False}, {}
    out = row["outputs"]["es.reviews"]
    alias, index = out["alias"], out["index"]
    targets = P.alias_targets(es, alias)
    es_count = P.count(es, alias) if targets else -1
    c = row["counts"]
    ident = int(c["silver_rows"]) == int(c["docs_sent"]) + int(c["excluded_empty_text"])
    matches = targets == [index]
    return {
        "present": True, "run_id": row["run_id"], "alias": alias, "alias_targets": targets,
        "index": index, "alias_matches_ledger": matches, "es_count": es_count,
        "records_out": row["records_out"], "silver_rows": c["silver_rows"],
        "excluded_empty_text": c["excluded_empty_text"], "identity": ident,
        "ok": matches and es_count == row["records_out"] and ident,
    }, row


def check_product_month(es, scope: str, category: str) -> dict[str, Any]:
    row = runs.latest_success("search_index_product_month", category=category, data_scope=scope)
    gold = runs.latest_success("gold", category=category, data_scope=scope)
    if row is None or gold is None:
        return {"present": False, "ok": False,
                "ledger_row": "none" if row is None else "present",
                "gold_run": "none" if gold is None else "present"}
    out = row["outputs"]["es.product_month"]
    alias, index = out["alias"], out["index"]
    targets = P.alias_targets(es, alias)
    es_count = P.count(es, alias) if targets else -1
    stamped = P.source_snapshot_of(es, alias, "source_gold_snapshot_id") if targets else None
    gold_snapshot = int(gold["outputs"]["gold.product_month"]["snapshot_id"])
    ledger_snapshot = int(row["inputs"]["gold"]["snapshot_id"])
    lineage = stamped == gold_snapshot == ledger_snapshot
    matches = targets == [index]
    return {
        "present": True, "run_id": row["run_id"], "alias": alias, "alias_targets": targets,
        "alias_matches_ledger": matches, "es_count": es_count,
        "gold_rows": row["counts"]["gold_rows"], "alias_snapshot": stamped,
        "ledger_snapshot": ledger_snapshot, "gold_snapshot": gold_snapshot, "lineage": lineage,
        "ok": matches and es_count == row["records_out"] == int(row["counts"]["gold_rows"]) and lineage,
    }


def check_contract(es, reviews_row: dict) -> dict[str, Any]:
    contract = load_contract("reviews")
    alias = reviews_row["outputs"]["es.reviews"]["alias"] if reviews_row else "reviews"
    unchanged = bool(reviews_row) and reviews_row["inputs"]["contract"]["hash"] == contract.hash
    tests = run_analyzer_tests(es, alias, contract) if es.indices.exists_alias(name=alias) else []
    passed = sum(t["ok"] for t in tests)
    try:
        validate_doc(contract, {"review_id": "gate-probe"})
        required_rejects = False
    except ContractViolation:
        required_rejects = True
    try:
        es.index(index=alias, id="gate-probe", document={"review_id": "gate-probe", "not_in_contract": 1})
        strict_rejects = False
        es.delete(index=alias, id="gate-probe", ignore=[404])
    except BadRequestError as exc:
        strict_rejects = "strict_dynamic_mapping_exception" in str(exc)
    return {
        "path": contract.rel_path(), "hash": contract.hash, "unchanged": unchanged,
        "analyzer_passed": passed, "analyzer_total": len(tests),
        "required_rejects": required_rejects, "strict_rejects": strict_rejects,
        "ok": bool(unchanged and tests and passed == len(tests) and required_rejects and strict_rejects),
    }


def check_kibana() -> dict[str, Any]:
    try:
        req = urllib.request.Request(f"{KIBANA_HOST}/api/saved_objects/dashboard/{DASHBOARD_ID}")
        req.add_header("kbn-xsrf", "true")
        with urllib.request.urlopen(req, timeout=15) as resp:
            obj = json.loads(resp.read())
        present = obj.get("id") == DASHBOARD_ID and not obj.get("error")
        reason = ""
    except Exception as exc:  # noqa: BLE001
        present, reason = False, type(exc).__name__
    return {"host": KIBANA_HOST, "dashboard": DASHBOARD_ID, "present": present,
            "reason": reason, "ok": present}


def check_judgements() -> tuple[dict[str, Any], J.JudgementSet]:
    qs = J.load_queries()
    systems = [s for s, v in SYSTEMS.items() if v["phase"] == "search"]
    comp = J.completeness(J.load_pool(), J.load_judgements(), systems=systems)
    complete = [q.id for q in qs.queries if comp.get(q.id, {}).get("complete")]
    return {
        "set_version": qs.version, "hash": qs.hash, "queries": len(qs.queries),
        "systems": systems, "pooled": sum(v["pooled"] for v in comp.values()),
        "judged": sum(v["judged"] for v in comp.values()), "complete": len(complete),
        "required": REQUIRED_QUERIES, "ok": len(complete) == REQUIRED_QUERIES,
    }, qs


def check_analyzer(qs: J.JudgementSet) -> dict[str, Any]:
    path = J.EVAL_DIR / "analyzer_comparison.json"
    decision = C.PROJECT_ROOT / "docs" / "decisions" / "search-analyzer.md"
    if not path.exists():
        return {"present": False, "ok": False}
    r = json.loads(path.read_text())
    matches = r.get("judgement_set_hash") == qs.hash
    cells = " ".join(f"{s}.{st}={m['macro_p5'] if m['macro_p5'] is None else round(m['macro_p5'], 3)}"
                     for s, strata in r["summary"].items() for st, m in strata.items() if st != "overall")
    return {
        "present": True, "complete": bool(r.get("complete")),
        "frozen_default": r.get("frozen_default"), "set_hash_matches": matches,
        "decision_file": decision.exists(), "cells": cells,
        "ok": bool(r.get("complete") and r.get("frozen_default") in SYSTEMS and matches
                   and decision.exists()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    es = P.client()
    reviews, reviews_row = check_reviews(es, args.scope, args.category)
    judgements, qs = check_judgements()
    facts = {
        "reviews": reviews,
        "product_month": check_product_month(es, args.scope, args.category),
        "contract": check_contract(es, reviews_row),
        "kibana": check_kibana(),
        "judgements": judgements,
        "analyzer": check_analyzer(qs),
    }
    v = gate.verdict(facts, scope=args.scope)
    v.emit()
    if not reviews_row:
        sys.exit(1)   # no ledger row: nothing to attribute the result to, so nothing to publish
    E.record(v, capability="search", phase="P4 Search", kind="reproducibility",
             protocol_hash=facts["contract"]["hash"],
             population={"name": f"{reviews['alias']} + product_month aliases",
                         "n": reviews["es_count"],
                         "product_month_docs": facts["product_month"].get("es_count"),
                         "gold_snapshot_id": facts["product_month"].get("gold_snapshot")},
             pipeline_run_id=reviews_row["run_id"], scope=args.scope,
             notes=[(f"judgement set {judgements['set_version']} "
                     f"({judgements['hash'][:12]}); analyzer default "
                     f"{facts['analyzer'].get('frozen_default')}")])
    sys.exit(0 if v.passed else 1)


if __name__ == "__main__":
    main()

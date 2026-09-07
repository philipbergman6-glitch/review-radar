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

Exit 0 on PASS, 1 otherwise.  Run:  ./run.sh python scripts/gate_search.py [--scope full]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

from elasticsearch import BadRequestError

from src.common import config as C
from src.common import runs
from src.serving import judgements as J
from src.serving import projection as P
from src.serving.contract import ContractViolation, load_contract, run_analyzer_tests, validate_doc
from src.serving.search import SYSTEMS

KIBANA_HOST = os.getenv("KIBANA_HOST", "http://localhost:5601")
DASHBOARD_ID = "pm-dashboard"


def b(x) -> str:
    return str(bool(x)).lower()


def check_reviews(es, scope: str, category: str) -> tuple[bool, dict]:
    row = runs.latest_success("search_index_reviews", category=category, data_scope=scope)
    if row is None:
        print("SEARCH_REVIEWS ledger_row=none")
        return False, {}
    out = row["outputs"]["es.reviews"]
    alias, index = out["alias"], out["index"]
    targets = P.alias_targets(es, alias)
    es_count = P.count(es, alias) if targets else -1
    c = row["counts"]
    ident = int(c["silver_rows"]) == int(c["docs_sent"]) + int(c["excluded_empty_text"])
    ok = targets == [index] and es_count == row["records_out"] and ident
    print(f"SEARCH_REVIEWS run_id={row['run_id']} alias={alias} alias_targets={','.join(targets) or 'none'} "
          f"ledger_index={index} alias_matches_ledger={b(targets == [index])} es_count={es_count} "
          f"records_out={row['records_out']} silver_rows={c['silver_rows']} "
          f"excluded_empty_text={c['excluded_empty_text']} identity={b(ident)} ok={b(ok)}")
    return ok, row


def check_product_month(es, scope: str, category: str) -> bool:
    row = runs.latest_success("search_index_product_month", category=category, data_scope=scope)
    gold = runs.latest_success("gold", category=category, data_scope=scope)
    if row is None or gold is None:
        print(f"SEARCH_PRODUCT_MONTH ledger_row={'none' if row is None else 'present'} "
              f"gold_run={'none' if gold is None else 'present'}")
        return False
    out = row["outputs"]["es.product_month"]
    alias, index = out["alias"], out["index"]
    targets = P.alias_targets(es, alias)
    es_count = P.count(es, alias) if targets else -1
    stamped = P.source_snapshot_of(es, alias, "source_gold_snapshot_id") if targets else None
    gold_snapshot = int(gold["outputs"]["gold.product_month"]["snapshot_id"])
    ledger_snapshot = int(row["inputs"]["gold"]["snapshot_id"])
    lineage = stamped == gold_snapshot == ledger_snapshot
    ok = targets == [index] and es_count == row["records_out"] == int(row["counts"]["gold_rows"]) and lineage
    print(f"SEARCH_PRODUCT_MONTH run_id={row['run_id']} alias={alias} alias_targets={','.join(targets) or 'none'} "
          f"alias_matches_ledger={b(targets == [index])} es_count={es_count} gold_rows={row['counts']['gold_rows']} "
          f"alias_snapshot={stamped} ledger_snapshot={ledger_snapshot} latest_gold_snapshot={gold_snapshot} "
          f"lineage={b(lineage)} ok={b(ok)}")
    return ok


def check_contract(es, reviews_row: dict) -> bool:
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
    ok = unchanged and tests and passed == len(tests) and required_rejects and strict_rejects
    print(f"SEARCH_CONTRACT path={contract.rel_path()} hash={contract.hash[:12]} unchanged_since_index={b(unchanged)} "
          f"analyzer_cases={passed}/{len(tests)} required_field_rejects={b(required_rejects)} "
          f"unknown_field_rejected_by_es={b(strict_rejects)} ok={b(ok)}")
    return bool(ok)


def check_kibana() -> bool:
    try:
        req = urllib.request.Request(f"{KIBANA_HOST}/api/saved_objects/dashboard/{DASHBOARD_ID}")
        req.add_header("kbn-xsrf", "true")
        with urllib.request.urlopen(req, timeout=15) as resp:
            obj = json.loads(resp.read())
        present = obj.get("id") == DASHBOARD_ID and not obj.get("error")
        reason = ""
    except Exception as exc:  # noqa: BLE001
        present, reason = False, f" reason={type(exc).__name__}"
    print(f"SEARCH_KIBANA host={KIBANA_HOST} dashboard={DASHBOARD_ID} present={b(present)}{reason} ok={b(present)}")
    return present


def check_judgements() -> tuple[bool, J.JudgementSet]:
    qs = J.load_queries()
    systems = [s for s, v in SYSTEMS.items() if v["phase"] == "search"]
    comp = J.completeness(J.load_pool(), J.load_judgements(), systems=systems)
    complete = [q.id for q in qs.queries if comp.get(q.id, {}).get("complete")]
    pooled = sum(v["pooled"] for v in comp.values())
    judged = sum(v["judged"] for v in comp.values())
    ok = len(complete) == 20
    print(f"SEARCH_JUDGEMENTS set_version={qs.version} hash={qs.hash[:12]} queries={len(qs.queries)} "
          f"systems={','.join(systems)} pooled={pooled} judged={judged} complete_queries={len(complete)}/20 ok={b(ok)}")
    return ok, qs


def check_analyzer(qs: J.JudgementSet) -> bool:
    path = J.EVAL_DIR / "analyzer_comparison.json"
    decision = C.PROJECT_ROOT / "docs" / "decisions" / "search-analyzer.md"
    if not path.exists():
        print("SEARCH_ANALYZER comparison=missing ok=false")
        return False
    r = json.loads(path.read_text())
    ok = r.get("complete") and r.get("frozen_default") in SYSTEMS and r.get("judgement_set_hash") == qs.hash \
        and decision.exists()
    cells = " ".join(f"{s}.{st}={m['macro_p5'] if m['macro_p5'] is None else round(m['macro_p5'], 3)}"
                     for s, strata in r["summary"].items() for st, m in strata.items() if st != "overall")
    print(f"SEARCH_ANALYZER complete={b(r.get('complete'))} frozen_default={r.get('frozen_default')} "
          f"set_hash_matches={b(r.get('judgement_set_hash') == qs.hash)} decision_file={b(decision.exists())} "
          f"{cells} ok={b(ok)}")
    return bool(ok)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--scope", default="full", choices=["full", "sample"])
    ap.add_argument("--category", default=C.CATEGORY)
    args = ap.parse_args()
    es = P.client()
    r_ok, reviews_row = check_reviews(es, args.scope, args.category)
    pm_ok = check_product_month(es, args.scope, args.category)
    c_ok = check_contract(es, reviews_row)
    k_ok = check_kibana()
    j_ok, qs = check_judgements()
    a_ok = check_analyzer(qs)
    ok = all((r_ok, pm_ok, c_ok, k_ok, j_ok, a_ok))
    print(f"SEARCH_GATE scope={args.scope} reviews={b(r_ok)} product_month={b(pm_ok)} contract={b(c_ok)} "
          f"kibana={b(k_ok)} judgements={b(j_ok)} analyzer={b(a_ok)} SEARCH_GATE={'PASS' if ok else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

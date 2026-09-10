"""P4 Search's verdict: six constituents re-derived from ES, the ledger, Kibana and the judgements.

`scripts/gate_search.py` does the reading -- one `check_*` function per constituent, each
returning a plain dict of facts. Everything below is pure over those dicts: the six named
lines, and the terminal `SEARCH_GATE=PASS|FAIL` that summarises them.

The analyzer comparison is the one constituent whose *scores* are outcomes rather than bars.
What is gated is that a comparison was completed on the current judgement set and a default
was frozen in a decision file -- not which analyzer won (ADR-0011: reproducibility blocks,
quality reports).
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.common.evaluation import Verdict, repro_verdict


def b(x: Any) -> str:
    return str(bool(x)).lower()


def reviews_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "SEARCH_REVIEWS ledger_row=none"
    return (f"SEARCH_REVIEWS run_id={f['run_id']} alias={f['alias']} "
            f"alias_targets={','.join(f['alias_targets']) or 'none'} "
            f"ledger_index={f['index']} alias_matches_ledger={b(f['alias_matches_ledger'])} "
            f"es_count={f['es_count']} records_out={f['records_out']} "
            f"silver_rows={f['silver_rows']} excluded_empty_text={f['excluded_empty_text']} "
            f"identity={b(f['identity'])} ok={b(f['ok'])}")


def product_month_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return (f"SEARCH_PRODUCT_MONTH ledger_row={f['ledger_row']} gold_run={f['gold_run']}")
    return (f"SEARCH_PRODUCT_MONTH run_id={f['run_id']} alias={f['alias']} "
            f"alias_targets={','.join(f['alias_targets']) or 'none'} "
            f"alias_matches_ledger={b(f['alias_matches_ledger'])} es_count={f['es_count']} "
            f"gold_rows={f['gold_rows']} alias_snapshot={f['alias_snapshot']} "
            f"ledger_snapshot={f['ledger_snapshot']} latest_gold_snapshot={f['gold_snapshot']} "
            f"lineage={b(f['lineage'])} ok={b(f['ok'])}")


def contract_line(f: Mapping[str, Any]) -> str:
    return (f"SEARCH_CONTRACT path={f['path']} hash={f['hash'][:12]} "
            f"unchanged_since_index={b(f['unchanged'])} "
            f"analyzer_cases={f['analyzer_passed']}/{f['analyzer_total']} "
            f"required_field_rejects={b(f['required_rejects'])} "
            f"unknown_field_rejected_by_es={b(f['strict_rejects'])} ok={b(f['ok'])}")


def kibana_line(f: Mapping[str, Any]) -> str:
    reason = f" reason={f['reason']}" if f.get("reason") else ""
    return (f"SEARCH_KIBANA host={f['host']} dashboard={f['dashboard']} "
            f"present={b(f['present'])}{reason} ok={b(f['present'])}")


def judgements_line(f: Mapping[str, Any]) -> str:
    return (f"SEARCH_JUDGEMENTS set_version={f['set_version']} hash={f['hash'][:12]} "
            f"queries={f['queries']} systems={','.join(f['systems'])} pooled={f['pooled']} "
            f"judged={f['judged']} complete_queries={f['complete']}/{f['required']} "
            f"ok={b(f['ok'])}")


def analyzer_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "SEARCH_ANALYZER comparison=missing ok=false"
    return (f"SEARCH_ANALYZER complete={b(f['complete'])} frozen_default={f['frozen_default']} "
            f"set_hash_matches={b(f['set_hash_matches'])} decision_file={b(f['decision_file'])} "
            f"{f['cells']} ok={b(f['ok'])}")


#: The six constituents, in the order the gate prints and summarises them.
CONSTITUENTS: tuple[tuple[str, str, Any], ...] = (
    ("reviews", "reviews", reviews_line),
    ("product_month", "product_month", product_month_line),
    ("contract", "contract", contract_line),
    ("kibana", "kibana", kibana_line),
    ("judgements", "judgements", judgements_line),
    ("analyzer", "analyzer", analyzer_line),
)


def gate_line(facts: Mapping[str, Mapping[str, Any]], *, scope: str,
              checks: Sequence[tuple[str, bool]]) -> str:
    named = dict(checks)
    ok = all(named.values())
    return (f"SEARCH_GATE scope={scope} reviews={b(named['reviews'])} "
            f"product_month={b(named['product_month'])} contract={b(named['contract'])} "
            f"kibana={b(named['kibana'])} judgements={b(named['judgements'])} "
            f"analyzer={b(named['analyzer'])} SEARCH_GATE={'PASS' if ok else 'FAIL'}")


def verdict(facts: Mapping[str, Mapping[str, Any]], *, scope: str) -> Verdict:
    """`SEARCH_GATE` over the six fact dicts the gate script loaded."""
    missing = [key for key, _, _ in CONSTITUENTS if key not in facts]
    if missing:
        raise ValueError(f"SEARCH_GATE: no facts for {', '.join(missing)} -- a constituent "
                         f"that was not measured cannot be summarised")
    lines = [render(facts[key]) for key, _, render in CONSTITUENTS]
    checks = [(name, bool(facts[key].get("ok"))) for key, name, _ in CONSTITUENTS]
    return repro_verdict("SEARCH_GATE", checks, gate_line(facts, scope=scope, checks=checks),
                         constituents=lines)

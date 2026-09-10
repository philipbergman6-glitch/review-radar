"""P5 Embeddings' verdict: six constituents re-derived from the spec, the ledger, Iceberg, ES.

`scripts/gate_embeddings.py` does the reading; everything here is pure over the fact dicts it
returns. Two of the six carry numbers that are outcomes rather than bars -- ANN recall and the
retrieval hypotheses. What is gated is that they were measured on the live index generation
under the active spec, never which way they came out (ADR-0011).
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.common.evaluation import Verdict, repro_verdict


def b(x: Any) -> str:
    return str(bool(x)).lower()


def spec_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return (f"EMBED_SPEC embeddings_run={f['embeddings_run']} silver_run={f['silver_run']} "
                f"ok=false")
    return (f"EMBED_SPEC run_id={f['run_id']} path={f['path']} hash={f['hash'][:12]} "
            f"ledger_hash={f['ledger_hash'][:12]} model={f['model']} "
            f"revision={f['revision'][:12]} unchanged_since_run={b(f['unchanged'])} "
            f"silver_snapshot={f['silver_snapshot']} "
            f"latest_silver_snapshot={f['latest_silver_snapshot']} "
            f"reads_latest_silver={b(f['fresh'])} ok={b(f['ok'])}")


def table_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "EMBED_TABLE embeddings_run=none ok=false"
    return (f"EMBED_TABLE table={f['table']} snapshot={f['snapshot']} "
            f"spec_hash={f['spec_hash'][:12]} rows={f['rows']} distinct={f['distinct']} "
            f"records_out={f['records_out']} silver_cohort={f['cohort']} "
            f"cohort_not_embedded={f['missing']} embedded_not_in_cohort={f['extra']} "
            f"dims_ok={f['dims_ok']} unit_norm={f['unit_norm']} ok={b(f['ok'])}")


def index_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "EMBED_INDEX ledger_row=none ok=false"
    return (f"EMBED_INDEX run_id={f['run_id']} spec_version={f['spec_version']} "
            f"alias={f['alias']} alias_targets={','.join(f['alias_targets']) or 'none'} "
            f"ledger_index={f['index']} alias_matches_ledger={b(f['alias_matches_ledger'])} "
            f"built_from_embeddings_run={b(f['built_from_embeddings_run'])} "
            f"es_vector_docs={f['es_docs']} table_rows={f['table_rows']} "
            f"es_only={f['es_only']} table_only={f['table_only']} "
            f"id_sets_equal={b(f['id_sets_equal'])} ok={b(f['ok'])}")


def recall_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "EMBED_RECALL artefact=missing ok=false"
    return (f"EMBED_RECALL index={f['index']} on_live_generation={b(f['on_live'])} "
            f"spec_matches={b(f['spec_matches'])} k={f['k']} "
            f"num_candidates={f['num_candidates']} doc_queries={f['doc_queries']} "
            f"doc_recall10_mean={f['doc_mean']} doc_recall10_min={f['doc_min']} "
            f"frozen_recall10_mean={f['frozen_mean']} frozen_recall10_min={f['frozen_min']} "
            f"ok={b(f['ok'])}")


def judgements_line(f: Mapping[str, Any]) -> str:
    return (f"EMBED_JUDGEMENTS set_version={f['set_version']} hash={f['hash'][:12]} "
            f"systems={','.join(f['systems'])} pooled={f['pooled']} judged={f['judged']} "
            f"complete_queries={f['complete']}/{f['required']} ok={b(f['ok'])}")


def hypotheses_line(f: Mapping[str, Any]) -> str:
    if not f.get("present"):
        return "EMBED_HYPOTHESES comparison=missing ok=false"
    return (f"EMBED_HYPOTHESES complete={b(f['complete'])} "
            f"set_hash_matches={b(f['set_hash_matches'])} spec_matches={b(f['spec_matches'])} "
            f"decision_file={b(f['decision_file'])} {f['cells']} judges={f['judges']} "
            f"ok={b(f['ok'])}")


#: The six constituents, in the order the gate prints and summarises them.
CONSTITUENTS: tuple[tuple[str, str, Any], ...] = (
    ("spec", "spec", spec_line),
    ("table", "table", table_line),
    ("index", "index", index_line),
    ("recall", "recall", recall_line),
    ("judgements", "judgements", judgements_line),
    ("hypotheses", "hypotheses", hypotheses_line),
)


def gate_line(*, scope: str, checks: Sequence[tuple[str, bool]]) -> str:
    named = dict(checks)
    ok = all(named.values())
    return (f"EMBED_GATE scope={scope} spec={b(named['spec'])} table={b(named['table'])} "
            f"index={b(named['index'])} recall={b(named['recall'])} "
            f"judgements={b(named['judgements'])} hypotheses={b(named['hypotheses'])} "
            f"EMBED_GATE={'PASS' if ok else 'FAIL'}")


def verdict(facts: Mapping[str, Mapping[str, Any]], *, scope: str) -> Verdict:
    """`EMBED_GATE` over the six fact dicts the gate script loaded."""
    missing = [key for key, _, _ in CONSTITUENTS if key not in facts]
    if missing:
        raise ValueError(f"EMBED_GATE: no facts for {', '.join(missing)} -- a constituent "
                         f"that was not measured cannot be summarised")
    lines = [render(facts[key]) for key, _, render in CONSTITUENTS]
    checks = [(name, bool(facts[key].get("ok"))) for key, name, _ in CONSTITUENTS]
    return repro_verdict("EMBED_GATE", checks, gate_line(scope=scope, checks=checks),
                         constituents=lines)

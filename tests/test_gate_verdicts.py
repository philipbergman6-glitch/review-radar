"""The four built phases' verdicts, and the artefacts they publish (ticket 02).

Every gate in this project is now split at one seam: a pure function in `src/gates/` decides,
and the script around it does the I/O. This file exercises the pure half -- with no Spark, no
Elasticsearch, no Postgres, no Ollama -- and asserts three things of each gate:

* **the terminal line is byte-identical** to what the gate printed before the split, so the
  prefactor is a prefactor;
* **every constituent can trip it** -- flipping any one named constituent turns PASS into
  FAIL. A gate that cannot fail is not a gate (audit F3);
* **the artefact it publishes validates** against `conf/eval-artifact.schema.json`, so an
  invalid result is a crash at the gate rather than a bad row in the table.
"""
from __future__ import annotations

import json

import pytest

from src.common import evaluation as E
from src.gates import embeddings as embeddings_gate
from src.gates import gold as gold_gate
from src.gates import search as search_gate
from src.gates import silver as silver_gate

RUN_ID = "8a1d2f4e-0000-4000-8000-000000000001"
COMMIT = "0f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c"


# ==================================================================== P2 Silver ====
def silver_facts(**over):
    counts = {"collision_rows_removed": 4, "review_id_distinct": 90, "join_cardinality_ok": True,
              "unmatched_review_rows": 0, "unmatched_parent_asins": 0}
    counts.update(over.pop("counts", {}))
    facts = {
        "scope": "full", "bronze_snapshot": 7788, "load_id": "cat-2026-09-01", "catalogue_rows": 500,
        "records_in": 100, "records_rejected": 6, "records_out": 90, "counts": counts,
        "reason_counts": {r: 1 for r in silver_gate.REJECT_REASONS},
        "collisions": {"groups": 3, "exact": 2, "conflicting": 1, "unresolvable": 0,
                       "table_rows": 7, "removed": 4},
    }
    facts.update(over)
    return facts


def test_silver_terminal_line_is_unchanged():
    v = silver_gate.verdict(**silver_facts())
    assert v.terminal == (
        "SILVER_GATE bronze_snapshot=7788 catalogue_load_id=cat-2026-09-01 "
        "catalogue_rows_read=500 bronze_rows=100 reject_rows=6 silver_rows=90 "
        "collision_rows_removed=4 unmatched_review_rows=0 unmatched_parent_asins=0 "
        "join_cardinality_ok=true review_id_unique=true identity=PASS "
        "gate_scope=full SILVER_GATE=PASS")


def test_silver_prints_one_line_per_reject_reason_then_collisions():
    v = silver_gate.verdict(**silver_facts())
    assert len(v.constituents) == len(silver_gate.REJECT_REASONS) + 1
    assert v.constituents[0].startswith("SILVER_REJECT_REASON reason=unparsable_json rows=1")
    assert "below_1995=" in v.constituents[3], "the timestamp reason carries its two diagnostics"
    assert v.constituents[-1].startswith("SILVER_COLLISIONS groups=3")


@pytest.mark.parametrize("over,constituent", [
    ({"records_out": 89}, "identity"),
    ({"counts": {"review_id_distinct": 89}}, "review_id_unique"),
    ({"counts": {"join_cardinality_ok": False}}, "join_cardinality"),
    ({"counts": {"unmatched_review_rows": 1}}, "catalogue_matched"),
])
def test_every_silver_constituent_can_trip_the_gate(over, constituent):
    v = silver_gate.verdict(**silver_facts(**over))
    assert not v.passed
    assert constituent in v.failed
    assert v.terminal.endswith("SILVER_GATE=FAIL")


def test_an_unmatched_catalogue_row_is_tolerated_on_a_sample_but_not_on_the_full_run():
    over = {"counts": {"unmatched_review_rows": 1}}
    assert not silver_gate.verdict(**silver_facts(**over)).passed
    assert silver_gate.verdict(**silver_facts(scope="sample", **over)).passed


def silver_ledger(**over):
    led = {"run_id": RUN_ID, "snapshots_stamped": True, "ledger_matches_tables": True,
           "commit": COMMIT, "dirty": False}
    led.update(over)
    return led


def test_the_gate_script_adds_two_constituents_the_job_cannot_check_about_itself():
    job = silver_gate.verdict(**silver_facts())
    checked = silver_gate.verdict(**silver_facts(), ledger=silver_ledger())
    assert len(checked.checks) == len(job.checks) + 2
    assert checked.terminal == job.terminal, "the extra evidence must not reshape the line"


@pytest.mark.parametrize("field", ["snapshots_stamped", "ledger_matches_tables"])
def test_an_unstamped_snapshot_or_a_disagreeing_ledger_fails_a_gate_the_job_called_pass(field):
    v = silver_gate.verdict(**silver_facts(), ledger=silver_ledger(**{field: False}))
    assert not v.passed and field in v.failed
    assert v.terminal.endswith("SILVER_GATE=FAIL")


def test_a_rerun_with_no_earlier_run_prints_n_a_and_is_counted_nowhere():
    without = silver_gate.verdict(**silver_facts())
    v = silver_gate.verdict(**silver_facts(), rerun={"previous_run": None, "identical": None})
    assert v.constituents[-1] == "SILVER_RERUN previous_run=none identical=n/a"
    assert len(v.checks) == len(without.checks), "an n/a comparison must not inflate the count"
    assert v.passed


def test_a_rerun_that_differs_fails_the_gate():
    v = silver_gate.verdict(**silver_facts(),
                            rerun={"previous_run": RUN_ID, "identical": False})
    assert not v.passed and "rerun_identical" in v.failed


def test_the_silver_artefact_validates_and_carries_its_run_id():
    v = silver_gate.verdict(**silver_facts(), ledger=silver_ledger())
    doc = E.build_artifact(v, capability="silver", phase="P2 Silver", kind="reproducibility",
                           protocol_hash=COMMIT,
                           population={"name": "All_Beauty/silver.reviews", "n": 90},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert doc["pipeline_run_id"] == RUN_ID
    assert doc["metric"] == {"name": "constituents_ok", "value": 6, "threshold": 6,
                             "direction": "eq"}
    assert doc["constituents"][-1] == v.terminal


# ------------------------------------------------------------- silver reproduction ----
def test_the_reproduction_terminal_line_is_unchanged():
    v = silver_gate.repro(scope="full", checks=[("rows_in", True), ("rejects", True)])
    assert v.terminal == ("SILVER_REPRO_GATE scope=full checks=2 failed=none "
                          "SILVER_REPRO_GATE=PASS")


def test_the_reproduction_names_every_check_that_failed_in_order():
    v = silver_gate.repro(scope="full", checks=[("rows_in", False), ("rejects", True),
                                                ("survivor_ids", False)])
    assert v.terminal == ("SILVER_REPRO_GATE scope=full checks=3 failed=rows_in,survivor_ids "
                          "SILVER_REPRO_GATE=FAIL")
    assert not v.passed


def test_a_reproduction_that_compared_nothing_cannot_print_pass():
    with pytest.raises(ValueError, match="no constituents"):
        silver_gate.repro(scope="full", checks=[])


# ====================================================================== P3 Gold ====
def gold_counts(**over):
    c = {
        "products_total": 120, "products_materialised": 100, "products_below_min_reviews": 20,
        "product_months": 2400, "product_months_expected": 2400, "active_product_months": 900,
        "reviews_on_spine": 5000, "silver_rows": 5000, "points": 800, "evaluable": 600,
        "condition_true": 40, "alerts": 12, "evaluable_products": 90,
        "unevaluable_reasons": {"baseline_reviews": 100, "baseline_active_months": 50,
                                "recent_reviews": 30, "recent_active_months": 20},
        "episodes": 12, "episodes_by_closure": {"recovery": 5, "gap": 4, "end_of_data": 3},
        "holdout_points": 200, "holdout_evaluable": 150, "holdout_alerts": 6,
        "holdout_eligible_products": 40, "holdout_alerted_products": 8, "holdout_episodes": 6,
    }
    c.update(over)
    return c


def test_gold_terminal_line_is_unchanged():
    v = gold_gate.verdict(gold_counts(), run_id=RUN_ID, scope="full", rule_status="frozen")
    assert v.terminal == (
        f"GOLD_GATE run_id={RUN_ID} scope=full spine_complete=true reviews_on_spine=5000 "
        f"silver_rows=5000 alerts_equal_episodes=true rule_status=frozen GOLD_GATE=PASS")


def test_gold_prints_its_three_counted_lines_before_the_verdict():
    v = gold_gate.verdict(gold_counts(), run_id=RUN_ID, scope="full", rule_status="frozen")
    assert [c.split()[0] for c in v.constituents] == ["GOLD_SPINE", "GOLD_POINTS", "GOLD_EPISODES"]
    assert "unevaluable_recent_active_months=20" in v.constituents[1]


@pytest.mark.parametrize("over,constituent", [
    ({"product_months": 2399}, "spine_complete"),
    ({"reviews_on_spine": 5001}, "reviews_on_spine"),
    ({"episodes": 11}, "alerts_equal_episodes"),
])
def test_every_gold_constituent_can_trip_the_gate(over, constituent):
    v = gold_gate.verdict(gold_counts(**over), run_id=RUN_ID, scope="full", rule_status="frozen")
    assert not v.passed and constituent in v.failed
    assert v.terminal.endswith("GOLD_GATE=FAIL")


def test_the_analytical_line_is_unchanged_and_reports_rather_than_blocks():
    v = gold_gate.analytical(gold_counts(), rule_status="frozen", holdout_start="2022-01",
                             config_hash=COMMIT)
    assert v.terminal == (
        "GOLD_ANALYTICAL period=holdout eligible_products=40 alerted_products=8 "
        "share_of_eligible=0.2000 holdout_alerts=6 holdout_episodes=6 rule_status=frozen "
        f"rule_config_hash={COMMIT[:12]} verdict=REPORTED")
    assert v.status == "REPORTED"


def test_before_the_freeze_the_analytical_line_says_development_and_names_the_holdout():
    v = gold_gate.analytical(gold_counts(), rule_status="provisional", holdout_start="2022-01",
                             config_hash=COMMIT)
    assert v.terminal.startswith("GOLD_ANALYTICAL period=development(before 2022-01)")
    assert v.status == "REPORTED"
    assert v.metric["threshold"] is None, "a frozen rule's alert rate has no bar to clear"


def test_an_empty_holdout_population_reports_no_number_and_says_why():
    v = gold_gate.analytical(gold_counts(holdout_eligible_products=0), rule_status="frozen",
                             holdout_start="2022-01", config_hash=COMMIT)
    assert v.status == "NOT_RUN"
    assert "no eligible products" in v.cut_reason
    doc = E.build_artifact(v, capability="gold_analytical", phase="P3 Gold", kind="quality",
                           protocol_hash=COMMIT, population={"name": "holdout", "n": 0},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert "metric" not in doc and doc["cut_reason"]


def test_the_two_gold_artefacts_validate_and_split_blocking_from_reported():
    c = gold_counts()
    repro = E.build_artifact(
        gold_gate.verdict(c, run_id=RUN_ID, scope="full", rule_status="frozen"),
        capability="gold", phase="P3 Gold", kind="reproducibility", protocol_hash=COMMIT,
        population={"name": "gold.product_month", "n": 2400}, pipeline_run_id=RUN_ID, scope="full")
    quality = E.build_artifact(
        gold_gate.analytical(c, rule_status="frozen", holdout_start="2022-01", config_hash=COMMIT),
        capability="gold_analytical", phase="P3 Gold", kind="quality", protocol_hash=COMMIT,
        population=gold_gate.analytical_population(c, rule_status="frozen"),
        pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(repro) == [] and E.validate_artifact(quality) == []
    assert repro["status"] == "PASS" and quality["status"] == "REPORTED"
    # REPORTED is a quality vocabulary; the contract must refuse it on a reproducibility claim.
    assert E.validate_artifact({**repro, "status": "REPORTED"}) != []


# ==================================================================== P4 Search ====
def search_facts(**over):
    facts = {
        "reviews": {"present": True, "run_id": RUN_ID, "alias": "reviews",
                    "alias_targets": ["reviews-000002"], "index": "reviews-000002",
                    "alias_matches_ledger": True, "es_count": 700, "records_out": 700,
                    "silver_rows": 720, "excluded_empty_text": 20, "identity": True, "ok": True},
        "product_month": {"present": True, "run_id": RUN_ID, "alias": "product_month",
                          "alias_targets": ["product_month-000001"],
                          "alias_matches_ledger": True, "es_count": 2400, "gold_rows": 2400,
                          "alias_snapshot": 991, "ledger_snapshot": 991, "gold_snapshot": 991,
                          "lineage": True, "ok": True},
        "contract": {"path": "conf/es/reviews.contract.json", "hash": COMMIT, "unchanged": True,
                     "analyzer_passed": 6, "analyzer_total": 6, "required_rejects": True,
                     "strict_rejects": True, "ok": True},
        "kibana": {"host": "http://localhost:5601", "dashboard": "pm-dashboard",
                   "present": True, "reason": "", "ok": True},
        "judgements": {"set_version": "1", "hash": COMMIT, "queries": 20,
                       "systems": ["bm25_default", "bm25_english"], "pooled": 400, "judged": 400,
                       "complete": 20, "required": 20, "ok": True},
        "analyzer": {"present": True, "complete": True, "frozen_default": "bm25_english",
                     "set_hash_matches": True, "decision_file": True,
                     "cells": "bm25_default.head=0.6", "ok": True},
    }
    for key, patch in over.items():
        facts[key] = {**facts[key], **patch}
    return facts


def test_search_terminal_line_is_unchanged():
    v = search_gate.verdict(search_facts(), scope="full")
    assert v.terminal == ("SEARCH_GATE scope=full reviews=true product_month=true contract=true "
                          "kibana=true judgements=true analyzer=true SEARCH_GATE=PASS")


def test_search_prints_its_six_constituents_in_order():
    v = search_gate.verdict(search_facts(), scope="full")
    assert [c.split()[0] for c in v.constituents] == [
        "SEARCH_REVIEWS", "SEARCH_PRODUCT_MONTH", "SEARCH_CONTRACT", "SEARCH_KIBANA",
        "SEARCH_JUDGEMENTS", "SEARCH_ANALYZER"]
    assert v.constituents[2].startswith(f"SEARCH_CONTRACT path=conf/es/reviews.contract.json "
                                        f"hash={COMMIT[:12]}"), "the hash prints truncated"


@pytest.mark.parametrize("constituent", ["reviews", "product_month", "contract", "kibana",
                                         "judgements", "analyzer"])
def test_every_search_constituent_can_trip_the_gate(constituent):
    v = search_gate.verdict(search_facts(**{constituent: {"ok": False}}), scope="full")
    assert not v.passed and constituent in v.failed
    assert v.terminal.endswith("SEARCH_GATE=FAIL")
    assert f"{constituent}=false" in v.terminal


def test_a_missing_ledger_row_still_prints_its_constituent_line():
    v = search_gate.verdict(search_facts(reviews={"present": False, "ok": False}), scope="full")
    assert v.constituents[0] == "SEARCH_REVIEWS ledger_row=none"


def test_a_search_constituent_that_was_never_measured_is_a_crash_not_a_pass():
    facts = search_facts()
    del facts["kibana"]
    with pytest.raises(ValueError, match="kibana"):
        search_gate.verdict(facts, scope="full")


def test_the_search_artefact_validates():
    v = search_gate.verdict(search_facts(), scope="full")
    doc = E.build_artifact(v, capability="search", phase="P4 Search", kind="reproducibility",
                           protocol_hash=COMMIT, population={"name": "reviews alias", "n": 700},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert doc["metric"]["value"] == doc["metric"]["threshold"] == 6


# ================================================================ P5 Embeddings ====
def embedding_facts(**over):
    facts = {
        "spec": {"present": True, "run_id": RUN_ID, "path": "conf/embedding-spec.json",
                 "hash": COMMIT, "ledger_hash": COMMIT, "model": "bge-small-en-v1.5",
                 "revision": COMMIT, "unchanged": True, "silver_snapshot": 7788,
                 "latest_silver_snapshot": 7788, "fresh": True, "ok": True},
        "table": {"present": True, "table": "gold.review_embeddings", "snapshot": 4242,
                  "spec_hash": COMMIT, "rows": 500, "distinct": 500, "records_out": 500,
                  "cohort": 500, "missing": 0, "extra": 0, "dims_ok": 500, "unit_norm": 500,
                  "ok": True},
        "index": {"present": True, "run_id": RUN_ID, "spec_version": "2", "alias": "reviews",
                  "alias_targets": ["reviews-000002"], "index": "reviews-000002",
                  "alias_matches_ledger": True, "built_from_embeddings_run": True,
                  "es_docs": 500, "table_rows": 500, "es_only": 0, "table_only": 0,
                  "id_sets_equal": True, "ok": True},
        "recall": {"present": True, "index": "reviews-000002", "on_live": True,
                   "spec_matches": True, "k": 10, "num_candidates": 100, "doc_queries": 200,
                   "doc_mean": 0.98, "doc_min": 0.8, "frozen_mean": 0.97, "frozen_min": 0.7,
                   "ok": True},
        "judgements": {"set_version": "1", "hash": COMMIT, "systems": ["knn", "hybrid"],
                       "pooled": 400, "judged": 400, "complete": 20, "required": 20, "ok": True},
        "hypotheses": {"present": True, "complete": True, "set_hash_matches": True,
                       "spec_matches": True, "decision_file": True, "cells": "H1=supported",
                       "judges": 2, "ok": True},
    }
    for key, patch in over.items():
        facts[key] = {**facts[key], **patch}
    return facts


def test_embeddings_terminal_line_is_unchanged():
    v = embeddings_gate.verdict(embedding_facts(), scope="full")
    assert v.terminal == ("EMBED_GATE scope=full spec=true table=true index=true recall=true "
                          "judgements=true hypotheses=true EMBED_GATE=PASS")


def test_embeddings_prints_its_six_constituents_in_order():
    v = embeddings_gate.verdict(embedding_facts(), scope="full")
    assert [c.split()[0] for c in v.constituents] == [
        "EMBED_SPEC", "EMBED_TABLE", "EMBED_INDEX", "EMBED_RECALL", "EMBED_JUDGEMENTS",
        "EMBED_HYPOTHESES"]


@pytest.mark.parametrize("constituent", ["spec", "table", "index", "recall", "judgements",
                                         "hypotheses"])
def test_every_embeddings_constituent_can_trip_the_gate(constituent):
    v = embeddings_gate.verdict(embedding_facts(**{constituent: {"ok": False}}), scope="full")
    assert not v.passed and constituent in v.failed
    assert v.terminal.endswith("EMBED_GATE=FAIL")


def test_a_run_that_never_reached_the_table_still_prints_the_line_that_says_so():
    """Before the split a missing embeddings run printed no EMBED_TABLE line at all -- silence
    where the spine requires either a number or a written reason."""
    v = embeddings_gate.verdict(
        embedding_facts(spec={"present": False, "ok": False, "embeddings_run": "none",
                              "silver_run": "present"},
                        table={"present": False, "ok": False},
                        index={"present": False, "ok": False}), scope="full")
    assert v.constituents[0] == "EMBED_SPEC embeddings_run=none silver_run=present ok=false"
    assert v.constituents[1] == "EMBED_TABLE embeddings_run=none ok=false"
    assert v.constituents[2] == "EMBED_INDEX ledger_row=none ok=false"


def test_the_embeddings_artefact_carries_its_model():
    v = embeddings_gate.verdict(embedding_facts(), scope="full")
    doc = E.build_artifact(v, capability="embeddings", phase="P5 Embeddings",
                           kind="reproducibility", protocol_hash=COMMIT,
                           model="bge-small-en-v1.5",
                           population={"name": "gold.review_embeddings", "n": 500},
                           pipeline_run_id=RUN_ID, scope="full")
    assert E.validate_artifact(doc) == []
    assert doc["model"] == "bge-small-en-v1.5"


# ============================================================ the writer contract ====
def test_write_artifact_refuses_an_artefact_that_does_not_match_the_contract(tmp_path):
    v = search_gate.verdict(search_facts(), scope="full")
    doc = E.build_artifact(v, capability="search", phase="P4 Search", kind="reproducibility",
                           protocol_hash="short", population={"name": "reviews", "n": 700},
                           pipeline_run_id=RUN_ID, scope="full")
    with pytest.raises(ValueError, match="protocol_hash"):
        E.write_artifact(doc, root=tmp_path)
    assert list(tmp_path.iterdir()) == [], "an invalid artefact must never reach disk"


def test_a_written_artefact_lands_where_the_chain_says_it_will(tmp_path):
    v = search_gate.verdict(search_facts(), scope="full")
    path = E.record(v, root=tmp_path, capability="search", phase="P4 Search",
                    kind="reproducibility", protocol_hash=COMMIT,
                    population={"name": "reviews", "n": 700}, pipeline_run_id=RUN_ID,
                    scope="full")
    assert path == tmp_path / "search" / "gate.json"
    assert json.loads(path.read_text())["gate_name"] == "SEARCH_GATE"


@pytest.mark.parametrize("capability_id,gate_name", [
    ("silver", "SILVER_GATE"), ("silver_repro", "SILVER_REPRO_GATE"), ("gold", "GOLD_GATE"),
    ("gold_analytical", "GOLD_ANALYTICAL"), ("search", "SEARCH_GATE"),
    ("embeddings", "EMBED_GATE"),
])
def test_every_capability_this_ticket_publishes_is_declared_in_the_chain(capability_id, gate_name):
    declared = {c.id: c for c in E.load_chain()}
    assert capability_id in declared, f"{capability_id} writes an artefact nothing renders"
    assert declared[capability_id].gate_name == gate_name


def test_a_verdict_that_did_not_run_must_say_why():
    with pytest.raises(ValueError, match="written reason"):
        E.Verdict(gate_name="GOLD_ANALYTICAL", status="NOT_RUN", constituents=(),
                  terminal="GOLD_ANALYTICAL verdict=NOT_RUN", metric={})

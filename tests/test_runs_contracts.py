"""Run contracts (ADR-0008): what each job must record, with named failures.

These are pure -- no database. The ledger writes themselves are exercised by
tests/integration/test_ledger_pg.py against the compose stack.
"""
from __future__ import annotations

import pytest

from src.common import runs
from src.common.runs import (
    JOB_NAMES,
    ContractViolation,
    contract_for,
    validate_finish,
    validate_start,
)

SILVER_OK_INPUTS = {
    "bronze": {"table": "lake.bronze.reviews_raw", "snapshot_id": 123},
    "catalogue": {"table": "products", "catalogue_load_id": "0f3a1f4e-0000-4000-8000-000000000000"},
}
SILVER_OK_OUTPUTS = {
    "silver.reviews": {"table": "lake.silver.reviews", "snapshot_id": 1},
    "silver.rejects": {"table": "lake.silver.rejects", "snapshot_id": 2},
    "silver.review_collisions": {"table": "lake.silver.review_collisions", "snapshot_id": 3},
}
SILVER_OK_COUNTS = {
    "collision_groups": 10, "exact_groups": 8, "conflicting_groups": 1,
    "unresolvable_groups": 1, "collision_table_rows": 22, "collision_rows_removed": 13,
    "unmatched_review_rows": 0, "unmatched_parent_asins": 0, "review_id_distinct": 80,
    "reject_reasons": {"unparsable_json": 1, "missing_key_field": 2, "invalid_rating": 3,
                       "timestamp_out_of_range": 1},
    "catalogue_rows_read": 5,
}


def test_every_job_in_the_vocabulary_has_a_contract():
    for job in JOB_NAMES:
        assert contract_for(job) is not None, job


def test_unknown_job_hard_fails():
    with pytest.raises(ContractViolation):
        validate_start("gold_but_typoed", "1", inputs={}, params={})


def test_silver_start_names_each_missing_input():
    failures = validate_start("silver", runs.SILVER_SPEC_VERSION,
                              inputs={"bronze": {"table": "t"}}, params={}, raise_=False)
    assert "inputs.bronze.snapshot_id missing" in failures
    assert "inputs.catalogue missing" in failures


def test_silver_identity_holds_on_consistent_counts():
    failures = validate_finish(
        "silver", runs.SILVER_SPEC_VERSION,
        records={"records_in": 100, "records_out": 80, "records_rejected": 7},
        outputs=SILVER_OK_OUTPUTS, counts=SILVER_OK_COUNTS, raise_=False)
    assert failures == []


def test_silver_identity_violation_is_named_not_boolean():
    failures = validate_finish(
        "silver", runs.SILVER_SPEC_VERSION,
        records={"records_in": 101, "records_out": 80, "records_rejected": 7},
        outputs=SILVER_OK_OUTPUTS, counts=SILVER_OK_COUNTS, raise_=False)
    assert any(f.startswith("identity: records_in") for f in failures)


def test_silver_collision_arithmetic_is_checked():
    counts = {**SILVER_OK_COUNTS, "collision_rows_removed": 12}
    failures = validate_finish(
        "silver", runs.SILVER_SPEC_VERSION,
        records={"records_in": 99, "records_out": 80, "records_rejected": 7},
        outputs=SILVER_OK_OUTPUTS, counts=counts, raise_=False)
    assert any("collision_rows_removed" in f for f in failures)


def test_silver_requires_all_three_outputs():
    outputs = {k: v for k, v in SILVER_OK_OUTPUTS.items() if k != "silver.rejects"}
    with pytest.raises(ContractViolation) as exc:
        validate_finish("silver", runs.SILVER_SPEC_VERSION,
                        records={"records_in": 100, "records_out": 80, "records_rejected": 7},
                        outputs=outputs, counts=SILVER_OK_COUNTS)
    assert "outputs.silver.rejects missing" in str(exc.value)


def test_catalogue_load_identity():
    ok = validate_finish(
        "catalogue_load", runs.CATALOGUE_LOAD_SPEC_VERSION,
        records={"records_in": 10, "records_out": 10, "records_rejected": 0},
        outputs={"products": {"table": "products", "catalogue_load_id": "x"}},
        counts={"source_rows": 10, "parsed_prices": 2, "missing_prices": 7, "invalid_prices": 1,
                "rows_loaded": 10, "final_count": 10}, raise_=False)
    assert ok == []
    bad = validate_finish(
        "catalogue_load", runs.CATALOGUE_LOAD_SPEC_VERSION,
        records={"records_in": 10, "records_out": 10, "records_rejected": 0},
        outputs={"products": {"table": "products", "catalogue_load_id": "x"}},
        counts={"source_rows": 10, "parsed_prices": 2, "missing_prices": 7, "invalid_prices": 1,
                "rows_loaded": 9, "final_count": 9}, raise_=False)
    assert any("rows_loaded" in f for f in bad)


def test_bronze_identity_needs_replay_evidence():
    failures = validate_finish(
        "bronze_drain", runs.BRONZE_SPEC_VERSION,
        records={"records_in": 10, "records_out": 6, "records_rejected": 0},
        outputs={"reviews_raw": {"table": "lake.bronze.reviews_raw", "snapshot_ids": [1]}},
        counts={"micro_batches": 1}, raise_=False)
    assert "counts.records_replayed missing" in failures


def test_job_check_list_matches_code_registry():
    sql = (runs.PROJECT_ROOT / "conf" / "postgres-init" / "01_schema.sql").read_text()
    for job in JOB_NAMES:
        assert f"'{job}'" in sql, f"{job} missing from the job_name CHECK in 01_schema.sql"

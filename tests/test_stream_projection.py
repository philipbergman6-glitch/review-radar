"""P8's projection beside batch: the control verdict and the job's contract (ticket 15).

Pure -- no Kafka, no Iceberg, no Postgres. The Spark-side claims (the streaming path calls
silver's validation, an unsorted topic makes natural drops non-zero) live in
tests/test_stream_spark.py, which needs a JDK.
"""
from __future__ import annotations

import pytest

from src.common import runs
from src.common.runs import validate_finish, validate_start
from src.gates import stream as gate

# A clean control run: the whole file read, the sort's 7,276 key collisions deduplicated by
# the watermark, nothing late, and every product-month equal to gold's.
CONTROL = {
    "replay_run_id": "44f2497c-53c6-4abb-a522-dc74630cd239",
    "topic": "reviews.stream",
    "records_acked": 701_528,
    "source_sha256": "ab" * 32,
    "replay_config_hash": "cd" * 32,
    "validation": {"function": "src.spark.silver.parse_and_validate",
                   "recorded": "ef" * 32, "expected": "ef" * 32},
    "dedupe": {"records_read": 701_528, "ledger_read": 701_528, "rows_valid": 701_528,
               "rows_rejected": 0, "unique": 694_252, "dropped": 7_276, "expected": 7_276,
               "sort_run_id": "1" * 36},
    "ingested_at": "2026-09-14T11:50:00+00:00",
    "watermark": {"delay": "30 days", "natural_drops": 0},
    "survivor": {"silver_run_id": "4" * 36, "groups": 6139, "exact": 6138,
                 "conflicting": 1, "unresolvable": 0},
    "reconcile": {"gold_run_id": "2" * 36, "compared": 193_939, "differing": 0,
                  "only_in_stream": 0, "only_in_gold": 0,
                  "only_in_stream_unmaterialised": 210_541},
}


def _verdict(**overrides):
    facts = {**CONTROL, **overrides}
    return gate.control(facts, run_id="3" * 36, scope="full")


def test_a_clean_control_run_passes_and_prints_its_terminal_line():
    v = _verdict()
    assert v.status == "PASS"
    assert v.terminal.endswith("STREAM_GATE=PASS")
    assert "run_kind=control" in v.terminal
    assert "natural_drops=0" in v.terminal
    assert "differing_product_months=0" in v.terminal
    assert v.failed == ()


def test_every_constituent_is_printed_before_the_verdict():
    v = _verdict()
    names = [line.split(" ", 1)[0] for line in v.constituents]
    assert names == ["STREAM_SOURCE", "STREAM_VALIDATION", "STREAM_DEDUPE",
                     "STREAM_WATERMARK", "STREAM_SURVIVOR", "STREAM_RECONCILE"]
    assert v.lines[-1] == v.terminal


def test_a_forked_validation_function_fails_the_gate():
    """The trip case for the one constituent that is about code rather than about rows."""
    v = _verdict(validation={**CONTROL["validation"], "recorded": "99" * 32})
    assert v.status == "FAIL"
    assert "validation_shared_with_silver" in v.failed
    assert "shared=false" in v.constituents[1]


def test_a_natural_drop_fails_the_control_run():
    """An unsorted topic shows up here: the control run's claim is that lateness is injected."""
    v = _verdict(watermark={"delay": "30 days", "natural_drops": 12},
                 dedupe={**CONTROL["dedupe"], "unique": 694_240})
    assert v.status == "FAIL"
    assert v.failed == ("zero_natural_drops",)
    assert "natural_drops=12" in v.terminal


def test_a_differing_product_month_fails_the_control_run():
    v = _verdict(reconcile={**CONTROL["reconcile"], "differing": 1})
    assert v.status == "FAIL"
    assert "zero_differing_product_months" in v.failed


@pytest.mark.parametrize("key", ["only_in_stream", "only_in_gold"])
def test_a_product_month_on_one_side_only_fails_the_control_run(key):
    v = _verdict(reconcile={**CONTROL["reconcile"], key: 3})
    assert v.status == "FAIL"
    assert f"no_product_month_{key}" in v.failed


def test_a_reconciliation_of_nothing_is_not_a_clean_reconciliation():
    """Audit F3: with nothing compared, every difference is trivially zero."""
    v = _verdict(reconcile={**CONTROL["reconcile"], "compared": 0})
    assert v.status == "FAIL"
    assert v.failed == ("product_months_compared",)


def test_dropping_a_different_number_of_duplicates_than_the_sort_counted_fails():
    """The expected count was recorded by `sort_replay` before this job existed."""
    v = _verdict(dedupe={**CONTROL["dedupe"], "dropped": 7_000, "unique": 694_528})
    assert v.status == "FAIL"
    assert "duplicates_match_sort_count" in v.failed


def test_a_micro_batch_the_stream_never_saw_fails_the_gate():
    """The gate's own read of the topic disagrees with the count the run recorded."""
    v = _verdict(dedupe={**CONTROL["dedupe"], "ledger_read": 651_528})
    assert v.status == "FAIL"
    assert "topic_fully_read" in v.failed


def test_valid_rows_that_add_up_to_nothing_in_particular_fail_the_gate():
    v = _verdict(dedupe={**CONTROL["dedupe"], "rows_valid": 700_000})
    assert v.status == "FAIL"
    assert "every_valid_row_accounted_for" in v.failed


def test_product_months_gold_never_materialised_are_reported_not_failed():
    """Gold keeps only products that could ever fill a decline window; the stream keeps all.

    The two tables answering different questions is not a reconciliation failure, and folding
    it in would have made the control run unpassable for a reason that is not a defect.
    """
    v = _verdict()
    assert v.status == "PASS"
    assert "only_in_stream_unmaterialised=210541" in v.constituents[5]


def test_a_month_gold_materialised_and_the_stream_invented_fails_the_gate():
    """What is left in `only_in_stream` after the split: reviews batch never saw."""
    v = _verdict(reconcile={**CONTROL["reconcile"], "only_in_stream": 4})
    assert v.status == "FAIL"
    assert "no_product_month_only_in_stream" in v.failed


def test_distinct_users_is_named_as_not_projected():
    """A column the projection cannot compute is declared, never silently left out."""
    assert "distinct_users" not in gate.RECONCILED_COLUMNS
    assert "not_projected=distinct_users" in _verdict().constituents[5]


# ------------------------------------------------------------------ the contract ----

OK_INPUTS = {
    "replay": {"run_id": "1" * 36, "topic": "reviews.stream"},
    "source": {"run_id": "2" * 36, "path": "data/raw/All_Beauty.sorted.jsonl",
               "sha256": "ab" * 32},
    "protocol": {"path": "conf/stream_replay.toml", "status": "frozen", "config_hash": "cd" * 32},
}
OK_OUTPUTS = {"stream.product_month": {"table": "lake.stream.product_month", "snapshot_id": 1},
              "stream.product_month_batches": {"table": "lake.stream.product_month_batches",
                                               "snapshot_id": 2}}
OK_COUNTS = {
    "records_read": 701_528, "unique_review_ids": 694_252, "natural_drops": 0,
    "micro_batches": 15, "product_months": 30_000, "reviews_on_projection": 694_252,
    "ingested_at": "2026-09-14T11:50:00+00:00", "watermark": "30 days",
    "validation_source_sha256": "ef" * 32, "replay_config_hash": "cd" * 32,
    "elapsed_s": 120.0,
}
OK_RECORDS = {"records_in": 701_528, "records_out": 30_000, "records_rejected": 7_276}


def test_stream_aggregate_is_in_the_vocabulary_with_a_contract():
    assert "stream_aggregate" in runs.JOB_NAMES
    assert runs.contract_for("stream_aggregate") is not None


def test_the_contract_holds_on_a_clean_control_run():
    assert validate_finish("stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
                           records=OK_RECORDS, outputs=OK_OUTPUTS, counts=OK_COUNTS,
                           raise_=False) == []


def test_start_names_the_replay_run_it_was_not_given():
    failures = validate_start("stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
                              inputs={**OK_INPUTS, "replay": {"topic": "reviews.stream"}},
                              params={}, raise_=False)
    assert "inputs.replay.run_id missing" in failures


def test_a_projection_that_lost_a_micro_batch_fails_the_contract():
    """The tally taken while the stream ran against the sum read back off the table."""
    failures = validate_finish("stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
                               records=OK_RECORDS, outputs=OK_OUTPUTS,
                               counts=OK_COUNTS | {"reviews_on_projection": 664_252},
                               raise_=False)
    assert any("reviews_on_projection 664252 != unique_review_ids 694252" in f
               for f in failures)


def test_a_stream_that_processed_no_batch_fails_the_contract():
    failures = validate_finish("stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
                               records=OK_RECORDS, outputs=OK_OUTPUTS,
                               counts=OK_COUNTS | {"micro_batches": 0}, raise_=False)
    assert any("projected nothing" in f for f in failures)


def test_a_ledger_row_whose_rejected_count_does_not_reconcile_fails_the_contract():
    failures = validate_finish("stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
                               records=OK_RECORDS | {"records_rejected": 7_000},
                               outputs=OK_OUTPUTS, counts=OK_COUNTS, raise_=False)
    assert any("records_rejected 7000 != records_read 701528 - unique_review_ids 694252" in f
               for f in failures)


def test_more_late_drops_than_unprojected_rows_fails_the_contract():
    failures = validate_finish("stream_aggregate", runs.STREAM_AGGREGATE_SPEC_VERSION,
                               records=OK_RECORDS, outputs=OK_OUTPUTS,
                               counts=OK_COUNTS | {"natural_drops": 7_300}, raise_=False)
    assert any("more rows were dropped as late than went unprojected" in f for f in failures)


def test_a_collision_class_with_no_batch_survivor_fails_the_gate():
    """Where the ratings disagree, silver keeps nobody and the stream keeps one."""
    v = _verdict(survivor={**CONTROL["survivor"], "unresolvable": 3})
    assert v.status == "FAIL"
    assert "no_unresolvable_collisions" in v.failed

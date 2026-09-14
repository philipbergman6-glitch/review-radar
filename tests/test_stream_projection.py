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


# ------------------------------------------------------------------ the demo run ----

# The demo run: the same topic contents in a different order, the far slice dropped exactly,
# every difference from gold explained by those rows, and a passing control run beside it.
DEMO = {
    **CONTROL,
    "replay_run_id": "5" * 36,
    "topic": "reviews.stream.demo",
    "dedupe": {**CONTROL["dedupe"], "unique": 692_252},
    "watermark": {"delay": "30 days", "natural_drops": 2_000},
    "reconcile": {"gold_run_id": "2" * 36, "compared": 193_800, "differing": 1_850,
                  "differing_explained": 1_850, "differing_unexplained": 0,
                  "only_in_stream": 0, "only_in_gold": 139, "only_in_gold_explained": 139,
                  "only_in_gold_unexplained": 0, "only_in_stream_unmaterialised": 210_541,
                  "dropped_product_months": 1_996, "dropped_outside_gold": 7},
    "injection": {"near_rows": 2_000, "near_lag_days": 7, "far_rows": 2_000,
                  "far_lag_days": 730, "held_back_sha256": "77" * 32,
                  "near_on_topic": 2_000, "far_on_topic": 2_000, "expected_drops": 2_000,
                  "frozen": {"near_rows": 2_000, "far_rows": 2_000, "near_lag_days": 7,
                             "far_lag_days": 730, "watermark_days": 30}},
    "control": {"run_id": "3" * 36, "artifact_run_id": "3" * 36, "ledger_run_found": True,
                "status": "PASS",
                "scope": "full", "ledger_status": "success", "natural_drops": 0,
                "differing": 0, "protocol_hash": "cd" * 32},
}


def _demo(**overrides):
    return gate.demo({**DEMO, **overrides}, run_id="6" * 36, scope="full")


def test_a_clean_demo_run_passes_and_names_both_runs():
    v = _demo()
    assert v.status == "PASS", v.failed
    assert v.terminal.endswith("STREAM_GATE=PASS")
    assert "run_kind=demo" in v.terminal
    assert "dropped_by_watermark=2000 expected_drops=2000" in v.terminal
    assert "unexplained=0" in v.terminal
    assert f"control_run={'3' * 8}" in v.terminal


def test_the_demo_prints_its_injection_and_control_constituents():
    v = _demo()
    names = [line.split(" ", 1)[0] for line in v.constituents]
    assert names == ["STREAM_SOURCE", "STREAM_VALIDATION", "STREAM_DEDUPE", "STREAM_WATERMARK",
                     "STREAM_SURVIVOR", "STREAM_RECONCILE", "STREAM_INJECTION",
                     "STREAM_EXPLAINED", "STREAM_CONTROL"]
    assert "run_kind=demo" in v.constituents[0]
    # The demo's drops are injected, not natural, and the line says which.
    assert "injected_drops=2000" in v.constituents[3]
    assert "near_accepted=derived" in v.constituents[6]
    assert "same_protocol=true" in v.constituents[8]


def test_a_zero_watermark_drops_the_near_slice_too_and_fails_the_gate():
    """The trip case ticket 16 names: both slices dropped is 4,000, not the frozen 2,000."""
    v = _demo(watermark={"delay": "0 days", "natural_drops": 4_000},
              dedupe={**DEMO["dedupe"], "unique": 690_252},
              reconcile={**DEMO["reconcile"], "differing": 3_700, "differing_unexplained": 1_850,
                         "only_in_gold": 280, "only_in_gold_unexplained": 141})
    assert v.status == "FAIL"
    assert "far_slice_dropped_exactly" in v.failed
    assert "near_slice_accepted" in v.failed
    assert "every_difference_explained_by_dropped_rows" in v.failed
    assert "near_accepted=false" in v.constituents[6]


def test_a_far_row_that_survived_fails_the_count_and_the_explanation():
    """One far row accepted: 1,999 drops, and a product-month gold and the stream agree on
    where the explanation says they should differ."""
    v = _demo(watermark={"delay": "30 days", "natural_drops": 1_999},
              dedupe={**DEMO["dedupe"], "unique": 692_253},
              reconcile={**DEMO["reconcile"], "differing": 1_849, "differing_explained": 1_849,
                         "differing_unexplained": 1})
    assert v.status == "FAIL"
    assert set(v.failed) >= {"far_slice_dropped_exactly", "near_slice_accepted",
                             "every_difference_explained_by_dropped_rows"}


def test_a_difference_the_dropped_rows_do_not_explain_fails_the_gate():
    v = _demo(reconcile={**DEMO["reconcile"], "differing": 1_851, "differing_unexplained": 1})
    assert v.status == "FAIL"
    assert v.failed == ("near_slice_accepted", "every_difference_explained_by_dropped_rows")


def test_a_gold_month_missing_for_no_dropped_reason_fails_the_gate():
    v = _demo(reconcile={**DEMO["reconcile"], "only_in_gold": 140,
                         "only_in_gold_unexplained": 1})
    assert v.status == "FAIL"
    assert "every_missing_month_explained_by_dropped_rows" in v.failed


def test_a_held_back_row_the_topic_never_received_fails_the_gate():
    v = _demo(injection={**DEMO["injection"], "far_on_topic": 1_999})
    assert v.status == "FAIL"
    assert "held_back_rows_on_topic" in v.failed


def test_a_demo_whose_dropped_rows_touch_no_gold_month_is_vacuous_and_fails():
    v = _demo(watermark={"delay": "30 days", "natural_drops": 2_000},
              reconcile={**DEMO["reconcile"], "differing": 0, "differing_explained": 0,
                         "only_in_gold": 0, "only_in_gold_explained": 0})
    assert v.status == "FAIL"
    assert v.failed == ("dropped_rows_visible_in_reconciliation",)


@pytest.mark.parametrize("injected, failing", [
    ({"near_rows": 1_999}, "injection_matches_frozen_config"),
    ({"far_rows": 1_999, "expected_drops": 1_999}, "injection_matches_frozen_config"),
    ({"far_lag_days": 60}, "injection_matches_frozen_config"),
    ({"expected_drops": 1_999}, "far_slice_dropped_exactly"),
])
def test_a_run_that_injected_something_other_than_the_frozen_counts_fails(injected, failing):
    """The frozen document, not the run's own claim: the gate reads conf/stream_replay.toml
    and a replay that recorded different sizes or lags is refused."""
    v = _demo(injection={**DEMO["injection"], **injected})
    assert v.status == "FAIL"
    assert failing in v.failed


@pytest.mark.parametrize("control, reason", [
    ({"status": "FAIL"}, "the control run failed"),
    ({"scope": "sample"}, "the control run is a smoke test"),
    ({"ledger_status": "failed"}, "the ledger does not hold the control as a success"),
    ({"artifact_run_id": "9" * 36}, "the control artefact judges a run that is not current"),
])
def test_the_demo_requires_a_passing_current_control_run(control, reason):
    """Both runs must be present: a gate that only ever saw the demo run could not tell a
    working watermark from an unsorted file."""
    v = _demo(control={**DEMO["control"], **control})
    assert v.status == "FAIL", reason
    assert v.failed == ("control_run_present_and_passing",)


@pytest.mark.parametrize("control, failing", [
    ({"run_id": "", "artifact_run_id": "", "ledger_run_found": False},
     "control_run_present_and_passing"),
    ({"natural_drops": 1}, "control_run_dropped_nothing"),
    ({"differing": 4}, "control_run_dropped_nothing"),
])
def test_a_control_run_that_is_missing_or_not_clean_fails_the_demo(control, failing):
    """No control run at all leaves both ids empty, which must not read as agreement; and a
    control that dropped rows or differed from batch is not a control this demo can lean on."""
    v = _demo(control={**DEMO["control"], **control})
    assert v.status == "FAIL"
    assert failing in v.failed


def test_the_two_runs_must_share_the_frozen_protocol():
    """A control run under a different watermark or slice sizes is not this run's control."""
    v = _demo(control={**DEMO["control"], "protocol_hash": "ee" * 32})
    assert v.status == "FAIL"
    assert v.failed == ("control_and_demo_share_protocol",)
    assert "same_protocol=false" in v.constituents[8]


def test_demo_population_names_both_runs_and_the_slices():
    pop = gate.demo_population(DEMO, category="All_Beauty")
    assert pop["control_run_id"] == "3" * 36
    assert (pop["near_rows"], pop["far_rows"]) == (2_000, 2_000)

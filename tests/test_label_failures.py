"""The parse-failure census (ADR-0003): why a labelling configuration failed, by named cause.

A low macro-F1 has two possible readings -- the model disagreed with the reference, or the
model was never heard because its answer would not validate. These are the functions that
tell the two apart, so they are pure and tested away from Spark and away from the model.
"""
from __future__ import annotations

from src.ai.label_failures import census, classify

# Verbatim validator messages: the census is only honest if it parses what validate_label writes.
NOT_VERBATIM = "themes[1] evidence_quote is not present in the review: 'the pump broke off'"
REPEATED = "themes[2] repeats theme_id 'does_not_work'"
TOO_LONG = "themes[0] evidence_quote has 21 words, limit 15"


def test_every_named_cause_is_recognised_from_the_validators_own_wording():
    assert classify(NOT_VERBATIM) == ["quote_not_verbatim"]
    assert classify(REPEATED) == ["repeated_theme"]
    assert classify(TOO_LONG) == ["quote_too_long"]


def test_one_message_carrying_several_violations_counts_each_kind_once():
    second_quote = "themes[2] evidence_quote is not present in the review: 'x'"
    error = f"{NOT_VERBATIM}; {REPEATED}; {second_quote}"
    assert classify(error) == ["quote_not_verbatim", "repeated_theme"]


def test_an_unrecognised_message_is_named_other_rather_than_dropped():
    """A cause the buckets do not know must still be counted, or the census silently shrinks."""
    assert classify("label_confidence 'certain' is not one of ['low', 'medium', 'high']") == ["other"]
    assert classify("") == []


def test_the_census_counts_rows_per_cause_and_rows_per_combination():
    c = census([NOT_VERBATIM, f"{NOT_VERBATIM}; {REPEATED}", REPEATED, TOO_LONG])
    assert c["rows"] == 4
    # A row with two causes is counted under both: the causes are what a prompt would fix.
    assert c["by_cause"] == {"quote_not_verbatim": 2, "repeated_theme": 2, "quote_too_long": 1}
    # ...and once under the combination, which is what the row actually looked like.
    assert sum(c["by_combination"].values()) == 4
    assert c["by_combination"]["quote_not_verbatim + repeated_theme"] == 1


def test_the_census_reports_which_theme_slot_the_violations_landed_in():
    """A model padding to its theme limit fails in the slots it padded, not in slot 0."""
    c = census([TOO_LONG, NOT_VERBATIM, REPEATED])
    assert c["by_theme_slot"] == {"0": 1, "1": 1, "2": 1}


def test_a_failed_row_with_no_recorded_error_is_still_a_row_in_the_census():
    c = census([None, NOT_VERBATIM])
    assert c["rows"] == 2
    assert c["by_combination"][""] == 1


# ------------------------------------- a failure is not the same claim as "no complaint" ----
def _row(status: str, parsed: dict | None) -> dict:
    from src.ai.theme_labels import label_row
    return label_row(idempotency_key="k", source_review_id="r1", budget_line="development",
                     label_source="local_llm", model_id="qwen3:8b", label_spec_version="1",
                     prompt_version="label-v5", inference_config_hash="h" * 12, api_mode="local",
                     status=status, parsed=parsed, attempts=[], source_silver_run_id="s",
                     run_id="run")


SAW_NOTHING = {"themes": [], "other": {"present": False, "phrase": None}, "abstain": False,
               "overall_sentiment": "negative", "label_confidence": "low"}


def test_a_parse_failure_is_stored_as_absent_labels_not_as_an_empty_theme_list():
    """`themes = []` is the model saying "no complaint"; a failure never gets to say that."""
    failed, answered = _row("parse_failed", None), _row("succeeded", SAW_NOTHING)
    assert failed["themes"] is None and failed["abstain"] is None
    assert answered["themes"] == [] and answered["abstain"] is False
    assert failed["label_status"] != answered["label_status"]


def test_the_scorer_reads_both_as_an_empty_prediction_while_coverage_keeps_them_apart():
    """Both cost the same recall -- the pipeline got no theme either way -- and the failure
    rate is what tells the reader which of the two happened."""
    from src.ai.theme_scoring import failure_coverage, score_themes
    reference = {"r_failed": {"a"}, "r_answered": {"a"}}
    system = {"r_failed": set(), "r_answered": set()}
    a = next(s for s in score_themes(reference, system, ["a"], min_support=1) if s.theme_id == "a")
    assert (a.tp, a.fn) == (0, 2)
    cov = failure_coverage({"r_failed": "parse_failed", "r_answered": "succeeded"},
                           ["r_failed", "r_answered"])
    assert cov["parse_failed"] == 1 and cov["succeeded"] == 1 and cov["failure_rate"] == 0.5

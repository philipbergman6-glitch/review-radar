"""The P8 replay protocol and the sort job (ADR-0010, ticket 14).

Pure -- no Kafka, no database. The claims under test are the ones the stream gate will
later rest on: the protocol is frozen before a run, the sort is total and reproducible, and
a replay that paced at the wrong rate is a failed run rather than a footnote.
"""
from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest

from src.common import runs
from src.common.canonical import review_id
from src.common.runs import validate_finish, validate_start
from src.ingest.replay_config import CONFIG_PATH, load_replay_config, slice_rank
from src.ingest.sort_replay import sort_file

# --------------------------------------------------------------- the protocol ----


def test_committed_protocol_is_frozen_and_loads():
    cfg = load_replay_config()
    assert cfg.frozen
    assert cfg.topic != cfg.sample_topic
    assert cfg.records_per_second > 0
    assert len(cfg.config_hash) == 64


def test_config_hash_covers_the_whole_document():
    """Any edit to the file changes the hash -- there is no unhashed 'execution setting'."""
    doc = tomllib.loads(CONFIG_PATH.read_text())
    keys = {f"{table}.{key}" for table, body in doc.items() for key in body}
    assert keys == {
        "protocol.status", "protocol.spec_version",
        "topic.name", "topic.sample_name", "topic.demo_name", "topic.partitions",
        "sort.tiebreak", "sort.suffix",
        "pacing.records_per_second", "pacing.clock_every_records",
        "stream.watermark", "stream.max_offsets_per_trigger",
        "slices.seed", "slices.near_lag_days", "slices.near_rows", "slices.near_salt",
        "slices.far_lag_days", "slices.far_rows", "slices.far_salt",
    }, "a new key needs a decision about whether it changes what a run means"


def _write_config(tmp_path: Path, **overrides: str) -> Path:
    raw = CONFIG_PATH.read_text()
    for old, new in overrides.items():
        raw = raw.replace(old, new)
    p = tmp_path / "stream_replay.toml"
    p.write_text(raw)
    return p


def test_provisional_protocol_refuses_to_load(tmp_path):
    p = _write_config(tmp_path, **{'status = "frozen"': 'status = "provisional"'})
    with pytest.raises(RuntimeError, match="committed before the first run"):
        load_replay_config(p)


def test_stream_may_not_share_the_sample_topic(tmp_path):
    p = _write_config(tmp_path,
                      **{'sample_name = "reviews.stream.sample"': 'sample_name = "reviews.stream"'})
    with pytest.raises(ValueError, match="must all differ"):
        load_replay_config(p)


def test_the_demo_run_has_its_own_topic_and_is_full_scope_only():
    """Two replays on one topic leave its record count belonging to neither (ticket 16)."""
    cfg = load_replay_config()
    assert cfg.topic_for("full", "demo") == cfg.demo_topic
    assert len({cfg.topic, cfg.sample_topic, cfg.demo_topic}) == 3
    assert cfg.topic_for("full") == cfg.topic_for("full", "control") == cfg.topic
    with pytest.raises(ValueError, match="full-scope only"):
        cfg.topic_for("sample", "demo")


def test_the_demo_topic_may_not_reuse_the_control_topic(tmp_path):
    p = _write_config(tmp_path,
                      **{'demo_name = "reviews.stream.demo"': 'demo_name = "reviews.stream"'})
    with pytest.raises(ValueError, match="must all differ"):
        load_replay_config(p)


def test_near_lag_must_be_shorter_than_far_lag(tmp_path):
    p = _write_config(tmp_path, **{"near_lag_days = 7": "near_lag_days = 900"})
    with pytest.raises(ValueError, match="shorter than"):
        load_replay_config(p)


def test_the_two_slices_must_draw_under_different_salts(tmp_path):
    p = _write_config(tmp_path,
                      **{'far_salt = "held_back_far"': 'far_salt = "held_back_near"'})
    with pytest.raises(ValueError, match="or they overlap"):
        load_replay_config(p)


def test_the_stream_topic_has_exactly_one_partition(tmp_path):
    """Order is a per-partition property in Kafka, and the watermark rests on order."""
    assert load_replay_config().partitions == 1
    p = _write_config(tmp_path, **{"partitions = 1": "partitions = 6"})
    with pytest.raises(ValueError, match="topic.partitions must be 1"):
        load_replay_config(p)


def test_watermark_sits_between_the_two_injected_lags():
    """The property ticket 16's assertion rests on, checked at load rather than assumed."""
    cfg = load_replay_config()
    assert cfg.near_lag_days < cfg.watermark_days < cfg.far_lag_days


@pytest.mark.parametrize("delay", ['watermark = "3 days"', 'watermark = "900 days"'])
def test_a_watermark_that_cannot_separate_the_slices_refuses_to_load(tmp_path, delay):
    p = _write_config(tmp_path, **{'watermark = "30 days"': delay})
    with pytest.raises(ValueError, match="does not separate the two slices"):
        load_replay_config(p)


def test_a_watermark_this_loader_cannot_compare_is_refused(tmp_path):
    """Spark would accept '4 weeks'; this loader may not, because it compares days."""
    p = _write_config(tmp_path, **{'watermark = "30 days"': 'watermark = "4 weeks"'})
    with pytest.raises(ValueError, match="must read '<n> days'"):
        load_replay_config(p)


def test_slice_draw_is_deterministic_and_salt_separated():
    a = slice_rank(20260914, "held_back_near", "abc")
    assert a == slice_rank(20260914, "held_back_near", "abc")
    assert a != slice_rank(20260914, "held_back_far", "abc")
    assert a != slice_rank(20260915, "held_back_near", "abc")


# ----------------------------------------------------------------- the sort ----

def _review(user: str, asin: str, ts: int, **extra) -> str:
    return json.dumps({"user_id": user, "parent_asin": asin, "timestamp": ts,
                       "rating": 5.0, "text": "t", **extra})


def _run_sort(tmp_path: Path, lines: list[str]) -> tuple[dict, list[dict]]:
    src = tmp_path / "in.jsonl"
    src.write_text("\n".join(lines) + "\n", encoding="utf-8")
    out = tmp_path / "out.sorted.jsonl"
    counts = sort_file(src, out)
    rows = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    return counts, rows


def test_sort_orders_by_event_time(tmp_path):
    counts, rows = _run_sort(tmp_path, [
        _review("u1", "p1", 3000), _review("u2", "p2", 1000), _review("u3", "p3", 2000)])
    assert [r["timestamp"] for r in rows] == [1000, 2000, 3000]
    assert counts["rows_sorted"] == 3
    assert (counts["first_event_ms"], counts["last_event_ms"]) == (1000, 3000)


def test_sorted_output_is_identical_whatever_the_input_order(tmp_path):
    """The property that makes `output_sha256` worth recording at all."""
    lines = [_review(f"u{i}", f"p{i % 3}", 1000) for i in range(8)]   # every timestamp tied
    a, b = tmp_path / "a", tmp_path / "b"
    a.mkdir()
    b.mkdir()
    first, _ = _run_sort(a, lines)
    second, _ = _run_sort(b, list(reversed(lines)))
    assert first["output_sha256"] == second["output_sha256"]


def test_ties_break_by_review_id_not_by_line_order(tmp_path):
    a, b = _review("uA", "pA", 500), _review("uB", "pB", 500)
    _, rows = _run_sort(tmp_path, [a, b])
    expected = sorted([json.loads(a), json.loads(b)],
                      key=lambda r: review_id(r["user_id"], r["parent_asin"], r["timestamp"]))
    assert [r["user_id"] for r in rows] == [r["user_id"] for r in expected]


def test_key_collisions_are_counted_not_merged(tmp_path):
    """review_id is not unique in this source; the sort carries the collisions and says so.

    Two rows sharing (user, product, timestamp) share a review_id. Silver later removes one;
    the stream must still replay both, because the stream is a projection beside batch and
    its drop count has to be explainable.
    """
    counts, rows = _run_sort(tmp_path, [
        _review("u1", "p1", 900, helpful_vote=0),
        _review("u1", "p1", 900, helpful_vote=3),     # same key, different content
        _review("u2", "p2", 800)])
    assert counts["rows_sorted"] == 3
    assert counts["distinct_review_ids"] == 2
    assert counts["key_collision_rows"] == 1
    assert counts["distinct_sort_keys"] == 3          # the digest separates the two
    assert counts["byte_identical_rows"] == 0
    assert len(rows) == 3


def test_byte_identical_duplicates_cannot_change_the_output(tmp_path):
    """The rows the content digest cannot separate -- and does not need to."""
    dup = _review("u1", "p1", 900)
    counts, rows = _run_sort(tmp_path, [dup, dup, _review("u2", "p2", 800)])
    assert counts["distinct_sort_keys"] == 2
    assert counts["byte_identical_rows"] == 1
    assert len(rows) == 3                             # replayed, not deduplicated


def test_unorderable_rows_are_counted_by_reason_never_dropped_silently(tmp_path):
    counts, rows = _run_sort(tmp_path, [
        _review("u1", "p1", 1000),
        "{not json",
        json.dumps({"user_id": "u2", "parent_asin": "p2", "timestamp": None}),
        json.dumps({"user_id": "  ", "parent_asin": "p3", "timestamp": 900}),
        json.dumps({"user_id": "u4", "parent_asin": "", "timestamp": 900}),
    ])
    assert counts["rows_sorted"] == 1
    assert counts["rows_rejected"] == 4
    assert counts["reject_reasons"] == {"unparsable_json": 1, "null_timestamp": 1,
                                        "blank_user_id": 1, "blank_parent_asin": 1}
    assert len(rows) == 1


def test_byte_offsets_survive_multibyte_text(tmp_path):
    """`tell()` on a text handle would drift here; the index reads bytes for this reason."""
    counts, rows = _run_sort(tmp_path, [
        _review("u1", "p1", 2000, text="crème brûlée — 香水 😀"),
        _review("u2", "p2", 1000, text="plain")])
    assert [r["timestamp"] for r in rows] == [1000, 2000]
    assert rows[1]["text"] == "crème brûlée — 香水 😀"
    assert counts["rows_rejected"] == 0


# ------------------------------------------------------------- the contracts ----

SORT_OK_INPUTS = {
    "source": {"path": "data/raw/All_Beauty.jsonl", "sha256": "ab" * 32, "bytes": 10},
    "protocol": {"path": "conf/stream_replay.toml", "status": "frozen", "config_hash": "cd" * 32},
}
SORT_OK_OUTPUTS = {"sorted_file": {"path": "data/raw/All_Beauty.sorted.jsonl",
                                   "sha256": "ef" * 32, "rows": 98}}
SORT_OK_COUNTS = {
    "rows_sorted": 98, "rows_rejected": 2, "reject_reasons": {"null_timestamp": 2},
    "distinct_review_ids": 96, "key_collision_rows": 2,
    "distinct_sort_keys": 97, "byte_identical_rows": 1,
    "first_event_ms": 1, "last_event_ms": 2,
    "output_sha256": "ef" * 32, "output_bytes": 500,
    "reject_path": "data/raw/All_Beauty.sorted.jsonl.rejects.jsonl",
    "replay_config_hash": "cd" * 32,
}
SORT_OK_RECORDS = {"records_in": 100, "records_out": 98, "records_rejected": 2}


def test_every_job_in_the_vocabulary_still_has_a_contract():
    for job in ("sort_replay", "stream_produce"):
        assert job in runs.JOB_NAMES
        assert runs.contract_for(job) is not None


def test_sort_contract_holds_on_consistent_counts():
    assert validate_finish("sort_replay", runs.SORT_REPLAY_SPEC_VERSION,
                           records=SORT_OK_RECORDS, outputs=SORT_OK_OUTPUTS,
                           counts=SORT_OK_COUNTS, raise_=False) == []


def test_sort_start_names_a_missing_protocol_hash():
    failures = validate_start("sort_replay", runs.SORT_REPLAY_SPEC_VERSION,
                              inputs={"source": SORT_OK_INPUTS["source"],
                                      "protocol": {"path": "p", "status": "frozen"}},
                              params={}, raise_=False)
    assert "inputs.protocol.config_hash missing" in failures


def test_a_sort_that_loses_rows_fails_the_contract():
    failures = validate_finish("sort_replay", runs.SORT_REPLAY_SPEC_VERSION,
                               records={"records_in": 100, "records_out": 90,
                                        "records_rejected": 2},
                               outputs=SORT_OK_OUTPUTS,
                               counts=SORT_OK_COUNTS | {"rows_sorted": 90,
                                                        "distinct_review_ids": 88,
                                                        "distinct_sort_keys": 89},
                               raise_=False)
    assert any("records_in 100 != rows_sorted 90 + rows_rejected 2" in f for f in failures)


def test_unaccounted_rows_fail_the_contract():
    """The tripping case: sort keys and byte-identical rows that do not add up to the file."""
    failures = validate_finish("sort_replay", runs.SORT_REPLAY_SPEC_VERSION,
                               records=SORT_OK_RECORDS, outputs=SORT_OK_OUTPUTS,
                               counts=SORT_OK_COUNTS | {"byte_identical_rows": 0},
                               raise_=False)
    assert any("distinct_sort_keys 97 + byte_identical_rows 0 != rows_sorted 98" in f
               for f in failures)


def test_more_review_ids_than_sort_keys_fails_the_contract():
    failures = validate_finish("sort_replay", runs.SORT_REPLAY_SPEC_VERSION,
                               records=SORT_OK_RECORDS, outputs=SORT_OK_OUTPUTS,
                               counts=SORT_OK_COUNTS | {"distinct_review_ids": 98,
                                                        "key_collision_rows": 0},
                               raise_=False)
    assert any("cannot merge rows the review id kept apart" in f for f in failures)


STREAM_OK_COUNTS = {
    "records_attempted": 1000, "records_acked": 1000,
    "first_event_ms": 1, "last_event_ms": 2,
    "records_per_second_target": 3000, "records_per_second_actual": 2950.0,
    "elapsed_s": 0.34, "replay_config_hash": "cd" * 32,
    "run_kind": "control", "near_rows": 0, "far_rows": 0, "held_back_rows": 0,
    "expected_near_accepted": 0, "expected_far_dropped": 0,
}
STREAM_DEMO_COUNTS = STREAM_OK_COUNTS | {
    "run_kind": "demo", "near_rows": 2000, "far_rows": 2000, "held_back_rows": 4000,
    "expected_near_accepted": 2000, "expected_far_dropped": 2000,
}
STREAM_OK_RECORDS = {"records_in": 1000, "records_out": 1000, "records_rejected": 0}
STREAM_OK_OUTPUTS = {"kafka": {"topic": "reviews.stream"},
                     "held_back": {"path": "data/raw/All_Beauty.sorted.jsonl.held_back.control.jsonl",
                                   "sha256": "ab" * 32, "rows": 0}}


def test_stream_contract_holds_on_a_clean_control_run():
    assert validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                           records=STREAM_OK_RECORDS, outputs=STREAM_OK_OUTPUTS,
                           counts=STREAM_OK_COUNTS, raise_=False) == []


def test_an_unacknowledged_record_fails_the_contract():
    failures = validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                               records=STREAM_OK_RECORDS | {"records_out": 999},
                               outputs=STREAM_OK_OUTPUTS,
                               counts=STREAM_OK_COUNTS | {"records_acked": 999},
                               raise_=False)
    assert any("acknowledges everything it sent" in f for f in failures)


def test_a_replay_that_missed_its_rate_fails_the_contract():
    """The tripping case for the pacing constituent: half the frozen rate."""
    failures = validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                               records=STREAM_OK_RECORDS, outputs=STREAM_OK_OUTPUTS,
                               counts=STREAM_OK_COUNTS | {"records_per_second_actual": 1500.0},
                               raise_=False)
    assert any("more than 10% from the frozen target" in f for f in failures)


def test_a_backwards_replay_fails_the_contract():
    failures = validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                               records=STREAM_OK_RECORDS, outputs=STREAM_OK_OUTPUTS,
                               counts=STREAM_OK_COUNTS | {"first_event_ms": 9,
                                                          "last_event_ms": 2},
                               raise_=False)
    assert any("not in event-time order" in f for f in failures)


def test_a_demo_replay_holding_back_the_frozen_slices_validates():
    assert validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                           records=STREAM_OK_RECORDS,
                           outputs=STREAM_OK_OUTPUTS | {"held_back": {"path": "p", "sha256": "s",
                                                                       "rows": 4000}},
                           counts=STREAM_DEMO_COUNTS, raise_=False) == []


def test_a_control_replay_that_held_rows_back_fails_the_contract():
    failures = validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                               records=STREAM_OK_RECORDS, outputs=STREAM_OK_OUTPUTS,
                               counts=STREAM_OK_COUNTS | {"far_rows": 5, "held_back_rows": 5,
                                                          "expected_far_dropped": 5},
                               raise_=False)
    assert any("a control run holds nothing back" in f for f in failures)


def test_a_demo_replay_not_predicted_to_drop_its_whole_far_slice_fails_the_contract():
    """The producer refuses such a plan; a success row saying otherwise skipped the check."""
    failures = validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                               records=STREAM_OK_RECORDS, outputs=STREAM_OK_OUTPUTS,
                               counts=STREAM_DEMO_COUNTS | {"expected_far_dropped": 1990},
                               raise_=False)
    assert any("expected_far_dropped 1990 != far_rows 2000" in f for f in failures)


def test_a_replay_without_a_held_back_sidecar_fails_the_contract():
    failures = validate_finish("stream_produce", runs.STREAM_PRODUCE_SPEC_VERSION,
                               records=STREAM_OK_RECORDS,
                               outputs={"kafka": {"topic": "reviews.stream"}},
                               counts=STREAM_OK_COUNTS, raise_=False)
    assert "outputs.held_back missing" in failures


# ------------------------------------------------------------- the demo producer ----

def test_the_producer_index_recomputes_the_identity_and_refuses_a_backwards_file(tmp_path):
    from src.ingest.stream_producer import index_source

    src = tmp_path / "in.sorted.jsonl"
    src.write_text("\n".join([_review("u1", "p1", 1000), _review("u2", "p2", 2000)]) + "\n")
    ts, ids, offsets = index_source(src)
    assert ts == [1000, 2000]
    assert ids == [review_id("u1", "p1", 1000), review_id("u2", "p2", 2000)]
    assert offsets[0] == 0 and offsets[1] > 0

    src.write_text("\n".join([_review("u1", "p1", 2000), _review("u2", "p2", 1000)]) + "\n")
    with pytest.raises(SystemExit, match="goes backwards in event time"):
        index_source(src)


def test_the_held_back_sidecar_is_empty_for_a_control_run_and_lists_every_row_for_a_demo(tmp_path):
    from src.ingest.lateness import DAY_MS, InjectionSpec, plan_injection
    from src.ingest.stream_producer import sequence, write_held_back

    _, rows = write_held_back(tmp_path / "control.jsonl", None)
    assert rows == 0 and (tmp_path / "control.jsonl").read_text() == ""

    ts = [1_600_000_000_000 + i * 5 * DAY_MS for i in range(400)]
    ids = [f"r{i}" for i in range(400)]
    plan = plan_injection(ts, ids, spec=InjectionSpec(
        seed=1, near_salt="n", near_rows=2, near_lag_days=7, far_salt="f", far_rows=2,
        far_lag_days=730, watermark_days=30, batch_size=10))
    _, rows = write_held_back(tmp_path / "demo.jsonl", plan)
    lines = [json.loads(x) for x in (tmp_path / "demo.jsonl").read_text().splitlines()]
    assert rows == 4 and len(lines) == 4
    assert {x["slice"] for x in lines} == {"near", "far"}
    assert {x["review_id"] for x in lines} == {h.review_id for h in plan.held_back}
    # The control send order is the file's; the demo's is the plan's.
    assert [i for i, _, _ in sequence(ts, None)] == list(range(400))
    assert sorted(i for i, _, _ in sequence(ts, plan)) == list(range(400))

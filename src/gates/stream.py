"""P8's control verdict: the streaming projection agrees with batch, and nothing was dropped.

`STREAM_GATE` is reproducibility, and for the *control* run it is a claim about absence:
with the topic replayed in event-time order and no lateness injected, the watermark must
drop nothing and the projection must differ from `gold.product_month` nowhere. Both zeros
are load-bearing. A passing demo run on an unsorted file would print drops and call them
lateness; a projection that quietly recomputed its own validation would agree with itself
rather than with batch. The control run is what makes ticket 16's injected counts mean
something, which is why ADR-0010 requires both runs and why this gate refuses to pass on a
reconciliation it never made (`product_months_compared = 0` is FAIL, always -- audit F3).

Four constituents carry the weight, and each names the thing it would catch:

* **validation is shared, not copied.** The stream records the digest of the source of the
  validation function it called; the gate recomputes it from `src/spark/silver.py`. A fork --
  a copy that drifted, a locally patched predicate -- changes the digest, and the gate says so
  before the aggregates are compared. This is the only constituent that is about code rather
  than about rows, and it is here because a divergence in validation is the one failure that
  makes every other number agree with the wrong thing.
* **every valid row on the topic is accounted for.** The gate re-reads the same offsets in a
  plain batch job and counts them itself, so `rows_valid` comes from a different pass than
  `unique_review_ids` does. The two only add up if the stream carried the whole topic; a lost
  micro-batch shows up here rather than as a quietly smaller aggregate.
* **the duplicates the stream dropped are the ones the sort counted.** `sort_replay` recorded
  `key_collision_rows` before any streaming job existed, so the expected count is not derived
  from the run being judged. `dropDuplicatesWithinWatermark` is exact for this source: the
  review id hashes the timestamp, so two rows sharing an id share an event time and cannot
  straddle a watermark.
* **the watermark dropped nothing.** Spark's own `numRowsDroppedByWatermark`, not a count the
  job computed about itself.
* **no collision group had no survivor in batch.** The two paths break a key collision
  differently -- batch by helpful vote, text length and canonical hash, the stream by first
  arrival -- and for one class that difference reaches the aggregate: where the ratings
  disagree, silver keeps nobody and the stream keeps one. Silver counted those groups long
  before this gate existed, so the reconciliation does not have to rest on the assumption
  silently.

**The demo run** (ticket 16) is the same gate with the zeros replaced by predictions. The
producer held back two frozen slices and released them late; Spark's drop count must equal
the far slice exactly, and every product-month where the projection differs from gold -- and
every gold month the projection has no row for -- must be explained, column for column, by
the far rows the gate finds in its own re-read of the topic. The near slice's acceptance is
not observed directly (the projection carries no review ids); it is the only thing left once
the drop count is the far slice and the differences are the far slice. The demo verdict also
requires the control run: a passing control artefact, made under the same frozen protocol,
for the stream_aggregate run the ledger holds as the current control. A gate that only ever
saw the demo run could not tell a working watermark from an unsorted file.

Pure over facts `scripts/gate_stream.py` has already loaded: no Spark, no Kafka, no Postgres.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from src.common.evaluation import Verdict, repro_verdict

GATE_NAME = "STREAM_GATE"
RUN_KINDS = ("control", "demo")

#: The projection's columns, compared value for value against gold's row for the same
#: (product, month). Every one is additive over micro-batches, which is what lets the
#: projection be the sum of its per-batch contributions. `distinct_users` is deliberately not
#: among them and not projected: a distinct count cannot be summed across batches, so
#: carrying it would mean either holding every user id in stream state or publishing a number
#: the projection cannot actually compute. Gold keeps it; the stream says it does not.
RECONCILED_COLUMNS = ("review_count", "rating_sum", "neg_count", "verified_count",
                      "verified_rating_sum", "nonempty_text_count", "long_text_count")

NOT_PROJECTED = ("distinct_users",)

#: Gold materialises only products that could ever fill a decline window, and every other
#: product is counted and excluded with the reason `below_min_reviews` (`src/spark/gold.py`).
#: The stream has no such rule -- it aggregates whatever the topic carries -- so a
#: product-month for a product gold never wrote is the two tables answering different
#: questions, not a disagreement about arithmetic. It is reported as
#: `only_in_stream_unmaterialised` and is not a constituent. What remains in `only_in_stream`
#: is the case that *is* a defect: a product gold did materialise, in a month gold recorded no
#: reviews for.


def _b(v: Any) -> str:
    return str(bool(v)).lower()


def constituent_lines(f: Mapping[str, Any], *, run_kind: str = "control") -> list[str]:
    """The counted lines the verdict summarises, in the order they are printed."""
    v, d, w, r, s = (f["validation"], f["dedupe"], f["watermark"], f["reconcile"],
                     f["survivor"])
    return [
        (f"STREAM_SOURCE run_kind={run_kind} replay_run={str(f['replay_run_id'])[:8]} "
         f"topic={f['topic']} records_acked={f['records_acked']} "
         f"sorted_file_sha256={str(f['source_sha256'])[:12]} "
         f"replay_config_hash={str(f['replay_config_hash'])[:12]}"),
        (f"STREAM_VALIDATION function={v['function']} digest={str(v['recorded'])[:12]} "
         f"silver_digest={str(v['expected'])[:12]} shared={_b(v['recorded'] == v['expected'])}"),
        (f"STREAM_DEDUPE records_read={d['records_read']} ledger_records_read={d['ledger_read']} "
         f"rows_valid={d['rows_valid']} rows_rejected={d['rows_rejected']} "
         f"unique_review_ids={d['unique']} duplicates_dropped={d['dropped']} "
         f"expected_from_sort={d['expected']} sort_run={str(d['sort_run_id'])[:8]}"),
        # `natural_drops` is the right name on the control run, where every row is natural,
        # and the wrong one on the demo, where the drops are the injected far slice -- so the
        # demo spells it `injected_drops` and only the control claims a natural row was late.
        (f"STREAM_WATERMARK delay={w['delay']} "
         f"{'injected_drops' if run_kind == 'demo' else 'natural_drops'}={w['natural_drops']} "
         f"source_order=event_time_sorted ingested_at={f['ingested_at']}"),
        (f"STREAM_SURVIVOR silver_run={str(s['silver_run_id'])[:8]} "
         f"collision_groups={s['groups']} exact={s['exact']} conflicting={s['conflicting']} "
         f"unresolvable={s['unresolvable']} stream_rule=first_arrival "
         f"batch_rule=helpful_vote,text_length,canonical_hash"),
        (f"STREAM_RECONCILE gold_run={str(r['gold_run_id'])[:8]} "
         f"product_months_compared={r['compared']} differing={r['differing']} "
         f"only_in_stream={r['only_in_stream']} only_in_gold={r['only_in_gold']} "
         f"only_in_stream_unmaterialised={r['only_in_stream_unmaterialised']} "
         f"columns={','.join(RECONCILED_COLUMNS)} not_projected={','.join(NOT_PROJECTED)}"),
    ]


def control_checks(f: Mapping[str, Any]) -> list[tuple[str, bool]]:
    """What the control run claims. Named so a failure says which zero was not zero."""
    v, d, w, r, s = (f["validation"], f["dedupe"], f["watermark"], f["reconcile"],
                     f["survivor"])
    return [
        ("validation_shared_with_silver", v["recorded"] == v["expected"]),
        # The two paths pick a different survivor from a key-collision group, and only one
        # collision class makes that visible in an aggregate. In an `unresolvable` group the
        # ratings disagree, so silver drops every row and the stream keeps one -- the two
        # tables then differ in `review_count`, and ADR-0010's claim that the survivor rule
        # "cannot change a rating aggregate" stops holding. This run reconciles because silver
        # measured zero of them, so the gate asserts that rather than resting on it unstated.
        # `conflicting` groups (same rating, something else differs) can still move
        # `nonempty_text_count` or `verified_count`; that is not asserted here because the
        # reconciliation itself catches it -- the count is printed so a reader knows where a
        # `differing` row would have come from.
        ("no_unresolvable_collisions", s["unresolvable"] == 0),
        # The gate's own read of the topic against the count the stream recorded: two passes
        # over the same offsets, so a micro-batch the stream never saw has nowhere to hide.
        ("topic_fully_read", d["records_read"] == d["ledger_read"]),
        ("every_valid_row_accounted_for",
         d["rows_valid"] == d["unique"] + d["dropped"] + w["natural_drops"]),
        ("duplicates_match_sort_count", d["dropped"] == d["expected"]),
        ("zero_natural_drops", w["natural_drops"] == 0),
        # A reconciliation of nothing is not a clean reconciliation: with no rows compared,
        # every difference count below is trivially zero and the gate would pass on silence.
        ("product_months_compared", r["compared"] > 0),
        ("zero_differing_product_months", r["differing"] == 0),
        ("no_product_month_only_in_stream", r["only_in_stream"] == 0),
        ("no_product_month_only_in_gold", r["only_in_gold"] == 0),
    ]


def terminal_line(f: Mapping[str, Any], *, run_kind: str, run_id: str, scope: str,
                  checks: Sequence[tuple[str, bool]]) -> str:
    failed = [name for name, ok in checks if not ok]
    return (f"{GATE_NAME} run_kind={run_kind} run_id={run_id} scope={scope} "
            f"natural_drops={f['watermark']['natural_drops']} "
            f"differing_product_months={f['reconcile']['differing']} "
            f"failed={','.join(failed) or 'none'} "
            f"{GATE_NAME}={'PASS' if not failed else 'FAIL'}")


def control(f: Mapping[str, Any], *, run_id: str, scope: str) -> Verdict:
    """`STREAM_GATE` for the control run: zero drops, zero differences, nothing forked."""
    checks = control_checks(f)
    return repro_verdict(GATE_NAME, checks,
                         terminal_line(f, run_kind="control", run_id=run_id, scope=scope,
                                       checks=checks),
                         constituents=constituent_lines(f))


def population(f: Mapping[str, Any], *, category: str) -> dict[str, Any]:
    """What the verdict was measured over -- named, so the evaluation table can say."""
    return {"name": f"{category}/stream.product_month vs gold.product_month",
            "n": f["reconcile"]["compared"],
            "records_read": f["dedupe"]["records_read"],
            "unique_review_ids": f["dedupe"]["unique"],
            "gold_run_id": f["reconcile"]["gold_run_id"],
            "replay_run_id": f["replay_run_id"]}


# ------------------------------------------------------------------- the demo run ----
def demo_lines(f: Mapping[str, Any]) -> list[str]:
    """The demo run's extra constituents: what was injected, and the control it leans on."""
    i, r, c = f["injection"], f["reconcile"], f["control"]
    return [
        (f"STREAM_INJECTION near_rows={i['near_rows']} near_lag_days={i['near_lag_days']} "
         f"far_rows={i['far_rows']} far_lag_days={i['far_lag_days']} "
         f"held_back_sha256={str(i['held_back_sha256'])[:12]} near_on_topic={i['near_on_topic']} "
         f"far_on_topic={i['far_on_topic']} expected_drops={i['expected_drops']} "
         f"observed_drops={f['watermark']['natural_drops']} "
         f"near_accepted={'derived' if _near_accepted(f) else 'false'}"),
        (f"STREAM_EXPLAINED differing={r['differing']} explained={r['differing_explained']} "
         f"unexplained={r['differing_unexplained']} only_in_gold={r['only_in_gold']} "
         f"only_in_gold_explained={r['only_in_gold_explained']} "
         f"only_in_gold_unexplained={r['only_in_gold_unexplained']} "
         f"dropped_product_months={r['dropped_product_months']} "
         f"dropped_outside_gold={r['dropped_outside_gold']}"),
        (f"STREAM_CONTROL control_run={str(c['run_id'])[:8]} status={c['status']} "
         f"scope={c['scope']} ledger_status={c['ledger_status']} "
         f"natural_drops={c['natural_drops']} differing={c['differing']} "
         f"protocol_hash={str(c['protocol_hash'])[:12]} "
         f"same_protocol={_b(c['protocol_hash'] == f['replay_config_hash'])}"),
    ]


def _near_accepted(f: Mapping[str, Any]) -> bool:
    """Derived, not observed: the drops are the far slice and the differences are the far slice,
    so nothing else -- the near slice included -- was dropped."""
    w, i, r = f["watermark"], f["injection"], f["reconcile"]
    return (w["natural_drops"] == i["far_rows"] and r["differing_unexplained"] == 0
            and r["only_in_gold_unexplained"] == 0 and i["near_on_topic"] == i["near_rows"])


def demo_checks(f: Mapping[str, Any]) -> list[tuple[str, bool]]:
    """What the demo run claims. The control run's zeros become the frozen predictions."""
    v, d, w, r, s = (f["validation"], f["dedupe"], f["watermark"], f["reconcile"],
                     f["survivor"])
    i, c = f["injection"], f["control"]
    return [
        ("validation_shared_with_silver", v["recorded"] == v["expected"]),
        ("no_unresolvable_collisions", s["unresolvable"] == 0),
        ("topic_fully_read", d["records_read"] == d["ledger_read"]),
        ("every_valid_row_accounted_for",
         d["rows_valid"] == d["unique"] + d["dropped"] + w["natural_drops"]),
        ("duplicates_match_sort_count", d["dropped"] == d["expected"]),
        # Both slices reached the topic whole: the gate's own re-read finds every held-back id
        # among the valid rows, once. A slice the producer wrote but the topic lost would
        # otherwise show up as a drop count that happens to match.
        ("held_back_rows_on_topic",
         i["near_on_topic"] == i["near_rows"] and i["far_on_topic"] == i["far_rows"]),
        # The sizes and lags the replay recorded are the frozen document's, not its own
        # invention: read from conf/stream_replay.toml by the gate and compared here.
        ("injection_matches_frozen_config",
         all(i[k] == i["frozen"][k] for k in ("near_rows", "far_rows", "near_lag_days",
                                              "far_lag_days"))
         and i["near_lag_days"] < i["frozen"]["watermark_days"] < i["far_lag_days"]),
        # Spark's own `numRowsDroppedByWatermark` against the size frozen before the run --
        # and the prediction the plan made is the far slice, whole and nothing else.
        ("far_slice_dropped_exactly",
         w["natural_drops"] == i["expected_drops"] == i["far_rows"]),
        ("near_slice_accepted", _near_accepted(f)),
        ("product_months_compared", r["compared"] > 0),
        # The claim is that the dropped rows explain the differences; with no differences at
        # all it would be vacuous, so the far slice has to be visible in the reconciliation.
        ("dropped_rows_visible_in_reconciliation",
         r["differing_explained"] + r["only_in_gold_explained"] > 0),
        ("every_difference_explained_by_dropped_rows", r["differing_unexplained"] == 0),
        ("every_missing_month_explained_by_dropped_rows", r["only_in_gold_unexplained"] == 0),
        ("no_product_month_only_in_stream", r["only_in_stream"] == 0),
        # Both runs, or neither means anything: the control artefact passed at full scope and
        # judges the run the ledger holds as the current control.
        ("control_run_present_and_passing",
         c["status"] == "PASS" and c["scope"] == "full" and c["ledger_status"] == "success"
         and c["ledger_run_found"] and bool(c["run_id"])
         and c["artifact_run_id"] == c["run_id"]),
        # The control run's own claim, asserted rather than only printed: an untouched replay
        # drops nothing and differs from batch nowhere. Without it the demo's explanations
        # would rest on a control that never demonstrated the watermark keeps sorted rows.
        ("control_run_dropped_nothing", c["natural_drops"] == 0 and c["differing"] == 0),
        ("control_and_demo_share_protocol", c["protocol_hash"] == f["replay_config_hash"]),
    ]


def demo_terminal_line(f: Mapping[str, Any], *, run_id: str, scope: str,
                       checks: Sequence[tuple[str, bool]]) -> str:
    failed = [name for name, ok in checks if not ok]
    return (f"{GATE_NAME} run_kind=demo run_id={run_id} scope={scope} "
            f"dropped_by_watermark={f['watermark']['natural_drops']} "
            f"expected_drops={f['injection']['expected_drops']} "
            f"differing_product_months={f['reconcile']['differing']} "
            f"unexplained={f['reconcile']['differing_unexplained'] + f['reconcile']['only_in_gold_unexplained']} "
            f"control_run={str(f['control']['run_id'])[:8]} "
            f"failed={','.join(failed) or 'none'} "
            f"{GATE_NAME}={'PASS' if not failed else 'FAIL'}")


def demo(f: Mapping[str, Any], *, run_id: str, scope: str) -> Verdict:
    """`STREAM_GATE` for the demo run: the far slice dropped, the near slice kept, and every
    difference from gold explained by the rows that were dropped."""
    checks = demo_checks(f)
    return repro_verdict(GATE_NAME, checks,
                         demo_terminal_line(f, run_id=run_id, scope=scope, checks=checks),
                         constituents=constituent_lines(f, run_kind="demo") + demo_lines(f))


def demo_population(f: Mapping[str, Any], *, category: str) -> dict[str, Any]:
    return population(f, category=category) | {
        "near_rows": f["injection"]["near_rows"], "far_rows": f["injection"]["far_rows"],
        "control_run_id": f["control"]["run_id"]}

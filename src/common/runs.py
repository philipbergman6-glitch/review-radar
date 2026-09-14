"""The run ledger: one `pipeline_runs` row per execution attempt of every job (ADR-0008).

A job is an independently rerunnable command. Its driver calls `start()` *before* doing
anything, gets a UUID `run_id`, stamps that id on every output it writes (Iceberg snapshot
summary, ES document, eval artefact), and then calls `success()` or `failed()`. The row
is the lineage: it names its inputs and outputs by concrete identity (table + snapshot id,
catalogue load id, source sha256), and the outputs name the row back.

Contracts. Each `(job_name, spec_version)` declares the input, output and count keys it
must record and the count identity its numbers must satisfy. Validation returns *named*
failures ("identity: records_in 101 != records_out 80 + ...") and hard-fails by default;
a run whose numbers do not add up is `failed`, never quietly `success`.

Contracts registered here: silver, gold, search_index_reviews (v1 text only, v2 with
vectors), search_index_product_month, embeddings, catalogue_load, bronze_drain, produce, sort_replay,
stream_produce, stream_aggregate. Bronze and produce are declared so the CHECK list and the registry agree
from day one; their drivers are instrumented in their own sessions (ADR-0008 §3), not in
silver's.
"""
from __future__ import annotations

import json
import subprocess
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from src.common import config as C
from src.common.pg import connect

PROJECT_ROOT = C.PROJECT_ROOT

JOB_NAMES: tuple[str, ...] = (
    "produce", "catalogue_load", "bronze_drain", "silver", "gold",
    "search_index_reviews", "search_index_product_month", "embeddings",
    "theme_samples", "theme_labels_llm", "theme_labels_reference", "theme_labels_human",
    "theme_classifier_train", "theme_classifier_score", "rag_answers", "rag_judgements",
    "sort_replay", "stream_produce", "stream_aggregate",
)

SILVER_SPEC_VERSION = "1"
GOLD_SPEC_VERSION = "1"
SEARCH_REVIEWS_SPEC_VERSION = "1"
SEARCH_PRODUCT_MONTH_SPEC_VERSION = "1"
EMBEDDINGS_SPEC_VERSION = "1"
THEME_SAMPLES_SPEC_VERSION = "2"   # v2 generalises the discovery-only counts to any frame (RR-22)
# `theme_labels_llm` carries two contracts: v1 is the taxonomy-free discovery job,
# v2 the frozen-taxonomy labeller. Same job name, different outputs and counts.
THEME_DISCOVERY_SPEC_VERSION = "1"
THEME_LABELS_SPEC_VERSION = "2"
THEME_REFERENCE_SPEC_VERSION = "1"
# Philip's blind stratified 50 (RR-21). A separate job from `theme_labels_reference` on
# purpose: same shape, different annotator, and `label_source="human"` is the one value the
# labels table reserves for it. One job writing under two provenances would make the
# correlated-error number this exists to publish unverifiable after the fact.
THEME_HUMAN_SPEC_VERSION = "1"
THEME_CLASSIFIER_SPEC_VERSION = "1"
RAG_ANSWERS_SPEC_VERSION = "1"
RAG_JUDGEMENTS_SPEC_VERSION = "1"
CATALOGUE_LOAD_SPEC_VERSION = "1"
BRONZE_SPEC_VERSION = "1"
PRODUCE_SPEC_VERSION = "1"
# P8 (ADR-0010, ticket 14). `sort_replay` orders the file by event time once; `stream_produce`
# paces that file into the stream topic. Two jobs and not one because the sort is a durable
# artefact with its own digest: a replay names the file it sent, and that file names the run
# that ordered it.
SORT_REPLAY_SPEC_VERSION = "1"
# v2 names the `sort_replay` run whose file it is replaying, so the ordering a replay carried
# is a join rather than a matching digest someone eyeballed. v1 recorded the digest alone and
# is kept so the smoke replays made under it still validate. v3 (ticket 16) records the
# held-back slices: which rows were released late, by how much, and what the watermark was
# predicted to do to them -- for a control run all of that is zero and the sidecar is empty,
# so the two run kinds validate under one contract and differ in `run_kind` alone.
STREAM_PRODUCE_SPEC_VERSION = "3"
# `stream_aggregate` is the watermarked projection beside batch (ticket 15). Its numbers are
# an accounting of one topic: everything read is a row the projection counted or a duplicate
# the watermark removed, and nothing else may go missing in between.
STREAM_AGGREGATE_SPEC_VERSION = "1"

# Files whose state decides `worktree_dirty`. data/ and notebooks/ are excluded on
# purpose: they are inputs and presentation, not the code that produced the run.
CLEANLINESS_POLICY_VERSION = "1"
CLEANLINESS_PATHS = ("src", "scripts", "conf", "tests", "pyproject.toml", "uv.lock",
                     "Makefile", "run.sh", "docker-compose.yml")


class ContractViolation(RuntimeError):
    def __init__(self, failures: list[str]):
        super().__init__("; ".join(failures))
        self.failures = failures


@dataclass(frozen=True)
class Contract:
    job_name: str
    spec_version: str
    inputs: dict[str, tuple[str, ...]]          # input name -> required keys
    outputs: dict[str, tuple[str, ...]]         # output name -> required keys
    counts: tuple[str, ...]
    identity: Callable[[dict[str, int | None], dict[str, Any]], list[str]] = \
        field(default=lambda records, counts: [])


def _n(d: dict[str, Any], key: str) -> int | None:
    v = d.get(key)
    return None if v is None else int(v)


def _silver_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    ri, ro, rr = records.get("records_in"), records.get("records_out"), records.get("records_rejected")
    removed = _n(counts, "collision_rows_removed")
    if None in (ri, ro, rr, removed):
        fails.append("identity: records_in/records_out/records_rejected/collision_rows_removed "
                     "must all be set on success")
        return fails
    if ri != ro + rr + removed:
        fails.append(f"identity: records_in {ri} != records_out {ro} + records_rejected {rr} "
                     f"+ collision_rows_removed {removed}")
    groups, unres, trows = (_n(counts, "collision_groups"), _n(counts, "unresolvable_groups"),
                            _n(counts, "collision_table_rows"))
    if None not in (groups, unres, trows) and removed != trows - groups + unres:
        fails.append(f"collision_rows_removed {removed} != collision_table_rows {trows} "
                     f"- collision_groups {groups} + unresolvable_groups {unres}")
    ex, conf = _n(counts, "exact_groups"), _n(counts, "conflicting_groups")
    if None not in (ex, conf, unres, groups) and ex + conf + unres != groups:
        fails.append(f"collision classes {ex}+{conf}+{unres} != collision_groups {groups}")
    distinct = _n(counts, "review_id_distinct")
    if distinct is not None and distinct != ro:
        fails.append(f"review_id_distinct {distinct} != records_out {ro}")
    reasons = counts.get("reject_reasons") or {}
    if sum(int(v) for v in reasons.values()) != rr:
        fails.append(f"reject_reasons sum {sum(int(v) for v in reasons.values())} != records_rejected {rr}")
    return fails


def _gold_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    pm, expected = _n(counts, "product_months"), _n(counts, "product_months_expected")
    if records.get("records_out") != pm:
        fails.append(f"records_out {records.get('records_out')} != product_months {pm}")
    if pm != expected:
        fails.append(f"product_months {pm} != calendar span {expected} (spine incomplete)")
    on_spine, silver_rows = _n(counts, "reviews_on_spine"), _n(counts, "silver_rows")
    if records.get("records_in") != silver_rows:
        fails.append(f"records_in {records.get('records_in')} != silver_rows {silver_rows}")
    if None not in (on_spine, silver_rows) and on_spine > silver_rows:
        fails.append(f"reviews_on_spine {on_spine} > silver_rows {silver_rows}")
    total, mat, below = (_n(counts, k) for k in ("products_total", "products_materialised",
                                                 "products_below_min_reviews"))
    if None not in (total, mat, below) and total != mat + below:
        fails.append(f"products_total {total} != materialised {mat} + below_min_reviews {below}")
    alerts, episodes = _n(counts, "alerts"), _n(counts, "episodes")
    if alerts != episodes:
        fails.append(f"alerts {alerts} != episodes {episodes} (one episode per alert)")
    closure = counts.get("episodes_by_closure") or {}
    if episodes is not None and sum(int(v) for v in closure.values()) != episodes:
        fails.append(f"episodes_by_closure sum {sum(int(v) for v in closure.values())} != episodes {episodes}")
    return fails


def _search_reviews_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    silver_rows, excluded = _n(counts, "silver_rows"), _n(counts, "excluded_empty_text")
    sent, es_count = _n(counts, "docs_sent"), _n(counts, "es_count")
    if records.get("records_in") != silver_rows:
        fails.append(f"records_in {records.get('records_in')} != silver_rows {silver_rows}")
    if not (records.get("records_out") == sent == es_count):
        fails.append(f"records_out {records.get('records_out')} / docs_sent {sent} / es_count {es_count} disagree")
    if None not in (silver_rows, sent, excluded) and silver_rows != sent + excluded:
        fails.append(f"silver_rows {silver_rows} != docs_sent {sent} + excluded_empty_text {excluded}")
    total, passed = _n(counts, "contract_tests_total"), _n(counts, "contract_tests_passed")
    if total is None or passed != total:
        fails.append(f"contract tests {passed}/{total} not all passed")
    if "embedding_rows" in counts:  # v2: every embedding row became a vector-bearing document
        emb, vsent, ves = (_n(counts, k) for k in ("embedding_rows", "vector_docs_sent", "vector_docs_es"))
        if not (emb == vsent == ves):
            fails.append(f"embedding_rows {emb} / vector_docs_sent {vsent} / vector_docs_es {ves} disagree")
        if sent is not None and vsent is not None and vsent > sent:
            fails.append(f"vector_docs_sent {vsent} > docs_sent {sent}")
    return fails


def _search_product_month_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    gold_rows, sent, es_count = (_n(counts, k) for k in ("gold_rows", "docs_sent", "es_count"))
    if records.get("records_in") != gold_rows:
        fails.append(f"records_in {records.get('records_in')} != gold_rows {gold_rows}")
    if not (records.get("records_out") == sent == es_count == gold_rows):
        fails.append(f"records_out {records.get('records_out')} / docs_sent {sent} / es_count {es_count} "
                     f"/ gold_rows {gold_rows} disagree")
    return fails


def _embeddings_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    silver_rows, cohort, below = (_n(counts, k) for k in ("silver_rows", "cohort_rows", "below_min_words"))
    if records.get("records_in") != silver_rows:
        fails.append(f"records_in {records.get('records_in')} != silver_rows {silver_rows}")
    if None in (silver_rows, cohort, below) or silver_rows != cohort + below:
        fails.append(f"silver_rows {silver_rows} != cohort_rows {cohort} + below_min_words {below}")
    same = [records.get("records_out"), cohort] + [_n(counts, k) for k in
            ("embedded_rows", "table_rows_for_spec", "review_id_distinct", "dims_ok_rows", "unit_norm_rows")]
    if None in same or len(set(same)) != 1:
        fails.append("records_out / cohort_rows / embedded_rows / table_rows_for_spec / review_id_distinct "
                     f"/ dims_ok_rows / unit_norm_rows disagree: {same}")
    return fails


def _catalogue_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    fails: list[str] = []
    src, loaded, final = _n(counts, "source_rows"), _n(counts, "rows_loaded"), _n(counts, "final_count")
    if records.get("records_in") != src:
        fails.append(f"records_in {records.get('records_in')} != source_rows {src}")
    if not (records.get("records_out") == loaded == final):
        fails.append(f"records_out {records.get('records_out')} / rows_loaded {loaded} / "
                     f"final_count {final} disagree")
    prices = sum(_n(counts, k) or 0 for k in ("parsed_prices", "missing_prices", "invalid_prices"))
    if prices != src:
        fails.append(f"parsed+missing+invalid prices {prices} != source_rows {src}")
    return fails


def _bronze_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    replayed = _n(counts, "records_replayed")
    if replayed is None:
        return ["counts.records_replayed missing"]
    ri, ro = records.get("records_in"), records.get("records_out")
    if ri != (ro or 0) + replayed:
        return [f"identity: records_in {ri} != records_out {ro} + records_replayed {replayed}"]
    return []


def _produce_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    attempted, acked = _n(counts, "records_attempted"), _n(counts, "records_acked")
    if records.get("records_out") != acked:
        return [f"records_out {records.get('records_out')} != records_acked {acked}"]
    if attempted is not None and acked is not None and acked > attempted:
        return [f"records_acked {acked} > records_attempted {attempted}"]
    return []


def _sort_replay_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Every input line is either ordered into the output or rejected with a named reason.

    The failure this refuses is a sort that quietly loses rows: an index built from a partly
    read file still sorts, still writes, and still looks plausible, so `rows_sorted +
    rows_rejected` must reconstruct the input.

    The reproducibility claim is about *sort keys*, not review ids. review_id is not unique
    in this source -- 6,139 key collision groups over 13,415 rows is the measured fact silver
    dedupes for -- so the key carries the line's content digest as its last component. Rows
    that still tie after that are byte-identical, and which of them is written first cannot
    change the output. What must hold is that the two accountings agree: distinct keys plus
    byte-identical rows is every row, and there are at least as many keys as review ids.
    """
    fails: list[str] = []
    sorted_, rejected = _n(counts, "rows_sorted"), _n(counts, "rows_rejected")
    if None in (sorted_, rejected):
        return ["identity: rows_sorted and rows_rejected must both be set on success"]
    if records.get("records_out") != sorted_:
        fails.append(f"identity: records_out {records.get('records_out')} != rows_sorted {sorted_}")
    if records.get("records_rejected") != rejected:
        fails.append(f"identity: records_rejected {records.get('records_rejected')} != "
                     f"rows_rejected {rejected}")
    if records.get("records_in") != sorted_ + rejected:
        fails.append(f"identity: records_in {records.get('records_in')} != rows_sorted {sorted_} "
                     f"+ rows_rejected {rejected}")
    reasons = counts.get("reject_reasons") or {}
    if sum(int(v) for v in reasons.values()) != rejected:
        fails.append(f"reject_reasons sum {sum(int(v) for v in reasons.values())} != "
                     f"rows_rejected {rejected}")
    ids, keys = _n(counts, "distinct_review_ids"), _n(counts, "distinct_sort_keys")
    collisions, identical = _n(counts, "key_collision_rows"), _n(counts, "byte_identical_rows")
    if None in (ids, keys, collisions, identical):
        fails.append("identity: distinct_review_ids/distinct_sort_keys/key_collision_rows/"
                     "byte_identical_rows must all be set on success")
        return fails
    if keys + identical != sorted_:
        fails.append(f"identity: distinct_sort_keys {keys} + byte_identical_rows {identical} "
                     f"!= rows_sorted {sorted_}")
    if ids + collisions != sorted_:
        fails.append(f"identity: distinct_review_ids {ids} + key_collision_rows {collisions} "
                     f"!= rows_sorted {sorted_}")
    if keys < ids:
        fails.append(f"distinct_sort_keys {keys} < distinct_review_ids {ids}: the content "
                     "digest cannot merge rows the review id kept apart")
    first, last = _n(counts, "first_event_ms"), _n(counts, "last_event_ms")
    if None not in (first, last) and first > last:
        fails.append(f"first_event_ms {first} > last_event_ms {last}: the output is not sorted")
    return fails


def _stream_produce_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Everything attempted was acknowledged, and the replay ran at the rate it claims.

    The rate check is part of the contract rather than a printed nicety: ADR-0010's demo
    claim is that event time is compressed at a *constant* record rate, and a run that
    silently paced at half its target would leave that claim unfalsifiable afterwards. 10%
    is the tolerance -- wide enough for a laptop sharing a VM with the stack, narrow enough
    that a stalled broker or a mis-set rate shows up as a failed run.
    """
    fails: list[str] = []
    attempted, acked = _n(counts, "records_attempted"), _n(counts, "records_acked")
    if None in (attempted, acked):
        return ["identity: records_attempted and records_acked must both be set on success"]
    if records.get("records_in") != attempted:
        fails.append(f"identity: records_in {records.get('records_in')} != "
                     f"records_attempted {attempted}")
    if records.get("records_out") != acked:
        fails.append(f"identity: records_out {records.get('records_out')} != records_acked {acked}")
    if acked != attempted:
        fails.append(f"identity: records_acked {acked} != records_attempted {attempted}: "
                     "a stream run acknowledges everything it sent or it failed")
    first, last = _n(counts, "first_event_ms"), _n(counts, "last_event_ms")
    if None in (first, last):
        fails.append("counts.first_event_ms and counts.last_event_ms must be set on success")
    elif first > last:
        fails.append(f"first_event_ms {first} > last_event_ms {last}: the replay was not in "
                     "event-time order")
    target = counts.get("records_per_second_target")
    actual = counts.get("records_per_second_actual")
    if target is None or actual is None:
        fails.append("counts.records_per_second_target/_actual must both be set on success")
    elif float(target) and abs(float(actual) - float(target)) / float(target) > 0.10:
        fails.append(f"pacing: {actual} rec/s is more than 10% from the frozen target {target}")
    return fails


def _stream_produce_v3_identity(records: dict[str, int | None],
                                counts: dict[str, Any]) -> list[str]:
    """v2's identity, plus: the slices are what the run kind says they are.

    A control run holds nothing back. A demo run holds back exactly the frozen sizes, the
    sidecar lists every held-back row, and the watermark model predicted the whole far slice
    dropped and the whole near slice accepted -- the producer refuses to send a plan that does
    not, so a success row saying otherwise is a row that skipped the check.
    """
    fails = _stream_produce_identity(records, counts)
    kind = counts.get("run_kind")
    if kind not in ("control", "demo"):
        return fails + [f"counts.run_kind must be control|demo, got {kind!r}"]
    near, far, held = (_n(counts, k) for k in ("near_rows", "far_rows", "held_back_rows"))
    accepted, dropped = _n(counts, "expected_near_accepted"), _n(counts, "expected_far_dropped")
    if None in (near, far, held, accepted, dropped):
        return fails + [("identity: near_rows/far_rows/held_back_rows/expected_near_accepted/"
                         "expected_far_dropped must all be set on success")]
    if held != near + far:
        fails.append(f"identity: held_back_rows {held} != near_rows {near} + far_rows {far}")
    if kind == "control" and (near or far):
        fails.append(f"a control run holds nothing back, but near_rows={near} far_rows={far}")
    if kind == "demo" and not (near > 0 and far > 0):
        fails.append(f"a demo run holds both slices back, but near_rows={near} far_rows={far}")
    if accepted != near:
        fails.append(f"expected_near_accepted {accepted} != near_rows {near}: the plan was not "
                     "predicted to accept the whole near slice")
    if dropped != far:
        fails.append(f"expected_far_dropped {dropped} != far_rows {far}: the plan was not "
                     "predicted to drop the whole far slice")
    return fails


REGISTRY: dict[tuple[str, str], Contract] = {}


def _register(c: Contract) -> None:
    REGISTRY[(c.job_name, c.spec_version)] = c


_register(Contract(
    "silver", SILVER_SPEC_VERSION,
    inputs={"bronze": ("table", "snapshot_id"), "catalogue": ("table", "catalogue_load_id")},
    outputs={"silver.reviews": ("table", "snapshot_id"), "silver.rejects": ("table", "snapshot_id"),
             "silver.review_collisions": ("table", "snapshot_id")},
    counts=("collision_groups", "exact_groups", "conflicting_groups", "unresolvable_groups",
            "collision_table_rows", "collision_rows_removed", "unmatched_review_rows",
            "unmatched_parent_asins", "review_id_distinct", "reject_reasons", "catalogue_rows_read"),
    identity=_silver_identity))

_register(Contract(
    "gold", GOLD_SPEC_VERSION,
    inputs={"silver": ("run_id", "table", "snapshot_id"), "rule": ("path", "config_hash", "status")},
    outputs={"gold.product_month": ("table", "snapshot_id"),
             "gold.evaluation_points": ("table", "snapshot_id"),
             "gold.decline_episodes": ("table", "snapshot_id")},
    counts=("products_total", "products_materialised", "products_below_min_reviews", "product_months",
            "product_months_expected", "active_product_months", "reviews_on_spine", "silver_rows",
            "points", "evaluable", "condition_true", "alerts", "evaluable_products",
            "unevaluable_reasons", "episodes", "episodes_by_closure", "holdout_points",
            "holdout_evaluable", "holdout_alerts", "holdout_eligible_products",
            "holdout_alerted_products", "holdout_episodes"),
    identity=_gold_identity))

_register(Contract(
    "search_index_reviews", SEARCH_REVIEWS_SPEC_VERSION,
    inputs={"silver": ("run_id", "table", "snapshot_id"), "contract": ("path", "version", "hash")},
    outputs={"es.reviews": ("index", "alias", "doc_count")},
    counts=("silver_rows", "excluded_empty_text", "docs_sent", "es_count", "previous_generations",
            "contract_tests_total", "contract_tests_passed", "analyzers", "contract_hash"),
    identity=_search_reviews_identity))

# v2: the generation also carries `text_vector` for the embedding cohort (P5).
_register(Contract(
    "search_index_reviews", "2",
    inputs={"silver": ("run_id", "table", "snapshot_id"), "contract": ("path", "version", "hash"),
            "embeddings": ("run_id", "table", "snapshot_id", "spec_hash")},
    outputs={"es.reviews": ("index", "alias", "doc_count")},
    counts=("silver_rows", "excluded_empty_text", "docs_sent", "es_count", "previous_generations",
            "contract_tests_total", "contract_tests_passed", "analyzers", "contract_hash",
            "embedding_rows", "vector_docs_sent", "vector_docs_es", "embedding_spec_hash"),
    identity=_search_reviews_identity))

_register(Contract(
    "embeddings", EMBEDDINGS_SPEC_VERSION,
    inputs={"silver": ("run_id", "table", "snapshot_id"),
            "spec": ("path", "version", "hash", "model", "revision")},
    outputs={"gold.review_embeddings": ("table", "snapshot_id", "spec_hash")},
    counts=("silver_rows", "cohort_rows", "below_min_words", "embedded_rows", "table_rows_for_spec",
            "review_id_distinct", "dims_ok_rows", "unit_norm_rows", "min_words", "elapsed_s",
            "reviews_per_s"),
    identity=_embeddings_identity))

_register(Contract(
    "search_index_product_month", SEARCH_PRODUCT_MONTH_SPEC_VERSION,
    inputs={"gold": ("run_id", "table", "snapshot_id"), "contract": ("path", "version", "hash")},
    outputs={"es.product_month": ("index", "alias", "doc_count")},
    counts=("gold_rows", "docs_sent", "es_count", "previous_generations", "contract_hash"),
    identity=_search_product_month_identity))

_register(Contract(
    "catalogue_load", CATALOGUE_LOAD_SPEC_VERSION,
    inputs={"source": ("path", "sha256")},
    outputs={"products": ("table", "catalogue_load_id")},
    counts=("source_rows", "parsed_prices", "missing_prices", "invalid_prices", "rows_loaded",
            "final_count"),
    identity=_catalogue_identity))

_register(Contract(
    "bronze_drain", BRONZE_SPEC_VERSION,
    inputs={"kafka": ("topic",)},
    outputs={"reviews_raw": ("table", "snapshot_ids")},
    counts=("micro_batches", "records_replayed"),
    identity=_bronze_identity))

_register(Contract(
    "sort_replay", SORT_REPLAY_SPEC_VERSION,
    inputs={"source": ("path", "sha256", "bytes"),
            "protocol": ("path", "status", "config_hash")},
    outputs={"sorted_file": ("path", "sha256", "rows")},
    counts=("rows_sorted", "rows_rejected", "reject_reasons", "distinct_review_ids",
            "key_collision_rows", "distinct_sort_keys", "byte_identical_rows",
            "first_event_ms", "last_event_ms", "output_sha256", "output_bytes",
            "reject_path", "replay_config_hash"),
    identity=_sort_replay_identity))

_STREAM_PRODUCE_COUNTS = ("records_attempted", "records_acked", "first_event_ms",
                          "last_event_ms", "records_per_second_target",
                          "records_per_second_actual", "elapsed_s", "replay_config_hash")

_register(Contract(
    "stream_produce", "1",
    inputs={"source": ("path", "sha256", "bytes"),
            "protocol": ("path", "status", "config_hash")},
    outputs={"kafka": ("topic",)},
    counts=_STREAM_PRODUCE_COUNTS,
    identity=_stream_produce_identity))

_register(Contract(
    "stream_produce", "2",
    inputs={"source": ("run_id", "path", "sha256", "bytes"),
            "protocol": ("path", "status", "config_hash")},
    outputs={"kafka": ("topic",)},
    counts=_STREAM_PRODUCE_COUNTS,
    identity=_stream_produce_identity))

_register(Contract(
    "stream_produce", STREAM_PRODUCE_SPEC_VERSION,
    inputs={"source": ("run_id", "path", "sha256", "bytes"),
            "protocol": ("path", "status", "config_hash")},
    outputs={"kafka": ("topic",),
             "held_back": ("path", "sha256", "rows")},
    counts=_STREAM_PRODUCE_COUNTS + ("run_kind", "near_rows", "far_rows", "held_back_rows",
                                     "expected_near_accepted", "expected_far_dropped"),
    identity=_stream_produce_v3_identity))

def _stream_aggregate_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """What the topic held, what reached the projection, and what the projection holds.

    Every count here is one the job *observed*: `records_read` and `natural_drops` come from
    the query's own progress, `unique_review_ids` is tallied per micro-batch as the rows are
    written, and `reviews_on_projection` is summed back off the materialised table. The split
    of the rows that never reached the projection -- validation rejects, duplicates, late rows
    -- is deliberately not decided here: it is a claim about the topic, so `STREAM_GATE`
    re-reads the same offsets and makes it, rather than the job grading its own arithmetic.

    The cross-check that earns its place is the last one. `unique_review_ids` is a running
    tally taken while the stream ran; `reviews_on_projection` is a sum over the Iceberg table
    afterwards. A micro-batch written twice, or one whose write failed silently, moves one and
    not the other.
    """
    fails: list[str] = []
    read, unique, late = (_n(counts, k) for k in
                          ("records_read", "unique_review_ids", "natural_drops"))
    months, on_projection = _n(counts, "product_months"), _n(counts, "reviews_on_projection")
    batches = _n(counts, "micro_batches")
    if None in (read, unique, late, months, on_projection, batches):
        return [("identity: records_read/unique_review_ids/natural_drops/product_months/"
                 "reviews_on_projection/micro_batches must all be set on success")]
    if records.get("records_in") != read:
        fails.append(f"identity: records_in {records.get('records_in')} != records_read {read}")
    if records.get("records_out") != months:
        fails.append(f"identity: records_out {records.get('records_out')} != "
                     f"product_months {months}")
    if records.get("records_rejected") != read - unique:
        fails.append(f"identity: records_rejected {records.get('records_rejected')} != "
                     f"records_read {read} - unique_review_ids {unique}")
    if late > read - unique:
        fails.append(f"identity: natural_drops {late} > the {read - unique} rows that never "
                     "reached the projection: more rows were dropped as late than went "
                     "unprojected at all")
    if on_projection != unique:
        fails.append(f"identity: reviews_on_projection {on_projection} != unique_review_ids "
                     f"{unique}: the projection holds rows the stream never counted, or lost "
                     "rows it did")
    if batches < 1:
        fails.append("identity: micro_batches 0 -- a stream that processed no batch has "
                     "projected nothing")
    return fails


_register(Contract(
    "stream_aggregate", STREAM_AGGREGATE_SPEC_VERSION,
    inputs={"replay": ("run_id", "topic"),
            "source": ("run_id", "path", "sha256"),
            "protocol": ("path", "status", "config_hash")},
    outputs={"stream.product_month": ("table", "snapshot_id"),
             "stream.product_month_batches": ("table", "snapshot_id")},
    counts=("records_read", "unique_review_ids", "natural_drops", "micro_batches",
            "product_months", "reviews_on_projection", "ingested_at", "watermark",
            "validation_source_sha256", "replay_config_hash", "elapsed_s"),
    identity=_stream_aggregate_identity))

_register(Contract(
    "produce", PRODUCE_SPEC_VERSION,
    inputs={"source": ("path", "sha256", "bytes")},
    outputs={"kafka": ("topic",)},
    counts=("records_attempted", "records_acked"),
    identity=_produce_identity))

def _theme_samples_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Every candidate is either matched or dropped with a named reason; the draw is exact.

    v2 (RR-22) is frame-agnostic: `drawn_rows` covers whichever sample the run drew, and the
    enriched/representative split is checked only when the frame declares one, so the
    training pool (no enrichment) and the audit set (both halves) validate under one rule.
    """
    fails: list[str] = []
    cands, matched = _n(counts, "candidate_episodes"), _n(counts, "matched_candidates")
    no_ctrl, not_text = _n(counts, "dropped_no_matching_control"), _n(counts, "dropped_not_text_characterisable")
    if None in (cands, matched, no_ctrl, not_text):
        fails.append("identity: candidate_episodes/matched_candidates/dropped_* must all be set on success")
        return fails
    if cands != matched + no_ctrl + not_text:
        fails.append(f"identity: candidate_episodes {cands} != matched_candidates {matched} + "
                     f"dropped_no_matching_control {no_ctrl} + "
                     f"dropped_not_text_characterisable {not_text}")
    drawn = _n(counts, "drawn_rows")
    if drawn is None:
        fails.append("identity: drawn_rows must be set on success")
        return fails
    if records.get("records_out") != drawn:
        fails.append(f"identity: records_out {records.get('records_out')} != drawn_rows {drawn}")
    eligible = _n(counts, "eligible_reviews")
    if eligible is not None and records.get("records_in") != eligible:
        fails.append(f"identity: records_in {records.get('records_in')} != eligible_reviews {eligible}")
    if eligible is not None and drawn > eligible:
        fails.append(f"identity: drawn_rows {drawn} > eligible_reviews {eligible}")
    split = [(k, _n(counts, k)) for k in ("enriched_rows", "representative_rows")]
    if any(v is not None for _, v in split):
        total = sum(v or 0 for _, v in split)
        if total != drawn:
            fails.append(f"identity: enriched_rows + representative_rows {total} != drawn_rows {drawn}")
    else:                                  # the discovery frame's own partition
        low, high = _n(counts, "low_rated_rows"), _n(counts, "high_rated_rows")
        if None in (low, high):
            fails.append("identity: an unenriched frame must report low_rated_rows and high_rated_rows")
        elif low + high != drawn:
            fails.append(f"identity: low_rated_rows {low} + high_rated_rows {high} != drawn_rows {drawn}")
    return fails


_register(Contract(
    "theme_samples", THEME_SAMPLES_SPEC_VERSION,
    inputs={"gold": ("run_id", "points_table", "points_snapshot_id",
                     "episodes_table", "episodes_snapshot_id"),
            "silver": ("run_id", "table", "snapshot_id"),
            "protocol": ("path", "status", "config_hash"),
            "rule": ("path", "config_hash", "status")},
    outputs={"gold.matched_controls": ("table", "snapshot_id"),
             "gold.theme_sample_assignments": ("table", "snapshot_id", "sample_name")},
    counts=("candidate_episodes", "matched_candidates", "dropped_not_text_characterisable",
            "dropped_no_matching_control", "control_rows", "distinct_control_products",
            "alerting_products_excluded", "source_reviews", "eligible_reviews", "drawn_rows",
            "low_rated_rows", "distinct_products", "from_candidates", "from_controls",
            "protocol_config_hash", "elapsed_s"),
    identity=_theme_samples_identity))

def _theme_labels_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Every selected review is a cache hit or an inference; every inference ends in one status."""
    fails: list[str] = []
    sel, hits, ran = (_n(counts, "reviews_selected"), _n(counts, "cache_hits"), _n(counts, "inferences_run"))
    ok, pf, af = (_n(counts, "succeeded"), _n(counts, "parse_failed"), _n(counts, "api_failed"))
    if None in (sel, hits, ran, ok, pf, af):
        fails.append("identity: reviews_selected/cache_hits/inferences_run/succeeded/parse_failed/"
                     "api_failed must all be set on success")
        return fails
    if sel != hits + ran:
        fails.append(f"identity: reviews_selected {sel} != cache_hits {hits} + inferences_run {ran}")
    if ran != ok + pf + af:
        fails.append(f"identity: inferences_run {ran} != succeeded {ok} + parse_failed {pf} + api_failed {af}")
    if records.get("records_out") != ok:
        fails.append(f"identity: records_out {records.get('records_out')} != succeeded {ok}")
    if records.get("records_rejected") != pf + af:
        fails.append(f"identity: records_rejected {records.get('records_rejected')} != "
                     f"parse_failed {pf} + api_failed {af}")
    rows, keys = _n(counts, "table_rows_for_config"), _n(counts, "distinct_keys_for_config")
    if rows is not None and keys is not None and rows != keys:
        fails.append(f"identity: table_rows_for_config {rows} != distinct_keys_for_config {keys}")
    return fails


_register(Contract(
    "theme_labels_llm", THEME_DISCOVERY_SPEC_VERSION,
    inputs={"samples": ("run_id", "table", "snapshot_id", "sample_name"),
            "silver": ("run_id", "table", "snapshot_id"),
            "spec": ("path", "version", "model_id", "prompt_version", "config_hash")},
    outputs={"gold.discovery_phrases": ("table", "snapshot_id", "budget_line")},
    counts=("reviews_selected", "cache_hits", "inferences_run", "succeeded", "parse_failed",
            "api_failed", "retried_inferences", "phrases_total", "distinct_aspects",
            "empty_complaint_lists", "table_rows_for_config", "distinct_keys_for_config",
            "inference_config_hash", "seconds_per_review", "elapsed_s"),
    identity=_theme_labels_identity))


def _theme_reference_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Ground truth is accepted or rejected; nothing is repaired on the way in (RR-21).

    A rejected reference label is a *hard failure* of the import, not a stored row: the agent
    re-labels the review and resubmits. So `accepted` must equal both `labels_submitted` and
    `reviews_selected` on success -- a partial ground-truth set would silently shrink the
    denominator of every metric that rests on it.
    """
    fails: list[str] = []
    sel, sub = _n(counts, "reviews_selected"), _n(counts, "labels_submitted")
    acc, rej = _n(counts, "accepted"), _n(counts, "rejected")
    if None in (sel, sub, acc, rej):
        fails.append("identity: reviews_selected/labels_submitted/accepted/rejected must be set")
        return fails
    if sub != acc + rej:
        fails.append(f"identity: labels_submitted {sub} != accepted {acc} + rejected {rej}")
    if rej:
        fails.append(f"identity: {rej} reference label(s) failed validation; ground truth is "
                     "never partially imported")
    if acc != sel:
        fails.append(f"identity: accepted {acc} != reviews_selected {sel}: the frame is not "
                     "fully labelled")
    if records.get("records_out") != acc:
        fails.append(f"identity: records_out {records.get('records_out')} != accepted {acc}")
    rows, keys = _n(counts, "table_rows_for_config"), _n(counts, "distinct_keys_for_config")
    if rows is not None and keys is not None and rows != keys:
        fails.append(f"identity: table_rows_for_config {rows} != distinct_keys_for_config {keys}")
    return fails


def _theme_label_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Same accounting as discovery, plus abstention -- which is an answer, not a failure.

    ADR-0003: `abstain=true` means the model deliberately declined; it is neither a transport
    nor a parse failure. Folding it into either would misreport the failure coverage, so it
    is its own terminal status and counts toward `records_out`.
    """
    fails: list[str] = []
    sel, hits, ran = (_n(counts, "reviews_selected"), _n(counts, "cache_hits"), _n(counts, "inferences_run"))
    ok, ab = _n(counts, "succeeded"), _n(counts, "model_abstained")
    pf, af = _n(counts, "parse_failed"), _n(counts, "api_failed")
    if None in (sel, hits, ran, ok, ab, pf, af):
        fails.append("identity: reviews_selected/cache_hits/inferences_run/succeeded/"
                     "model_abstained/parse_failed/api_failed must all be set on success")
        return fails
    if sel != hits + ran:
        fails.append(f"identity: reviews_selected {sel} != cache_hits {hits} + inferences_run {ran}")
    if ran != ok + ab + pf + af:
        fails.append(f"identity: inferences_run {ran} != succeeded {ok} + model_abstained {ab} + "
                     f"parse_failed {pf} + api_failed {af}")
    if records.get("records_out") != ok + ab:
        fails.append(f"identity: records_out {records.get('records_out')} != succeeded {ok} + "
                     f"model_abstained {ab}")
    if records.get("records_rejected") != pf + af:
        fails.append(f"identity: records_rejected {records.get('records_rejected')} != "
                     f"parse_failed {pf} + api_failed {af}")
    rows, keys = _n(counts, "table_rows_for_config"), _n(counts, "distinct_keys_for_config")
    if rows is not None and keys is not None and rows != keys:
        fails.append(f"identity: table_rows_for_config {rows} != distinct_keys_for_config {keys}")
    return fails


_register(Contract(
    "theme_labels_llm", THEME_LABELS_SPEC_VERSION,
    inputs={"samples": ("run_id", "table", "snapshot_id", "sample_name"),
            "silver": ("run_id", "table", "snapshot_id"),
            "spec": ("path", "version", "model_id", "prompt_version", "config_hash")},
    outputs={"gold.review_theme_labels": ("table", "snapshot_id", "budget_line", "label_source")},
    counts=("reviews_selected", "cache_hits", "inferences_run", "succeeded", "model_abstained",
            "parse_failed", "api_failed", "retried_inferences", "theme_hits", "other_present",
            "no_theme_labels", "table_rows_for_config", "distinct_keys_for_config",
            "inference_config_hash", "taxonomy_hash", "seconds_per_review", "elapsed_s"),
    identity=_theme_label_identity))


_register(Contract(
    "theme_labels_reference", THEME_REFERENCE_SPEC_VERSION,
    inputs={"samples": ("run_id", "table", "snapshot_id", "sample_name"),
            "silver": ("run_id", "table", "snapshot_id"),
            "blind_export": ("path", "map_path", "rows", "sha256"),
            "taxonomy": ("path", "version", "file_hash")},
    outputs={"gold.review_theme_labels": ("table", "snapshot_id", "budget_line", "label_source")},
    counts=("reviews_selected", "labels_submitted", "accepted", "rejected", "abstained",
            "theme_hits", "other_present", "no_theme_labels", "table_rows_for_config",
            "distinct_keys_for_config", "inference_config_hash", "taxonomy_hash", "elapsed_s"),
    identity=_theme_reference_identity))

# The same contract for Philip's 50, and the same identity check: a partially imported
# adjudication subset shrinks the denominator of the agreement number, which is the one number
# published to price the correlated-error risk of machine-made ground truth (RR-21).
_register(Contract(
    "theme_labels_human", THEME_HUMAN_SPEC_VERSION,
    inputs={"samples": ("run_id", "table", "snapshot_id", "sample_name"),
            "silver": ("run_id", "table", "snapshot_id"),
            "blind_export": ("path", "map_path", "rows", "sha256"),
            "taxonomy": ("path", "version", "file_hash")},
    outputs={"gold.review_theme_labels": ("table", "snapshot_id", "budget_line", "label_source")},
    counts=("reviews_selected", "labels_submitted", "accepted", "rejected", "abstained",
            "theme_hits", "other_present", "no_theme_labels", "table_rows_for_config",
            "distinct_keys_for_config", "inference_config_hash", "taxonomy_hash", "elapsed_s"),
    identity=_theme_reference_identity))


def _classifier_train_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """The pool splits into what trained the model and what was dropped for having no label."""
    fails: list[str] = []
    pool, used, dropped = (_n(counts, "pool_rows"), _n(counts, "training_rows"),
                           _n(counts, "dropped_failed_rows"))
    if None in (pool, used, dropped):
        fails.append("identity: pool_rows/training_rows/dropped_failed_rows must be set on success")
        return fails
    if pool != used + dropped:
        fails.append(f"identity: pool_rows {pool} != training_rows {used} + "
                     f"dropped_failed_rows {dropped}")
    if records.get("records_out") != used:
        fails.append(f"identity: records_out {records.get('records_out')} != training_rows {used}")
    if records.get("records_rejected") != dropped:
        fails.append(f"identity: records_rejected {records.get('records_rejected')} != "
                     f"dropped_failed_rows {dropped}")
    return fails


_register(Contract(
    "theme_classifier_train", THEME_CLASSIFIER_SPEC_VERSION,
    inputs={"labels": ("table", "budget_line", "label_source", "inference_config_hash"),
            "taxonomy": ("path", "version", "file_hash")},
    outputs={"theme_classifier_model": ("path", "spec_hash", "themes", "vocabulary")},
    counts=("pool_rows", "training_rows", "dropped_failed_rows", "theme_positives",
            "vocabulary", "spec_hash", "elapsed_s"),
    identity=_classifier_train_identity))


def _classifier_score_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Scoring is total: every review of the frame gets a prediction, empty or not."""
    scored = _n(counts, "reviews_scored")
    if scored is None:
        return ["identity: reviews_scored must be set on success"]
    fails = []
    if records.get("records_in") != scored or records.get("records_out") != scored:
        fails.append(f"identity: records_in/records_out must both equal reviews_scored {scored}")
    empty = _n(counts, "no_predicted_theme")
    if empty is not None and empty > scored:
        fails.append(f"identity: no_predicted_theme {empty} > reviews_scored {scored}")
    return fails


_register(Contract(
    "theme_classifier_score", THEME_CLASSIFIER_SPEC_VERSION,
    inputs={"model": ("path", "spec_hash"), "taxonomy": ("path", "version", "file_hash")},
    outputs={"gold.review_theme_labels": ("table", "snapshot_id", "budget_line", "label_source")},
    counts=("reviews_scored", "theme_hits", "no_predicted_theme", "spec_hash", "elapsed_s"),
    identity=_classifier_score_identity))

# Registered by its own phase (ADR-0008 §2); in the vocabulary now so the CHECK constraint and
# this registry stay in step. v0 is the placeholder it was registered under before P7 existed
# and is kept so a run written under it still validates.
_register(Contract("rag_answers", "0", inputs={}, outputs={}, counts=()))


def _rag_answers_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Every question reaches exactly one terminal state, and the answer file holds them all.

    The failure this refuses is a generator that quietly answers twenty of thirty: `records_in`
    is the question count, and the four terminal tallies must add back up to it, so a question
    that was skipped cannot disappear between the manifest and the artefact.
    """
    fails: list[str] = []
    total = _n(counts, "questions_total")
    if total is None:
        return ["counts.questions_total must be set on success"]
    if records.get("records_in") != total:
        fails.append(f"identity: records_in {records.get('records_in')} != questions_total {total}")
    parts = [_n(counts, k) for k in ("answered", "refused", "parse_failed", "api_failed")]
    if None in parts:
        fails.append("counts.answered/refused/parse_failed/api_failed must all be set")
    elif sum(parts) != total:  # type: ignore[arg-type]
        fails.append(f"identity: answered+refused+parse_failed+api_failed {sum(parts)} "  # type: ignore[arg-type]
                     f"!= questions_total {total}")
    else:
        if records.get("records_out") != parts[0] + parts[1]:  # type: ignore[operator]
            fails.append(f"identity: records_out {records.get('records_out')} != answered+refused")
        if records.get("records_rejected") != parts[2] + parts[3]:  # type: ignore[operator]
            fails.append(f"identity: records_rejected {records.get('records_rejected')} != "
                         "parse_failed+api_failed")
    ledger, ceiling = _n(counts, "calls_in_ledger"), _n(counts, "call_ceiling")
    if ledger is not None and ceiling is not None and ledger > ceiling:
        fails.append(f"identity: P7's call ledger holds {ledger} calls, over the ceiling {ceiling}")
    return fails


_register(Contract(
    "rag_answers", RAG_ANSWERS_SPEC_VERSION,
    inputs={"questions": ("path", "version", "spec_hash"),
            "spec": ("path", "version", "model_id", "prompt_version", "config_hash"),
            "search": ("run_id", "alias", "generation")},
    outputs={"eval.rag_answers": ("path", "questions", "sha256")},
    counts=("questions_total", "answered", "refused", "parse_failed", "api_failed", "calls",
            "calls_in_ledger", "call_ceiling", "cache_hits", "retrieved_total", "elapsed_s"),
    identity=_rag_answers_identity))


def _rag_judgements_identity(records: dict[str, int | None], counts: dict[str, Any]) -> list[str]:
    """Judging is total or it is nothing: every manifest question carries exactly one judgement.

    The failure this refuses is a partial judging pass that shrinks a denominator -- the four
    `RAG_QUALITY` rows are counted over the manifest's 20 and 10 (ADR-0006), so an import that
    accepted twenty-eight rows would publish a number over a population nobody froze. All
    thirty or none, exactly like Philip's 50 (`theme_labels_human`).
    """
    fails: list[str] = []
    total, judged = _n(counts, "questions_total"), _n(counts, "judged")
    if None in (total, judged):
        return ["counts.questions_total and counts.judged must be set on success"]
    if judged != total:
        fails.append(f"identity: judged {judged} != questions_total {total}")
    if records.get("records_in") != total or records.get("records_out") != judged:
        fails.append(f"identity: records_in {records.get('records_in')} must equal "
                     f"questions_total {total} and records_out {records.get('records_out')} "
                     f"must equal judged {judged}")
    if records.get("records_rejected") not in (0, None):
        fails.append("identity: an import that rejected a row must not succeed")
    ans, un = _n(counts, "answerable"), _n(counts, "unanswerable")
    if None not in (ans, un) and ans + un != total:  # type: ignore[operator]
        fails.append(f"identity: answerable {ans} + unanswerable {un} != questions_total {total}")
    return fails


# The judge's pass over the thirty (ticket 13). Its input names the `rag_answers` run it judged,
# by run id and by the answers file's digest, so the lineage walk joins the judgement to the
# exact bytes that were judged; a judgement of run 1 cannot be imported against run 2.
_register(Contract(
    "rag_judgements", RAG_JUDGEMENTS_SPEC_VERSION,
    inputs={"answers": ("run_id", "path", "questions", "sha256"),
            "questions": ("path", "version", "spec_hash"),
            "rubric": ("path", "sha256")},
    outputs={"eval.rag_judgements": ("path", "rows", "sha256")},
    counts=("questions_total", "answerable", "unanswerable", "judged", "answered_answerable",
            "refused_answerable", "refused_unanswerable", "answered_unanswerable", "malformed",
            "uncertain", "elapsed_s"),
    identity=_rag_judgements_identity))


def contract_for(job_name: str, spec_version: str | None = None) -> Contract | None:
    if spec_version is not None:
        return REGISTRY.get((job_name, spec_version))
    matches = [c for (j, _), c in REGISTRY.items() if j == job_name]
    return matches[-1] if matches else None


def _check_keys(section: str, required: dict[str, tuple[str, ...]], given: dict[str, Any]) -> list[str]:
    fails = []
    for name, keys in required.items():
        entry = given.get(name)
        if entry is None:
            fails.append(f"{section}.{name} missing")
            continue
        for k in keys:
            if not isinstance(entry, dict) or entry.get(k) is None:
                fails.append(f"{section}.{name}.{k} missing")
    return fails


def _finish(failures: list[str], raise_: bool) -> list[str]:
    if failures and raise_:
        raise ContractViolation(failures)
    return failures


def validate_start(job_name: str, spec_version: str, *, inputs: dict[str, Any],
                   params: dict[str, Any], raise_: bool = True) -> list[str]:
    c = REGISTRY.get((job_name, spec_version))
    if c is None:
        return _finish([f"no contract registered for ({job_name}, {spec_version})"], raise_)
    fails = _check_keys("inputs", c.inputs, inputs)
    if not isinstance(params, dict):
        fails.append("params must be a mapping")
    return _finish(fails, raise_)


def validate_finish(job_name: str, spec_version: str, *, records: dict[str, int | None],
                    outputs: dict[str, Any], counts: dict[str, Any], raise_: bool = True) -> list[str]:
    c = REGISTRY.get((job_name, spec_version))
    if c is None:
        return _finish([f"no contract registered for ({job_name}, {spec_version})"], raise_)
    fails = _check_keys("outputs", c.outputs, outputs)
    fails += [f"counts.{k} missing" for k in c.counts if k not in counts]
    fails += c.identity(records, counts)
    return _finish(fails, raise_)


# ------------------------------------------------------------------ git state ----
def git_state() -> tuple[str | None, bool]:
    """(commit sha, dirty) over the cleanliness paths; sha None outside a checkout."""
    try:
        sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, check=True,
                             capture_output=True, text=True).stdout.strip()
        status = subprocess.run(["git", "status", "--porcelain", "--", *CLEANLINESS_PATHS],
                                cwd=PROJECT_ROOT, check=True, capture_output=True, text=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None, True
    return sha, bool(status.strip())


# --------------------------------------------------------------------- ledger ----
def _json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, default=str)


@dataclass
class Run:
    run_id: str
    job_name: str
    spec_version: str
    started_at: datetime

    @property
    def short(self) -> str:
        return f"{self.job_name} · {self.started_at:%Y-%m-%d %H:%M} · {self.run_id[:4]}…"


def start(job_name: str, spec_version: str, *, category: str, data_scope: str,
          inputs: dict[str, Any], params: dict[str, Any]) -> Run:
    """Insert the `running` row and return the id to stamp on every output."""
    if data_scope not in ("sample", "full"):
        raise ValueError(f"data_scope must be 'sample' or 'full', got {data_scope!r}")
    validate_start(job_name, spec_version, inputs=inputs, params=params)
    run_id = str(uuid.uuid4())
    sha, dirty = git_state()
    started = datetime.now(UTC)
    with connect() as conn:
        conn.execute(
            """INSERT INTO pipeline_runs (run_id, job_name, spec_version, status, category,
                   data_scope, started_at, git_commit_sha, worktree_dirty,
                   cleanliness_policy_version, inputs, outputs, counts, params)
               VALUES (%s, %s, %s, 'running', %s, %s, %s, %s, %s, %s, %s::jsonb, '{}'::jsonb,
                       '{}'::jsonb, %s::jsonb)""",
            (run_id, job_name, spec_version, category, data_scope, started, sha, dirty,
             CLEANLINESS_POLICY_VERSION, _json(inputs), _json(params)))
    print(f"[ledger] {job_name} run {run_id} started (commit {sha or 'n/a'}"
          f"{', DIRTY' if dirty else ''})", flush=True)
    return Run(run_id, job_name, spec_version, started)


def success(run: Run, *, records_in: int, records_out: int, records_rejected: int,
            outputs: dict[str, Any], counts: dict[str, Any]) -> None:
    """Finalize to `success` only if the contract validates; otherwise mark `failed`."""
    records = {"records_in": records_in, "records_out": records_out,
               "records_rejected": records_rejected}
    fails = validate_finish(run.job_name, run.spec_version, records=records,
                            outputs=outputs, counts=counts, raise_=False)
    if fails:
        failed(run, notes="contract: " + "; ".join(fails), outputs=outputs, counts=counts,
               records=records)
        raise ContractViolation(fails)
    with connect() as conn:
        conn.execute(
            """UPDATE pipeline_runs SET status='success', finished_at=%s, records_in=%s,
                   records_out=%s, records_rejected=%s, outputs=%s::jsonb, counts=%s::jsonb
               WHERE run_id=%s AND status='running'""",
            (datetime.now(UTC), records_in, records_out, records_rejected,
             _json(outputs), _json(counts), run.run_id))
    print(f"[ledger] {run.job_name} run {run.run_id} success", flush=True)


def failed(run: Run, *, notes: str, outputs: dict[str, Any] | None = None,
           counts: dict[str, Any] | None = None,
           records: dict[str, int | None] | None = None) -> None:
    """Mark `failed`, keeping whatever partial outputs exist (never rolled back)."""
    records = records or {}
    with connect() as conn:
        conn.execute(
            """UPDATE pipeline_runs SET status='failed', finished_at=%s, notes=%s,
                   outputs=%s::jsonb, counts=%s::jsonb, records_in=%s, records_out=%s,
                   records_rejected=%s
               WHERE run_id=%s AND status='running'""",
            (datetime.now(UTC), notes[:4000], _json(outputs or {}), _json(counts or {}),
             records.get("records_in"), records.get("records_out"),
             records.get("records_rejected"), run.run_id))
    print(f"[ledger] {run.job_name} run {run.run_id} FAILED: {notes}", flush=True)


#: The ledger columns a reader gets back, in order. One list, so `by_id` and `latest_success`
#: hand back the same shape and a caller can swap one for the other.
_LEDGER_COLUMNS = ("run_id", "spec_version", "status", "started_at", "finished_at",
                   "records_in", "records_out", "records_rejected", "inputs", "outputs",
                   "counts", "params", "git_commit_sha", "worktree_dirty")


def by_id(run_id: str) -> dict[str, Any] | None:
    """One ledger row by its id, in the same shape `latest_success` returns.

    For a consumer that recorded *which* upstream run it read: pinning by id is the difference
    between re-deriving a claim against the run that produced it and re-deriving it against
    whichever run of that job happens to be newest (ticket 15).
    """
    with connect() as conn:
        row = conn.execute(
            f"""SELECT {', '.join(_LEDGER_COLUMNS)} FROM pipeline_runs WHERE run_id = %s""",
            (run_id,)).fetchone()
    if row is None:
        return None
    return {k: (str(v) if k == "run_id" else v) for k, v in zip(_LEDGER_COLUMNS, row)}


def latest_success(job_name: str, *, category: str, data_scope: str,
                   params_match: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """The newest successful run of a job, optionally narrowed to one set of params.

    `theme_samples` runs once per frame, so "the latest theme_samples run" is not the same
    question as "the run that drew the audit frame". `params_match` asks the second one:
    `params_match={"sample": "audit"}` matches on a JSONB containment, so a consumer pins the
    run that produced the frame it is about to read instead of whichever ran last.
    """
    clause, args = "", [job_name, category, data_scope]
    if params_match:
        clause = " AND params @> %s::jsonb"
        args.append(json.dumps(params_match))
    with connect() as conn:
        row = conn.execute(
            f"""SELECT {', '.join(_LEDGER_COLUMNS)}
               FROM pipeline_runs
               WHERE job_name=%s AND status='success' AND category=%s AND data_scope=%s{clause}
               ORDER BY started_at DESC LIMIT 1""",
            tuple(args)).fetchone()
    if row is None:
        return None
    return {k: (str(v) if k == "run_id" else v) for k, v in zip(_LEDGER_COLUMNS, row)}

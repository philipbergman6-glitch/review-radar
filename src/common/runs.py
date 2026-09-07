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
vectors), search_index_product_month, embeddings, catalogue_load, bronze_drain, produce. Bronze and
produce are declared so the CHECK list and the registry agree from day one; their drivers
are instrumented in their own sessions (ADR-0008 §3), not in silver's.
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
    "theme_samples", "theme_labels_llm", "theme_labels_reference",
    "theme_classifier_train", "theme_classifier_score", "rag_answers",
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
CATALOGUE_LOAD_SPEC_VERSION = "1"
BRONZE_SPEC_VERSION = "1"
PRODUCE_SPEC_VERSION = "1"

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

# Jobs whose contracts are registered by their own phase (ADR-0008 §2). They exist in the
# vocabulary now so the CHECK constraint and this registry stay in step.
for _later in ("theme_classifier_train", "theme_classifier_score",
               "rag_answers"):
    _register(Contract(_later, "0", inputs={}, outputs={}, counts=()))


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
            f"""SELECT run_id, spec_version, started_at, finished_at, records_in, records_out,
                      records_rejected, inputs, outputs, counts, params, git_commit_sha,
                      worktree_dirty
               FROM pipeline_runs
               WHERE job_name=%s AND status='success' AND category=%s AND data_scope=%s{clause}
               ORDER BY started_at DESC LIMIT 1""",
            tuple(args)).fetchone()
    if row is None:
        return None
    keys = ("run_id", "spec_version", "started_at", "finished_at", "records_in", "records_out",
            "records_rejected", "inputs", "outputs", "counts", "params", "git_commit_sha",
            "worktree_dirty")
    return {k: (str(v) if k == "run_id" else v) for k, v in zip(keys, row)}

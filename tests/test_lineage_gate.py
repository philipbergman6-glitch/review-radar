"""`LINEAGE_GATE` decides, and the declared chain it decides over (ticket 04).

The gate's whole claim is that provenance is a *join*: an artefact names a run, the run names
its outputs and inputs, and every one of those names resolves. So the tests here are mostly
trip cases -- break one link at a time and assert the gate says so -- plus the two the ticket
names outright: an empty chain must not pass, and an artefact whose run id has no ledger row
must fail.

Nothing here starts Postgres, Spark or Elasticsearch. `scripts/gate_lineage.py` loads the
facts; this file is the decision and the two pure helpers that read a ledger row's shape.
"""
from __future__ import annotations

import pytest

from scripts import gate_lineage as S
from src.common import evaluation as E
from src.common import runs
from src.gates import lineage as gate
from src.gates import silver as silver_gate

RUN = "4447727f-ee66-48eb-be9e-8a23e061d708"
UPSTREAM = "84af7fa0-e6db-4b53-b58e-f4df321e0436"


def artefact(**over):
    kw = {"capability": "silver", "run_id": RUN, "job": "silver", "resolved": True,
          "status": "success", "job_declared": True, "scope_matches": True, "pinned": True}
    kw.update(over)
    return gate.artefact_link(**kw)


def output(**over):
    kw = {"job": "silver", "run_id": RUN, "output": "silver.reviews", "store": "iceberg",
          "identity": "lake.silver.reviews@8507728480475534879", "exists": True,
          "stamped": True, "current": True}
    kw.update(over)
    return gate.output_link(**kw)


def edge(**over):
    kw = {"downstream_job": "gold", "downstream_run": RUN, "input_name": "silver",
          "upstream_job": "silver", "upstream_run": UPSTREAM,
          "identity": "lake.silver.reviews@8507728480475534879", "declared": True,
          "resolves": True, "pinned": True}
    kw.update(over)
    return gate.edge_link(**kw)


def clean_links():
    return [artefact(), gate.pin_link(capability="silver", job="silver", run_id=RUN,
                                      source="artifact", status="success"),
            output(), edge()]


def verdict(links=None, **over):
    kw = {"mode": "development", "pending": 0, "stale_outputs": 0, "orphan_running": 0}
    kw.update(over)
    return gate.verdict(clean_links() if links is None else links, **kw)


# =========================================================== the terminal shape ====
def test_the_terminal_line_names_the_mode_both_flags_and_the_links_it_checked():
    v = verdict()
    assert v.terminal == ("LINEAGE_GATE gate_mode=development chain_clean=true "
                          "publication_ready=true chain_links_checked=4 LINEAGE_GATE=PASS")


def test_a_chain_of_nothing_is_not_a_clean_chain():
    """The trip case the ticket names: a passing verdict over an empty chain is impossible."""
    v = verdict([])
    assert v.status == "FAIL"
    assert "chain_links_checked=0" in v.terminal and "chain_clean=false" in v.terminal
    assert v.failed == ("chain_not_empty",)


def test_a_kind_with_no_links_is_not_a_constituent_that_cannot_fail():
    v = verdict([artefact()])
    assert [name for name, _ in v.checks] == ["chain_not_empty", "artefacts_resolve"]


# =================================================================== the links ====
def test_an_artefact_whose_run_id_has_no_ledger_row_fails():
    """The second trip case: a run id nothing can resolve is a broken chain, not a detail."""
    v = verdict([artefact(resolved=False, status=None, job=None, job_declared=False,
                          pinned=False)])
    assert v.status == "FAIL" and v.failed == ("artefacts_resolve",)
    assert "resolved=false run_status=none" in v.constituents[0]


@pytest.mark.parametrize("over", [{"status": "running"}, {"status": "failed"},
                                  {"job_declared": False}, {"scope_matches": False},
                                  {"pinned": False}])
def test_every_way_an_artefact_can_be_misattributed_trips_the_gate(over):
    assert verdict([artefact(**over)]).status == "FAIL"


def test_a_declared_job_with_no_successful_run_is_a_claim_with_nothing_behind_it():
    v = verdict([gate.pin_link(capability="search", job="search_index_product_month",
                               run_id=None, source="latest_success", status=None)])
    assert v.status == "FAIL" and v.failed == ("jobs_pinned",)
    assert "run=none source=latest_success status=none ok=false" in v.constituents[0]


@pytest.mark.parametrize("over", [{"exists": False}, {"stamped": False}])
def test_an_output_that_is_gone_or_unstamped_fails_but_a_superseded_one_does_not(over):
    assert verdict([output(**over)]).status == "FAIL"
    assert verdict([output(current=False)]).status == "PASS"


def test_an_output_names_the_check_that_attributed_it():
    """A weaker mechanism is legible, never silent: the line says which one answered."""
    assert "attribution=snapshot_summary" in output().line
    by_column = output(attribution="column:run_id", detail="rows_written=200 rows_at_head=200")
    assert "attribution=column:run_id rows_written=200 rows_at_head=200" in by_column.line
    assert by_column.ok


def test_a_superseded_output_withholds_publication_without_breaking_the_chain():
    v = verdict([output(current=False)], mode="publication", stale_outputs=1)
    assert v.status == "FAIL"
    assert "chain_clean=true publication_ready=false" in v.terminal


@pytest.mark.parametrize("over, why", [({"declared": False}, "an edge nobody declared"),
                                       ({"resolves": False}, "an identity the upstream never wrote"),
                                       ({"pinned": False}, "a stale branch of the chain")])
def test_every_way_an_edge_can_fail_to_join(over, why):
    v = verdict([edge(**over)])
    assert v.status == "FAIL" and v.failed == ("edges_join",), why


def test_a_failed_runs_partial_outputs_must_still_be_there():
    kept = gate.retention_link(job="silver", run_id=RUN, outputs=2, present=2, notes="boom")
    lost = gate.retention_link(job="silver", run_id=RUN, outputs=2, present=1, notes="boom")
    assert verdict([kept]).status == "PASS"
    assert verdict([lost]).status == "FAIL"
    assert "status=failed outputs=2 present=1" in lost.line


def test_an_unjoined_input_is_still_checked_for_existence():
    joined_none = gate.input_link(job="silver", run_id=RUN, input_name="bronze",
                                  identity="lake.bronze.reviews_raw@167", exists=True,
                                  reason="the drain writes no ledger row")
    assert joined_none.ok and "joined=false" in joined_none.line
    assert verdict([gate.input_link(job="silver", run_id=RUN, input_name="bronze",
                                    identity="lake.bronze.reviews_raw@167", exists=False,
                                    reason="x")]).status == "FAIL"


def test_an_unknown_link_kind_is_a_crash_not_a_silent_skip():
    with pytest.raises(ValueError):
        gate.Link("vibes", "LINEAGE_VIBES ok=true", True)


# ============================================== development beside publication ====
def test_a_phase_that_has_not_run_leaves_a_development_pass_publication_unready():
    dev = verdict(pending=3)
    assert dev.status == "PASS"
    assert "chain_clean=true publication_ready=false" in dev.terminal
    assert verdict(pending=3, mode="publication").failed == ("publication_ready",)


def test_a_run_a_killed_driver_left_running_withholds_publication():
    assert verdict(orphan_running=1, mode="publication").status == "FAIL"
    assert verdict(orphan_running=1).status == "PASS"


def test_the_mode_vocabulary_is_closed():
    with pytest.raises(ValueError):
        verdict(mode="whenever")


# ================================================ what other gates must attest ====
def test_a_phase_gate_that_cannot_name_its_contracts_fails():
    v = silver_gate.verdict(scope="full", bronze_snapshot=7788, load_id="cat-1",
                            catalogue_rows=500, records_in=100, records_rejected=6,
                            records_out=90,
                            counts={"collision_rows_removed": 4, "review_id_distinct": 90,
                                    "join_cardinality_ok": True, "unmatched_review_rows": 0,
                                    "unmatched_parent_asins": 0},
                            reason_counts={r: 1 for r in silver_gate.REJECT_REASONS},
                            collisions={"groups": 3, "exact": 2, "conflicting": 1,
                                        "unresolvable": 0, "table_rows": 7, "removed": 4})
    assert v.status == "PASS"
    attested = gate.attest_contracts(v, jobs=["silver"], missing=[])
    assert attested.status == "PASS" and attested.terminal.endswith("SILVER_GATE=PASS")
    assert attested.constituents[-1] == ("RUN_CONTRACTS gate=SILVER_GATE jobs=silver "
                                         "registered=1/1 missing=none "
                                         "run_contract_registered=true")
    assert attested.metric["threshold"] == v.metric["threshold"] + 1

    broken = gate.attest_contracts(v, jobs=["silver", "ghost"], missing=["ghost"])
    assert broken.status == "FAIL" and broken.terminal.endswith("SILVER_GATE=FAIL")
    assert broken.failed == ("run_contract_registered",)
    assert "missing=ghost run_contract_registered=false" in broken.constituents[-1]


def test_a_capability_that_declares_no_jobs_is_left_alone_rather_than_given_a_free_pass():
    v = verdict()
    assert gate.attest_contracts(v, jobs=[], missing=[]) is v


def test_the_attestation_belongs_to_a_reproducibility_verdict():
    quality = E.Verdict(gate_name="GOLD_ANALYTICAL", status="REPORTED", constituents=(),
                        terminal="GOLD_ANALYTICAL alert_rate=0.1", checks=(("pop", True),),
                        metric={"name": "alert_rate", "value": 0.1, "threshold": None,
                                "direction": "none"})
    with pytest.raises(ValueError):
        gate.attest_contracts(quality, jobs=["gold"], missing=[])


# ============================================================== the declaration ====
def test_every_job_the_chain_declares_has_a_registered_run_contract():
    """What `attest` asserts at runtime, asserted once here over the whole chain."""
    for cap in E.load_chain():
        for job in cap.jobs:
            assert job in runs.JOB_NAMES, f"{cap.id} declares unknown job {job}"
            assert runs.contract_for(job) is not None, f"{cap.id}: {job} has no contract"


def test_every_declared_edge_names_jobs_the_ledger_knows():
    edges = E.load_edges()
    assert edges, "the chain declares no edges, so the gate could never check a join"
    for e in edges:
        assert e.downstream in runs.JOB_NAMES and e.upstream in runs.JOB_NAMES, e.name


def test_a_cut_edge_must_say_why_and_a_declared_one_must_not(tmp_path):
    def write(body):
        p = tmp_path / "chain.toml"
        p.write_text(body)
        return p

    with pytest.raises(ValueError, match="cut_reason is mandatory"):
        E.load_edges(write('[[edge]]\ndownstream="a"\ninput="b"\nupstream="c"\nstatus="cut"\n'))
    with pytest.raises(ValueError, match="cut edge only"):
        E.load_edges(write('[[edge]]\ndownstream="a"\ninput="b"\nupstream="c"\n'
                           'cut_reason="because"\n'))
    with pytest.raises(ValueError, match="declared twice"):
        E.load_edges(write('[[edge]]\ndownstream="a"\ninput="b"\nupstream="c"\n'
                           '[[edge]]\ndownstream="a"\ninput="b"\nupstream="d"\n'))
    with pytest.raises(ValueError, match="missing upstream"):
        E.load_edges(write('[[edge]]\ndownstream="a"\ninput="b"\n'))


def test_an_edge_that_lets_the_downstream_choose_its_upstream_must_say_why(tmp_path):
    def write(body):
        p = tmp_path / "chain.toml"
        p.write_text('[[edge]]\ndownstream="a"\ninput="b"\nupstream="c"\n' + body)
        return p

    with pytest.raises(ValueError, match="pin_reason is mandatory"):
        E.load_edges(write('upstream_pin="recorded"\n'))
    with pytest.raises(ValueError, match="recorded pin only"):
        E.load_edges(write('pin_reason="because"\n'))
    with pytest.raises(ValueError, match="upstream_pin must be one of"):
        E.load_edges(write('upstream_pin="whatever"\n'))
    (edge,) = E.load_edges(write('upstream_pin="recorded"\npin_reason="one frame per run"\n'))
    assert edge.upstream_pin == "recorded" and edge.pin_reason == "one frame per run"


def test_the_default_pin_is_the_strict_one():
    """Every edge is checked against the chain's own pin unless it says otherwise in writing."""
    loose = [e.name for e in E.load_edges() if e.upstream_pin == "recorded"]
    assert all(e.pin_reason for e in E.load_edges() if e.upstream_pin == "recorded")
    assert set(loose) == {"theme_labels_llm<-theme_samples.samples",
                          "theme_labels_reference<-theme_samples.samples",
                          "theme_samples<-gold.gold",
                          # P8 replays twice -- control and demo topics -- so each projection
                          # names the replay it read rather than the later of the two (ticket 16).
                          "stream_aggregate<-stream_produce.replay"}


def test_a_table_attributed_by_column_declares_the_column_and_the_reason(tmp_path):
    def write(body):
        p = tmp_path / "chain.toml"
        p.write_text(body)
        return p

    with pytest.raises(ValueError, match="is missing reason"):
        E.load_attributions(write('[[attribution]]\ntable="t"\nmechanism="column"\n'
                                  'column="run_id"\n'))
    with pytest.raises(ValueError, match="only mechanism the gate knows"):
        E.load_attributions(write('[[attribution]]\ntable="t"\nmechanism="vibes"\n'
                                  'column="run_id"\nreason="x"\n'))
    declared = E.load_attributions()
    labels = declared["lake.gold.review_theme_labels"]
    assert labels.column == "run_id" and "MERGE INTO" in labels.reason


def test_an_edge_narrowed_to_one_spec_version_applies_to_that_version_only():
    edges = E.load_edges()
    embed = E.edge_for(edges, "search_index_reviews", "embeddings", "2")
    assert embed is not None and embed.upstream == "embeddings"
    assert E.edge_for(edges, "search_index_reviews", "embeddings", "1") is None
    assert E.edge_for(edges, "search_index_reviews", "silver", "1") is not None


# ================================================ reading a ledger row's shape ====
def test_an_input_that_names_two_tables_yields_two_identities():
    entry = {"run_id": RUN, "points_table": "lake.gold.evaluation_points",
             "points_snapshot_id": 55, "episodes_table": "lake.gold.decline_episodes",
             "episodes_snapshot_id": 66}
    assert sorted(S.identities(entry)) == [("lake.gold.decline_episodes", 66),
                                           ("lake.gold.evaluation_points", 55)]


def test_a_table_with_no_snapshot_id_names_no_identity():
    assert S.identities({"table": "products", "catalogue_load_id": UPSTREAM}) == []


FRAME_EDGES = (E.Edge(downstream="theme_labels_llm", input="samples", upstream="theme_samples",
                      status="declared", cut_reason=None, spec_version=None,
                      upstream_pin="recorded", pin_reason="one frame per run"),
               E.Edge(downstream="theme_samples", input="gold", upstream="gold",
                      status="declared", cut_reason=None, spec_version=None),)


def ledger_row(run_id, job, inputs=None, status="success"):
    return {"run_id": run_id, "job_name": job, "spec_version": "2", "status": status,
            "inputs": inputs or {}, "outputs": {}}


def test_the_frame_a_run_read_is_pinned_beside_the_one_that_was_drawn_last():
    """The defect ticket 10a names: `latest_success` pins a frame nothing in the chain read."""
    read = ledger_row("frame-audit", "theme_samples")
    drew_last = ledger_row("frame-pool", "theme_samples")
    labels = ledger_row("labels-1", "theme_labels_llm",
                        {"samples": {"run_id": "frame-audit", "table": "lake.gold.t",
                                     "snapshot_id": 1}})
    pins = {"theme_labels_llm": {"labels-1": labels}, "theme_samples": {"frame-pool": drew_last}}
    by_id = {r["run_id"]: r for r in (read, drew_last, labels)}
    assert S.consumed_pins(pins, FRAME_EDGES, by_id) == [(read, "theme_labels_llm")]


def test_a_recorded_upstream_that_never_finished_is_not_pinned_into_the_chain():
    """An unpinned upstream is what the edge link then reports -- not a run quietly adopted."""
    crashed = ledger_row("frame-audit", "theme_samples", status="running")
    labels = ledger_row("labels-1", "theme_labels_llm",
                        {"samples": {"run_id": "frame-audit", "table": "lake.gold.t",
                                     "snapshot_id": 1}})
    pins = {"theme_labels_llm": {"labels-1": labels}}
    assert S.consumed_pins(pins, FRAME_EDGES, {r["run_id"]: r for r in (crashed, labels)}) == []


def test_only_an_edge_that_declares_it_may_name_its_own_upstream():
    """`theme_samples <- gold` here is a strict edge, so the gold run it read is not adopted."""
    gold = ledger_row("gold-old", "gold")
    frame = ledger_row("frame-audit", "theme_samples",
                       {"gold": {"run_id": "gold-old", "points_table": "lake.gold.p",
                                 "points_snapshot_id": 2}})
    pins = {"theme_samples": {"frame-audit": frame}}
    assert S.consumed_pins(pins, FRAME_EDGES, {r["run_id"]: r for r in (gold, frame)}) == []


def test_the_walk_closes_over_what_the_recorded_runs_themselves_recorded():
    """Two hops: the frame the labels read, then the gold run that frame was drawn from."""
    recorded_gold = E.Edge(downstream="theme_samples", input="gold", upstream="gold",
                           status="declared", cut_reason=None, spec_version=None,
                           upstream_pin="recorded", pin_reason="the draw is frozen")
    gold = ledger_row("gold-old", "gold")
    frame = ledger_row("frame-audit", "theme_samples",
                       {"gold": {"run_id": "gold-old", "points_table": "lake.gold.p",
                                 "points_snapshot_id": 2}})
    labels = ledger_row("labels-1", "theme_labels_llm",
                        {"samples": {"run_id": "frame-audit", "table": "lake.gold.t",
                                     "snapshot_id": 1}})
    by_id = {r["run_id"]: r for r in (gold, frame, labels)}
    found = S.consumed_pins({"theme_labels_llm": {"labels-1": labels}},
                            (FRAME_EDGES[0], recorded_gold), by_id)
    assert found == [(frame, "theme_labels_llm"), (gold, "theme_samples")]


def test_the_catalogue_load_id_is_a_run_reference_like_any_other():
    assert S.run_reference({"table": "products", "catalogue_load_id": UPSTREAM}) == UPSTREAM
    assert S.run_reference({"run_id": RUN, "table": "t", "snapshot_id": 1}) == RUN
    assert S.run_reference({"path": "conf/decline_rule.toml", "config_hash": "abc"}) is None

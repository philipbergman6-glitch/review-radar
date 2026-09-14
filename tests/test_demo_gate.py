"""`DEMO_GATE`: what makes an export a rehearsal, and what makes the gate fail (ticket 18).

The pure half, with no notebook executed and no services running. Three things are asserted:

* **a rehearsal is derived, never declared** -- every fact the gate counts on comes out of the
  exported notebook or the ledger, so an export cannot vouch for itself;
* **every named constituent can trip it** -- one rehearsal, a failed cell, a dead stream, an
  HTML backup from a different run and a missing design doc each turn PASS into FAIL;
* **the artefact it publishes validates** against `conf/eval-artifact.schema.json`.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from src.common import evaluation as E
from src.gates import demo as gate

RUN_ID = "50caffcf-0000-4000-8000-000000000001"
COMMIT = "0f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c"
T0 = datetime(2026, 9, 14, 9, 0, 0, tzinfo=UTC)

LIVE = "DEMO_LIVE batches=6 rows=412887 event_time_reached=2019-04 elapsed_s=208"


def stamp(offset_s: int) -> str:
    return (T0 + timedelta(seconds=offset_s)).isoformat().replace("+00:00", "Z")


def code_cell(n: int, *, start: int, end: int, text: str = "", error: str | None = None,
              executed: bool = True):
    outputs = [{"output_type": "stream", "name": "stdout", "text": [text]}] if text else []
    if error:
        outputs.append({"output_type": "error", "ename": error, "evalue": "boom",
                        "traceback": [f"{error}: boom"]})
    return {"cell_type": "code", "execution_count": n if executed else None,
            "source": ["..."], "outputs": outputs,
            "metadata": {"execution": {"iopub.status.busy": stamp(start),
                                       "shell.execute_reply": stamp(end)}}}


def notebook(*, elapsed: int = 210, live: str | None = LIVE, error_at: int | None = None,
             unexecuted_at: int | None = None, timing: bool = True):
    """A plausible executed export: move 1 prints the commit, move 10 prints DEMO_LIVE."""
    cells = [
        {"cell_type": "markdown", "source": ["# Review Radar"]},
        code_cell(1, start=0, end=5, text=f"git_commit  {COMMIT}\nscope  full"),
        code_cell(2, start=6, end=20, text="[live] batch 0: 70,000 deduplicated rows"),
        code_cell(3, start=21, end=elapsed, text=live or ""),
    ]
    if error_at is not None:
        cells[error_at] = code_cell(error_at, start=6, end=20, error="RuntimeError")
    if unexecuted_at is not None:
        cells[unexecuted_at] = code_cell(unexecuted_at, start=6, end=20, executed=False)
    if not timing:
        for c in cells:
            c["metadata"] = {}
    return {"cells": cells, "metadata": {}, "nbformat": 4}


def html_for(nb) -> str:
    """The HTML export as `nbconvert --to html` makes it: the same notebook, rendered."""
    return f"<html><body><pre>{gate.notebook_text(nb)}</pre></body></html>"


def replay_runs(*, started: int = 10, finished: int | None = 260):
    return [{"run_id": RUN_ID, "started_at": stamp(started),
             "finished_at": None if finished is None else stamp(finished)}]


DOCS = [path for _, path in gate.REQUIRED_DOCS]


def rehearsal(name: str = "2026-09-14", **over) -> gate.Rehearsal:
    nb = over.pop("nb", None) or notebook(**over.pop("nb_kwargs", {}))
    return gate.assess(name, nb,
                       present_files=over.pop("present_files", list(gate.EXPORT_FILES)),
                       html=over.pop("html", html_for(nb)),
                       replay_runs=over.pop("replay_runs", replay_runs()))


def two_rehearsals():
    return [rehearsal("2026-09-14a"), rehearsal("2026-09-14b")]


# ============================================================== what counts as one ====
def test_a_clean_export_counts_and_carries_its_derived_facts():
    r = rehearsal()
    assert r.counts, r.faults
    assert (r.all_cells_ok, r.stream_running, r.backup_playable) == (True, True, True)
    assert r.elapsed_s == 210
    assert (r.micro_batches, r.rows) == (6, 412887)
    assert r.replay_run_id == RUN_ID
    assert r.commit == COMMIT


def test_an_export_with_a_failed_cell_does_not_count():
    r = rehearsal(nb_kwargs={"error_at": 2})
    assert not r.counts and not r.all_cells_ok
    assert any("failed cells" in f and "RuntimeError" in f for f in r.faults)


def test_an_export_with_an_unexecuted_cell_does_not_count():
    r = rehearsal(nb_kwargs={"unexecuted_at": 2})
    assert not r.counts and not r.all_cells_ok
    assert any("unexecuted cells" in f for f in r.faults)


def test_an_export_over_the_ceiling_does_not_count():
    r = rehearsal(nb_kwargs={"elapsed": gate.MAX_ELAPSED_S + 1})
    assert not r.counts
    assert any("over the 300s ceiling" in f for f in r.faults)
    assert rehearsal(nb_kwargs={"elapsed": gate.MAX_ELAPSED_S}).counts, "the ceiling is inclusive"


def test_an_export_without_recorded_timing_cannot_be_timed_and_does_not_count():
    r = rehearsal(nb_kwargs={"timing": False})
    assert not r.counts and r.elapsed_s is None
    assert any("no recorded cell timing" in f for f in r.faults)


def test_an_export_whose_projection_saw_nothing_does_not_count():
    r = rehearsal(nb_kwargs={"live": "DEMO_LIVE batches=0 rows=0 elapsed_s=208"})
    assert not r.counts and not r.stream_running
    assert any("the stream was not running" in f for f in r.faults)


def test_a_projection_the_ledger_cannot_attest_does_not_count():
    """The notebook's own counter is not enough: a replay run has to overlap the window."""
    r = rehearsal(replay_runs=replay_runs(started=5_000, finished=6_000))
    assert not r.counts and not r.stream_running
    assert any("unattested" in f for f in r.faults)


def test_a_replay_still_running_at_the_end_attests_the_window():
    assert rehearsal(replay_runs=replay_runs(finished=None)).counts


def test_a_replay_that_began_before_the_cells_did_attests_nothing():
    """Move 2 starts the replay inside the notebook. The looser reading -- alive across the
    window -- is satisfied forever by one killed producer left `running` in the ledger."""
    r = rehearsal(replay_runs=replay_runs(started=-30, finished=None))
    assert not r.counts and r.replay_run_id is None
    assert any("unattested" in f for f in r.faults)


def test_the_replay_a_rehearsal_started_itself_wins_over_an_earlier_one():
    older = {"run_id": "older", "started_at": stamp(8), "finished_at": stamp(250)}
    assert rehearsal(replay_runs=[older, *replay_runs()]).replay_run_id == RUN_ID


def test_an_html_backup_from_a_different_run_is_not_playable():
    other = notebook(live="DEMO_LIVE batches=9 rows=1 elapsed_s=44")
    r = rehearsal(html=html_for(other))
    assert not r.counts and not r.backup_playable
    assert any("not derived from this notebook" in f for f in r.faults)


def test_a_rehearsal_without_the_backup_pair_still_counts_but_is_not_playable():
    """ADR-0009 splits them: whether the demo ran clean is a question about the cells, and
    neither the Kibana PNG nor the exactly-once transcript comes out of running them."""
    r = rehearsal(present_files=[gate.EXPORT_NOTEBOOK, gate.EXPORT_HTML])
    assert r.counts and r.all_cells_ok and r.stream_running
    assert not r.backup_playable
    assert gate.rehearsal_line(r).count("backup_missing=kibana-dashboard.png,exactly-once.txt")


def test_a_directory_with_no_exported_notebook_is_not_a_rehearsal():
    r = gate.assess("empty", {}, present_files=[], html=None, replay_runs=replay_runs())
    assert not r.counts
    assert any(gate.EXPORT_NOTEBOOK in f and gate.EXPORT_HTML in f for f in r.faults)
    assert any("no code cells" in f for f in r.faults)


# ====================================================================== the verdict ====
def test_two_clean_rehearsals_and_four_docs_pass():
    v = gate.verdict(two_rehearsals(), DOCS, scope="full")
    assert v.status == "PASS"
    assert v.terminal == (
        "DEMO_GATE rehearsals=2/2 max_elapsed_s=210 all_cells_ok=true stream_running=true "
        "backup_playable=true docs_present=4/4 gate_scope=full DEMO_GATE=PASS")
    assert v.constituents[0].startswith("DEMO_REHEARSAL 2026-09-14a cells=3 all_cells_ok=true")
    assert v.constituents[-1] == "DEMO_DOCS present=4/4 missing=none"


def test_one_rehearsal_does_not_reach_the_threshold():
    v = gate.verdict([rehearsal()], DOCS, scope="full")
    assert v.status == "FAIL"
    assert v.failed == ("rehearsals_at_threshold",)
    assert "rehearsals=1/2" in v.terminal


def test_no_rehearsals_at_all_fails_rather_than_passing_vacuously():
    v = gate.verdict([], DOCS, scope="full")
    assert v.status == "FAIL"
    assert set(v.failed) == {"rehearsals_at_threshold", "all_cells_ok", "stream_running",
                             "backup_playable"}
    assert "max_elapsed_s=none" in v.terminal


def test_a_failed_cell_trips_all_cells_ok_even_beside_two_good_ones():
    """The aggregate flags run over every export found, not over the ones that survived."""
    v = gate.verdict([*two_rehearsals(), rehearsal("bad", nb_kwargs={"error_at": 2})],
                     DOCS, scope="full")
    assert v.status == "FAIL"
    assert v.failed == ("all_cells_ok",)
    assert "rehearsals=2/2" in v.terminal, "the good two still count, and the gate still fails"


def test_two_counting_rehearsals_with_no_kibana_png_still_fail_on_the_backup():
    """The backup is blocking in its own right: two clean runs with nothing to play back is
    still a demo with no insurance (ADR-0009)."""
    pair = [rehearsal("a", present_files=list(gate.REHEARSAL_FILES)),
            rehearsal("b", present_files=list(gate.REHEARSAL_FILES))]
    v = gate.verdict(pair, DOCS, scope="full")
    assert v.status == "FAIL"
    assert v.failed == ("backup_playable",)
    assert "rehearsals=2/2 max_elapsed_s=210 all_cells_ok=true stream_running=true " \
           "backup_playable=false" in v.terminal


def test_a_missing_design_doc_blocks():
    present = [p for p in DOCS if p != "docs/DESIGN.md"]
    v = gate.verdict(two_rehearsals(), present, scope="full")
    assert v.status == "FAIL"
    assert v.failed == ("docs_present",)
    assert "docs_present=3/4" in v.terminal
    assert v.constituents[-1] == "DEMO_DOCS present=3/4 missing=docs/DESIGN.md"


@pytest.mark.parametrize("drop", [p for _, p in gate.REQUIRED_DOCS])
def test_every_one_of_the_four_documents_is_checked(drop):
    v = gate.verdict(two_rehearsals(), [p for p in DOCS if p != drop], scope="full")
    assert v.status == "FAIL" and v.failed == ("docs_present",)


def test_the_metric_is_the_constituent_count():
    v = gate.verdict(two_rehearsals(), DOCS, scope="full")
    assert v.metric == {"name": "constituents_ok", "value": 5, "threshold": 5, "direction": "eq"}


# ===================================================================== the artefact ====
def test_the_published_artefact_validates():
    v = gate.verdict(two_rehearsals(), DOCS, scope="full")
    doc = E.build_artifact(
        v, capability="demo", phase="Deliverables track", kind="reproducibility",
        protocol_hash="a" * 12, pipeline_run_id=RUN_ID, scope="full",
        population={"name": "committed demo exports", "n": 2,
                    "required": gate.REHEARSALS_REQUIRED},
        notes=gate.notes(two_rehearsals(), DOCS))
    assert E.validate_artifact(doc) == []
    assert json.loads(json.dumps(doc))["status"] == "PASS"


def test_notes_name_the_exports_that_did_not_count_and_the_documents_that_are_absent():
    rs = [rehearsal(), rehearsal("bad", nb_kwargs={"error_at": 2})]
    present = [p for p in DOCS if p != "docs/SLIDES.md"]
    text = " ".join(gate.notes(rs, present))
    assert "bad" in text and "failed cells" in text
    assert "docs/SLIDES.md" in text and "ticket 19" in text

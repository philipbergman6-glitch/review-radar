"""The demo stage's contract: the running order, the helpers' refusals, and the notebook.

What is asserted here is what the demo promises a grader before anything is run -- ten moves
inside the budget, a move whose phase is missing saying so, a gate that cannot be shown from
an artefact that is not there, and a notebook that is a runbook rather than a program. What is
not asserted is anything a run decides: no verdict, no count, no retrieval result.
"""
from __future__ import annotations

import json

import pandas as pd
import pytest

from src.common import config as C
from src.common import evaluation as E
from src.serving import demo

NOTEBOOK = C.PROJECT_ROOT / "notebooks" / "demo.ipynb"


# -------------------------------------------------------------------- the order ----
def test_ten_moves_numbered_in_order():
    assert [m.n for m in demo.MOVES] == list(range(1, 11))


def test_the_live_budget_is_four_forty_of_five_minutes():
    b = demo.budget()
    assert b["moves_s"] + b["switches_s"] == b["live_s"] == demo.LIVE_BUDGET_S == 280
    assert b["live_s"] < b["ceiling_s"] == 300
    assert b["spare_s"] == 20


def test_the_replay_starts_at_move_two_and_the_gate_prints_at_move_ten():
    assert "replay" in demo.MOVES[1].title.lower()
    assert "gate" in demo.MOVES[9].title.lower()


def test_kibana_and_the_terminal_carry_exactly_one_move_each():
    surfaces = [m.surface for m in demo.MOVES]
    assert surfaces.count("kibana") == 1
    assert surfaces.count("terminal") == 1
    assert surfaces.count("notebook") == 8


def test_every_move_says_something_and_a_pending_one_says_why():
    for m in demo.MOVES:
        assert m.says.strip(), f"move {m.n} has nothing said over it"
        if m.pending is not None:
            assert m.pending.strip(), f"move {m.n} is marked pending with no written reason"


def test_the_run_sheet_shows_the_pending_moves_rather_than_dropping_them():
    sheet = demo.run_sheet()
    assert len(sheet) == len(demo.MOVES)
    pending = [m.n for m in demo.MOVES if m.pending]
    assert pending, "the sheet is only worth checking while some phase is unbuilt"
    assert (sheet[sheet["move"].isin(pending)]["pending"] != "").all()


# ----------------------------------------------------------- recorded artefacts ----
def test_a_gate_with_no_artefact_stops_the_cell(monkeypatch, tmp_path):
    monkeypatch.setattr(E, "EVAL_ROOT", tmp_path)
    with pytest.raises(FileNotFoundError, match="no recorded gate"):
        demo.recorded_gate("silver")


def test_a_gate_artefact_that_does_not_validate_stops_the_cell(monkeypatch, tmp_path):
    monkeypatch.setattr(E, "EVAL_ROOT", tmp_path)
    (tmp_path / "silver").mkdir()
    (tmp_path / "silver" / "gate.json").write_text(json.dumps({"capability": "silver"}))
    with pytest.raises(ValueError, match="does not match"):
        demo.recorded_gate("silver")


# --------------------------------------------------------------------- the moves ----
def _hits(rows):
    return pd.DataFrame([{"fused_rank": i + 1, "bm25_rank": b, "knn_rank": k,
                          "fused_score": 0.0, "rating": 1, "title": "t", "review_id": "r"}
                         for i, (b, k) in enumerate(rows)])


def test_fusion_story_separates_a_decision_from_an_agreement():
    decided = demo.fusion_story(_hits([(6, 3), (1, None), (None, 1)]))
    assert "neither list puts it first" in decided
    agreed = demo.fusion_story(_hits([(2, 1), (1, None)]))
    assert "fusion agreed rather than decided" in agreed


def _one_question(*, declared: dict, cited_window: str, stored_window: str):
    q = {"question_id": "q1", "family": "temporal", "answerability": "answerable",
         "scope": {"windows": declared}}
    a = {"question_id": "q1", "status": "succeeded",
         "parsed": {"refused": False,
                    "claims": [{"claim": "c", "citations": [{"cite_id": "R1",
                                                             "window": cited_window}]}]},
         "retrieved": [{"cite_id": "R1", "review_id": "x", "window": stored_window,
                        "review_month": "2017-04", "title": "t"}]}
    return [q], [a]


def test_a_citation_outside_the_declared_window_is_shown_as_out_of_scope(monkeypatch):
    windows = {"baseline": {"start": "2016-10", "end": "2017-03"},
               "recent": {"start": "2017-04", "end": "2017-09"}}
    monkeypatch.setattr(demo, "_rag_docs", lambda: _one_question(
        declared=windows, cited_window="recent", stored_window="baseline"))
    assert demo.citations("q1")["in_declared_window"].tolist() == ["false"]
    assert "claim 1 cites" in demo.citation_contract("q1")


def test_a_citation_inside_the_declared_window_resolves(monkeypatch):
    windows = {"recent": {"start": "2017-04", "end": "2017-09"}}
    monkeypatch.setattr(demo, "_rag_docs", lambda: _one_question(
        declared=windows, cited_window="recent", stored_window="recent"))
    assert demo.citations("q1")["in_declared_window"].tolist() == ["true"]
    assert "no violation" in demo.citation_contract("q1")


def test_a_question_outside_the_frozen_thirty_is_refused(monkeypatch):
    monkeypatch.setattr(demo, "_rag_docs", lambda: _one_question(
        declared={"recent": {"start": "2017-04", "end": "2017-09"}},
        cited_window="recent", stored_window="recent"))
    with pytest.raises(KeyError, match="frozen thirty"):
        demo.citations("q2")


def test_clean_question_refuses_rather_than_showing_a_violating_example(monkeypatch):
    windows = {"recent": {"start": "2017-04", "end": "2017-09"}}
    monkeypatch.setattr(demo, "_rag_docs", lambda: _one_question(
        declared=windows, cited_window="recent", stored_window="baseline"))
    with pytest.raises(LookupError, match="satisfies the citation contract"):
        demo.clean_question(family="temporal")


def test_the_top_candidate_is_the_best_ranked_candidate_slot(tmp_path):
    path = tmp_path / "slots.json"
    path.write_text(json.dumps({"slots": [
        {"slot_role": "control", "decline_rank": 1, "parent_asin": "C"},
        {"slot_role": "candidate", "decline_rank": 7, "parent_asin": "B"},
        {"slot_role": "candidate", "decline_rank": 3, "parent_asin": "A"}]}))
    assert demo.top_candidate(slots_path=path)["parent_asin"] == "A"


def test_slots_with_no_candidate_stop_the_move(tmp_path):
    path = tmp_path / "slots.json"
    path.write_text(json.dumps({"slots": [{"slot_role": "control", "decline_rank": 1}]}))
    with pytest.raises(ValueError, match="no candidate slot"):
        demo.top_candidate(slots_path=path)


def test_the_replay_is_the_ledgered_producer_and_resets_only_when_asked():
    demo_cmd = demo.replay_command(scope="full", reset=True)
    assert demo_cmd[:4] == ["./run.sh", "python", "-m", "src.ingest.stream_producer"]
    assert "--reset" in demo_cmd
    assert "--reset" not in demo.replay_command(scope="sample", reset=False)


def test_the_stage_refuses_a_scope_that_is_not_one():
    with pytest.raises(ValueError, match="scope must be"):
        demo.open_stage(scope="everything")


# ---------------------------------------------------------------- the notebook ----
#: What a cell must not do, because a runbook that opens its own connection or writes its own
#: query is the parallel app ADR-0009 refused.
FORBIDDEN = ("SparkSession", "Elasticsearch(", "psycopg", "SELECT ", "spark.read",
             "import pyspark")


def _notebook() -> dict:
    return json.loads(NOTEBOOK.read_text())


def test_the_notebook_has_one_markdown_group_per_move():
    text = "\n".join("".join(c["source"]) for c in _notebook()["cells"]
                     if c["cell_type"] == "markdown")
    for m in demo.MOVES:
        assert f"## Move {m.n} —" in text, f"move {m.n} has no cell group in the notebook"


def test_the_notebook_opens_one_stage_and_closes_it_once():
    code = "\n".join("".join(c["source"]) for c in _notebook()["cells"]
                     if c["cell_type"] == "code")
    assert code.count("demo.open_stage(") == 1
    assert code.count("demo.close_stage(") == 1


def test_every_code_cell_calls_a_serving_helper_and_nothing_else():
    for i, cell in enumerate(_notebook()["cells"]):
        if cell["cell_type"] != "code":
            continue
        src = "".join(cell["source"])
        assert "demo." in src, f"code cell {i} does not call a demo helper"
        for bad in FORBIDDEN:
            assert bad not in src, f"code cell {i} reimplements the pipeline: {bad!r}"


def test_the_notebook_is_committed_with_outputs_stripped():
    for i, cell in enumerate(_notebook()["cells"]):
        if cell["cell_type"] != "code":
            continue
        assert cell["outputs"] == [], f"code cell {i} carries committed output"
        assert cell["execution_count"] is None, f"code cell {i} carries an execution count"

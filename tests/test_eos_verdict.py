"""The gate must fail on runs that tested nothing.

F3: the old script printed a NOTE for `partial == 0` and for `partial >=
records` but computed `ok` from `final == records and dupes == 0` alone, so a
run where the job finished before the kill still printed PASS. A degenerate
run is now a failure, not a footnote.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "prove_exactly_once", Path(__file__).resolve().parents[1] / "scripts" / "prove_exactly_once.py")
_mod = importlib.util.module_from_spec(_spec)
sys.modules["prove_exactly_once"] = _mod
_spec.loader.exec_module(_mod)
failures = _mod.failures


def test_a_real_interrupted_run_passes():
    assert failures(expected=120_000, partial=95_969, final=120_000, dupes=0) == []


def test_nothing_committed_before_the_kill_is_a_failure():
    assert failures(expected=120_000, partial=0, final=120_000, dupes=0) != []


def test_job_finished_before_the_kill_is_a_failure():
    assert failures(expected=120_000, partial=120_000, final=120_000, dupes=0) != []


def test_lost_rows_are_a_failure():
    assert failures(expected=120_000, partial=50_000, final=119_999, dupes=0) != []


def test_duplicates_are_a_failure():
    assert failures(expected=120_000, partial=50_000, final=120_000, dupes=7) != []


@pytest.mark.parametrize("kwargs,needle", [
    ({"expected": 10, "partial": 0, "final": 10, "dupes": 0}, "before anything was committed"),
    ({"expected": 10, "partial": 10, "final": 10, "dupes": 0}, "finished before the kill"),
    ({"expected": 10, "partial": 5, "final": 9, "dupes": 0}, "lost"),
    ({"expected": 10, "partial": 5, "final": 10, "dupes": 3}, "duplicate"),
])
def test_each_failure_says_which_one_it_is(kwargs, needle):
    assert any(needle in r for r in failures(**kwargs))

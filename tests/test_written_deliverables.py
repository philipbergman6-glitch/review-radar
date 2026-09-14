"""The three written deliverables, checked against the repo rather than against prose (ticket 19).

The design doc, the slide deck and the README are the only artefacts a grader reads before
they read code, so the failure mode worth engineering against is a document that drifts
away from the system it describes. Four seams are checked here and nowhere else:

* **the phase table is the evaluation table's spine** -- `docs/DESIGN.md` §4 carries one row
  per capability declared in `conf/lineage_chain.toml`, with that capability's phase, gate
  name and *today's* verdict as `src.common.evaluation.build_rows` computes it. A capability
  added to the chain, a gate renamed, or a verdict that moved all fail here;
* **the talk fits the ceiling** -- the slide table's seconds plus the demo block sum to the
  budget `docs/SLIDES.md` claims, and that claim sits under the brief's 10:00;
* **the wording that was made binding stays binding** -- the four declines (ticket 19,
  RR-18 §4) are checked as literal phrases, because each one is a phrase that stops a
  schedule cut being re-told as a principled one;
* **every prepared Q&A answer carries a number**, which is the whole point of preparing it.

Pure: filesystem reads of committed files, no services.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from src.common import evaluation as E

DESIGN = E.PROJECT_ROOT / "docs" / "DESIGN.md"
SLIDES = E.PROJECT_ROOT / "docs" / "SLIDES.md"
README = E.PROJECT_ROOT / "README.md"


def read(p: Path) -> str:
    if not p.exists():                      # the gate's own failure, restated where it is fixable
        pytest.fail(f"{p.relative_to(E.PROJECT_ROOT)} does not exist -- DEMO_GATE counts it")
    return p.read_text()


def table_rows(text: str, heading: str) -> list[list[str]]:
    """The pipe-table immediately under `heading`, as lists of stripped cells.

    Header and separator rows are dropped; everything else is returned in document order.
    """
    after = text.split(heading, 1)
    if len(after) != 2:
        pytest.fail(f"no heading {heading!r}")
    rows: list[list[str]] = []
    for line in after[1].splitlines():
        s = line.strip()
        if not s.startswith("|"):
            if rows:                        # the table ended
                break
            continue
        cells = [c.strip().strip("`*") for c in s.strip("|").split("|")]
        if all(set(c) <= {"-", ":"} for c in cells):
            continue
        rows.append(cells)
    if len(rows) < 2:
        pytest.fail(f"no table under {heading!r}")
    return rows[1:]                          # drop the header


# --------------------------------------------------- the doc's table is the chain ----
def chain_verdicts() -> dict[str, tuple[str, str, str]]:
    """capability id -> (phase, gate name, verdict) as the evaluation table renders it today."""
    chain = E.load_chain()
    docs = {c.id: (json.loads(p.read_text()) if (p := E.PROJECT_ROOT / c.artifact_path).exists()
                   else None) for c in chain}
    rows, _ = E.build_rows(chain, docs, {})
    by_id = {r.capability: r for r in rows if not r.capability.endswith(" (prior)")}
    return {c.id: (c.phase, c.gate_name, by_id[c.id].verdict) for c in chain}


def test_design_phase_table_covers_every_declared_capability():
    rows = table_rows(read(DESIGN), "## 4. The pipeline, phase by phase")
    assert [r[1] for r in rows] == list(chain_verdicts()), (
        "docs/DESIGN.md §4 must carry one row per capability in conf/lineage_chain.toml, "
        "in chain order")


def test_design_phase_table_states_the_verdict_eval_table_prints():
    expected = chain_verdicts()
    for phase, cap, gate, verdict, *_ in table_rows(read(DESIGN), "## 4. The pipeline, phase by phase"):
        assert (phase, gate, verdict) == expected[cap], f"{cap} disagrees with make eval-table"


# ----------------------------------------------------------------- the talk's clock ----
CEILING_S = 600          # the brief's 10:00
CLAIMED_S = 570          # 9:30 -- 5:00 of slides + 4:40 of demo + 20 s slack (RR-18 §1)


def test_slide_deck_is_nine_entries_and_fits_the_ceiling():
    rows = table_rows(read(SLIDES), "## The running order")
    slides = [r for r in rows if r[0] != "—"]
    assert len(slides) == 9, "a title plus eight slides (RR-18 §1)"
    total = sum(int(r[2]) for r in rows)
    assert total == CLAIMED_S <= CEILING_S, f"the running order sums to {total}s, not {CLAIMED_S}s"


def test_slide_two_ships_both_variants_and_a_fork_that_names_a_number():
    text = read(SLIDES)
    for heading in ("### Slide 2, variant A", "### Slide 2, variant B"):
        assert heading in text, f"{heading} is missing -- both variants are written in advance"
    fork = text.split("### The fork", 1)
    assert len(fork) == 2, "the fork rule must be written down, not remembered"
    assert re.search(r"\d", fork[1].split("###", 1)[0]), "the fork rule must name a printed number"


# ------------------------------------------------------------- the binding wording ----
#: Each phrase is load-bearing: RR-18 §4 makes the wording binding so that a decline taken
#: for schedule reasons cannot later be re-told as a decline taken on the merits.
BINDING = (
    "dropped for time",                                    # Kafka Connect ES sink
    "MinIO is not equivalent",                             # single-node HDFS
    "Apache Sqoop moved into the Attic in June 2021",      # Sqoop and Pig keep the citation
    "an opinion",                                          # Oozie is owned as one
)


@pytest.mark.parametrize("phrase", BINDING)
def test_both_documents_carry_the_binding_decline_wording(phrase):
    for path in (DESIGN, SLIDES):
        assert phrase in read(path), f"{path.name} drops the binding wording {phrase!r}"


# ------------------------------------------------------------------- the Q&A answers ----
def test_five_prepared_answers_each_contain_a_number():
    text = read(SLIDES)
    body = text.split("## Prepared Q&A", 1)
    assert len(body) == 2, "the prepared answers must be in the deck, not in someone's memory"
    answers = re.findall(r"^\*\*A\.\*\*(.+?)(?=^\*\*Q|\Z)", body[1], re.MULTILINE | re.DOTALL)
    assert len(answers) == 5, f"five prepared answers, found {len(answers)}"
    for i, a in enumerate(answers, 1):
        assert re.search(r"\d", a), f"prepared answer {i} contains no number"


# ------------------------------------------------------------------- the two FAILs ----
#: The viva sheet restates two gate headlines in prose. Prose drifts; the artefacts do not.
FAIL_SHEET_FIGURES = (
    ("eval/gold_calibration/gate.json", "0.1857", "0.8"),
    ("eval/themes_quality/gate.json", "0.4583", "0.7"),
)


@pytest.mark.parametrize("artifact,value,threshold", FAIL_SHEET_FIGURES)
def test_fail_sheet_quotes_the_gate_artefacts(artifact, value, threshold):
    doc = json.loads(read(E.PROJECT_ROOT / artifact))
    assert f"{doc['metric']['value']:.4f}".startswith(value), \
        f"{artifact} moved to {doc['metric']['value']} -- docs/QA-FAILS.md still says {value}"
    assert doc["metric"]["threshold"] == float(threshold), \
        f"{artifact} bar moved to {doc['metric']['threshold']} -- bars do not move (ADR-0012)"
    assert doc["status"] == "FAIL", f"{artifact} is no longer a FAIL -- docs/QA-FAILS.md is stale"
    text = read(E.PROJECT_ROOT / "docs" / "QA-FAILS.md")
    assert value in text, f"docs/QA-FAILS.md drops the {artifact} headline {value}"


def test_fail_sheet_never_claims_the_rule_finds_declines():
    """The one sentence ADR-0012 forbids, checked where it would be said out loud."""
    text = read(E.PROJECT_ROOT / "docs" / "QA-FAILS.md")
    assert "Does not survive:" in text, "the forbidden claim must be named, not just avoided"
    assert "floor, not a census" in text


# ------------------------------------------------------------------------ the README ----
def test_readme_only_names_make_targets_that_exist():
    targets = set(re.findall(r"^([a-zA-Z0-9_.-]+):", (E.PROJECT_ROOT / "Makefile").read_text(), re.MULTILINE))
    named = set(re.findall(r"`make ([a-z0-9-]+)", read(README)))
    assert named <= targets, f"README names make targets that do not exist: {sorted(named - targets)}"

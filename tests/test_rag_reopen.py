"""The sanctioned reopen of the thirty (ADR-0006, RR-24).

Two seams, no Elasticsearch and no Ollama:

* **`check_reopen`** -- the only movement a reopen may carry is the one that could not have
  been chosen on the held-out answers: a validator limit. Prompt, model, seed, schema,
  retrieval and the questions must be exactly what the seal recorded, and the seal's config
  hash must be reproducible from today's spec with the sealed commit's limits. A reopen with
  nothing moved is refused too: a same-identity run needs no reopen.
* **`archive_prior`** -- seal 1, its answers and its gate artefact are archived beside the
  new ones, numbered, never overwritten. Run 1 stays the first result on record.
"""
from __future__ import annotations

import json

import pytest

from src.ai import rag_run

IDENTITY = {"model_id": "qwen3:8b", "answer_spec_version": "1", "prompt_version": "rag-v5",
            "inference_config_hash": "5ac248c8" * 8, "question_spec_hash": "fdaf8546" * 8,
            "retrieval_hash": "00e4c444" * 8, "generation": "reviews_s85_d2e5"}
SEAL = {"opened_at": "2026-09-14T01:24:06+00:00", "git_commit_sha": "5e8358b7" * 5,
        "run_id": "ffbbe884-5425-4ce8-9ecd-90f5a6201106", "questions": 30, "identity": IDENTITY}
TODAY = IDENTITY | {"inference_config_hash": "aaaaaaaa" * 8}
REASON = "subject_max_words rejected two answers unread; it stops being a rejection reason"


def test_a_reopen_that_moved_only_the_limits_is_allowed():
    c = rag_run.check_reopen(SEAL, reason=REASON, identity=TODAY,
                             sealed_hash_recomputed=IDENTITY["inference_config_hash"])
    assert c.prior_run_id == SEAL["run_id"] and c.seal_no == 1
    assert c.moved == ("inference_config_hash",) and c.reason == REASON


@pytest.mark.parametrize("reason", ["", "   "])
def test_a_reopen_without_a_written_reason_is_refused(reason):
    with pytest.raises(ValueError, match="reason"):
        rag_run.check_reopen(SEAL, reason=reason, identity=TODAY,
                             sealed_hash_recomputed=IDENTITY["inference_config_hash"])


def test_a_reopen_with_no_seal_is_refused():
    with pytest.raises(ValueError, match="nothing to reopen"):
        rag_run.check_reopen(None, reason=REASON, identity=TODAY,
                             sealed_hash_recomputed=IDENTITY["inference_config_hash"])


@pytest.mark.parametrize("field", ["prompt_version", "model_id", "retrieval_hash",
                                   "question_spec_hash", "generation"])
def test_a_reopen_that_moved_the_prompt_model_retrieval_or_questions_is_refused(field):
    with pytest.raises(ValueError, match=field):
        rag_run.check_reopen(SEAL, reason=REASON, identity=TODAY | {field: "moved"},
                             sealed_hash_recomputed=IDENTITY["inference_config_hash"])


def test_a_reopen_whose_hash_moved_for_more_than_the_limits_is_refused():
    """The seed, the prompt text and the schema live inside the config hash. The check is that
    the sealed hash comes back from today's spec with the sealed limits -- if it does not,
    something other than a limit moved, and that is tuning."""
    with pytest.raises(ValueError, match="limits"):
        rag_run.check_reopen(SEAL, reason=REASON, identity=TODAY,
                             sealed_hash_recomputed="bbbbbbbb" * 8)


def test_a_reopen_under_an_unchanged_identity_is_refused():
    with pytest.raises(ValueError, match="unchanged"):
        rag_run.check_reopen(SEAL, reason=REASON, identity=IDENTITY,
                             sealed_hash_recomputed=IDENTITY["inference_config_hash"])


def test_a_second_reopen_numbers_its_archive_after_the_first():
    c = rag_run.check_reopen(SEAL | {"seal_no": 2}, reason=REASON, identity=TODAY,
                             sealed_hash_recomputed=IDENTITY["inference_config_hash"])
    assert c.seal_no == 2


# ---------------------------------------------------------------------- the archive ----
def test_the_prior_run_is_archived_beside_the_new_one(tmp_path):
    for name in ("answers.json", "answer-seal.json", "gate.json"):
        (tmp_path / name).write_text(json.dumps({"file": name}))
    archived = rag_run.archive_prior(tmp_path, seal_no=1)
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "answer-seal.1.json", "answers.1.json", "gate.1.json"]
    assert json.loads((tmp_path / "answers.1.json").read_text()) == {"file": "answers.json"}
    assert archived == {"prior_seal": "answer-seal.1.json", "prior_answers": "answers.1.json",
                        "prior_gate": "gate.1.json"}


def test_an_archive_never_overwrites_an_earlier_archive(tmp_path):
    for name in ("answers.json", "answer-seal.json", "answers.1.json"):
        (tmp_path / name).write_text("{}")
    with pytest.raises(FileExistsError):
        rag_run.archive_prior(tmp_path, seal_no=1)
    assert (tmp_path / "answers.json").exists()


def test_an_archive_without_a_prior_run_is_refused(tmp_path):
    (tmp_path / "answer-seal.json").write_text("{}")
    with pytest.raises(FileNotFoundError):
        rag_run.archive_prior(tmp_path, seal_no=1)

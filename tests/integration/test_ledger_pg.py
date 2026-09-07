"""Run-ledger writes against the real PostgreSQL from the compose stack (ADR-0008 §3).

Skipped when Postgres is unreachable. Uses job `produce` rows tagged with a test category
and deletes them afterwards, so the real ledger is never touched.
"""
from __future__ import annotations

import uuid

import pytest

from src.common import runs

TEST_CATEGORY = "pytest-ledger"


@pytest.fixture(scope="module")
def pg():
    try:
        from src.common.pg import connect
        with connect() as conn:
            conn.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"PostgreSQL unreachable: {exc}")
    yield connect
    with connect() as conn:
        conn.execute("DELETE FROM pipeline_runs WHERE category = %s", (TEST_CATEGORY,))


def _start(**over):
    kw = {"category": TEST_CATEGORY, "data_scope": "sample",
          "inputs": {"source": {"path": "x.jsonl", "sha256": "0" * 64, "bytes": 1}},
          "params": {"rate": 0}}
    kw.update(over)
    return runs.start("produce", runs.PRODUCE_SPEC_VERSION, **kw)


def test_start_inserts_running_row_with_uuid_and_git_state(pg):
    run = _start()
    uuid.UUID(run.run_id)
    with pg() as conn:
        row = conn.execute("SELECT status, job_name, data_scope, git_commit_sha, worktree_dirty, "
                           "cleanliness_policy_version, inputs FROM pipeline_runs WHERE run_id=%s",
                           (run.run_id,)).fetchone()
    assert row[0] == "running" and row[1] == "produce" and row[2] == "sample"
    assert row[5] == runs.CLEANLINESS_POLICY_VERSION
    assert row[6]["source"]["sha256"] == "0" * 64


def test_success_finalizes_only_when_contract_holds(pg):
    run = _start()
    runs.success(run, records_in=10, records_out=10, records_rejected=0,
                 outputs={"kafka": {"topic": "reviews.test"}},
                 counts={"records_attempted": 10, "records_acked": 10})
    with pg() as conn:
        status, fin, out = conn.execute("SELECT status, finished_at, records_out FROM pipeline_runs "
                                        "WHERE run_id=%s", (run.run_id,)).fetchone()
    assert status == "success" and fin is not None and out == 10


def test_contract_violation_marks_failed_and_raises(pg):
    run = _start()
    with pytest.raises(runs.ContractViolation):
        runs.success(run, records_in=10, records_out=9, records_rejected=0,
                     outputs={"kafka": {"topic": "reviews.test"}},
                     counts={"records_attempted": 10, "records_acked": 10})
    with pg() as conn:
        status, notes = conn.execute("SELECT status, notes FROM pipeline_runs WHERE run_id=%s",
                                     (run.run_id,)).fetchone()
    assert status == "failed" and "records_out 9 != records_acked 10" in notes


def test_failed_keeps_partial_outputs(pg):
    run = _start()
    runs.failed(run, notes="boom", outputs={"kafka": {"topic": "partial"}})
    with pg() as conn:
        status, outputs = conn.execute("SELECT status, outputs FROM pipeline_runs WHERE run_id=%s",
                                       (run.run_id,)).fetchone()
    assert status == "failed" and outputs == {"kafka": {"topic": "partial"}}


def test_invalid_scope_or_job_never_reaches_the_database(pg):
    with pytest.raises(ValueError):
        _start(data_scope="everything")
    with pytest.raises(runs.ContractViolation):
        runs.start("gold_typo", "1", category=TEST_CATEGORY, data_scope="full", inputs={}, params={})


def test_schema_check_rejects_unknown_job_name(pg):
    import psycopg
    with pytest.raises(psycopg.errors.CheckViolation), pg() as conn:
        conn.execute("INSERT INTO pipeline_runs (run_id, job_name, spec_version, status, category, "
                     "data_scope, cleanliness_policy_version) VALUES (%s, 'nope', '1', 'running', %s, "
                     "'full', '1')", (str(uuid.uuid4()), TEST_CATEGORY))


def test_fresh_init_and_migrated_schema_agree(pg):
    """01_schema.sql applied to a scratch schema must equal the live (migrated) tables."""
    from src.common.config import PROJECT_ROOT
    sql = (PROJECT_ROOT / "conf" / "postgres-init" / "01_schema.sql").read_text()
    q = """SELECT table_name, column_name, data_type, is_nullable, column_default
           FROM information_schema.columns WHERE table_schema = %s
             AND table_name IN ('products', 'pipeline_runs', 'schema_migrations')
           ORDER BY table_name, column_name"""
    with pg() as conn:
        conn.execute("DROP SCHEMA IF EXISTS fresh_check CASCADE")
        conn.execute("CREATE SCHEMA fresh_check")
        conn.execute("SET LOCAL search_path TO fresh_check")
        conn.execute(sql)
        fresh = conn.execute(q, ("fresh_check",)).fetchall()
        conn.execute("SET LOCAL search_path TO public")
        live = conn.execute(q, ("public",)).fetchall()
        checks_q = """SELECT c.conrelid::regclass::text, c.conname, pg_get_constraintdef(c.oid)
                      FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace
                      WHERE n.nspname = %s
                        AND c.conrelid::regclass::text ~ '(products|pipeline_runs|schema_migrations)$'
                      ORDER BY 1, 3"""
        fresh_checks = [(t.split(".")[-1], d) for t, _, d in conn.execute(checks_q, ("fresh_check",)).fetchall()]
        live_checks = [(t.split(".")[-1], d) for t, _, d in conn.execute(checks_q, ("public",)).fetchall()]
        conn.execute("DROP SCHEMA fresh_check CASCADE")
    assert fresh == live
    assert sorted(fresh_checks) == sorted(live_checks)

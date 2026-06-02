"""Tests for the orchestrator's self-healing retry behaviour."""
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


def _make_db(tmp_path):
    db = tmp_path / "t.db"
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE documents (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, url TEXT, doc_type TEXT, header TEXT,
            announcement_date TEXT,
            parse_status TEXT NOT NULL DEFAULT 'pending',
            parse_error TEXT,
            failure_class TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT
        );
    """)
    conn.commit()
    return db, conn


@pytest.fixture
def db_conn(tmp_path):
    db, conn = _make_db(tmp_path)

    def _get():
        c = sqlite3.connect(str(db))
        c.row_factory = sqlite3.Row
        return c

    with patch("pipeline.orchestrator.get_connection", _get):
        yield conn


def _insert(conn, **kw):
    cols = ", ".join(kw)
    qs = ", ".join("?" * len(kw))
    cur = conn.execute(f"INSERT INTO documents ({cols}) VALUES ({qs})", tuple(kw.values()))
    conn.commit()
    return cur.lastrowid


def test_transient_failure_schedules_retry(db_conn):
    from pipeline.orchestrator import _record_failure
    doc_id = _insert(db_conn, ticker="AAA", doc_type="study_dfs", parse_status="classified")
    _record_failure(doc_id, "llm_api_error:429 RESOURCE_EXHAUSTED")
    row = db_conn.execute("SELECT * FROM documents WHERE document_id=?", (doc_id,)).fetchone()
    assert row["parse_status"] == "retry_scheduled"
    assert row["failure_class"] == "transient"
    assert row["retry_count"] == 1
    assert row["next_retry_at"] > datetime.now(timezone.utc).isoformat()


def test_permanent_failure_is_terminal(db_conn):
    from pipeline.orchestrator import _record_failure
    doc_id = _insert(db_conn, ticker="BBB", doc_type="study_pfs", parse_status="classified")
    _record_failure(doc_id, "minimum_data_missing:requires_npv_and_initial_capex")
    row = db_conn.execute("SELECT * FROM documents WHERE document_id=?", (doc_id,)).fetchone()
    assert row["parse_status"] == "failed"
    assert row["failure_class"] == "permanent"
    assert row["retry_count"] == 0


def test_transient_exhaustion_is_terminal(db_conn):
    from pipeline.orchestrator import _record_failure
    doc_id = _insert(db_conn, ticker="CCC", doc_type="study_dfs",
                     parse_status="retry_scheduled", retry_count=5)
    _record_failure(doc_id, "llm_api_error:429 RESOURCE_EXHAUSTED")
    row = db_conn.execute("SELECT * FROM documents WHERE document_id=?", (doc_id,)).fetchone()
    assert row["parse_status"] == "failed"
    assert row["failure_class"] == "transient"
    assert row["parse_error"].endswith(":retries_exhausted")


def test_select_extractable_includes_classified_and_due_retries(db_conn):
    from pipeline.orchestrator import _select_extractable
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    _insert(db_conn, ticker="C1", doc_type="study_dfs", parse_status="classified",
            announcement_date="2024-01-01")
    _insert(db_conn, ticker="C2", doc_type="study_dfs", parse_status="retry_scheduled",
            next_retry_at=past, announcement_date="2024-01-02")
    _insert(db_conn, ticker="C3", doc_type="study_dfs", parse_status="retry_scheduled",
            next_retry_at=future, announcement_date="2024-01-03")
    _insert(db_conn, ticker="C4", doc_type="study_dfs", parse_status="parsed",
            announcement_date="2024-01-04")
    now = datetime.now(timezone.utc).isoformat()
    tickers = {r["ticker"] for r in _select_extractable(db_conn, now)}
    assert tickers == {"C1", "C2"}


def test_transient_retry_count_increments_mid_sequence(db_conn):
    from pipeline.orchestrator import _record_failure
    doc_id = _insert(db_conn, ticker="DDD", doc_type="study_dfs",
                     parse_status="retry_scheduled", retry_count=2)
    _record_failure(doc_id, "llm_api_error:503 overloaded")
    row = db_conn.execute("SELECT * FROM documents WHERE document_id=?", (doc_id,)).fetchone()
    assert row["parse_status"] == "retry_scheduled"
    assert row["retry_count"] == 3
    assert row["failure_class"] == "transient"


def test_run_orchestrator_invokes_stage_backfill():
    from unittest.mock import patch
    import pipeline.orchestrator as orch
    with patch.object(orch, "classify_pending", return_value=0), \
         patch.object(orch, "extract_classified", return_value={"extracted": 0}), \
         patch("scripts.backfill_project_stages.run_backfill",
               return_value={"classified": 2}) as mock_bf:
        stats = orch.run_orchestrator()
    mock_bf.assert_called_once()
    assert stats["stage_backfill"] == {"classified": 2}


def test_run_orchestrator_survives_backfill_failure():
    from unittest.mock import patch
    import pipeline.orchestrator as orch
    with patch.object(orch, "classify_pending", return_value=0), \
         patch.object(orch, "extract_classified", return_value={"extracted": 0}), \
         patch("scripts.backfill_project_stages.run_backfill",
               side_effect=RuntimeError("gemini down")):
        stats = orch.run_orchestrator()  # must not raise
    assert stats["stage_backfill"] is None

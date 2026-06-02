"""Tests for api/health.py — GET /api/health/ingest."""
import sqlite3
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest


@pytest.fixture
def client(tmp_path):
    db = tmp_path / "h.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE documents (
            document_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, doc_type TEXT,
            parse_status TEXT NOT NULL DEFAULT 'pending',
            parse_error TEXT, failure_class TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0, next_retry_at TEXT
        );
    """)
    past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    rows = [
        ("AAA", "study_dfs", "parsed", None, None, None),
        ("BBB", "study_dfs", "retry_scheduled",
         "llm_api_error:429 RESOURCE_EXHAUSTED", "transient", past),
        ("CCC", "study_pfs", "failed",
         "minimum_data_missing:requires_npv_and_initial_capex", "permanent", None),
    ]
    for tk, dt, st, err, fc, nra in rows:
        conn.execute(
            "INSERT INTO documents (ticker, doc_type, parse_status, parse_error, failure_class, next_retry_at) VALUES (?,?,?,?,?,?)",
            (tk, dt, st, err, fc, nra),
        )
    conn.commit(); conn.close()

    def _get():
        c = sqlite3.connect(str(db)); c.row_factory = sqlite3.Row; return c

    with patch("api.health.get_connection", _get):
        from api.health import bp
        from flask import Flask
        app = Flask(__name__); app.register_blueprint(bp); app.config["TESTING"] = True
        with app.test_client() as c:
            yield c


def test_health_shape_and_gap(client):
    data = client.get("/api/health/ingest").get_json()
    assert data["study_coverage"]["companies_with_study_doc"] == 3
    assert data["study_coverage"]["companies_with_parsed_study"] == 1
    assert data["study_coverage"]["recoverable_gap"] == 2
    assert data["failures_by_class"]["permanent"] == 1
    assert data["retry_queue"]["scheduled"] == 1
    assert data["retry_queue"]["due_now"] == 1
    assert any(b["count"] >= 1 for b in data["error_buckets"])

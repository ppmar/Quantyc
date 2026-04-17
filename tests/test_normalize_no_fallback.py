"""
Test that normalize_from_5b refuses to create a company_financials row
when the staging row has no effective_date.
"""

import json
from datetime import datetime, timezone

import pytest

from db import get_connection, init_db
from pipeline.normalize.company_financials import normalize_from_5b


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    """Point DB at a temp file and initialize schema."""
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("db.DB_PATH", db_path)
    init_db()
    yield


@pytest.fixture
def seed_doc_and_stg_no_date():
    """Insert a document and a staging row with effective_date = NULL."""
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        "INSERT INTO companies (ticker, first_seen_at, last_updated_at) VALUES (?, ?, ?)",
        ("TST", now, now),
    )
    cursor = conn.execute(
        """INSERT INTO documents
           (ticker, url, sha256, source, announcement_date, ingested_at,
            doc_type, header, parse_status, local_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'classified', '')""",
        ("TST", "https://example.com/test.pdf", "abc123hash", "asx_api",
         "2025-10-31", now, "appendix_5b", "Appendix 5B"),
    )
    doc_id = cursor.lastrowid

    conn.execute(
        """INSERT INTO _stg_appendix_5b
           (document_id, effective_date, cash, debt,
            quarterly_opex_burn, quarterly_invest_burn, raw_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (doc_id, None, 5000000, None, 200000, 100000, json.dumps({}), now),
    )
    conn.commit()
    conn.close()
    return doc_id


def test_normalize_refuses_when_stg_effective_date_is_null(seed_doc_and_stg_no_date):
    """If the staging row has no effective_date, normalize must NOT
    fall back to announcement_date — it must refuse and mark the doc failed."""
    doc_id = seed_doc_and_stg_no_date
    result = normalize_from_5b(doc_id)
    assert result is False

    conn = get_connection()
    cf_row = conn.execute(
        "SELECT COUNT(*) AS n FROM company_financials WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    doc_row = conn.execute(
        "SELECT parse_status, parse_error FROM documents WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()

    assert cf_row["n"] == 0
    assert doc_row["parse_status"] == "failed"
    assert "missing_effective_date" in doc_row["parse_error"]

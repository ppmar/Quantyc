"""Study dedup must also work when post_tax_npv is NULL.

The dedup query used `post_tax_npv = ?`; NULL never equals NULL in SQL, so a
re-ingested npv-less study (e.g. early Scoping with only capex) duplicated on
every reprocess.
"""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from db import get_connection, init_db
from parsers.dfs_study_schemas import StudyExtraction
from pipeline.orchestrator import _persist_study


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("db.DB_PATH", db_path)
    init_db()
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO companies (ticker, first_seen_at, last_updated_at) VALUES ('TST', ?, ?)",
        (now, now),
    )
    conn.execute(
        """INSERT INTO documents (document_id, ticker, url, sha256, source,
           announcement_date, ingested_at, doc_type, header, parse_status, local_path)
           VALUES (901, 'TST', 'https://x/d.pdf', 'h901', 'asx_api',
                   '2026-05-01', ?, 'study_pfs', 'Project PFS', 'classified', '')""",
        (now,),
    )
    conn.commit()
    conn.close()
    yield


def _result(npv) -> StudyExtraction:
    return StudyExtraction(
        project_name="Testaroo",
        study_type="PFS",
        primary_commodity="Au",
        reporting_currency="AUD",
        discount_rate_pct=Decimal("8.0"),
        post_tax_npv_millions=npv,
        initial_capex_millions=Decimal("100"),
    )


def _study_count() -> int:
    conn = get_connection()
    n = conn.execute("SELECT COUNT(*) FROM studies").fetchone()[0]
    conn.close()
    return n


def test_null_npv_study_not_duplicated():
    _persist_study(901, "TST", _result(None), "test-model")
    _persist_study(901, "TST", _result(None), "test-model")
    assert _study_count() == 1


def test_same_npv_still_deduped():
    _persist_study(901, "TST", _result(Decimal("450")), "test-model")
    _persist_study(901, "TST", _result(Decimal("450")), "test-model")
    assert _study_count() == 1

"""study_date falls back to announcement_date when the study has no 'as at'.

A NULL study_date loses every latest-study ordering race and shows no vintage.
The announcement date is an honest upper bound; the fallback is recorded in
extraction_warnings so provenance is explicit.
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
           VALUES (902, 'TST', 'https://x/d.pdf', 'h902', 'asx_api',
                   '2026-05-01', ?, 'study_pfs', 'Project PFS', 'classified', '')""",
        (now,),
    )
    conn.commit()
    conn.close()
    yield


def _result(effective_date=None) -> StudyExtraction:
    return StudyExtraction(
        project_name="Testaroo",
        study_type="PFS",
        primary_commodity="Au",
        reporting_currency="AUD",
        discount_rate_pct=Decimal("8.0"),
        post_tax_npv_millions=Decimal("450"),
        initial_capex_millions=Decimal("100"),
        effective_date=effective_date,
    )


def test_null_effective_date_falls_back_to_announcement():
    study_id = _persist_study(902, "TST", _result(None), "test-model")
    conn = get_connection()
    row = conn.execute(
        "SELECT study_date, extraction_warnings FROM studies WHERE study_id = ?",
        (study_id,),
    ).fetchone()
    conn.close()
    assert row["study_date"] == "2026-05-01"
    assert "study_date_from_announcement_date" in (row["extraction_warnings"] or "")

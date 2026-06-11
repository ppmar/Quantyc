"""Convertible notes make the naive FD count unreliable.

shares_fd_naive adds the CN *face count* 1:1, but one note can convert into
hundreds of underlying shares — FD can be off by orders of magnitude. Until
conversion terms are extracted, a snapshot with CNs present must be flagged
for review with an explicit reason.
"""
import json
from datetime import date, datetime, timezone

import pytest

from db import get_connection, init_db
from parsers.appendix_2a_schemas import Appendix2ACapitalStructure, QuotedClass, UnquotedInstrument
from pipeline.normalize.company_financials import normalize_from_2a


@pytest.fixture(autouse=True)
def fresh_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setattr("db.DB_PATH", db_path)
    init_db()
    yield


def _seed_doc() -> int:
    conn = get_connection()
    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """INSERT INTO documents
           (ticker, url, sha256, source, announcement_date, ingested_at,
            doc_type, header, parse_status, local_path)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'classified', '')""",
        ("TST", "https://example.com/2a.pdf", "hash2a", "asx_api",
         "2026-05-01", now, "appendix_2a", "Appendix 2A"),
    )
    doc_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return doc_id


def _capital_structure(cn_count: int) -> Appendix2ACapitalStructure:
    unquoted = []
    if cn_count:
        unquoted.append(UnquotedInstrument(
            asx_code="TSTCN", description="CONVERTIBLE NOTES",
            instrument_type="convertible_note", total_on_issue=cn_count,
            expiry_date=None, strike_aud=None, raw_line="TSTCN : CONVERTIBLE NOTES",
        ))
    return Appendix2ACapitalStructure(
        ticker="TST", doc_id="1", snapshot_date=date(2026, 5, 1),
        parsed_at=datetime.now(timezone.utc), parser_version="t",
        quoted_classes=[QuotedClass("TST", "ORDINARY FULLY PAID", 100_000_000)],
        unquoted_instruments=unquoted,
        shares_basic=100_000_000,
        shares_fd_naive=100_000_000 + cn_count,
        options_outstanding=0,
        convertible_notes_face_count=cn_count,
        performance_rights_count=0,
    )


def test_cn_present_flags_fd_unreliable():
    doc_id = _seed_doc()
    assert normalize_from_2a(doc_id, _capital_structure(cn_count=50_000))
    conn = get_connection()
    row = conn.execute(
        "SELECT needs_review, review_reason FROM company_financials WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()
    assert row["needs_review"] == 1
    assert "cn_present_fd_unreliable" in (row["review_reason"] or "")


def test_no_cn_no_flag():
    doc_id = _seed_doc()
    assert normalize_from_2a(doc_id, _capital_structure(cn_count=0))
    conn = get_connection()
    row = conn.execute(
        "SELECT needs_review, review_reason FROM company_financials WHERE document_id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()
    assert row["needs_review"] == 0

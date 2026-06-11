"""_check_review_flags must actually set needs_review when reasons exist.

It previously computed missing-field and >50%-deviation reasons, then
hardwired `return False` — the verification layer existed on paper only.
"""
import sqlite3
import pytest

from pipeline.normalize.company_financials import _check_review_flags


@pytest.fixture
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    c.executescript("""
        CREATE TABLE company_financials (
            financial_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER, effective_date TEXT,
            cash REAL, shares_fd REAL, quarterly_opex_burn REAL
        );
    """)
    return c


def test_clean_inputs_no_flag(conn):
    needs_review, reason = _check_review_flags(
        conn, 1, cash=5e6, shares_fd=100e6, opex_burn=2e6
    )
    assert needs_review is False
    assert reason is None


def test_missing_fields_flagged(conn):
    needs_review, reason = _check_review_flags(
        conn, 1, cash=None, shares_fd=None, opex_burn=None
    )
    assert needs_review is True
    assert "missing_cash" in reason
    assert "missing_shares_fd" in reason
    assert "missing_opex_burn" in reason


def test_50pct_deviation_flagged(conn):
    conn.execute(
        "INSERT INTO company_financials (company_id, effective_date, cash, shares_fd, quarterly_opex_burn)"
        " VALUES (1, '2026-03-31', 10e6, 100e6, 2e6)"
    )
    conn.commit()
    needs_review, reason = _check_review_flags(
        conn, 1, cash=2e6, shares_fd=100e6, opex_burn=2e6
    )
    assert needs_review is True
    assert "cash_50pct_deviation" in reason

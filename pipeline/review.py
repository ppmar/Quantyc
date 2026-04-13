"""
Review Queue Helpers

Flagging rules and query utilities for the review system.
"""

import logging

from db import get_connection

logger = logging.getLogger(__name__)


def get_flagged_financials(limit: int = 50) -> list[dict]:
    """Get company_financials rows flagged for review."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT cf.financial_id, cf.effective_date, cf.announcement_date,
                  cf.shares_basic, cf.shares_fd, cf.options_outstanding,
                  cf.cash, cf.debt, cf.quarterly_opex_burn, cf.quarterly_invest_burn,
                  cf.extraction_method, cf.confidence, cf.review_reason,
                  c.ticker, d.url, d.header
           FROM company_financials cf
           JOIN companies c ON cf.company_id = c.company_id
           JOIN documents d ON cf.document_id = d.document_id
           WHERE cf.needs_review = 1
           ORDER BY cf.created_at DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_latest_financials(ticker: str) -> dict | None:
    """Get the most recent company_financials snapshot for a ticker."""
    conn = get_connection()
    row = conn.execute(
        """SELECT cf.*, c.ticker
           FROM company_financials cf
           JOIN companies c ON cf.company_id = c.company_id
           WHERE c.ticker = ?
           ORDER BY cf.effective_date DESC
           LIMIT 1""",
        (ticker.upper(),),
    ).fetchone()
    conn.close()
    return dict(row) if row else None

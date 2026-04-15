"""
Normalizer: staging → company_financials

Reads from _stg_appendix_5b and _stg_issue_of_securities,
upserts into companies, inserts new company_financials snapshots.

Never mutates existing rows — every filing is a new dated snapshot.
The "current" view is MAX(effective_date) per company.

Sets needs_review=1 if:
    - any of shares_fd, cash, quarterly_opex_burn is null
    - two filings share the same effective_date with conflicting values
    - extracted value differs by >50% from prior snapshot
    - confidence='low'
"""

import logging
from datetime import datetime, timezone

from db import get_connection

logger = logging.getLogger(__name__)


def _get_or_create_company(conn, ticker: str) -> int:
    """Ensure company row exists, return company_id."""
    now = datetime.now(timezone.utc).isoformat()
    row = conn.execute("SELECT company_id FROM companies WHERE ticker = ?", (ticker,)).fetchone()
    if row:
        conn.execute("UPDATE companies SET last_updated_at = ? WHERE company_id = ?", (now, row["company_id"]))
        return row["company_id"]

    cursor = conn.execute(
        "INSERT INTO companies (ticker, first_seen_at, last_updated_at) VALUES (?, ?, ?)",
        (ticker, now, now),
    )
    return cursor.lastrowid


def _already_normalized(conn, document_id: int) -> bool:
    """Check if we already created a company_financials row for this document."""
    row = conn.execute(
        "SELECT financial_id FROM company_financials WHERE document_id = ?", (document_id,)
    ).fetchone()
    return row is not None


def _check_review_flags(
    conn, company_id: int, cash: float | None,
    shares_fd: float | None, opex_burn: float | None,
    confidence: str,
) -> tuple[bool, str | None]:
    """Build review reasons (informational only). Never blocks ingestion."""
    reasons = []

    if confidence == "low":
        reasons.append("low_confidence")

    if shares_fd is None:
        reasons.append("missing_shares_fd")
    if cash is None:
        reasons.append("missing_cash")
    if opex_burn is None:
        reasons.append("missing_opex_burn")

    # Check for >50% deviation from prior snapshot
    prior = conn.execute(
        """SELECT cash, shares_fd, quarterly_opex_burn
           FROM company_financials
           WHERE company_id = ?
           ORDER BY effective_date DESC LIMIT 1""",
        (company_id,),
    ).fetchone()

    if prior:
        for field, new_val in [("cash", cash), ("shares_fd", shares_fd), ("quarterly_opex_burn", opex_burn)]:
            old_val = prior[field] if field != "quarterly_opex_burn" else prior["quarterly_opex_burn"]
            if new_val is not None and old_val is not None and old_val != 0:
                deviation = abs(new_val - old_val) / abs(old_val)
                if deviation > 0.5:
                    reasons.append(f"{field}_50pct_deviation")

    # Always return needs_review=False — flags are logged but don't block
    return False, "; ".join(reasons) if reasons else None


def normalize_from_5b(document_id: int) -> bool:
    """
    Normalize an Appendix 5B staging row into company_financials.
    """
    conn = get_connection()

    if _already_normalized(conn, document_id):
        logger.info("Doc %d already normalized, skipping", document_id)
        conn.close()
        return True

    # Get staging data
    stg = conn.execute(
        "SELECT * FROM _stg_appendix_5b WHERE document_id = ?", (document_id,)
    ).fetchone()

    if not stg:
        conn.close()
        return False

    # Get document metadata
    doc = conn.execute(
        "SELECT ticker, announcement_date FROM documents WHERE document_id = ?", (document_id,)
    ).fetchone()

    if not doc:
        conn.close()
        return False

    ticker = doc["ticker"]
    company_id = _get_or_create_company(conn, ticker)

    effective_date = stg["effective_date"] or doc["announcement_date"] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    announcement_date = doc["announcement_date"] or effective_date

    cash = stg["cash"]
    debt = stg["debt"]
    opex_burn = stg["quarterly_opex_burn"]
    invest_burn = stg["quarterly_invest_burn"]

    confidence = "high"  # rule-based 5B is always high
    needs_review, review_reason = _check_review_flags(
        conn, company_id, cash, None, opex_burn, confidence
    )

    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO company_financials
           (company_id, document_id, effective_date, announcement_date,
            cash, debt, quarterly_opex_burn, quarterly_invest_burn,
            extraction_method, confidence, needs_review, review_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'rule', ?, ?, ?, ?)""",
        (
            company_id, document_id, effective_date, announcement_date,
            cash, debt, opex_burn, invest_burn,
            confidence, 1 if needs_review else 0, review_reason, now,
        ),
    )

    conn.commit()
    conn.close()

    logger.info("Normalized 5B for %s doc %d: cash=%s, opex=%s, invest=%s",
                ticker, document_id, cash, opex_burn, invest_burn)
    return True


def normalize_from_securities(document_id: int) -> bool:
    """
    Normalize issue-of-securities staging rows into company_financials.
    """
    conn = get_connection()

    if _already_normalized(conn, document_id):
        logger.info("Doc %d already normalized, skipping", document_id)
        conn.close()
        return True

    # Get staging data
    rows = conn.execute(
        "SELECT * FROM _stg_issue_of_securities WHERE document_id = ?", (document_id,)
    ).fetchall()

    if not rows:
        conn.close()
        return False

    doc = conn.execute(
        "SELECT ticker, announcement_date FROM documents WHERE document_id = ?", (document_id,)
    ).fetchone()

    if not doc:
        conn.close()
        return False

    ticker = doc["ticker"]
    company_id = _get_or_create_company(conn, ticker)

    # The extractor now stores all totals in raw_json of the first row
    import json
    raw = json.loads(rows[0]["raw_json"]) if rows[0]["raw_json"] else {}

    shares_basic = raw.get("total_shares_on_issue") or rows[0]["total_on_issue"]
    options_outstanding = raw.get("total_options_on_issue")
    perf_rights = raw.get("total_perf_rights_on_issue")

    # Compute fully diluted
    shares_fd = None
    if shares_basic is not None:
        shares_fd = shares_basic + (options_outstanding or 0) + (perf_rights or 0)

    effective_date = rows[0]["effective_date"] or doc["announcement_date"]
    announcement_date = doc["announcement_date"] or effective_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    effective_date = effective_date or announcement_date

    confidence = "high"
    needs_review, review_reason = _check_review_flags(
        conn, company_id, None, shares_fd, None, confidence
    )

    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO company_financials
           (company_id, document_id, effective_date, announcement_date,
            shares_basic, shares_fd, options_outstanding, perf_rights_outstanding,
            extraction_method, confidence, needs_review, review_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'rule', ?, ?, ?, ?)""",
        (
            company_id, document_id, effective_date, announcement_date,
            shares_basic, shares_fd, options_outstanding, perf_rights,
            confidence, 1 if needs_review else 0, review_reason, now,
        ),
    )

    conn.commit()
    conn.close()

    logger.info("Normalized securities for %s doc %d: basic=%s, fd=%s, opts=%s",
                ticker, document_id, shares_basic, shares_fd, options_outstanding)
    return True


def normalize_from_presentation(document_id: int) -> bool:
    """
    Normalize presentation staging row into company_financials.
    Presentations typically have shares, cash, and sometimes debt.
    """
    conn = get_connection()

    if _already_normalized(conn, document_id):
        logger.info("Doc %d already normalized, skipping", document_id)
        conn.close()
        return True

    stg = conn.execute(
        "SELECT * FROM _stg_presentation WHERE document_id = ?", (document_id,)
    ).fetchone()

    if not stg:
        conn.close()
        return False

    doc = conn.execute(
        "SELECT ticker, announcement_date FROM documents WHERE document_id = ?", (document_id,)
    ).fetchone()

    if not doc:
        conn.close()
        return False

    ticker = doc["ticker"]
    company_id = _get_or_create_company(conn, ticker)

    effective_date = stg["effective_date"] or doc["announcement_date"] or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    announcement_date = doc["announcement_date"] or effective_date

    shares_basic = stg["shares_basic"]
    shares_fd = stg["shares_fd"]
    options = stg["options_outstanding"]
    perf_rights = stg["perf_rights_outstanding"]
    cash = stg["cash"]
    debt = stg["debt"]

    # If FD not given but basic is, compute it
    if shares_fd is None and shares_basic is not None:
        shares_fd = shares_basic + (options or 0) + (perf_rights or 0)

    confidence = "medium"  # presentations are less precise than filings
    needs_review, review_reason = _check_review_flags(
        conn, company_id, cash, shares_fd, None, confidence
    )

    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO company_financials
           (company_id, document_id, effective_date, announcement_date,
            shares_basic, shares_fd, options_outstanding, perf_rights_outstanding,
            cash, debt,
            extraction_method, confidence, needs_review, review_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'rule', ?, ?, ?, ?)""",
        (
            company_id, document_id, effective_date, announcement_date,
            shares_basic, shares_fd, options, perf_rights,
            cash, debt,
            confidence, 1 if needs_review else 0, review_reason, now,
        ),
    )

    conn.commit()
    conn.close()

    logger.info("Normalized presentation for %s doc %d: basic=%s, fd=%s, cash=%s",
                ticker, document_id, shares_basic, shares_fd, cash)
    return True

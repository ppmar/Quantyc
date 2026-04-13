"""
Issue-of-Securities Extractor

Rule-based parsing of ASX Appendix 2A/3B/3G templates.
Updates shares_basic, options_outstanding, perf_rights_outstanding.
Writes to _stg_issue_of_securities staging table.
"""

import io
import json
import logging
import re
from datetime import datetime, timezone

import pdfplumber

from db import get_connection

logger = logging.getLogger(__name__)

# --- Patterns for Appendix 3G / 2A / 3B forms ---

SECURITIES_ISSUED_PATTERN = re.compile(
    r"number\s+of\s+\+?securities\s*\n?\s*(\d[\d,]+)", re.I,
)

# Part 4 quoted total: "CHESS DEPOSITARY INTERESTS 1:1  137,886,534"
QUOTED_TOTAL_PATTERN = re.compile(
    r"(?:CHESS\s+DEPOSITARY\s+INTERESTS|ORDINARY\s+(?:FULLY\s+PAID\s+)?SHARES?)"
    r".*?(\d{2,3},\d{3},\d{3})",
    re.I,
)

# Part 4 unquoted lines: "OPTION EXPIRING ... 11,170,000"
UNQUOTED_LINE_PATTERN = re.compile(
    r":\s*(?:OPTION\s+EXPIRING|RESTRICTED\s+STO|PERFORMANCE\s+RIGHTS|RSU)"
    r"[^\d]{0,80}?(\d[\d,]+)",
    re.I,
)

# Detect security class from context
OPTION_INDICATOR = re.compile(r"option|warrant", re.I)
PERF_RIGHTS_INDICATOR = re.compile(r"performance\s+right|RSU|restricted\s+stock", re.I)

# Exercise price
EXERCISE_PRICE_PATTERN = re.compile(
    r"exercise\s+price[^\d$]{0,30}?[\$A]?\s*\$?([\d.]+)", re.I,
)

# Date of change
DATE_PATTERN = re.compile(r"date\s+of\s+change\s*\n?\s*(\d{1,2}/\d{1,2}/\d{4})", re.I)
DATE_PATTERN_2 = re.compile(r"date\s+of\s+this\s+announcement\s*\n?\s*(?:\w+\s+)?(\d{1,2}/\d{1,2}/\d{4})", re.I)


def _parse_number(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(text: str) -> str | None:
    """Parse dd/mm/yyyy to ISO date."""
    try:
        return datetime.strptime(text.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _extract(pages: list[str]) -> list[dict]:
    """Extract securities issuance rows from page texts.

    Returns list of dicts, one per security class detected.
    """
    full_text = "\n".join(pages)
    rows = []

    # Effective date
    effective_date = None
    for pat in (DATE_PATTERN, DATE_PATTERN_2):
        m = pat.search(full_text)
        if m:
            effective_date = _parse_date(m.group(1))
            if effective_date:
                break

    # Number of securities issued in this transaction
    issued_count = None
    m = SECURITIES_ISSUED_PATTERN.search(full_text)
    if m:
        issued_count = _parse_number(m.group(1))

    # Detect what class was issued
    security_class = "ordinary"  # default
    if PERF_RIGHTS_INDICATOR.search(full_text[:2000]):
        security_class = "performance_right"
    elif OPTION_INDICATOR.search(full_text[:2000]):
        security_class = "option"

    exercise_price = None
    m = EXERCISE_PRICE_PATTERN.search(full_text)
    if m:
        exercise_price = _parse_number(m.group(1))

    # Total on issue from Part 4
    part4_match = re.search(r"Part\s*4.*", full_text, re.I | re.DOTALL)
    part4_text = part4_match.group() if part4_match else full_text

    total_quoted = None
    m = QUOTED_TOTAL_PATTERN.search(part4_text)
    if m:
        val = _parse_number(m.group(1))
        if val and val > 1_000_000:
            total_quoted = val

    # Unquoted securities count
    total_unquoted = None
    unquoted_entries = UNQUOTED_LINE_PATTERN.findall(part4_text)
    if unquoted_entries:
        total_unquoted = sum(_parse_number(e) or 0 for e in unquoted_entries)

    # Quoted total from Part 4 is ALWAYS total ordinary shares/CDIs
    if total_quoted:
        rows.append({
            "security_class": "ordinary",
            "quantity": issued_count if security_class == "ordinary" else None,
            "total_on_issue": total_quoted,
            "exercise_price": None,
            "effective_date": effective_date,
        })

    # The issued securities (if not ordinary) get their own row
    if security_class != "ordinary" and issued_count:
        rows.append({
            "security_class": security_class,
            "quantity": issued_count,
            "total_on_issue": None,
            "exercise_price": exercise_price,
            "effective_date": effective_date,
        })

    # Unquoted securities from Part 4.2 (options/rights totals)
    if total_unquoted and total_unquoted > 0:
        unquoted_class = "option"
        if PERF_RIGHTS_INDICATOR.search(part4_text):
            unquoted_class = "performance_right"
        rows.append({
            "security_class": unquoted_class,
            "quantity": None,
            "total_on_issue": total_unquoted,
            "exercise_price": None,
            "effective_date": effective_date,
        })

    return rows


def extract_issue_of_securities(document_id: int, pdf_bytes: bytes) -> list[dict] | None:
    """
    Extract issue-of-securities data and write to _stg_issue_of_securities.
    Returns list of extracted rows or None on failure.
    """
    logger.info("Extracting issue-of-securities for doc %d", document_id)

    pages = []
    try:
        bio = io.BytesIO(pdf_bytes)
        with pdfplumber.open(bio) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
    except Exception as e:
        logger.error("Failed to open PDF for doc %d: %s", document_id, e)
        return None

    if not pages:
        return None

    rows = _extract(pages)
    if not rows:
        logger.warning("No securities data extracted from doc %d", document_id)
        return None

    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()

    for row in rows:
        conn.execute(
            """INSERT OR REPLACE INTO _stg_issue_of_securities
               (document_id, effective_date, security_class, quantity,
                total_on_issue, exercise_price, raw_json, extraction_method, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'rule', ?)""",
            (
                document_id,
                row["effective_date"],
                row["security_class"],
                row["quantity"],
                row["total_on_issue"],
                row["exercise_price"],
                json.dumps(row, default=str),
                now,
            ),
        )

    conn.commit()
    conn.close()

    logger.info("Securities staging for doc %d: %d rows", document_id, len(rows))
    return rows

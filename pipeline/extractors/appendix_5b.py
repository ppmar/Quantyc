"""
Appendix 5B Extractor

Rule-based extraction from the ASX-mandated quarterly cash flow template.
Writes results to _stg_appendix_5b staging table.

Target fields:
    Section 1 → quarterly_opex_burn
    Section 2 → quarterly_invest_burn
    Section 4 → cash
    Section 6 → debt
    Period end → effective_date
"""

import io
import json
import logging
import re
from datetime import datetime, timezone

import pdfplumber

from db import get_connection

logger = logging.getLogger(__name__)

# Row labels → field mapping (checked in order)
ROW_PATTERNS = {
    "cash": [
        "cash and cash equivalents at end of",
        "cash at end of quarter",
        "cash and cash equiv",
    ],
    "quarterly_opex_burn": [
        "net cash from / (used in) operating activities",
        "net cash from/(used in) operating",
        "net cash used in operating",
        "net cash from operating",
    ],
    "quarterly_invest_burn": [
        "net cash from / (used in) investing activities",
        "net cash from/(used in) investing",
        "net cash used in investing",
        "net cash from investing",
    ],
    "debt": [
        "total borrowings",
        "loan facilities",
        "borrowings",
    ],
}

# Period end date patterns
PERIOD_PATTERNS = [
    re.compile(r"quarter\s+ended\s+(\d{1,2}\s+\w+\s+\d{4})", re.I),
    re.compile(r"period\s+ended?\s+(\d{1,2}\s+\w+\s+\d{4})", re.I),
    re.compile(r"(\d{1,2}/\d{1,2}/\d{4})"),
    re.compile(r"(\d{4}-\d{2}-\d{2})"),
]


def _parse_amount(text: str) -> float | None:
    """Parse a dollar amount, handling parentheses for negatives."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace("$", "").replace(" ", "")
    if not text or text in ("-", "–", "—"):
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    match = re.search(r"[-]?\d+\.?\d*", text)
    if not match:
        return None

    value = float(match.group())
    return -value if negative else value


def _parse_period_date(full_text: str) -> str | None:
    """Try to extract the period-end date from the document text."""
    for pat in PERIOD_PATTERNS:
        m = pat.search(full_text)
        if m:
            raw = m.group(1)
            for fmt in ("%d %B %Y", "%d %b %Y", "%d/%m/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
    return None


def _extract_from_tables(pdf_bytes: bytes) -> dict:
    """Extract cash flow fields from Appendix 5B tables.

    Values are in A$'000 as reported. We convert to dollars in the normalizer.
    """
    results = {field: None for field in ROW_PATTERNS}
    full_text = ""

    try:
        bio = io.BytesIO(pdf_bytes)
        pdf = pdfplumber.open(bio)
    except Exception as e:
        logger.error("Failed to open PDF: %s", e)
        return results

    for page in pdf.pages:
        page_text = page.extract_text() or ""
        full_text += page_text + "\n"

        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if not table:
                continue
            for row in table:
                if not row or not row[0]:
                    continue

                label = str(row[0]).lower().strip()
                for field, patterns in ROW_PATTERNS.items():
                    if results[field] is not None:
                        continue
                    if any(p in label for p in patterns):
                        # Value is typically in the "current quarter" column
                        for cell in row[1:]:
                            if cell:
                                val = _parse_amount(str(cell))
                                if val is not None:
                                    results[field] = val
                                    break

    # Try to extract period-end date
    results["effective_date"] = _parse_period_date(full_text)

    pdf.close()
    return results


def extract_appendix_5b(document_id: int, pdf_bytes: bytes) -> dict | None:
    """
    Extract Appendix 5B data and write to _stg_appendix_5b.
    Returns extracted values dict or None on failure.
    """
    logger.info("Extracting Appendix 5B for doc %d", document_id)

    results = _extract_from_tables(pdf_bytes)

    cash = results.get("cash")
    opex = results.get("quarterly_opex_burn")
    invest = results.get("quarterly_invest_burn")

    if cash is None and opex is None:
        logger.warning("No usable 5B data for doc %d", document_id)
        return None

    # Convert from A$'000 to dollars
    if cash is not None:
        cash = cash * 1000
    if opex is not None:
        opex = abs(opex) * 1000  # store burn as positive
    if invest is not None:
        invest = abs(invest) * 1000

    debt = results.get("debt")
    if debt is not None:
        debt = debt * 1000

    now = datetime.now(timezone.utc).isoformat()

    conn = get_connection()
    conn.execute(
        """INSERT OR REPLACE INTO _stg_appendix_5b
           (document_id, effective_date, cash, debt,
            quarterly_opex_burn, quarterly_invest_burn,
            raw_json, extraction_method, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'rule', ?)""",
        (
            document_id,
            results.get("effective_date"),
            cash,
            debt,
            opex,
            invest,
            json.dumps(results, default=str),
            now,
        ),
    )
    conn.commit()
    conn.close()

    logger.info("5B staging for doc %d: cash=%s, opex_burn=%s, invest_burn=%s", document_id, cash, opex, invest)
    return {"cash": cash, "debt": debt, "quarterly_opex_burn": opex, "quarterly_invest_burn": invest}

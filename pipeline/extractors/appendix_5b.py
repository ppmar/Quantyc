"""
Appendix 5B Extractor

Rule-based extraction from the ASX-mandated quarterly cash flow template.
Writes results to _stg_appendix_5b staging table.

The Appendix 5B is a standardized ASX form with fixed item numbers:
    1.9  → Net cash from operating activities
    2.6  → Net cash from investing activities
    3.10 → Net cash from financing activities
    4.6  → Cash and cash equivalents at end of period
    7.1  → Loan facilities (total / drawn)
    7.4  → Total financing facilities (total / drawn)
    8.1  → Net cash from operating (repeated)

All monetary values are reported in A$'000.
"""

import io
import json
import logging
import re
from datetime import datetime, timezone

import pdfplumber

from db import get_connection

logger = logging.getLogger(__name__)

# ── Text-based extraction using standardized item numbers ─────────────

# Match "item_number  ...  number  number" on a single line
# The first number after the item is current quarter, second is YTD
def _build_item_pattern(item: str) -> re.Pattern:
    """Build regex for a numbered line item like '1.9' or '4.6'.

    Matches: '1.9 Net cash from / (used in) operating 62,609 116,326'
    Also handles negative values in parentheses: '(42,657)'
    """
    escaped = re.escape(item)
    return re.compile(
        escaped
        + r"\s+.*?"                                     # label text
        + r"(\(?\d[\d,]*\.?\d*\)?)"                     # first number (current quarter)
        + r"(?:\s+(\(?\d[\d,]*\.?\d*\)?))?",            # optional second number (YTD)
        re.I,
    )


# Section 7 has a different layout: "7.1 Loan facilities  100,000  100,000"
# Column 1 = total facility, Column 2 = amount drawn
ITEM_PATTERNS = {
    "operating":  _build_item_pattern("1.9"),
    "investing":  _build_item_pattern("2.6"),
    "financing":  _build_item_pattern("3.10"),
    "cash":       _build_item_pattern("4.6"),
    "loan_total": _build_item_pattern("7.1"),
    "facility_total": _build_item_pattern("7.4"),
}

# Period end date: "Quarter ended ("current quarter")\n... 31 December 2025"
# or inline: "Quarter ended 31 December 2025"
QUARTER_ENDED_PATTERN = re.compile(
    r"quarter\s+ended.*?(\d{1,2}\s+\w+\s+\d{4})",
    re.I | re.DOTALL,
)

# Fallback: "31 December 2025" near top of document
DATE_NEAR_TOP = re.compile(
    r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
    re.I,
)

# Compliance statement date: "Date: 20 January 2026"
COMPLIANCE_DATE = re.compile(
    r"Date:\s*(\d{1,2}\s+\w+\s+\d{4})",
    re.I,
)


def _parse_amount(text: str) -> float | None:
    """Parse a dollar amount from 5B, handling parentheses for negatives."""
    if not text:
        return None
    text = text.strip()
    if not text or text in ("-", "–", "—", "N/A"):
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    text = text.replace(",", "").strip()
    try:
        value = float(text)
        return -value if negative else value
    except ValueError:
        return None


def _parse_date(text: str) -> str | None:
    """Parse 'dd Month yyyy' to ISO date."""
    text = text.strip()
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


_5B_MARKERS = ["appendix 5b", "quarterly cash flow report", "mining exploration entity"]


def _find_5b_pages(pages: list[str]) -> str:
    """Find pages that belong to the Appendix 5B section.

    Returns concatenated text of only the 5B pages.
    For standalone 5B docs, returns all pages.
    For quarterly reports with embedded 5B, returns only the 5B section.
    """
    # Check if ANY page contains a 5B marker
    start_idx = None
    for i, text in enumerate(pages):
        lower = text.lower()
        if any(marker in lower for marker in _5B_MARKERS):
            start_idx = i
            break

    if start_idx is None:
        return ""

    # Take from the 5B start page to the end (5B is always at the end of quarterly reports)
    return "\n".join(pages[start_idx:])


def _extract_from_text(pdf_bytes: bytes) -> dict:
    """Extract cash flow fields from Appendix 5B using text-based parsing.

    Uses standardized ASX item numbers for reliable matching.
    Values are in A$'000 as reported.
    """
    results = {
        "cash": None,
        "quarterly_opex_burn": None,
        "quarterly_invest_burn": None,
        "debt": None,
        "effective_date": None,
    }

    try:
        bio = io.BytesIO(pdf_bytes)
        pdf = pdfplumber.open(bio)
    except Exception as e:
        logger.error("Failed to open PDF: %s", e)
        return results

    # Extract all page texts
    pages = []
    for page in pdf.pages:
        pages.append(page.extract_text() or "")
    pdf.close()

    # Find only the 5B section pages (skip narrative pages in quarterly reports)
    full_text = _find_5b_pages(pages)

    if not full_text.strip():
        return results

    # ── Extract period-end date ──
    m = QUARTER_ENDED_PATTERN.search(full_text[:1500])
    if m:
        results["effective_date"] = _parse_date(m.group(1))

    if not results["effective_date"]:
        # Try any date near top of page 1
        m = DATE_NEAR_TOP.search(full_text[:1500])
        if m:
            results["effective_date"] = _parse_date(m.group(1))

    # ── Extract item 4.6: Cash at end of period ──
    m = ITEM_PATTERNS["cash"].search(full_text)
    if m:
        results["cash"] = _parse_amount(m.group(1))

    # ── Extract item 1.9: Net operating cash flow ──
    m = ITEM_PATTERNS["operating"].search(full_text)
    if m:
        results["quarterly_opex_burn"] = _parse_amount(m.group(1))

    # ── Extract item 2.6: Net investing cash flow ──
    m = ITEM_PATTERNS["investing"].search(full_text)
    if m:
        results["quarterly_invest_burn"] = _parse_amount(m.group(1))

    # ── Extract debt from section 7 ──
    # Try 7.4 (total facilities) first, then 7.1 (loan facilities)
    # Column 2 = "Amount drawn at quarter end"
    for key in ("facility_total", "loan_total"):
        m = ITEM_PATTERNS[key].search(full_text)
        if m:
            # The second number is "amount drawn"
            drawn = _parse_amount(m.group(2)) if m.group(2) else _parse_amount(m.group(1))
            if drawn and drawn > 0:
                results["debt"] = drawn
                break

    return results


def extract_appendix_5b(document_id: int, pdf_bytes: bytes) -> dict | None:
    """
    Extract Appendix 5B data and write to _stg_appendix_5b.
    Returns extracted values dict or None on failure.
    """
    logger.info("Extracting Appendix 5B for doc %d", document_id)

    results = _extract_from_text(pdf_bytes)

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
            raw_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
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

    logger.info("5B staging for doc %d: cash=%s, opex_burn=%s, invest_burn=%s, debt=%s",
                document_id, cash, opex, invest, debt)
    return {"cash": cash, "debt": debt, "quarterly_opex_burn": opex, "quarterly_invest_burn": invest}

"""
Presentation / Corporate Update Extractor

Rule-based extraction of capital structure data from investor presentations.
Looks for "Company Snapshot" / "Capital Structure" / "Corporate Summary" slides
with shares issued, fully diluted, cash, debt, market cap.

Writes to _stg_presentation staging table.
"""

import io
import json
import logging
import re
from datetime import datetime, timezone

import pdfplumber

from db import get_connection

logger = logging.getLogger(__name__)

# ── Page scoring: find the slide with capital structure data ─────────

SNAPSHOT_KEYWORDS = [
    "company snapshot", "capital structure", "corporate summary",
    "capitalization summary", "share structure", "corporate snapshot",
    "capital summary", "key statistics", "company overview",
]

FIELD_KEYWORDS = [
    "shares issued", "shares on issue", "fully diluted",
    "market cap", "cash", "debt",
]


def _score_page(text: str) -> int:
    """Score a page for likelihood of containing capital structure data."""
    t = text.lower()
    score = 0
    for kw in SNAPSHOT_KEYWORDS:
        if kw in t:
            score += 10
    for kw in FIELD_KEYWORDS:
        if kw in t:
            score += 3
    return score


# ── Number parsing ───────────────────────────────────────────────────

def _parse_amount(text: str) -> float | None:
    """Parse a number with optional M/B/million/billion suffix and currency prefix.

    Examples: 'A$133M', 'C$2.7B', '259.0M', '$507.6 million', '$1.128 billion',
              '1,234,567', '~$181m'
    """
    if not text:
        return None
    text = text.strip()
    # Remove leading ~ (approximate)
    text = text.lstrip('~').strip()
    # Remove currency prefixes
    text = re.sub(r'^[A-Z]{0,3}\$', '', text)
    text = text.strip()

    multiplier = 1
    lower = text.lower()
    if lower.endswith('billion'):
        multiplier = 1e9
        text = text[:-7]
    elif lower.endswith('million'):
        multiplier = 1e6
        text = text[:-7]
    elif text.upper().endswith('B'):
        multiplier = 1e9
        text = text[:-1]
    elif text.upper().endswith('M'):
        multiplier = 1e6
        text = text[:-1]
    elif text.upper().endswith('K'):
        multiplier = 1e3
        text = text[:-1]

    text = text.replace(',', '').strip()
    try:
        return float(text) * multiplier
    except ValueError:
        return None


# ── Field extraction patterns ────────────────────────────────────────

# Shares issued / on issue: "Shares Issued 259.0M" or "Shares on Issue: 259M"
SHARES_BASIC_PATTERN = re.compile(
    r"(?:shares?\s+(?:issued|on\s+issue|outstanding))\s*[:\-–]?\s*"
    r"([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Fully diluted: "Fully Diluted 271.0M" or "FD Shares: 271M"
SHARES_FD_PATTERN = re.compile(
    r"(?:fully\s+diluted|fd\s+shares?|diluted\s+shares?)\s*[:\-–]?\s*"
    r"([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Cash: "Cash A$133M" or "Cash: $133M" or "Cash and cash equivalents ... 130,384,269"
CASH_PATTERN = re.compile(
    r"(?:^|\s)cash(?:\s+(?:and\s+cash\s+equivalents|position|balance|at\s+bank))?\s*[:\-–]?\s*"
    r"([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Narrative cash patterns for quarterly reports
# "cash and gold on hand ... was $507.6 million"
CASH_NARRATIVE_1 = re.compile(
    r"cash\s+(?:and\s+(?:gold|bullion)\s+)?(?:on\s+hand|balance)"
    r".*?(?:was|of|to)\s+"
    r"(?:~\s*)?([A-Z]{0,3}\$?\d[\d,.]*\s*(?:million|billion|[MBK]))",
    re.I,
)

# "A$606.5M cash & gold" or "A$606.5M3 cash"
CASH_NARRATIVE_2 = re.compile(
    r"([A-Z]{0,3}\$\d[\d,.]*[MBK]?)\d?\s+cash(?:\s+(?:&|and)\s+(?:gold|bullion))?",
    re.I,
)

# "balance to $1.128 billion" (preceded by "cash" context)
CASH_NARRATIVE_3 = re.compile(
    r"(?:cash|bullion)\s+.*?balance\s+.*?(?:of|to)\s+"
    r"(?:~\s*)?([A-Z]{0,3}\$?\d[\d,.]*\s*(?:million|billion|[MBK]))",
    re.I,
)

# "Cash on hand A$810M"
CASH_NARRATIVE_4 = re.compile(
    r"cash\s+on\s+hand\s+([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Debt: "Debt: $5M" or "Net Debt: -$10M" or "Total Debt A$0"
DEBT_PATTERN = re.compile(
    r"(?:total\s+)?(?:net\s+)?debt\s*[:\-–]?\s*"
    r"(-?[A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Market cap: "Market Capitalization C$2.7B" or "Market Cap: A$2.8B"
MARKET_CAP_PATTERN = re.compile(
    r"(?:basic\s+)?market\s+cap(?:italization)?\s*[:\-–]?\s*"
    r"([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Options outstanding: "Options: 12.5M" or "Options Outstanding 12,500,000"
OPTIONS_PATTERN = re.compile(
    r"options?\s+(?:outstanding|on\s+issue|issued)\s*[:\-–]?\s*"
    r"([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# Performance rights: "Performance Rights: 5.2M"
PERF_RIGHTS_PATTERN = re.compile(
    r"performance\s+rights?\s+(?:outstanding|on\s+issue|issued)?\s*[:\-–]?\s*"
    r"([A-Z]{0,3}\$?\d[\d,.]*[MBK]?)",
    re.I,
)

# "As at" / "as of" date: "AS AT 16 FEBRUARY 2026"
AS_AT_DATE_PATTERN = re.compile(
    r"as\s+(?:at|of)\s+(\d{1,2}\s+\w+\s+\d{4})",
    re.I,
)

AS_AT_DATE_PATTERN_2 = re.compile(
    r"as\s+(?:at|of)\s+(\d{1,2}/\d{1,2}/\d{4})",
    re.I,
)


def _parse_date_text(text: str) -> str | None:
    """Parse 'dd MONTH yyyy' or 'dd/mm/yyyy' to ISO date."""
    text = text.strip()
    for fmt in ("%d %B %Y", "%d %b %Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract(pages: list[str]) -> dict | None:
    """Extract capital structure data from presentation pages.

    Finds the best candidate page(s) and extracts fields.
    Returns dict or None.
    """
    if not pages:
        return None

    # Score pages and pick the best ones
    scored = [(i, _score_page(p), p) for i, p in enumerate(pages)]
    scored.sort(key=lambda x: x[1], reverse=True)

    # Need at least some signal (3 = at least one field keyword)
    if scored[0][1] < 3:
        return None

    # Use top 3 pages for extraction
    best_pages = [p for _, s, p in scored[:3] if s >= 3]
    text = "\n".join(best_pages)

    result = {}

    # Shares basic
    m = SHARES_BASIC_PATTERN.search(text)
    if m:
        result["shares_basic"] = _parse_amount(m.group(1))

    # Shares FD
    m = SHARES_FD_PATTERN.search(text)
    if m:
        result["shares_fd"] = _parse_amount(m.group(1))

    # Cash — try structured pattern first, then narrative fallbacks
    m = CASH_PATTERN.search(text)
    if m:
        result["cash"] = _parse_amount(m.group(1))
    if not result.get("cash"):
        for pat in (CASH_NARRATIVE_1, CASH_NARRATIVE_2, CASH_NARRATIVE_3, CASH_NARRATIVE_4):
            m = pat.search(text)
            if m:
                val = _parse_amount(m.group(1))
                if val:
                    result["cash"] = val
                    break

    # Debt
    m = DEBT_PATTERN.search(text)
    if m:
        result["debt"] = _parse_amount(m.group(1))

    # Options
    m = OPTIONS_PATTERN.search(text)
    if m:
        result["options_outstanding"] = _parse_amount(m.group(1))

    # Perf rights
    m = PERF_RIGHTS_PATTERN.search(text)
    if m:
        result["perf_rights_outstanding"] = _parse_amount(m.group(1))

    # Effective date ("as at")
    for pat in (AS_AT_DATE_PATTERN, AS_AT_DATE_PATTERN_2):
        m = pat.search(text)
        if m:
            d = _parse_date_text(m.group(1))
            if d:
                result["effective_date"] = d
                break

    # Need at least shares or cash to be useful
    if not result.get("shares_basic") and not result.get("shares_fd") and not result.get("cash"):
        return None

    return result


def extract_presentation(document_id: int, pdf_bytes: bytes) -> dict | None:
    """
    Extract capital structure data from a presentation PDF.
    Writes to _stg_presentation staging table.
    Returns extracted dict or None on failure.
    """
    logger.info("Extracting presentation data for doc %d", document_id)

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

    result = _extract(pages)
    if not result:
        logger.warning("No capital structure data in presentation doc %d", document_id)
        return None

    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()

    conn.execute(
        """INSERT OR REPLACE INTO _stg_presentation
           (document_id, effective_date, shares_basic, shares_fd,
            options_outstanding, perf_rights_outstanding, cash, debt,
            raw_json, extraction_method, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'rule', ?)""",
        (
            document_id,
            result.get("effective_date"),
            result.get("shares_basic"),
            result.get("shares_fd"),
            result.get("options_outstanding"),
            result.get("perf_rights_outstanding"),
            result.get("cash"),
            result.get("debt"),
            json.dumps(result, default=str),
            now,
        ),
    )

    conn.commit()
    conn.close()

    logger.info("Presentation staging for doc %d: %s", document_id, result)
    return result

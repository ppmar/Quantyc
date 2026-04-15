"""
Issue-of-Securities Extractor

Rule-based parsing of ASX Appendix 2A / 3B / 3G templates.
These are standardized forms with consistent structure:

  Part 1: Entity details (ticker, date)
  Part 2: Issue type (new shares, option exercise, placement, etc.)
  Part 3: Details of the issue (number, price, from/to class)
  Part 4: Total securities on issue after the transaction
    4.1 Quoted securities (CDIs, ordinary shares)
    4.2 Unquoted securities (common shares, options, RSUs, perf rights)

Key output: total shares on issue = quoted CDIs + unquoted common/ordinary shares
Plus: options on issue, performance rights / RSUs on issue.

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

# ── Part 4 security line patterns ─────────────────────────────────────
# Each line in Part 4 looks like:
#   SX2 : CHESS DEPOSITARY INTERESTS 1:1  137,886,534
#   SX2AC : COMMON SHARES  121,603,987
#   SX2AA : OPTION EXPIRING VARIOUS DATES EX VARIOUS PRICES  11,170,000

# Match: CODE : DESCRIPTION  NUMBER
PART4_LINE = re.compile(
    r"([A-Z0-9]+)\s*:\s*(.+?)\s+(\d[\d,]+)\s*$",
    re.MULTILINE,
)

# Classification of security descriptions
SHARES_DESCRIPTIONS = re.compile(
    r"chess\s+depositary\s+interest|ordinary\s+(?:fully\s+paid\s+)?share|common\s+share|fully\s+paid\s+ordinary",
    re.I,
)
OPTIONS_DESCRIPTIONS = re.compile(
    r"option|warrant",
    re.I,
)
PERF_RIGHTS_DESCRIPTIONS = re.compile(
    r"performance\s+right|restricted\s+stock|RSU",
    re.I,
)

# ── Transaction details ───────────────────────────────────────────────
# "Number of +securities to be issued/transferred" or "Number of +securities to be quoted"
ISSUED_COUNT = re.compile(
    r"(?:number\s+of\s+\+?securities\s+(?:to\s+be\s+(?:issued|quoted|transferred))?)"
    r"\s*\n?\s*(\d[\d,]+)",
    re.I,
)

# Exercise / issue price: "AUD 0.87000000" or "$0.87" or "issue price per +security ... AUD 0.87"
ISSUE_PRICE = re.compile(
    r"(?:issue\s+price|exercise\s+price|consideration)[^\d$]*?(?:AUD|A\$|\$)\s*([\d.]+)",
    re.I,
)

# Date patterns
DATE_ANNOUNCEMENT = re.compile(
    r"date\s+of\s+this\s+announcement\s*\n?\s*(?:\w+\s+)?(\w+\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})",
    re.I,
)
DATE_ISSUE = re.compile(
    r"(?:issue\s+date|date\s+.*?issued)\s*\n?\s*(\d{1,2}/\d{1,2}/\d{4})",
    re.I,
)

# FROM/TO class for conversions
FROM_CLASS = re.compile(
    r"FROM\s+\(Existing\s+Class\)\s*\n?\s*.*?\n?\s*([A-Z0-9]+)\s*:\s*(.+?)(?:\n|$)",
    re.I,
)
TO_CLASS = re.compile(
    r"TO\s+\(Existing\s+Class\)\s*\n?\s*.*?\n?\s*([A-Z0-9]+)\s*:\s*(.+?)(?:\n|$)",
    re.I,
)


def _parse_number(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_date(text: str) -> str | None:
    """Parse various date formats to ISO date."""
    text = text.strip()
    for fmt in ("%d/%m/%Y", "%B %d, %Y", "%B %d %Y", "%d %B %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _classify_security(description: str) -> str:
    """Classify a security description into a standard class."""
    if SHARES_DESCRIPTIONS.search(description):
        return "ordinary"
    if OPTIONS_DESCRIPTIONS.search(description):
        return "option"
    if PERF_RIGHTS_DESCRIPTIONS.search(description):
        return "performance_right"
    return "other"


def _extract(pages: list[str]) -> dict | None:
    """Extract securities data from Appendix 2A/3G/3B pages.

    Returns a single dict with aggregated totals from Part 4, plus transaction details.
    """
    full_text = "\n".join(pages)

    # ── Effective date ──
    effective_date = None
    for pat in (DATE_ISSUE, DATE_ANNOUNCEMENT):
        m = pat.search(full_text)
        if m:
            effective_date = _parse_date(m.group(1))
            if effective_date:
                break

    # ── Transaction details ──
    issued_count = None
    m = ISSUED_COUNT.search(full_text)
    if m:
        issued_count = _parse_number(m.group(1))

    issue_price = None
    m = ISSUE_PRICE.search(full_text)
    if m:
        issue_price = _parse_number(m.group(1))

    # What was converted from/to
    converted_from = None
    m = FROM_CLASS.search(full_text)
    if m:
        converted_from = _classify_security(m.group(2))

    converted_to = None
    m = TO_CLASS.search(full_text)
    if m:
        converted_to = _classify_security(m.group(2))

    # ── Part 4: Total securities on issue ──
    # Find Part 4 section
    part4_match = re.search(r"Part\s*4\b.*", full_text, re.I | re.DOTALL)
    part4_text = part4_match.group() if part4_match else full_text

    # Parse all security lines in Part 4
    total_shares = 0
    total_options = 0
    total_perf_rights = 0
    found_any = False

    for m in PART4_LINE.finditer(part4_text):
        code = m.group(1)
        description = m.group(2).strip()
        count = _parse_number(m.group(3))

        if not count or count < 100:  # skip page numbers etc
            continue

        security_class = _classify_security(description)

        if security_class == "ordinary":
            total_shares += count
            found_any = True
        elif security_class == "option":
            total_options += count
            found_any = True
        elif security_class == "performance_right":
            total_perf_rights += count
            found_any = True

    if not found_any:
        return None

    result = {
        "effective_date": effective_date,
        "total_shares_on_issue": total_shares if total_shares > 0 else None,
        "total_options_on_issue": total_options if total_options > 0 else None,
        "total_perf_rights_on_issue": total_perf_rights if total_perf_rights > 0 else None,
        "issued_count": issued_count,
        "issue_price": issue_price,
        "converted_from": converted_from,
        "converted_to": converted_to,
    }

    return result


def extract_issue_of_securities(document_id: int, pdf_bytes: bytes) -> dict | None:
    """
    Extract issue-of-securities data and write to _stg_issue_of_securities.
    Returns extracted dict or None on failure.
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

    result = _extract(pages)
    if not result:
        logger.warning("No securities data extracted from doc %d", document_id)
        return None

    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()

    conn.execute(
        """INSERT OR REPLACE INTO _stg_issue_of_securities
           (document_id, effective_date, security_class, quantity,
            total_on_issue, exercise_price, raw_json, extraction_method, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'rule', ?)""",
        (
            document_id,
            result["effective_date"],
            "ordinary",
            result["issued_count"],
            result["total_shares_on_issue"],
            result["issue_price"],
            json.dumps(result, default=str),
            now,
        ),
    )

    conn.commit()
    conn.close()

    logger.info("Securities staging for doc %d: shares=%s, options=%s, perf_rights=%s",
                document_id,
                result.get("total_shares_on_issue"),
                result.get("total_options_on_issue"),
                result.get("total_perf_rights_on_issue"))
    return result

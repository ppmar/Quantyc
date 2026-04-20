"""
Appendix 5B Extractor

Rule-based extraction from the ASX-mandated quarterly cash flow template.
Writes results to _stg_appendix_5b staging table.

The Appendix 5B is a standardized ASX form with fixed item numbers:
    1.9  → Net cash from operating activities
    2.1(d) → Exploration & evaluation payments (investing)
    2.6  → Net cash from investing activities
    3.10 → Net cash from financing activities
    4.6  → Cash and cash equivalents at end of period
    7.1  → Loan facilities (total / drawn)
    7.4  → Total financing facilities (total / drawn)
    8.7  → Estimated quarters of funding available

All monetary values are reported in A$'000.

Two extraction strategies:
    1. Primary: pdfplumber table extraction with section classification
    2. Fallback: regex on raw text (original approach)
"""

import io
import json
import logging
import re
from datetime import datetime, timezone

import pdfplumber

from db import get_connection

logger = logging.getLogger(__name__)


# ── Shared constants ────────────────────────────────────────────────

_5B_MARKERS = ["appendix 5b", "quarterly cash flow report", "mining exploration entity"]

# Footer printed at the bottom of every genuine ASX Appendix 5B page.
# Pattern is deliberately broad on the date stamp to survive future template revisions.
_5B_FOOTER_PATTERN = re.compile(
    r"asx\s+listing\s+rules\s+appendix\s*5b\s*\(\d{2}/\d{2}/\d{2}\)",
    re.I,
)


# ── Gate 1: strict first-page content markers ───────────────────────

# Required positive markers (case-insensitive, whitespace-tolerant)
# At least one must appear on page 1.
_GATE1_POSITIVE_PATTERNS = [
    re.compile(r"appendix\s*5b", re.I),
    re.compile(
        r"mining\s+exploration\s+entity\s+or\s+oil\s+and\s+gas\s+exploration\s+entity",
        re.I,
    ),
    re.compile(r"rule\s*5\.5", re.I),
]

# If any of these appear on page 1 AND no positive marker matches, reject.
# These are other ASX forms whose keywords might leak past the headline filter.
_GATE1_DISQUALIFIER_PATTERNS = [
    re.compile(r"appendix\s*4c", re.I),   # producer quarterly
    re.compile(r"appendix\s*5a", re.I),   # mining production quarterly
    re.compile(r"appendix\s*4d", re.I),   # half-year financial
    re.compile(r"appendix\s*4e", re.I),   # preliminary final report
]


def _gate1_first_page_check(pdf_bytes: bytes) -> tuple[bool, str]:
    """
    Verify the PDF is genuinely an Appendix 5B by scanning all pages.

    Acceptance criteria (ANY one is sufficient):
      A) The ASX footer "_5B_FOOTER_PATTERN" appears on at least one page.
      B) At least one _GATE1_POSITIVE_PATTERN matches on the first page
         (retains compatibility with standalone 5B PDFs where the form
         starts on page 1 and the footer may be cut off by pdfplumber).
      C) At least one _GATE1_POSITIVE_PATTERN matches on any later page
         (for embedded 5B docs that don't use the ASX footer, e.g. WR1).

    Rejection: a disqualifier pattern appears on page 1 AND no positive
    signal was found.

    Returns (passed, reason). reason is the gate-failure code on failure,
    or "ok" on pass.
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return False, "pdf_no_pages"
            page_texts = [page.extract_text() or "" for page in pdf.pages]
    except Exception as e:
        return False, f"pdf_read_error:{type(e).__name__}"

    # Strategy A: footer on any page — most reliable for embedded 5B docs
    for text in page_texts:
        normalized = re.sub(r"\s+", " ", text)
        if _5B_FOOTER_PATTERN.search(normalized):
            return True, "ok"

    # Strategy B: positive markers on first page — for standalone 5B PDFs
    first_page_normalized = re.sub(r"\s+", " ", page_texts[0])
    has_positive = any(p.search(first_page_normalized) for p in _GATE1_POSITIVE_PATTERNS)
    if has_positive:
        for disq in _GATE1_DISQUALIFIER_PATTERNS:
            m = disq.search(first_page_normalized)
            if m:
                return False, f"disqualifier:{m.group(0).lower()}"
        return True, "ok"

    # Strategy C: positive markers on any page — for embedded 5B without ASX footer
    # (e.g. WR1 uses company footer instead of ASX-mandated footer)
    for text in page_texts[1:]:
        normalized = re.sub(r"\s+", " ", text)
        if any(p.search(normalized) for p in _GATE1_POSITIVE_PATTERNS):
            return True, "ok"

    return False, "no_5b_marker_on_any_page"


# ── Gate 2: effective_date must land on a fiscal quarter-end ────────

_VALID_QUARTER_END_MMDD = {"03-31", "06-30", "09-30", "12-31"}


def _gate2_quarter_end_check(effective_date: str | None) -> tuple[bool, str]:
    """
    Validate that effective_date (ISO-8601 string YYYY-MM-DD) is a
    fiscal quarter-end. No tolerance window — the ASX form can only
    report to a true quarter-end.
    """
    if not effective_date:
        return False, "missing_effective_date"
    try:
        mmdd = effective_date[5:10]  # 'YYYY-MM-DD' → 'MM-DD'
    except Exception:
        return False, f"unparseable_date:{effective_date}"
    if mmdd not in _VALID_QUARTER_END_MMDD:
        return False, f"not_quarter_end:{effective_date}"
    return True, "ok"


def _mark_doc_failed(document_id: int, error: str) -> None:
    """Mark document as failed with a short error code. Used by gates."""
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET parse_status = 'failed', parse_error = ? "
        "WHERE document_id = ?",
        (error, document_id),
    )
    conn.commit()
    conn.close()


# ── Number parsing ──────────────────────────────────────────────────

def _parse_amount(text: str) -> float | None:
    """Parse a dollar amount from 5B, handling parentheses for negatives."""
    if not text:
        return None
    text = text.strip()
    if not text or text in ("-", "–", "—", "N/A", "n/a", "nil", "Nil"):
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


# ── Page filtering ──────────────────────────────────────────────────

# Strong signal: the formal 5B form header (not a passing reference)
_5B_FORM_HEADER = re.compile(
    r"appendix\s*5b\s*[–—-]\s*mining\s+exploration\s+entity",
    re.I,
)

# Section 1 header that only appears on the actual cash-flow form page
_5B_SECTION1 = re.compile(
    r"1\.\s*Cash\s+flows\s+from\s+operating\s+activities",
    re.I,
)


def _find_5b_pages(pages: list[str]) -> tuple[str, int]:
    """Find pages that belong to the Appendix 5B section.

    Detection strategy (in priority order):
      1. First page whose text contains _5B_FOOTER_PATTERN   <- ASX footer
      2. First page with formal 5B form header or Section 1  <- strong signal
      3. First page whose text contains any of _5B_MARKERS   <- loose fallback

    Returns (concatenated text of 5B pages, start page index).
    Returns ("", -1) if no 5B section is found.
    """
    # Strategy 1: footer-based detection (reliable for embedded docs)
    for i, text in enumerate(pages):
        normalized = re.sub(r"\s+", " ", text)
        if _5B_FOOTER_PATTERN.search(normalized):
            return "\n".join(pages[i:]), i

    # Strategy 2: formal 5B form header or Section 1 on same page
    for i, text in enumerate(pages):
        normalized = re.sub(r"\s+", " ", text)
        if _5B_FORM_HEADER.search(normalized) or (
            _5B_SECTION1.search(normalized)
            and any(m in text.lower() for m in _5B_MARKERS)
        ):
            return "\n".join(pages[i:]), i

    # Strategy 3: marker-based fallback (legacy standalone docs)
    for i, text in enumerate(pages):
        lower = text.lower()
        if any(marker in lower for marker in _5B_MARKERS):
            return "\n".join(pages[i:]), i

    return "", -1


# ── Strategy 1: pdfplumber table extraction ─────────────────────────

# Section heading patterns to classify extracted tables
_SECTION_HEADINGS = {
    "section_1": re.compile(r"1\.\s*Cash flows from operating activities", re.I),
    "section_2": re.compile(r"2\.\s*Cash flows from investing activities", re.I),
    "section_3": re.compile(r"3\.\s*Cash flows from financing activities", re.I),
    "section_4": re.compile(r"4\.\s*Net\s+(increase|decrease).*cash", re.I),
    "section_5": re.compile(r"5\.\s*Reconciliation of cash", re.I),
    "section_6": re.compile(r"6\.\s*Payments to related parties", re.I),
    "section_7": re.compile(r"7\.\s*Financing facilit", re.I),
    "section_8": re.compile(r"8\.\s*Estimated cash available", re.I),
}

# Row reference patterns for extracting specific line items from tables
_ROW_PATTERNS = {
    "1.9":    re.compile(r"^\s*1\.9\b"),
    "2.1(d)": re.compile(r"^\s*2\.1\s*\(?\s*d\s*\)?\b"),
    "2.6":    re.compile(r"^\s*2\.6\b"),
    "3.10":   re.compile(r"^\s*3\.10\b"),
    "4.1":    re.compile(r"^\s*4\.1\b"),
    "4.6":    re.compile(r"^\s*4\.6\b"),
    "7.1":    re.compile(r"^\s*7\.1\b"),
    "7.4":    re.compile(r"^\s*7\.4\b"),
    "8.1":    re.compile(r"^\s*8\.1\b"),
    "8.2":    re.compile(r"^\s*8\.2\b"),
    "8.3":    re.compile(r"^\s*8\.3\b"),
    "8.4":    re.compile(r"^\s*8\.4\b"),
    "8.5":    re.compile(r"^\s*8\.5\b"),
    "8.6":    re.compile(r"^\s*8\.6\b"),
    "8.7":    re.compile(r"^\s*8\.7\b"),
}


def _find_row_in_table(table: list[list[str]], row_ref: str) -> list[str] | None:
    """Find a row in a table by its item reference number."""
    pattern = _ROW_PATTERNS.get(row_ref)
    if not pattern:
        return None
    for row in table:
        if row and row[0] and pattern.match(str(row[0]).strip()):
            return row
    return None


def _get_numeric_cells(row: list[str]) -> list[float | None]:
    """Extract numeric values from the rightmost cells of a table row."""
    values = []
    for cell in reversed(row):
        val = _parse_amount(str(cell) if cell else "")
        values.insert(0, val)
    return values


def _extract_from_tables(pdf_bytes: bytes, start_page: int) -> dict:
    """Extract fields using pdfplumber table extraction.

    Returns dict with all extracted fields, or empty dict if tables can't be found.
    """
    results = {}

    try:
        bio = io.BytesIO(pdf_bytes)
        pdf = pdfplumber.open(bio)
    except Exception as e:
        logger.warning("Table extraction: failed to open PDF: %s", e)
        return results

    # Collect all tables from 5B pages
    all_tables = []
    for page in pdf.pages[start_page:]:
        tables = page.extract_tables(table_settings={
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "intersection_y_tolerance": 3,
            "intersection_x_tolerance": 3,
        })
        for t in tables:
            if t:
                all_tables.append(t)

    pdf.close()

    if not all_tables:
        return results

    # Flatten all tables into one big list of rows for searching
    all_rows = []
    for table in all_tables:
        all_rows.extend(table)

    # Extract critical fields by item reference
    # 1.9 — Net cash from operating activities
    row = _find_row_in_table(all_rows, "1.9")
    if not row:
        # Try searching each table separately
        for table in all_tables:
            row = _find_row_in_table(table, "1.9")
            if row:
                break
    if row:
        nums = _get_numeric_cells(row)
        # Last two numeric values are typically current_quarter and YTD
        non_none = [v for v in nums if v is not None]
        if non_none:
            results["operating"] = non_none[-2] if len(non_none) >= 2 else non_none[-1]

    # 2.1(d) — Exploration & evaluation payments
    for table in all_tables:
        row = _find_row_in_table(table, "2.1(d)")
        if row:
            nums = _get_numeric_cells(row)
            non_none = [v for v in nums if v is not None]
            if non_none:
                results["exploration_evaluation"] = non_none[-2] if len(non_none) >= 2 else non_none[-1]
            break

    # 2.6 — Net cash from investing
    for table in all_tables:
        row = _find_row_in_table(table, "2.6")
        if row:
            nums = _get_numeric_cells(row)
            non_none = [v for v in nums if v is not None]
            if non_none:
                results["investing"] = non_none[-2] if len(non_none) >= 2 else non_none[-1]
            break

    # 3.10 — Net cash from financing
    for table in all_tables:
        row = _find_row_in_table(table, "3.10")
        if row:
            nums = _get_numeric_cells(row)
            non_none = [v for v in nums if v is not None]
            if non_none:
                results["financing"] = non_none[-2] if len(non_none) >= 2 else non_none[-1]
            break

    # 4.6 — Cash at end of quarter
    for table in all_tables:
        row = _find_row_in_table(table, "4.6")
        if row:
            nums = _get_numeric_cells(row)
            non_none = [v for v in nums if v is not None]
            if non_none:
                results["cash"] = non_none[-2] if len(non_none) >= 2 else non_none[-1]
            break

    # 4.1 — Cash at beginning of period
    for table in all_tables:
        row = _find_row_in_table(table, "4.1")
        if row:
            nums = _get_numeric_cells(row)
            non_none = [v for v in nums if v is not None]
            if non_none:
                results["cash_beginning"] = non_none[-2] if len(non_none) >= 2 else non_none[-1]
            break

    # Section 7 — Financing facilities (debt)
    for key in ("7.4", "7.1"):
        for table in all_tables:
            row = _find_row_in_table(table, key)
            if row:
                nums = _get_numeric_cells(row)
                non_none = [v for v in nums if v is not None]
                if len(non_none) >= 2:
                    # Column 1 = total facility, Column 2 = amount drawn
                    results["debt"] = non_none[-1]  # amount drawn
                elif non_none:
                    results["debt"] = non_none[0]
                break
        if "debt" in results:
            break

    # Section 8 — Runway
    for table in all_tables:
        row = _find_row_in_table(table, "8.7")
        if row:
            # 8.7 can be a number, "N/A", or "> 50"
            for cell in reversed(row):
                cell_str = str(cell).strip() if cell else ""
                if not cell_str:
                    continue
                if cell_str.lower() in ("n/a", "not applicable", "nil"):
                    results["quarters_of_funding"] = None
                    break
                # Handle "> 50" style
                m = re.match(r">\s*(\d+\.?\d*)", cell_str)
                if m:
                    results["quarters_of_funding"] = float(m.group(1))
                    break
                val = _parse_amount(cell_str)
                if val is not None:
                    results["quarters_of_funding"] = val
                    break
            break

    return results


# ── Strategy 2: regex on raw text (fallback) ────────────────────────

def _build_item_pattern(item: str) -> re.Pattern:
    """Build regex for a numbered line item like '1.9' or '4.6'."""
    escaped = re.escape(item)
    return re.compile(
        escaped
        + r"\s+.*?"                                     # label text
        + r"(\(?\d[\d,]*\.?\d*\)?)"                     # first number (current quarter)
        + r"(?:\s+(\(?\d[\d,]*\.?\d*\)?))?",            # optional second number (YTD)
        re.I,
    )


_REGEX_PATTERNS = {
    "operating":           _build_item_pattern("1.9"),
    "exploration_eval":    _build_item_pattern("2.1"),
    "investing":           _build_item_pattern("2.6"),
    "financing":           _build_item_pattern("3.10"),
    "cash":                _build_item_pattern("4.6"),
    "loan_total":          _build_item_pattern("7.1"),
    "facility_total":      _build_item_pattern("7.4"),
}

# More specific pattern for 2.1(d) exploration & evaluation
_REGEX_21D = re.compile(
    r"2\.1\s*\(?\s*d\s*\)?\s+.*?"
    r"(\(?\d[\d,]*\.?\d*\)?)"
    r"(?:\s+(\(?\d[\d,]*\.?\d*\)?))?",
    re.I,
)

# Runway pattern: "8.7 ... N quarters"
_REGEX_87 = re.compile(
    r"8\.7\s+.*?"
    r"([\d,]+\.?\d*|N/?A|not applicable|nil|>\s*\d+)",
    re.I,
)

# Period end date patterns
QUARTER_ENDED_PATTERN = re.compile(
    r"quarter\s+ended.*?(\d{1,2}\s+\w+\s+\d{4})",
    re.I | re.DOTALL,
)

DATE_NEAR_TOP = re.compile(
    r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
    re.I,
)


def _extract_from_text(full_text: str) -> dict:
    """Extract fields using regex on raw text. Fallback strategy."""
    results = {}

    if not full_text.strip():
        return results

    # Operating (1.9)
    m = _REGEX_PATTERNS["operating"].search(full_text)
    if m:
        results["operating"] = _parse_amount(m.group(1))

    # Exploration & evaluation (2.1(d))
    m = _REGEX_21D.search(full_text)
    if m:
        results["exploration_evaluation"] = _parse_amount(m.group(1))

    # Investing (2.6)
    m = _REGEX_PATTERNS["investing"].search(full_text)
    if m:
        results["investing"] = _parse_amount(m.group(1))

    # Financing (3.10)
    m = _REGEX_PATTERNS["financing"].search(full_text)
    if m:
        results["financing"] = _parse_amount(m.group(1))

    # Cash (4.6)
    m = _REGEX_PATTERNS["cash"].search(full_text)
    if m:
        results["cash"] = _parse_amount(m.group(1))

    # Debt from section 7
    for key in ("facility_total", "loan_total"):
        m = _REGEX_PATTERNS[key].search(full_text)
        if m:
            drawn = _parse_amount(m.group(2)) if m.group(2) else _parse_amount(m.group(1))
            if drawn and drawn > 0:
                results["debt"] = drawn
                break

    # Runway (8.7)
    m = _REGEX_87.search(full_text)
    if m:
        val_str = m.group(1).strip().lower()
        if val_str in ("n/a", "not applicable", "nil"):
            results["quarters_of_funding"] = None
        else:
            gt = re.match(r">\s*(\d+\.?\d*)", val_str)
            if gt:
                results["quarters_of_funding"] = float(gt.group(1))
            else:
                parsed = _parse_amount(val_str)
                if parsed is not None:
                    results["quarters_of_funding"] = parsed

    return results


# ── Date extraction ─────────────────────────────────────────────────

def _extract_effective_date(full_text: str) -> str | None:
    """Extract the quarter-end date from the 5B text.

    Only accepts dates that appear in a 'Quarter ended ...' context.
    Does NOT fall back to 'any date near the top' — that fallback was
    producing non-quarter-end effective_dates on malformed filings.
    """
    m = QUARTER_ENDED_PATTERN.search(full_text[:1500])
    if not m:
        return None
    return _parse_date(m.group(1))


# ── Main extraction function ────────────────────────────────────────

def _extract_all_fields(pdf_bytes: bytes) -> dict:
    """Extract all fields from an Appendix 5B PDF.

    Uses pdfplumber tables as primary strategy, regex on text as fallback.
    Returns dict with keys: cash, operating, investing, financing,
    exploration_evaluation, debt, quarters_of_funding, effective_date.
    """
    try:
        bio = io.BytesIO(pdf_bytes)
        pdf = pdfplumber.open(bio)
    except Exception as e:
        logger.error("Failed to open PDF: %s", e)
        return {}

    # Extract all page texts
    pages = []
    for page in pdf.pages:
        pages.append(page.extract_text() or "")
    pdf.close()

    # Find the 5B section
    full_text, start_page = _find_5b_pages(pages)
    if not full_text.strip():
        return {}

    # Strategy 1: pdfplumber table extraction
    results = {}
    if start_page >= 0:
        results = _extract_from_tables(pdf_bytes, start_page)
        if results:
            logger.debug("Table extraction got %d fields", len(results))

    # Strategy 2: regex fallback for any missing critical fields
    critical_fields = ["cash", "operating", "investing"]
    missing = [f for f in critical_fields if f not in results or results[f] is None]

    if missing:
        regex_results = _extract_from_text(full_text)
        for key, val in regex_results.items():
            if key not in results or results[key] is None:
                results[key] = val
                logger.debug("Regex fallback filled '%s'", key)

    # Always extract date from text (not in tables)
    results["effective_date"] = _extract_effective_date(full_text)

    return results


# ── Public API ──────────────────────────────────────────────────────

def extract_appendix_5b(document_id: int, pdf_bytes: bytes) -> dict | None:
    """
    Extract Appendix 5B data and write to _stg_appendix_5b.
    Returns extracted values dict or None on gate failure / empty extraction.
    """
    logger.info("Extracting Appendix 5B for doc %d", document_id)

    # ─── GATE 1: content check (first page must look like a real 5B) ───
    gate1_ok, gate1_reason = _gate1_first_page_check(pdf_bytes)
    if not gate1_ok:
        logger.warning("Doc %d rejected by gate 1: %s", document_id, gate1_reason)
        _mark_doc_failed(document_id, f"gate1:{gate1_reason}")
        return None

    results = _extract_all_fields(pdf_bytes)

    # ─── GATE 2: effective_date must be a fiscal quarter-end ───────────
    gate2_ok, gate2_reason = _gate2_quarter_end_check(results.get("effective_date"))
    if not gate2_ok:
        logger.warning("Doc %d rejected by gate 2: %s", document_id, gate2_reason)
        _mark_doc_failed(document_id, f"gate2:{gate2_reason}")
        return None

    # ─── existing logic below ───
    cash = results.get("cash")
    opex = results.get("operating")
    invest = results.get("investing")

    if cash is None and opex is None:
        logger.warning("No usable 5B data for doc %d", document_id)
        _mark_doc_failed(document_id, "no_usable_data_after_gates")
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

    # Additional fields (stored in raw_json for now)
    financing = results.get("financing")
    if financing is not None:
        financing = financing * 1000

    exploration_eval = results.get("exploration_evaluation")
    if exploration_eval is not None:
        exploration_eval = abs(exploration_eval) * 1000

    cash_beginning = results.get("cash_beginning")
    if cash_beginning is not None:
        cash_beginning = cash_beginning * 1000

    quarters_of_funding = results.get("quarters_of_funding")

    # Build rich raw_json with all extracted fields
    raw = {
        "effective_date": results.get("effective_date"),
        "cash": cash,
        "cash_beginning": cash_beginning,
        "debt": debt,
        "quarterly_opex_burn": opex,
        "quarterly_invest_burn": invest,
        "quarterly_financing": financing,
        "exploration_evaluation": exploration_eval,
        "quarters_of_funding": quarters_of_funding,
    }

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
            json.dumps(raw, default=str),
            now,
        ),
    )
    conn.commit()
    conn.close()

    logger.info(
        "5B staging for doc %d: cash=%s, opex_burn=%s, invest_burn=%s, "
        "debt=%s, financing=%s, exploration=%s, runway=%s quarters",
        document_id, cash, opex, invest, debt, financing,
        exploration_eval, quarters_of_funding,
    )
    return {
        "cash": cash,
        "debt": debt,
        "quarterly_opex_burn": opex,
        "quarterly_invest_burn": invest,
        "quarterly_financing": financing,
        "exploration_evaluation": exploration_eval,
        "quarters_of_funding": quarters_of_funding,
    }

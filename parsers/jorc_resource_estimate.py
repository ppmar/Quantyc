"""
JORC Resource Estimate Parser — Mineral Resource & Ore Reserve extraction.

Parses JORC-compliant summary tables from standalone resource/reserve
announcement PDFs. Extracts category breakdowns (Measured/Indicated/Inferred),
tonnes, grade, contained metal, cutoff grade, project name, and commodity.

Pure regex + pdfplumber. No LLM, no OCR, no DB writes, stateless.
"""

import io
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

import pdfplumber

from .appendix_2a import ExtractionError, MalformedDocumentError

PARSER_VERSION = "0.1.0"

# ── Profile detection ──────────────────────────────────────────────────

_PROFILE_PATTERNS = [
    re.compile(r"Mineral\s+Resource\s+Estimate", re.I),
    re.compile(r"Ore\s+Reserve\s+Estimate", re.I),
    re.compile(r"JORC\s*(?:Code)?\s*\(?\s*201[24]\s*\)?", re.I),
    re.compile(r"Maiden\s+(?:Mineral\s+)?Resource", re.I),
    re.compile(r"Resource\s+(?:Update|Upgrade)", re.I),
    re.compile(r"Updated\s+Mineral\s+Resource", re.I),
]

_DISQUALIFIER_PATTERNS = [
    re.compile(r"Appendix\s*5B", re.I),
    re.compile(r"Quarterly\s+Activities?\s+Report", re.I),
    re.compile(r"Notification\s+of\s+issue", re.I),
    re.compile(r"Appendix\s*[23][A-H]", re.I),
]

_JORC_CATEGORIES = {"measured", "indicated", "inferred", "proven", "proved", "probable", "total"}


def _has_jorc_table(pdf_bytes: bytes) -> bool:
    """Check if any extracted table contains JORC category labels."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:20]:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue
                    # Check first column of each row for category labels
                    cats_found = set()
                    for row in table:
                        if row and row[0]:
                            cell = row[0].strip().lower()
                            for cat in _JORC_CATEGORIES:
                                if cat in cell:
                                    cats_found.add(cat)
                    if len(cats_found) >= 2:
                        return True
    except Exception:
        return False
    return False


def detect_profile(pdf_bytes: bytes) -> bool:
    """Return True if the PDF looks like a JORC resource/reserve estimate."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return False

            # Check first 3 pages for profile patterns
            first_pages = ""
            for page in pdf.pages[:3]:
                first_pages += (page.extract_text() or "") + "\n"

            if not first_pages.strip():
                return False

            # Must have at least one profile pattern
            if not any(p.search(first_pages) for p in _PROFILE_PATTERNS):
                return False

            # Must NOT have disqualifiers on page 1
            page1_text = pdf.pages[0].extract_text() or ""
            if any(p.search(page1_text) for p in _DISQUALIFIER_PATTERNS):
                return False

    except Exception:
        return False

    # Must have a detectable JORC table
    return _has_jorc_table(pdf_bytes)


# ── Field extraction helpers ───────────────────────────────────────────

_COMMODITY_MAP = [
    (re.compile(r"\bgold\b|(?<!\.)\bAu\b", re.I), "Au"),
    (re.compile(r"\bsilver\b|(?<!\.)\bAg\b", re.I), "Ag"),
    (re.compile(r"\bcopper\b|(?<!\.)\bCu\b", re.I), "Cu"),
    (re.compile(r"\blithium\b|\bLi2?O\b|\bLCE\b", re.I), "Li2O"),
    (re.compile(r"\buranium\b|\bU3O8\b", re.I), "U3O8"),
    (re.compile(r"\bnickel\b|(?<!\.)\bNi\b", re.I), "Ni"),
    (re.compile(r"\bzinc\b|(?<!\.)\bZn\b", re.I), "Zn"),
    (re.compile(r"\biron\s*ore\b|(?<!\.)\bFe\b", re.I), "Fe"),
    (re.compile(r"\brare\s*earth\b|\bREE\b|\bTREO\b", re.I), "TREO"),
    (re.compile(r"\bcobalt\b|(?<!\.)\bCo\b", re.I), "Co"),
    (re.compile(r"\bgraphite\b|\bTGC\b", re.I), "Graphite"),
]

_PROJECT_NAME_PATTERNS = [
    # "at the X Project" / "for the X Deposit"
    re.compile(
        r"(?:at|for)\s+(?:the\s+)?([A-Z][A-Za-z0-9 \-']{2,60})\s+(?:Project|Deposit|Mine|Operation)",
        re.I,
    ),
    # Title-case name preceding "Mineral Resource Estimate"
    re.compile(
        r"^([A-Z][A-Za-z0-9 \-']{3,80}?)\s+(?:Mineral\s+Resource|Ore\s+Reserve|JORC\s+Resource|Resource\s+Estimate)",
        re.I | re.M,
    ),
]

_PROJECT_SUFFIXES = re.compile(r"\s+(?:Project|Deposit|Mine|Operation)\s*$", re.I)

_CUTOFF_PATTERNS = [
    re.compile(r"cut[\-\s]?off\s+grade\s*(?:of\s*)?(\d+\.?\d*)\s*([a-zA-Z%/]+)", re.I),
    re.compile(r"(\d+\.?\d*)\s*([a-zA-Z%/]+)\s*cut[\-\s]?off", re.I),
]

_EFFECTIVE_DATE_PATTERNS = [
    re.compile(r"(?:effective|as\s+at|as\s+of)\s+(\d{1,2}\s+\w+\s+\d{4})", re.I),
    re.compile(r"(?:effective|as\s+at|as\s+of)\s+(\d{1,2}/\d{1,2}/\d{4})", re.I),
]


def _infer_commodity(text: str) -> tuple[Optional[str], list[str]]:
    """Infer primary commodity from text. Returns (commodity, warnings)."""
    matches = []
    for pattern, code in _COMMODITY_MAP:
        if pattern.search(text):
            matches.append(code)
    if not matches:
        return None, []
    if len(matches) > 1:
        return matches[0], [f"polymetallic_deposit_detected:{'+'.join(matches)}_using_{matches[0]}"]
    return matches[0], []


def _extract_project_name(text: str) -> Optional[str]:
    """Extract project name from first pages text."""
    for pattern in _PROJECT_NAME_PATTERNS:
        m = pattern.search(text)
        if m:
            name = m.group(1).strip()
            name = _PROJECT_SUFFIXES.sub("", name).strip()
            return name
    return None


def _extract_cutoff(text: str) -> tuple[Optional[Decimal], Optional[str]]:
    """Extract cutoff grade and unit."""
    for pattern in _CUTOFF_PATTERNS:
        m = pattern.search(text)
        if m:
            try:
                val = Decimal(m.group(1))
                unit = m.group(2).strip()
                return val, unit
            except InvalidOperation:
                continue
    return None, None


def _parse_effective_date(text: str) -> Optional[date]:
    """Extract the estimate effective date."""
    for pattern in _EFFECTIVE_DATE_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1).strip()
            for fmt in ("%d %B %Y", "%d %b %Y", "%d/%m/%Y"):
                try:
                    return datetime.strptime(raw, fmt).date()
                except ValueError:
                    continue
    return None


# ── Numeric parsing ────────────────────────────────────────────────────

_STRIP_SUFFIXES = re.compile(r"\s*(Mt|kt|t|g/t|%|ppm|lb/t|Moz|koz|Mlb|oz)\s*$", re.I)
_NUMBER_RE = re.compile(r"^[\d,]+\.?\d*$")
_NULL_TOKENS = {"—", "–", "-", "nil", "n/a", ""}


def _parse_decimal(cell: str) -> Optional[Decimal]:
    """Parse a numeric cell, handling commas and suffixes."""
    if not cell:
        return None
    cleaned = cell.strip()
    if cleaned.lower() in _NULL_TOKENS:
        return None
    cleaned = _STRIP_SUFFIXES.sub("", cleaned).strip()
    cleaned = cleaned.replace(",", "")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


# ── Table detection and parsing ────────────────────────────────────────

_CATEGORY_MAP = {
    "measured": "Measured",
    "indicated": "Indicated",
    "inferred": "Inferred",
    "proven": "Proven",
    "proved": "Proven",
    "probable": "Probable",
    "total": "Total",
    "measured + indicated": "Total",
    "measured+indicated": "Total",
    "m+i": "Total",
}

# Column header tokens
_TONNES_HEADERS = {"tonnes", "mt", "kt", "tonnage", "million tonnes"}
_GRADE_HEADERS = {"g/t", "%", "ppm", "lb/t", "grade"}
_CONTAINED_HEADERS = {"contained", "moz", "koz", "kt", "mlb", "metal", "ounces", "oz"}


def _classify_header(header: str) -> Optional[str]:
    """Classify a column header as 'tonnes', 'grade', or 'contained'."""
    h = header.strip().lower()
    for token in _TONNES_HEADERS:
        if token in h:
            return "tonnes"
    for token in _GRADE_HEADERS:
        if token in h:
            return "grade"
    for token in _CONTAINED_HEADERS:
        if token in h:
            return "contained"
    return None


def _detect_grade_unit(headers: list[str]) -> str:
    """Infer grade unit from column headers."""
    for h in headers:
        hl = h.strip().lower()
        if "g/t" in hl:
            return "g/t"
        if "%" in hl:
            return "%"
        if "ppm" in hl:
            return "ppm"
        if "lb/t" in hl:
            return "lb/t"
    return "g/t"  # default


def _detect_contained_unit(headers: list[str]) -> Optional[str]:
    """Infer contained metal unit from column headers."""
    for h in headers:
        hl = h.strip().lower()
        for unit in ["moz", "koz", "mlb", "kt", "oz"]:
            if unit in hl:
                return unit.capitalize() if unit[0] != "g" else unit
    return None


def _detect_tonnes_source_unit(headers: list[str]) -> str:
    """Detect whether tonnes are in Mt, kt, or t."""
    for h in headers:
        hl = h.strip().lower()
        if "kt" in hl:
            return "kt"
        if "mt" in hl or "million" in hl:
            return "Mt"
    return "Mt"  # default assumption


def _normalize_tonnes(val: Optional[Decimal], source_unit: str) -> Optional[Decimal]:
    """Normalize tonnes to millions (Mt)."""
    if val is None:
        return None
    if source_unit == "kt":
        return val / Decimal("1000")
    if source_unit == "t":
        return val / Decimal("1000000")
    return val  # already Mt


def _find_jorc_tables(pdf_bytes: bytes) -> list[tuple[list[str], list[list[str]]]]:
    """Find JORC summary tables. Returns list of (headers, data_rows)."""
    results = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 3:
                        continue

                    # Check if this table has JORC categories in first column
                    cats_found = set()
                    for row in table:
                        if row and row[0]:
                            cell = row[0].strip().lower()
                            for cat in _JORC_CATEGORIES:
                                if cat in cell:
                                    cats_found.add(cat)

                    if len(cats_found) < 2:
                        continue

                    # Find the first row containing a JORC category
                    first_cat_idx = None
                    for i, row in enumerate(table):
                        if row and row[0]:
                            cell = row[0].strip().lower()
                            if any(cat in cell for cat in _JORC_CATEGORIES):
                                first_cat_idx = i
                                break

                    if first_cat_idx is None:
                        continue

                    # Scan all rows BEFORE the first category row for column
                    # header tokens. Real PDFs often have empty rows, merged
                    # cells, or section sub-headers between the column headers
                    # and the data rows (e.g. "Open Pit (cut-off grade = ...)").
                    merged_header = [""] * len(table[0])
                    for i in range(first_cat_idx):
                        row = table[i]
                        if not row:
                            continue
                        for j, cell in enumerate(row):
                            if cell and j < len(merged_header):
                                existing = merged_header[j].strip()
                                addition = str(cell).strip()
                                if addition:
                                    merged_header[j] = (
                                        f"{existing} {addition}" if existing else addition
                                    )

                    headers = merged_header
                    data_rows = []
                    for row in table[first_cat_idx:]:
                        cells = [str(c or "") for c in row]
                        data_rows.append(cells)

                    results.append((headers, data_rows))
    except Exception:
        pass
    return results


def _parse_jorc_table(
    headers: list[str],
    data_rows: list[list[str]],
    grade_unit_override: Optional[str] = None,
) -> tuple[list, list[str]]:
    """Parse a single JORC table into JORCRow list + warnings."""
    from .jorc_resource_estimate_schemas import JORCRow

    warnings = []

    # Classify columns (skip col 0 — always the category label)
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        if i == 0:
            continue
        ctype = _classify_header(h)
        if ctype and ctype not in col_map:
            col_map[ctype] = i

    if "tonnes" not in col_map:
        warnings.append("no_tonnes_column_found")
        return [], warnings

    grade_unit = grade_unit_override or _detect_grade_unit(headers)
    contained_unit = _detect_contained_unit(headers)
    tonnes_source = _detect_tonnes_source_unit(headers)

    if tonnes_source != "Mt":
        warnings.append(f"tonnes_converted_from_{tonnes_source}")

    rows = []
    for row_cells in data_rows:
        if not row_cells or not row_cells[0]:
            continue

        label = row_cells[0].strip().lower()

        # Match category
        category = None
        for key, cat in _CATEGORY_MAP.items():
            if key in label:
                category = cat
                break

        if category is None:
            continue

        raw_line = " | ".join(c.strip() for c in row_cells if c.strip())

        tonnes_raw = _parse_decimal(row_cells[col_map["tonnes"]]) if "tonnes" in col_map and col_map["tonnes"] < len(row_cells) else None
        tonnes_mt = _normalize_tonnes(tonnes_raw, tonnes_source)

        grade_val = _parse_decimal(row_cells[col_map["grade"]]) if "grade" in col_map and col_map["grade"] < len(row_cells) else None

        contained_val = _parse_decimal(row_cells[col_map["contained"]]) if "contained" in col_map and col_map["contained"] < len(row_cells) else None

        rows.append(JORCRow(
            category=category,
            tonnes_mt=tonnes_mt,
            grade=grade_val,
            grade_unit=grade_unit,
            contained_metal=contained_val,
            contained_metal_unit=contained_unit,
            raw_line=raw_line,
        ))

    return rows, warnings


# ── Validation ─────────────────────────────────────────────────────────

# Grade plausibility ranges per commodity
_GRADE_RANGES: dict[str, tuple[float, float]] = {
    "Au":       (0.1, 100.0),      # g/t
    "Cu":       (0.05, 30.0),      # %
    "U3O8":     (50.0, 50000.0),   # ppm
    "Li2O":     (0.1, 5.0),        # %
    "Ni":       (0.1, 10.0),       # %
    "Zn":       (0.1, 30.0),       # %
    "Fe":       (15.0, 70.0),      # %
    "Ag":       (1.0, 2000.0),     # g/t
}


def _validate_estimate(rows: list, commodity: str) -> list[str]:
    """Run post-extraction validation checks. Returns list of warnings."""
    warnings = []

    # 1. Tonnes ordering: Total ≈ sum of categories
    total_row = None
    category_rows = []
    for row in rows:
        if row.category == "Total":
            total_row = row
        else:
            category_rows.append(row)

    if total_row and total_row.tonnes_mt is not None and category_rows:
        cat_sum = sum(r.tonnes_mt for r in category_rows if r.tonnes_mt is not None)
        if cat_sum > 0:
            deviation = abs(float(total_row.tonnes_mt) - float(cat_sum)) / float(cat_sum)
            if deviation > 0.05:
                warnings.append(f"total_tonnes_deviation_{deviation:.1%}")

    # 2. Grade range plausibility
    grade_range = _GRADE_RANGES.get(commodity)
    if grade_range:
        lo, hi = grade_range
        for row in rows:
            if row.grade is not None and row.category != "Total":
                g = float(row.grade)
                if g < lo or g > hi:
                    warnings.append(f"grade_outlier_{row.category}_{g}")

    # 3. Contained metal sanity (tonnes_mt × grade ≈ contained_metal)
    for row in rows:
        if (
            row.tonnes_mt is not None
            and row.grade is not None
            and row.contained_metal is not None
            and row.category != "Total"
        ):
            # This is an approximation — unit conversion depends on commodity
            # For gold (g/t → oz): tonnes_mt * 1e6 * grade_g_t / 31.1035 = oz
            # We just check relative consistency between rows
            pass  # Full unit-aware check deferred to future iteration

    return warnings


# ── Public API ─────────────────────────────────────────────────────────

def parse(
    pdf_bytes: bytes,
    ticker: str,
    doc_id: str,
    announcement_date: date,
) -> "JORCEstimate":
    """
    Parse a JORC resource/reserve estimate PDF.

    Returns a JORCEstimate dataclass with category breakdowns.
    Raises ExtractionError or MalformedDocumentError on failure.
    """
    from .jorc_resource_estimate_schemas import JORCEstimate

    warnings: list[str] = []

    # Extract text from first pages
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                raise ExtractionError("scanned_pdf_no_text")
            all_text = ""
            for page in pdf.pages[:5]:
                all_text += (page.extract_text() or "") + "\n"
            if not all_text.strip():
                raise ExtractionError("scanned_pdf_no_text")
    except (ExtractionError, MalformedDocumentError):
        raise
    except Exception as e:
        raise ExtractionError(f"pdf_read_error:{type(e).__name__}")

    # Project name
    project_name = _extract_project_name(all_text)
    if not project_name:
        raise MalformedDocumentError("project_name_not_found")

    # Commodity
    commodity, commodity_warnings = _infer_commodity(all_text)
    warnings.extend(commodity_warnings)
    if not commodity:
        warnings.append("commodity_not_inferred")
        commodity = "Unknown"

    # Cutoff grade
    cutoff_grade, cutoff_unit = _extract_cutoff(all_text)

    # Effective date
    effective_date = _parse_effective_date(all_text)
    if not effective_date:
        warnings.append("effective_date_fallback_to_announcement")
        effective_date = announcement_date

    # Find and parse JORC tables
    tables = _find_jorc_tables(pdf_bytes)
    if not tables:
        raise MalformedDocumentError("no_jorc_table_found")

    # Parse the first (primary) table
    all_rows: list = []
    resource_or_reserve = "resource"

    for headers, data_rows in tables:
        rows, table_warnings = _parse_jorc_table(headers, data_rows)
        warnings.extend(table_warnings)

        # Check for reserve rows
        for row in rows:
            if row.category in ("Proven", "Probable"):
                if not any(r.category in ("Measured", "Indicated", "Inferred") for r in rows):
                    resource_or_reserve = "reserve"
                else:
                    warnings.append("mixed_resource_reserve_table")
                break

        # Keep only resource rows (spec: reserve parsing is future)
        resource_rows = [r for r in rows if r.category not in ("Proven", "Probable")]
        if any(r.category in ("Proven", "Probable") for r in rows):
            warnings.append("reserve_rows_present_but_ignored")

        all_rows.extend(resource_rows)

        if all_rows:
            break  # use first table with results

    if not all_rows:
        raise MalformedDocumentError("no_category_rows_extracted")

    # Check at least one non-empty row
    has_data = any(r.tonnes_mt is not None for r in all_rows)
    if not has_data:
        raise MalformedDocumentError("all_rows_empty")

    # Validation
    validation_warnings = _validate_estimate(all_rows, commodity)
    warnings.extend(validation_warnings)

    return JORCEstimate(
        ticker=ticker,
        doc_id=doc_id,
        snapshot_date=effective_date,
        announcement_date=announcement_date,
        parsed_at=datetime.now(timezone.utc),
        parser_version=PARSER_VERSION,
        project_name=project_name,
        commodity=commodity,
        resource_or_reserve=resource_or_reserve,
        cutoff_grade=cutoff_grade,
        cutoff_grade_unit=cutoff_unit,
        rows=all_rows,
        extraction_warnings=warnings,
    )

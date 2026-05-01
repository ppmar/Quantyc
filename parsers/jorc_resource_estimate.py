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
    """Check if any extracted table OR text contains JORC category labels."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:20]:
                # Check grid-based tables
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue
                    cats_found = set()
                    for row in table:
                        if not row:
                            continue
                        for cell in row:
                            if cell:
                                cell_lower = cell.strip().lower()
                                for cat in _JORC_CATEGORIES:
                                    if cat in cell_lower:
                                        cats_found.add(cat)
                    if len(cats_found) >= 2:
                        return True

                # Fallback: check raw text for category labels (no grid lines)
                text = (page.extract_text() or "").lower()
                text_cats = set()
                for cat in _JORC_CATEGORIES:
                    if cat in text:
                        text_cats.add(cat)
                if len(text_cats) >= 2:
                    # Also need at least one number nearby to avoid matching prose
                    if re.search(r"\b\d+[,.]?\d*\s*(?:mt|kt|t|g/t|%|ppm|moz|koz)\b", text, re.I):
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
    # Combined / roll-up rows → Total
    "total": "Total",
    "sub-total": "Total",
    "sub total": "Total",
    "subtotal": "Total",
    "grand total": "Total",
    "total resource": "Total",
    "total reserve": "Total",
    "total mineral resource": "Total",
    "total ore reserve": "Total",
    "in-situ total": "Total",
    "global": "Total",
    "combined": "Total",
    # Measured + Indicated combined rows
    "measured + indicated": "Measured+Indicated",
    "measured+indicated": "Measured+Indicated",
    "measured and indicated": "Measured+Indicated",
    "measured & indicated": "Measured+Indicated",
    "m+i": "Measured+Indicated",
    "m & i": "Measured+Indicated",
    "m&i": "Measured+Indicated",
    # Indicated + Inferred combined
    "indicated + inferred": "Indicated+Inferred",
    "indicated+inferred": "Indicated+Inferred",
    "indicated and inferred": "Indicated+Inferred",
    # Reserve combined
    "proven + probable": "Proven+Probable",
    "proven+probable": "Proven+Probable",
    "proved + probable": "Proven+Probable",
    "proved+probable": "Proven+Probable",
    "proven and probable": "Proven+Probable",
    "proved and probable": "Proven+Probable",
    "p+p": "Proven+Probable",
    "2p": "Proven+Probable",
}

# Column header tokens
_TONNES_HEADERS = {"tonnes", "mt", "kt", "tonnage", "million tonnes", "million\ntonnes", "tons"}
_GRADE_HEADERS = {"g/t", "%", "ppm", "lb/t", "grade"}
_CONTAINED_HEADERS = {"contained", "moz", "koz", "mlb", "metal", "ounces", "oz"}
# Tokens that indicate the category/classification column (skip for numeric classification)
_CATEGORY_HEADERS = {"category", "classification", "class", "resource category", "reserve category",
                     "deposit", "type", "area"}


def _normalize_header(header: str) -> str:
    """Normalize a column header for matching: lowercase, collapse whitespace/linebreaks, strip subscript artifacts."""
    h = header.strip().lower()
    # pdfplumber subscript artifacts: "Li O %\n2" → "li o % 2" → "li2o %"
    h = re.sub(r"\s+", " ", h)
    return h


def _classify_header(header: str) -> Optional[str]:
    """Classify a column header as 'category', 'tonnes', 'grade', or 'contained'."""
    h = _normalize_header(header)
    if not h:
        return None
    # Category column
    for token in _CATEGORY_HEADERS:
        if token in h:
            return "category"
    # Tonnes
    for token in _TONNES_HEADERS:
        if token in h:
            return "tonnes"
    # Reject cut-off grade before grade match
    if "cut-off" in h or "cutoff" in h or "cut off" in h:
        return None
    # Grade
    for token in _GRADE_HEADERS:
        if token in h:
            return "grade"
    # Contained metal
    for token in _CONTAINED_HEADERS:
        if token in h:
            return "contained"
    return None


def _detect_grade_unit(headers: list[str]) -> str:
    """Infer grade unit from column headers."""
    for h in headers:
        hl = _normalize_header(h)
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
        hl = _normalize_header(h)
        for unit in ["moz", "koz", "mlb", "kt", "oz"]:
            if unit in hl:
                return unit.capitalize() if unit[0] != "g" else unit
    return None


def _detect_tonnes_source_unit(headers: list[str]) -> str:
    """Detect whether tonnes are in Mt, kt, or t."""
    for h in headers:
        hl = _normalize_header(h)
        if "kt" in hl and "million" not in hl:
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


def _find_category_column(table: list[list[str]]) -> tuple[int | None, set[str]]:
    """Find which column holds JORC category labels. Returns (col_idx, categories_found)."""
    ncols = max((len(r) for r in table if r), default=0)
    best_col = None
    best_cats: set[str] = set()
    for col_idx in range(ncols):
        cats = set()
        for row in table:
            if row and col_idx < len(row) and row[col_idx]:
                cell = row[col_idx].strip().lower()
                for cat in _JORC_CATEGORIES:
                    if cat in cell:
                        cats.add(cat)
        if len(cats) > len(best_cats):
            best_cats = cats
            best_col = col_idx
    return best_col, best_cats


def _find_jorc_tables(pdf_bytes: bytes) -> list[tuple[list[str], list[list[str]], int]]:
    """Find JORC summary tables. Returns list of (headers, data_rows, category_col_idx)."""
    results = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:25]:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 3:
                        continue

                    # Find which column holds JORC categories
                    cat_col, cats_found = _find_category_column(table)

                    if cat_col is None or len(cats_found) < 2:
                        continue

                    # Find the first row containing a JORC category in the detected column
                    first_cat_idx = None
                    for i, row in enumerate(table):
                        if row and cat_col < len(row) and row[cat_col]:
                            cell = row[cat_col].strip().lower()
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

                    # Compact: remove columns that are empty in both headers and all data rows.
                    # pdfplumber often splits merged cells into multiple empty columns.
                    ncols = len(headers)
                    keep = []
                    for ci in range(ncols):
                        if headers[ci].strip():
                            keep.append(ci)
                            continue
                        if any(dr[ci].strip() for dr in data_rows if ci < len(dr)):
                            keep.append(ci)
                    if keep:
                        headers = [headers[ci] for ci in keep]
                        data_rows = [
                            [row[ci] if ci < len(row) else "" for ci in keep]
                            for row in data_rows
                        ]
                        # Re-map cat_col after compaction
                        cat_col = keep.index(cat_col) if cat_col in keep else 0

                    results.append((headers, data_rows, cat_col))
    except Exception:
        pass
    return results


# ── Text-based table fallback ─────────────────────────────────────────

_TEXT_CATEGORY_RE = re.compile(
    r"^\s*(Measured|Indicated|Inferred|Proven|Proved|Probable|Total|Sub[\-\s]?total|"
    r"Measured\s*[+&]\s*Indicated|Indicated\s*[+&]\s*Inferred|"
    r"Proven\s*[+&]\s*Probable|Proved\s*[+&]\s*Probable)\b",
    re.I,
)

_TEXT_NUMBER_RE = re.compile(r"[\d,]+\.?\d*")


def _find_jorc_tables_from_text(pdf_bytes: bytes) -> list[tuple[list[str], list[list[str]], int]]:
    """Fallback: extract JORC tables from aligned text when pdfplumber finds no grid tables.

    Scans each page's raw text for lines starting with a JORC category label,
    then splits the remaining part of each line on whitespace to get numeric columns.
    """
    results = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages[:25]:
                text = page.extract_text() or ""
                lines = text.split("\n")

                # Collect lines that start with a JORC category
                cat_lines: list[tuple[str, list[str]]] = []
                for line in lines:
                    m = _TEXT_CATEGORY_RE.match(line)
                    if m:
                        cat_label = m.group(1).strip()
                        rest = line[m.end():]
                        numbers = _TEXT_NUMBER_RE.findall(rest)
                        if numbers:
                            cat_lines.append((cat_label, numbers))

                if len(cat_lines) < 2:
                    continue

                # Determine number of numeric columns from the most common count
                col_counts = [len(nums) for _, nums in cat_lines]
                ncols = max(set(col_counts), key=col_counts.count)

                # Try to find a header line above the first category line
                first_cat_line_idx = None
                for i, line in enumerate(lines):
                    if _TEXT_CATEGORY_RE.match(line):
                        first_cat_line_idx = i
                        break

                headers = ["Category"] + [f"Col{j+1}" for j in range(ncols)]
                if first_cat_line_idx is not None and first_cat_line_idx > 0:
                    # Look at the line(s) just above for header tokens
                    for scan_idx in range(max(0, first_cat_line_idx - 3), first_cat_line_idx):
                        hline = lines[scan_idx]
                        # Split on 2+ spaces (tabular alignment)
                        parts = re.split(r"\s{2,}", hline.strip())
                        if len(parts) >= 2:
                            headers = ["Category"] + parts[-ncols:] if len(parts) > ncols else ["Category"] + parts
                            break

                # Build data rows
                data_rows = []
                for cat_label, numbers in cat_lines:
                    row = [cat_label] + numbers[:ncols]
                    # Pad if needed
                    while len(row) < len(headers):
                        row.append("")
                    data_rows.append(row)

                results.append((headers, data_rows, 0))
    except Exception:
        pass
    return results


def _parse_jorc_table(
    headers: list[str],
    data_rows: list[list[str]],
    grade_unit_override: Optional[str] = None,
    category_col: int = 0,
) -> tuple[list, list[str]]:
    """Parse a single JORC table into JORCRow list + warnings."""
    from .jorc_resource_estimate_schemas import JORCRow

    warnings = []

    # Classify columns (skip the category column)
    col_map: dict[str, int] = {}
    for i, h in enumerate(headers):
        if i == category_col:
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

    def _fuzzy_read(row: list[str], col: int) -> float | None:
        """Read a numeric value from col, falling back to adjacent columns (±1).
        pdfplumber merged-cell tables often offset data by one column."""
        for c in (col, col - 1, col + 1):
            if 0 <= c < len(row):
                val = _parse_decimal(row[c])
                if val is not None:
                    return val
        return None

    rows = []
    for row_cells in data_rows:
        if not row_cells or category_col >= len(row_cells) or not row_cells[category_col]:
            continue

        label = row_cells[category_col].strip().lower()

        # Match category (try longer keys first so "measured + indicated" beats "measured")
        category = None
        for key, cat in sorted(_CATEGORY_MAP.items(), key=lambda x: -len(x[0])):
            if key in label:
                category = cat
                break

        if category is None:
            continue

        raw_line = " | ".join(c.strip() for c in row_cells if c.strip())

        tonnes_raw = _fuzzy_read(row_cells, col_map["tonnes"]) if "tonnes" in col_map else None
        tonnes_mt = _normalize_tonnes(tonnes_raw, tonnes_source)

        grade_val = _fuzzy_read(row_cells, col_map["grade"]) if "grade" in col_map else None

        contained_val = _fuzzy_read(row_cells, col_map["contained"]) if "contained" in col_map else None

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

    # Project name (best-effort — don't fail if missing)
    project_name = _extract_project_name(all_text)
    if not project_name:
        warnings.append("project_name_not_found")
        project_name = "Unknown"

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

    # Find and parse JORC tables (grid-based first, text fallback second)
    tables = _find_jorc_tables(pdf_bytes)
    if not tables:
        tables = _find_jorc_tables_from_text(pdf_bytes)
        if tables:
            warnings.append("table_from_text_fallback")
    if not tables:
        raise MalformedDocumentError("no_jorc_table_found")

    # Parse ALL tables and aggregate (real PDFs split by pit/UG/oxide/fresh)
    all_rows: list = []
    resource_or_reserve = "resource"

    for headers, data_rows, cat_col in tables:
        rows, table_warnings = _parse_jorc_table(headers, data_rows, category_col=cat_col)
        warnings.extend(table_warnings)

        # Check for reserve rows
        has_reserve = any(r.category in ("Proven", "Probable", "Proven+Probable") for r in rows)
        has_resource = any(r.category in ("Measured", "Indicated", "Inferred", "Measured+Indicated", "Indicated+Inferred") for r in rows)

        if has_reserve and not has_resource:
            resource_or_reserve = "reserve"
        elif has_reserve and has_resource:
            warnings.append("mixed_resource_reserve_table")

        # Keep only resource rows (spec: reserve parsing is future)
        resource_rows = [r for r in rows if r.category not in ("Proven", "Probable", "Proven+Probable")]
        if has_reserve:
            warnings.append("reserve_rows_present_but_ignored")

        all_rows.extend(resource_rows)

    # When aggregating multiple tables, drop intermediate Total/sub-total rows
    # and recompute a single Total from category rows
    _INDIVIDUAL_CATS = {"Measured", "Indicated", "Inferred", "Measured+Indicated", "Indicated+Inferred"}
    category_rows = [r for r in all_rows if r.category in _INDIVIDUAL_CATS]

    if len(tables) > 1 and category_rows:
        # Multiple tables → totals are per-section, not meaningful aggregated
        # Keep only individual category rows; downstream can sum if needed
        all_rows = category_rows
        warnings.append("intermediate_totals_dropped")

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

"""
Appendix 2A Parser — Post-issuance Capital Structure

Extracts Part 4 (Issued capital following quotation) from ASX Appendix 2A PDFs.
Produces shares_basic, unquoted_instruments[], and shares_fd_naive.

Pure regex + pdfplumber. No LLM, no OCR, no DB writes, stateless.
"""

import io
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import pdfplumber

from .appendix_2a_schemas import (
    Appendix2ACapitalStructure,
    QuotedClass,
    UnquotedInstrument,
)

PARSER_VERSION = "0.1.0"


# ── Exceptions ─────────────────────────────────────────────────────────

class ExtractionError(Exception):
    pass


class MalformedDocumentError(Exception):
    pass


class ReconciliationError(Exception):
    pass


# ── Profile detection ──────────────────────────────────────────────────

_PROFILE_FIRST_PAGE = re.compile(
    r"Appendix\s*2A\s*[-–]\s*Application\s+for\s+quotation",
    re.I,
)

_PART4_MARKER = re.compile(
    r"Part\s*4\s*[-–]\s*Issued\s+capital\s+following\s+quotation",
    re.I,
)


def detect_profile(pdf_bytes: bytes) -> bool:
    """Return True if this PDF is an Appendix 2A with Part 4 present."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return False
            # Check first 2 pages for the Appendix 2A header
            first_pages_text = ""
            for page in pdf.pages[:2]:
                first_pages_text += (page.extract_text() or "") + "\n"
            if not _PROFILE_FIRST_PAGE.search(first_pages_text):
                return False
            # Check all pages for Part 4 marker
            for page in pdf.pages:
                text = page.extract_text() or ""
                if _PART4_MARKER.search(text):
                    return True
    except Exception:
        return False
    return False


# ── Part 4 text extraction ─────────────────────────────────────────────

# Running footer to strip
_FOOTER_RE = re.compile(
    r"Appendix\s*2A\s*[-–]\s*Application\s+for\s+quotation\s+of\s+securities\s*\d+\s*/\s*\d+",
    re.I,
)

# Also strip the "For personal use only" watermark
_WATERMARK_RE = re.compile(r"ylno\s*esu\s*lanosrep\s*roF", re.I)


def _locate_part_4_text(pdf_bytes: bytes) -> str:
    """Extract text from Part 4 onwards. Raises on failure."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                raise ExtractionError("scanned_pdf_no_text")

            all_texts = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                all_texts.append(text)

            if not any(t.strip() for t in all_texts):
                raise ExtractionError("scanned_pdf_no_text")

            # Find the page where Part 4 starts
            part4_start = None
            for i, text in enumerate(all_texts):
                if _PART4_MARKER.search(text):
                    part4_start = i
                    break

            if part4_start is None:
                raise MalformedDocumentError("appendix_2a_missing_part4")

            # Concatenate from Part 4 page to end
            raw = "\n".join(all_texts[part4_start:])

            # Strip footers and watermarks
            raw = _FOOTER_RE.sub("", raw)
            raw = _WATERMARK_RE.sub("", raw)

            return raw

    except (ExtractionError, MalformedDocumentError):
        raise
    except Exception as e:
        raise ExtractionError(f"pdf_read_error:{type(e).__name__}")


# ── Part 4.1: Quoted securities ────────────────────────────────────────

_SECTION_41_START = re.compile(r"4\.1\s+Quoted\s+\+?securities", re.I)
_SECTION_42_START = re.compile(r"4\.2\s+Unquoted\s+\+?securities", re.I)

# Match: CODE : DESCRIPTION  NUMBER
_QUOTED_ROW_RE = re.compile(
    r"^([A-Z0-9]{2,6})\s*:\s*([A-Z][A-Z0-9 .\-/()]+?)\s+([\d,]+)\s*$",
    re.MULTILINE,
)


def _extract_quoted_classes(part4_text: str) -> list[QuotedClass]:
    """Extract quoted security classes from Part 4.1 section."""
    # Find the text between 4.1 and 4.2 headers
    m_start = _SECTION_41_START.search(part4_text)
    m_end = _SECTION_42_START.search(part4_text)

    if not m_start:
        return []

    section_text = part4_text[m_start.end():m_end.start() if m_end else len(part4_text)]

    results = []
    for m in _QUOTED_ROW_RE.finditer(section_text):
        code = m.group(1).strip()
        desc = m.group(2).strip()
        count = int(m.group(3).replace(",", ""))
        results.append(QuotedClass(asx_code=code, description=desc, total_on_issue=count))

    return results


# ── Part 4.2: Unquoted securities ──────────────────────────────────────

_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_expiry(s: str) -> Optional[date]:
    """Parse '07-OCT-2028' to date."""
    m = re.match(r"(\d{2})-([A-Z]{3})-(\d{4})", s)
    if not m:
        return None
    day, mon, year = int(m.group(1)), _MONTH_MAP.get(m.group(2)), int(m.group(3))
    if mon is None:
        return None
    return date(year, mon, day)


# Option: CODE : OPTION EXPIRING DD-MON-YYYY EX $X.XX  COUNT
_OPT_RE = re.compile(
    r"^([A-Z0-9]{3,8})\s*:\s*"
    r"OPTION\s+EXPIRING\s+(\d{2}-[A-Z]{3}-\d{4})\s+"
    r"EX\s+\$([\d]+\.[\d]+)\s+"
    r"([\d,]+)\s*$",
    re.MULTILINE,
)

# Convertible note: CODE : CONVERTIBLE NOTE(S)  COUNT
_CN_RE = re.compile(
    r"^([A-Z0-9]{3,8})\s*:\s*"
    r"CONVERTIBLE\s+NOTES?\s+"
    r"([\d,]+)\s*$",
    re.MULTILINE,
)

# Performance rights: CODE : PERFORMANCE RIGHT(S) [EXPIRING DD-MON-YYYY]  COUNT
_PERF_RE = re.compile(
    r"^([A-Z0-9]{3,8})\s*:\s*"
    r"PERFORMANCE\s+RIGHTS?"
    r"(?:\s+EXPIRING\s+(\d{2}-[A-Z]{3}-\d{4}))?\s+"
    r"([\d,]+)\s*$",
    re.MULTILINE,
)

# Generic unquoted row (fallback for anything starting with CODE :)
_GENERIC_ROW_RE = re.compile(
    r"^([A-Z0-9]{3,8})\s*:\s*(.+?)\s+([\d,]+)\s*$",
    re.MULTILINE,
)


def _extract_unquoted_list(part4_text: str) -> tuple[list[UnquotedInstrument], list[str]]:
    """Extract unquoted instruments from Part 4.2 section.

    Returns (instruments, warnings).
    """
    m_start = _SECTION_42_START.search(part4_text)
    if not m_start:
        return [], []

    section_text = part4_text[m_start.end():]
    warnings: list[str] = []

    # Find all generic rows first to know the universe
    all_rows: dict[int, str] = {}  # offset -> raw line
    for m in _GENERIC_ROW_RE.finditer(section_text):
        all_rows[m.start()] = m.group(0).strip()

    if not all_rows and section_text.strip():
        warnings.append("unquoted_section_present_but_no_rows_parsed")
        return [], warnings

    instruments: list[UnquotedInstrument] = []
    matched_offsets: set[int] = set()

    # Options
    for m in _OPT_RE.finditer(section_text):
        code = m.group(1)
        expiry = _parse_expiry(m.group(2))
        strike = Decimal(m.group(3))
        count = int(m.group(4).replace(",", ""))
        raw = m.group(0).strip()
        instruments.append(UnquotedInstrument(
            asx_code=code,
            description=raw[len(code)+3:].rsplit(m.group(4), 1)[0].strip(),
            instrument_type="option",
            total_on_issue=count,
            expiry_date=expiry,
            strike_aud=strike,
            raw_line=raw,
        ))
        matched_offsets.add(m.start())
        if expiry is None:
            warnings.append(f"option_unparsed_expiry: {raw}")

    # Convertible notes
    for m in _CN_RE.finditer(section_text):
        code = m.group(1)
        count = int(m.group(2).replace(",", ""))
        raw = m.group(0).strip()
        instruments.append(UnquotedInstrument(
            asx_code=code,
            description="CONVERTIBLE NOTES",
            instrument_type="convertible_note",
            total_on_issue=count,
            expiry_date=None,
            strike_aud=None,
            raw_line=raw,
        ))
        matched_offsets.add(m.start())

    # Performance rights
    for m in _PERF_RE.finditer(section_text):
        code = m.group(1)
        expiry = _parse_expiry(m.group(2)) if m.group(2) else None
        count = int(m.group(3).replace(",", ""))
        raw = m.group(0).strip()
        instruments.append(UnquotedInstrument(
            asx_code=code,
            description="PERFORMANCE RIGHTS",
            instrument_type="performance_right",
            total_on_issue=count,
            expiry_date=expiry,
            strike_aud=None,
            raw_line=raw,
        ))
        matched_offsets.add(m.start())

    # Catch unmatched rows as "other"
    for offset, raw_line in all_rows.items():
        if offset in matched_offsets:
            continue
        m = _GENERIC_ROW_RE.match(raw_line)
        if not m:
            continue
        code = m.group(1)
        desc = m.group(2).strip()
        count = int(m.group(3).replace(",", ""))
        instruments.append(UnquotedInstrument(
            asx_code=code,
            description=desc,
            instrument_type="other",
            total_on_issue=count,
            expiry_date=None,
            strike_aud=None,
            raw_line=raw_line,
        ))
        warnings.append(f"unquoted_row_unparsed: {raw_line}")

    # Sort by order of appearance (by ASX code as proxy — maintain doc order)
    # Re-sort based on original position in section_text
    def _sort_key(inst: UnquotedInstrument) -> int:
        pos = section_text.find(inst.raw_line)
        return pos if pos >= 0 else 9999
    instruments.sort(key=_sort_key)

    return instruments, warnings


# ── Validation & reconciliation ────────────────────────────────────────

def _validate_and_reconcile(
    quoted: list[QuotedClass],
    unquoted: list[UnquotedInstrument],
    warnings: list[str],
) -> tuple[int, int, int, int, int]:
    """Compute derived totals. Raises ReconciliationError if no quoted classes."""
    shares_basic = sum(q.total_on_issue for q in quoted)
    if shares_basic == 0:
        raise ReconciliationError("no_quoted_classes_parsed")

    options = sum(u.total_on_issue for u in unquoted if u.instrument_type == "option")
    cn = sum(u.total_on_issue for u in unquoted if u.instrument_type == "convertible_note")
    pr = sum(u.total_on_issue for u in unquoted if u.instrument_type == "performance_right")

    total_unquoted = sum(u.total_on_issue for u in unquoted)
    shares_fd_naive = shares_basic + total_unquoted

    # Invariant check
    assert options + cn + pr + sum(
        u.total_on_issue for u in unquoted if u.instrument_type == "other"
    ) == total_unquoted

    return shares_basic, shares_fd_naive, options, cn, pr


# ── Public API ─────────────────────────────────────────────────────────

def parse(
    pdf_bytes: bytes,
    ticker: str,
    doc_id: str,
    announcement_date: date,
) -> Appendix2ACapitalStructure:
    """
    Parse an ASX Appendix 2A PDF and return the post-issuance capital structure.

    Only Part 4 of the document is parsed.

    Args:
        pdf_bytes:         raw PDF bytes from the ingestion layer
        ticker:            issuer ticker (e.g. "HTG")
        doc_id:            Quantyc document ID for provenance
        announcement_date: from ASX API metadata — used as snapshot_date

    Returns:
        Appendix2ACapitalStructure — fully-validated, reconciled dataclass.

    Raises:
        ExtractionError:         scanned PDF / empty text
        MalformedDocumentError:  profile matched but Part 4 missing
        ReconciliationError:     no quoted classes parsed (parser bug or corrupt doc)
    """
    part4_text = _locate_part_4_text(pdf_bytes)

    quoted = _extract_quoted_classes(part4_text)
    unquoted, warnings = _extract_unquoted_list(part4_text)

    shares_basic, shares_fd_naive, options, cn, pr = _validate_and_reconcile(
        quoted, unquoted, warnings
    )

    return Appendix2ACapitalStructure(
        ticker=ticker,
        doc_id=doc_id,
        snapshot_date=announcement_date,
        parsed_at=datetime.now(timezone.utc),
        parser_version=PARSER_VERSION,
        quoted_classes=quoted,
        unquoted_instruments=unquoted,
        shares_basic=shares_basic,
        shares_fd_naive=shares_fd_naive,
        options_outstanding=options,
        convertible_notes_face_count=cn,
        performance_rights_count=pr,
        extraction_warnings=warnings,
    )

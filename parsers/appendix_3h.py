"""
Appendix 3G / 3H Parser — Post-cessation / post-issue Capital Structure.

Parses Part 3 (Issued capital following changes) of ASX Appendix 3G
(Notification of issue, conversion or payment) and Appendix 3H (Notification
of cessation of securities) PDFs.

Produces the same Appendix2ACapitalStructure dataclass as the 2A parser so
the downstream normalizer is reused.

Pure regex + pdfplumber. No LLM, no OCR, no DB writes, stateless.
"""

import io
import re
from datetime import date, datetime, timezone

import pdfplumber

from .appendix_2a import (
    _QUOTED_ROW_RE, _FOOTER_RE, _WATERMARK_RE,
    _extract_quoted_classes, _parse_unquoted_section,
    _validate_and_reconcile,
    ExtractionError, MalformedDocumentError, ReconciliationError,
)
from .appendix_2a_schemas import Appendix2ACapitalStructure

PARSER_VERSION = "0.1.0"

# ── Profile detection ──────────────────────────────────────────────────

_PROFILE_3H_FIRST_PAGE = re.compile(
    r"Appendix\s*3H\s*[-–]\s*Notification\s+of\s+cessation",
    re.I,
)
_PROFILE_3G_FIRST_PAGE = re.compile(
    r"Appendix\s*3G\s*[-–]\s*Notification\s+of\s+issue",
    re.I,
)
_PART3_MARKER = re.compile(
    r"Part\s*3\s*[-–]\s*Issued\s+capital\s+following\s+changes",
    re.I,
)

# Section anchors inside Part 3
_SECTION_31_START = re.compile(r"3\.1\s+Quoted\s+\+?equity\s+securities", re.I)
_SECTION_32_START = re.compile(r"3\.2\s+Unquoted\s+\+?equity\s+securities", re.I)

# Footer patterns to strip
_FOOTER_3H_RE = re.compile(
    r"Appendix\s*3[HG]\s*[-–].*?\d+\s*/\s*\d+",
    re.I,
)


def detect_profile(pdf_bytes: bytes) -> tuple[bool, str | None]:
    """Return (True, subtype) or (False, None).

    subtype is 'appendix_3h' or 'appendix_3g'.
    Returns (False, None) for 3B or any non-3G/3H PDF.
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if not pdf.pages:
                return False, None

            # Check first 2 pages for 3G or 3H title
            first_pages = ""
            for page in pdf.pages[:2]:
                first_pages += (page.extract_text() or "") + "\n"

            subtype = None
            if _PROFILE_3H_FIRST_PAGE.search(first_pages):
                subtype = "appendix_3h"
            elif _PROFILE_3G_FIRST_PAGE.search(first_pages):
                subtype = "appendix_3g"
            else:
                return False, None

            # Must have Part 3 somewhere (rejects 3B)
            for page in pdf.pages:
                text = page.extract_text() or ""
                if _PART3_MARKER.search(text):
                    return True, subtype

    except Exception:
        return False, None

    return False, None


# ── Part 3 text extraction ─────────────────────────────────────────────

def _locate_part_3_text(pdf_bytes: bytes) -> str:
    """Extract text from Part 3 onwards. Raises on failure."""
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

            part3_start = None
            for i, text in enumerate(all_texts):
                if _PART3_MARKER.search(text):
                    part3_start = i
                    break

            if part3_start is None:
                raise MalformedDocumentError("appendix_3h_missing_part3")

            raw = "\n".join(all_texts[part3_start:])

            # Strip footers and watermarks
            raw = _FOOTER_3H_RE.sub("", raw)
            raw = _WATERMARK_RE.sub("", raw)

            return raw

    except (ExtractionError, MalformedDocumentError):
        raise
    except Exception as e:
        raise ExtractionError(f"pdf_read_error:{type(e).__name__}")


# ── Section extraction ─────────────────────────────────────────────────

def _extract_quoted_from_part3(part3_text: str) -> tuple:
    """Extract quoted classes from section 3.1."""
    m_start = _SECTION_31_START.search(part3_text)
    m_end = _SECTION_32_START.search(part3_text)

    if not m_start:
        return [], []

    section_text = part3_text[m_start.end():m_end.start() if m_end else len(part3_text)]
    return _extract_quoted_classes(section_text)


def _extract_unquoted_from_part3(part3_text: str) -> tuple[list, list[str]]:
    """Extract unquoted instruments from section 3.2."""
    m_start = _SECTION_32_START.search(part3_text)
    if not m_start:
        return [], []
    return _parse_unquoted_section(part3_text[m_start.end():])


# ── Public API ─────────────────────────────────────────────────────────

def parse(
    pdf_bytes: bytes,
    ticker: str,
    doc_id: str,
    announcement_date: date,
) -> Appendix2ACapitalStructure:
    """
    Parse an ASX Appendix 3G or 3H PDF and return the post-event capital structure.

    Same signature and return type as parsers.appendix_2a.parse so the
    downstream normalizer is agnostic.
    """
    part3_text = _locate_part_3_text(pdf_bytes)

    quoted_shares, quoted_non_shares = _extract_quoted_from_part3(part3_text)
    unquoted, warnings = _extract_unquoted_from_part3(part3_text)
    # HC4: quoted options go into unquoted list
    unquoted = list(quoted_non_shares) + list(unquoted)

    shares_basic, shares_fd_naive, options, cn, pr = _validate_and_reconcile(
        quoted_shares, unquoted, warnings,
    )

    return Appendix2ACapitalStructure(
        ticker=ticker,
        doc_id=doc_id,
        snapshot_date=announcement_date,
        parsed_at=datetime.now(timezone.utc),
        parser_version=PARSER_VERSION,
        quoted_classes=quoted_shares,
        unquoted_instruments=unquoted,
        shares_basic=shares_basic,
        shares_fd_naive=shares_fd_naive,
        options_outstanding=options,
        convertible_notes_face_count=cn,
        performance_rights_count=pr,
        extraction_warnings=warnings,
    )

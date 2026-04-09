"""
Exploration Results Parser

Deterministic, LLM-free parser for ASX/TSX exploration results announcements.
Validated against Southern Cross Gold (ASX:SX2 / TSX:SXGC) drill-results releases.

Usage:
    from pipeline.parsers.exploration_results import parse
    payload = parse(pdf_path, ticker="SX2", doc_id="abc123")
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber

from pipeline.parsers._extractors.header import extract_release_date
from pipeline.parsers._extractors.headline import extract_headline_intercepts
from pipeline.parsers._extractors.metal_equivalent import extract_metal_equivalent
from pipeline.parsers._extractors.project_totals import extract_project_totals
from pipeline.parsers._extractors.tables import extract_all_tables
from pipeline.parsers._extractors.validation import validate_composite_intersections
from pipeline.parsers.schemas import (
    ExplorationResultsPayload,
    ExtractionError,
    ExtractionWarning,
)

PARSER_VERSION = "0.1.0"

logger = logging.getLogger("parsers.exploration_results")


class WrongDocumentTypeError(Exception):
    """Raised when the PDF does not match the exploration_results profile."""

    def __init__(self, reason: str, detected_profile: str | None = None):
        self.reason = reason
        self.detected_profile = detected_profile
        super().__init__(reason)


class PDFReadError(Exception):
    """Raised when pdfplumber cannot open the PDF."""


class EmptyPDFError(Exception):
    """Raised when the PDF has zero pages."""


# --- Document profile detection ---

_POSITIVE_KEYWORDS = [
    "drill hole", "drilling", "drill results", "intersection",
    "intercepted", "vein set", "g/t au", "g/t aueq",
]

_NEGATIVE_KEYWORDS = [
    "shelf prospectus", "index inclusion", "appendix 5b",
    "quarterly cash flow", "change of director", "ceasing to be",
    "substantial holder", "placement", "capital raising",
]


def _detect_profile(pdf: pdfplumber.PDF, early_page_texts: list[str]) -> None:
    """
    Verify the PDF matches the exploration_results profile.
    Uses pre-extracted text for early pages and targeted search for JORC.
    Raises WrongDocumentTypeError if it doesn't.
    """
    if len(pdf.pages) == 0:
        raise EmptyPDFError("PDF has 0 pages")

    # Check pages 1-2 for positive keywords
    check_text = " ".join(early_page_texts[:2]).lower()

    has_positive = any(kw in check_text for kw in _POSITIVE_KEYWORDS)
    if not has_positive:
        raise WrongDocumentTypeError(
            reason="No drill/exploration keywords found on pages 1-2",
            detected_profile="unknown",
        )

    # Check page 1 title/headline area for negative keywords
    title_area = early_page_texts[0].lower()[:600]
    for kw in _NEGATIVE_KEYWORDS:
        if kw in title_area:
            raise WrongDocumentTypeError(
                reason=f"Negative keyword '{kw}' found in title area",
                detected_profile=kw.replace(" ", "_"),
            )

    # Check for JORC Table 1 section — search from the back (JORC is at the end)
    # Only check last 5 pages for speed
    has_jorc = False
    start_page = max(0, len(pdf.pages) - 5)
    for page in pdf.pages[start_page:]:
        text = (page.extract_text() or "").lower()
        if "jorc code explanation" in text or "section 1 sampling techniques" in text or "section 1: sampling techniques" in text:
            has_jorc = True
            break
    # If not found in last 5, try last 15 (JORC section can start earlier)
    if not has_jorc:
        start_page2 = max(0, len(pdf.pages) - 15)
        for page in pdf.pages[start_page2:start_page]:
            text = (page.extract_text() or "").lower()
            if "jorc code explanation" in text or "section 1 sampling techniques" in text or "section 1: sampling techniques" in text:
                has_jorc = True
                break
    if not has_jorc:
        raise WrongDocumentTypeError(
            reason="No JORC Table 1 section found in document",
            detected_profile="non_jorc_exploration",
        )

    logger.info("Document profile detection passed: exploration_results")


def parse(
    pdf_path: Path | str,
    ticker: str,
    doc_id: str,
) -> ExplorationResultsPayload:
    """
    Parse an exploration results PDF and return a structured payload.

    Args:
        pdf_path: Path to the PDF file
        ticker: Company ticker (e.g. "SX2")
        doc_id: Quantyc internal document ID

    Returns:
        ExplorationResultsPayload with all extracted data

    Raises:
        WrongDocumentTypeError: PDF is not an exploration results document
        PDFReadError: pdfplumber cannot open the file
        EmptyPDFError: PDF has zero pages
    """
    pdf_path = Path(pdf_path)
    warnings: list[ExtractionWarning] = []
    errors: list[ExtractionError] = []

    # Open PDF
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        raise PDFReadError(f"Cannot open {pdf_path}: {e}") from e

    try:
        # Extract text from narrative pages only — metal equiv is typically on page 8
        # Skip figure/image pages (7, 9-14) which are slow and have no text data
        narrative_limit = min(9, len(pdf.pages))
        narrative_texts = [(pdf.pages[i].extract_text() or "") for i in range(narrative_limit)]

        # Profile detection — uses narrative texts + targeted JORC search
        _detect_profile(pdf, narrative_texts)

        # --- Header: release date ---
        release_date = extract_release_date(narrative_texts[0])
        if release_date is None:
            warnings.append(ExtractionWarning(
                code="MISSING_RELEASE_DATE",
                message="Could not extract release date from page 1 header",
                severity="medium",
                source_page=1,
            ))

        # --- Headline intercepts ---
        headline_intercepts = extract_headline_intercepts(narrative_texts[0])
        headline = headline_intercepts[0] if headline_intercepts else None
        if headline is None:
            warnings.append(ExtractionWarning(
                code="MISSING_HEADLINE_INTERCEPT",
                message="No headline intercept found on page 1",
                severity="medium",
                source_page=1,
            ))

        # --- Project totals (search narrative pages) ---
        project_totals, totals_warnings = extract_project_totals(narrative_texts)
        warnings.extend(totals_warnings)
        if project_totals is None:
            warnings.append(ExtractionWarning(
                code="MISSING_PROJECT_TOTALS",
                message="Could not extract project totals snapshot",
                severity="medium",
            ))

        # --- Metal equivalent (search narrative pages) ---
        metal_equivalent = extract_metal_equivalent(narrative_texts)
        if metal_equivalent is None:
            warnings.append(ExtractionWarning(
                code="MISSING_METAL_EQUIVALENT",
                message="AuEq formula and price assumptions not found",
                severity="high",
            ))

        # --- Tables (single-pass extraction) ---
        collars, composites, individuals, table_warnings = extract_all_tables(pdf)
        warnings.extend(table_warnings)

        if not composites and not individuals:
            errors.append(ExtractionError(
                code="NO_TABLES_EXTRACTED",
                message="No intercept tables could be extracted from the document",
            ))

        # --- Validation ---
        multiplier = metal_equivalent.multiplier if metal_equivalent else None
        validation_warnings = validate_composite_intersections(composites, multiplier)
        warnings.extend(validation_warnings)

        payload = ExplorationResultsPayload(
            doc_id=doc_id,
            ticker=ticker,
            parser_version=PARSER_VERSION,
            parsed_at=datetime.now(timezone.utc),
            release_date=release_date,
            headline_intercept=headline,
            all_headline_intercepts=headline_intercepts,
            project_totals=project_totals,
            metal_equivalent=metal_equivalent,
            drill_collars=collars,
            composite_intersections=composites,
            individual_assays=individuals,
            extraction_warnings=warnings,
            extraction_errors=errors,
        )

        logger.info(
            "Parse complete: %d collars, %d composites, %d assays, %d warnings, %d errors",
            len(collars), len(composites), len(individuals), len(warnings), len(errors),
        )
        return payload

    finally:
        pdf.close()

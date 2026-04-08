"""
Section Finder

Finds the most relevant pages within a PDF using keyword scoring.
Used to select the 2-3 pages to send to the LLM extractor,
ensuring we never send a full document.

Usage:
    from pipeline.section_finder import find_relevant_pages
    pages = find_relevant_pages(pdf_path, "resource")
    pages = find_relevant_pages(io.BytesIO(pdf_bytes), "resource")
"""

import io
import logging
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)

# Keyword maps — each key maps to a list of terms that indicate
# the page is relevant to that field type.
SECTION_KEYWORDS = {
    "resource": [
        "mineral resource", "jorc", "measured", "indicated", "inferred",
        "contained metal", "resource estimate", "resource statement",
        "ore reserve", "proven", "probable", "cut-off grade", "cut off grade",
    ],
    "cash": [
        "cash and cash equivalents", "appendix 5b", "net cash",
        "cash at end", "cash flow",
    ],
    "capex": [
        "capital expenditure", "initial capital", "capex", "capital cost",
        "pre-production capital", "sustaining capital",
    ],
    "npv": [
        "net present value", "npv", "irr", "internal rate",
        "post-tax", "post tax", "pre-tax", "discount rate",
    ],
    "ownership": [
        "ownership", "earn-in", "earn in", "joint venture",
        "royalty", "nsr", "attributable", "interest",
    ],
    "shares": [
        "shares on issue", "fully diluted", "options on issue",
        "performance rights", "warrants", "convertible",
    ],
    "study": [
        "scoping study", "pre-feasibility", "pfs", "definitive feasibility",
        "dfs", "feasibility study", "mine life", "recovery", "throughput",
        "annual production", "life of mine", "operating cost", "opex",
    ],
    "capital_raise": [
        "placement", "share purchase plan", "entitlement offer",
        "rights issue", "issue of securities", "new shares",
        "exercise price", "option", "warrant",
    ],
}


def extract_page_texts(pdf_source: str | Path | io.BytesIO) -> list[str]:
    """
    Extract text from each page of a PDF.
    Returns a list where index i is the text of page i.
    Accepts a file path (str/Path) or in-memory BytesIO.
    """
    pages = []
    try:
        pdf = pdfplumber.open(pdf_source)
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages.append(text)
        pdf.close()
    except Exception as e:
        logger.error("Failed to read PDF %s: %s", type(pdf_source).__name__, e)
    return pages


def score_page(page_text: str, keywords: list[str]) -> int:
    """Score a page by counting keyword matches (case-insensitive)."""
    text_lower = page_text.lower()
    return sum(1 for kw in keywords if kw in text_lower)


def find_relevant_pages(
    pdf_source: str | Path | io.BytesIO,
    section_type: str,
    max_pages: int = 3,
    min_score: int = 1,
) -> list[str]:
    """
    Find the top pages in a PDF relevant to a given section type.

    Args:
        pdf_source: Path to the PDF file, or in-memory BytesIO.
        section_type: Key into SECTION_KEYWORDS (e.g. "resource", "npv").
        max_pages: Maximum number of pages to return.
        min_score: Minimum keyword score for a page to be included.

    Returns:
        List of page text strings, sorted by relevance score (highest first).
    """
    keywords = SECTION_KEYWORDS.get(section_type)
    if keywords is None:
        logger.error("Unknown section type: %s", section_type)
        return []

    page_texts = extract_page_texts(pdf_source)
    if not page_texts:
        return []

    # Score each page
    scored = []
    for i, text in enumerate(page_texts):
        s = score_page(text, keywords)
        if s >= min_score:
            scored.append((s, i, text))

    # Sort by score descending, then by page number ascending (prefer earlier pages)
    scored.sort(key=lambda x: (-x[0], x[1]))

    results = [text for _, _, text in scored[:max_pages]]
    label = pdf_source if isinstance(pdf_source, str) else type(pdf_source).__name__
    if results:
        page_nums = [i + 1 for _, i, _ in scored[:max_pages]]
        logger.info(
            "Found %d relevant pages for '%s' in %s (pages: %s)",
            len(results), section_type, label, page_nums,
        )
    else:
        logger.warning("No pages scored >= %d for '%s' in %s", min_score, section_type, label)

    return results


def find_relevant_pages_multi(
    pdf_source: str | Path | io.BytesIO,
    section_types: list[str],
    max_pages: int = 3,
    min_score: int = 1,
) -> list[str]:
    """
    Find relevant pages across multiple section types, deduplicating.

    Useful when a single extraction needs keywords from multiple categories
    (e.g., a study needs both 'npv' and 'capex' keywords).
    """
    # Merge keywords from all section types
    merged_keywords = []
    for st in section_types:
        kws = SECTION_KEYWORDS.get(st, [])
        merged_keywords.extend(kws)

    if not merged_keywords:
        logger.error("No keywords found for section types: %s", section_types)
        return []

    page_texts = extract_page_texts(pdf_source)
    if not page_texts:
        return []

    scored = []
    for i, text in enumerate(page_texts):
        s = score_page(text, merged_keywords)
        if s >= min_score:
            scored.append((s, i, text))

    scored.sort(key=lambda x: (-x[0], x[1]))

    results = [text for _, _, text in scored[:max_pages]]
    if results:
        label = pdf_source if isinstance(pdf_source, str) else type(pdf_source).__name__
        page_nums = [i + 1 for _, i, _ in scored[:max_pages]]
        logger.info(
            "Found %d relevant pages for %s in %s (pages: %s)",
            len(results), section_types, label, page_nums,
        )

    return results

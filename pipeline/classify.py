"""
Document Classifier

Deterministic, no-LLM classification of ASX announcements.

Strategy:
1. Try the ASX-provided headline string against a keyword→type map.
2. Fall back to the first page's text (via pdfplumber on BytesIO).
3. If no match, return 'other'.

Output doc_types:
    appendix_5b, quarterly_activity, annual_report, half_year_report,
    issue_of_securities, placement, resource_update,
    study_scoping, study_pfs, study_dfs,
    presentation, other
"""

import io
import logging

logger = logging.getLogger(__name__)

# Checked in priority order — most specific first.
HEADLINE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("appendix_5b", [
        "appendix 5b", "quarterly cash flow",
    ]),
    ("study_dfs", [
        "definitive feasibility", "dfs",
    ]),
    ("study_pfs", [
        "pre-feasibility", "pfs",
    ]),
    ("study_scoping", [
        "scoping study", "preliminary economic assessment",
    ]),
    ("issue_of_securities", [
        "appendix 3g", "appendix 3b", "appendix 2a",
        "notification of issue", "issue, conversion or payment",
        "conversion of securities", "exercise of options",
        "vesting of performance rights",
        "application for quotation of securities",
        "statement of cdis", "cdis on issue",
        "cessation of securities",
    ]),
    ("placement", [
        "placement", "entitlement offer", "rights issue",
        "capital raising", "share purchase plan", "spp",
    ]),
    ("resource_update", [
        "resource estimate", "reserve estimate", "jorc resource",
        "mineral resource", "ore reserve", "resource update", "maiden resource",
    ]),
    ("half_year_report", [
        "half year", "half-year", "interim financial", "6 month",
    ]),
    ("annual_report", [
        "annual report", "annual financial",
    ]),
    ("quarterly_activity", [
        "quarterly activity", "quarterly report", "operations update",
        "quarterly activities", "quarterly production",
        "quarterly update", "quarter update", "quarter report",
        "quarter financials",
        "march quarter", "june quarter",
        "september quarter", "december quarter",
        "first quarter", "second quarter", "third quarter", "fourth quarter",
        "q1 ", "q2 ", "q3 ", "q4 ",
        "investment update",
    ]),
    ("presentation", [
        "investor presentation", "corporate presentation",
        "investor update", "corporate update", "presentation",
    ]),
]

# First-page text patterns (fallback when headline is missing/unhelpful)
FIRST_PAGE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("appendix_5b", [
        "appendix 5b", "mining exploration entity",
        "quarterly cash flow report",
    ]),
    ("issue_of_securities", [
        "notification of issue", "appendix 3g",
        "securities issued or to be issued",
    ]),
    ("annual_report", [
        "annual report", "directors' report",
    ]),
    ("half_year_report", [
        "half year financial", "interim financial report",
    ]),
]


def classify_headline(headline: str) -> str | None:
    """Classify from headline text. Returns doc_type or None."""
    h = headline.lower().strip()
    for doc_type, keywords in HEADLINE_KEYWORDS:
        for kw in keywords:
            if kw in h:
                return doc_type
    return None


def classify_first_page(text: str) -> str | None:
    """Classify from first-page text. Returns doc_type or None."""
    t = text.lower()
    for doc_type, keywords in FIRST_PAGE_KEYWORDS:
        for kw in keywords:
            if kw in t:
                return doc_type
    return None


def classify(
    headline: str | None = None,
    pdf_bytes: bytes | None = None,
) -> str:
    """
    Classify a document. Tries headline first, then first-page text.
    Returns a doc_type string.
    """
    # Strategy 1: headline
    if headline:
        result = classify_headline(headline)
        if result:
            return result

    # Strategy 2: first page of PDF
    if pdf_bytes:
        try:
            import pdfplumber
            bio = io.BytesIO(pdf_bytes)
            with pdfplumber.open(bio) as pdf:
                if pdf.pages:
                    text = pdf.pages[0].extract_text() or ""
                    result = classify_first_page(text)
                    if result:
                        return result
        except Exception as e:
            logger.warning("Could not read PDF for classification: %s", e)

    return "other"

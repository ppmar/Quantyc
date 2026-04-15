"""
Narrative / General LLM Extractor

Extracts capital structure data from any document type using LLM
when rule-based extraction fails or isn't available.

Handles: quarterly_activity, presentation (fallback), other docs
that may contain cash, shares, or production data.

Writes to _stg_presentation staging table (same schema).
"""

import io
import json
import logging
from datetime import datetime, timezone

import pdfplumber

from db import get_connection
from pipeline.extractors.llm_fallback import extract_with_llm
from pipeline.extractors.presentation import _score_page

logger = logging.getLogger(__name__)

# Fields we want the LLM to extract
TARGET_FIELDS = {
    "cash": "Cash and cash equivalents or cash on hand in reporting currency (number, no units). Include gold/bullion value if combined with cash.",
    "debt": "Total debt in reporting currency (number, no units). null if not mentioned.",
    "shares_basic": "Basic shares on issue / outstanding (number). null if not mentioned.",
    "shares_fd": "Fully diluted shares (number). null if not mentioned.",
    "options_outstanding": "Options on issue (number). null if not mentioned.",
    "perf_rights_outstanding": "Performance rights on issue (number). null if not mentioned.",
    "effective_date": "Date the figures are as-of, in YYYY-MM-DD format. Use quarter-end date if mentioned.",
    "quarterly_opex_burn": "Operating cash outflow for the quarter (number, positive). null if not mentioned.",
}

# Pages that are likely to have financial data
FINANCIAL_KEYWORDS = [
    "cash", "bullion", "balance sheet", "financial position",
    "shares on issue", "capital structure", "market cap",
    "cash flow", "operating cashflow", "free cash flow",
    "production", "quarterly",
]


def _score_financial_page(text: str) -> int:
    """Score a page for financial content relevance."""
    t = text.lower()
    score = 0
    for kw in FINANCIAL_KEYWORDS:
        if kw in t:
            score += 3
    # Bonus for presentation-style pages
    score += _score_page(text)
    return score


def _pick_best_pages(pages: list[str], max_pages: int = 3) -> list[str]:
    """Pick the most relevant pages for extraction."""
    if len(pages) <= max_pages:
        return pages

    scored = [(i, _score_financial_page(p), p) for i, p in enumerate(pages)]
    scored.sort(key=lambda x: x[1], reverse=True)

    # Need at least some signal
    best = [p for _, s, p in scored[:max_pages] if s >= 3]
    return best if best else []


def _normalize_value(val) -> float | None:
    """Normalize LLM-returned values to float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        val = val.strip().replace(",", "").replace("$", "")
        val = val.replace("A", "").replace("C", "").replace("US", "")
        multiplier = 1
        if val.upper().endswith("B"):
            multiplier = 1e9
            val = val[:-1]
        elif val.upper().endswith("M"):
            multiplier = 1e6
            val = val[:-1]
        elif val.upper().endswith("K"):
            multiplier = 1e3
            val = val[:-1]
        try:
            return float(val.strip()) * multiplier
        except ValueError:
            return None
    return None


def extract_narrative(document_id: int, pdf_bytes: bytes) -> dict | None:
    """
    Extract financial data from any document using LLM.
    Writes to _stg_presentation staging table.
    Returns extracted dict or None.
    """
    logger.info("LLM extracting narrative data for doc %d", document_id)

    pages = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
    except Exception as e:
        logger.error("Failed to open PDF for doc %d: %s", document_id, e)
        return None

    if not pages:
        return None

    best_pages = _pick_best_pages(pages)
    if not best_pages:
        logger.info("No financially relevant pages in doc %d", document_id)
        return None

    result, _confidence = extract_with_llm(
        doc_type="financial_narrative",
        page_texts=best_pages,
        target_fields=TARGET_FIELDS,
    )

    if not result:
        logger.warning("LLM extraction returned nothing for doc %d", document_id)
        return None

    # Normalize numeric values
    cleaned = {}
    for field in ["cash", "debt", "shares_basic", "shares_fd",
                  "options_outstanding", "perf_rights_outstanding",
                  "quarterly_opex_burn"]:
        cleaned[field] = _normalize_value(result.get(field))

    # Keep date as string
    cleaned["effective_date"] = result.get("effective_date")

    # Need at least cash or shares to be useful
    if not cleaned.get("cash") and not cleaned.get("shares_basic") and not cleaned.get("shares_fd"):
        logger.info("LLM found no cash/shares in doc %d", document_id)
        return None

    # Write to staging
    now = datetime.now(timezone.utc).isoformat()
    conn = get_connection()

    conn.execute(
        """INSERT OR REPLACE INTO _stg_presentation
           (document_id, effective_date, shares_basic, shares_fd,
            options_outstanding, perf_rights_outstanding, cash, debt,
            raw_json, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            document_id,
            cleaned.get("effective_date"),
            cleaned.get("shares_basic"),
            cleaned.get("shares_fd"),
            cleaned.get("options_outstanding"),
            cleaned.get("perf_rights_outstanding"),
            cleaned.get("cash"),
            cleaned.get("debt"),
            json.dumps(result, default=str),
            now,
        ),
    )

    conn.commit()
    conn.close()

    logger.info("LLM narrative staging for doc %d: %s", document_id, cleaned)
    return cleaned

"""
Generic Parser

Handles any ASX announcement type. Uses rule-based regex + table extraction
first, then tries LLM as an optional enhancement if credits are available.
Works as the fallback for doc types without a specialized parser.

Usage:
    from pipeline.parsers.generic import parse_generic
    result = parse_generic(doc_id, pdf_source=bio)
"""

import logging
import re

import pdfplumber

from db import get_connection
from pipeline.section_finder import extract_page_texts

logger = logging.getLogger(__name__)

# ── Regex patterns for common mining/financial fields ──

CASH_PATTERNS = [
    re.compile(r"cash\s+(?:and\s+cash\s+equiv[^\n]*?|at\s+end[^\n]*?|balance[^\n]*?)\$?([\d,]+(?:\.\d+)?)\s*(?:m(?:illion)?|M)", re.I),
    re.compile(r"cash\s+(?:and\s+cash\s+equiv[^\n]*?|at\s+end[^\n]*?)\(?\$?([\d,]+(?:\.\d+)?)\)?", re.I),
]

PRODUCTION_PATTERNS = [
    # "XX,XXX oz" or "XX koz"
    re.compile(r"(?:total\s+)?(?:gold\s+)?production[^\n]{0,40}?([\d,]+(?:\.\d+)?)\s*(oz|koz|Moz|tonnes?|kt|Mt|Mlb|t)", re.I),
    re.compile(r"([\d,]+(?:\.\d+)?)\s*(oz|koz|Moz|tonnes?|kt|Mt)\s+(?:of\s+)?(?:gold|copper|lithium|silver)", re.I),
    re.compile(r"(?:produced|poured)\s+([\d,]+(?:\.\d+)?)\s*(oz|koz|tonnes?|kt)", re.I),
]

AISC_PATTERNS = [
    re.compile(r"AISC[^\n]{0,30}?\$?([\d,]+(?:\.\d+)?)\s*/?\s*(oz|t|tonne)", re.I),
    re.compile(r"all.in\s+sustaining\s+cost[^\n]{0,30}?\$?([\d,]+(?:\.\d+)?)\s*/?\s*(oz|t)", re.I),
]

REVENUE_PATTERNS = [
    re.compile(r"revenue[^\n]{0,30}?\$?([\d,]+(?:\.\d+)?)\s*(?:m(?:illion)?|M)\b", re.I),
    re.compile(r"sales[^\n]{0,20}?\$?([\d,]+(?:\.\d+)?)\s*(?:m(?:illion)?|M)\b", re.I),
]

RESOURCE_PATTERNS = [
    re.compile(r"([\d,.]+)\s*(?:million\s+)?(?:tonnes?|Mt)\s+(?:at|@|grading)\s+([\d,.]+)\s*(g/t|%|ppm)", re.I),
]

NPV_PATTERNS = [
    re.compile(r"NPV[^\n]{0,30}?\$?([\d,]+(?:\.\d+)?)\s*(?:m(?:illion)?|M|bn|B)", re.I),
    re.compile(r"net\s+present\s+value[^\n]{0,30}?\$?([\d,]+(?:\.\d+)?)\s*(?:m(?:illion)?|M)", re.I),
]

IRR_PATTERNS = [
    re.compile(r"IRR[^\n]{0,20}?([\d,.]+)\s*%", re.I),
    re.compile(r"internal\s+rate\s+of\s+return[^\n]{0,20}?([\d,.]+)\s*%", re.I),
]

SHARES_PATTERNS = [
    re.compile(r"shares?\s+on\s+issue[^\n]{0,20}?([\d,]+(?:\.\d+)?)\s*(?:m(?:illion)?|M)?", re.I),
]

# LLM schemas per doc type (used when API credits are available)
LLM_SCHEMAS = {
    "quarterly_report": {
        "cash_at_end_quarter_aud": "Cash and cash equivalents at end of quarter in AUD (numeric)",
        "operating_cashflow_aud": "Net cash from/used in operating activities in AUD",
        "investing_cashflow_aud": "Net cash from/used in investing activities in AUD",
        "quarterly_production": "Total production for the quarter (numeric, null if not stated)",
        "production_unit": "Unit of production (e.g. oz, t, kt, koz)",
        "revenue_aud": "Revenue or sales for the quarter in AUD (null if not stated)",
        "aisc_per_unit": "All-in sustaining cost per unit (numeric, null if not stated)",
        "aisc_unit": "Unit of AISC (e.g. A$/oz, $/t)",
    },
    "annual_report": {
        "cash_at_year_end_aud": "Cash at year end in AUD",
        "total_revenue_aud": "Total revenue for the year in AUD",
        "shares_on_issue": "Total shares on issue (integer)",
        "annual_production": "Annual production (numeric, null if not a producer)",
        "production_unit": "Unit of production",
    },
    "other": {
        "document_summary": "One-sentence summary of what this announcement is about",
        "cash_mentioned_aud": "Any cash balance or amount raised mentioned in AUD (null if none)",
        "production_mentioned": "Any production figures mentioned (numeric, null if none)",
        "production_unit": "Unit of production if mentioned",
        "resource_mentioned_mt": "Any resource tonnage in million tonnes (null if none)",
        "grade_mentioned": "Any grade figure mentioned (null if none)",
        "grade_unit": "Unit of grade (e.g. g/t, %, ppm)",
    },
}


def _parse_number(text: str) -> float | None:
    """Parse a number, stripping commas."""
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def _extract_from_tables(pdf_source) -> dict:
    """Try to extract cash flow data from tables (works for Appendix 5B-like docs)."""
    results = {}
    cash_labels = ["cash and cash equivalents at end", "cash at end of quarter"]
    op_labels = ["net cash from / (used in) operating", "net cash from/(used in) operating",
                 "net cash used in operating", "net cash from operating"]
    inv_labels = ["net cash from / (used in) investing", "net cash from/(used in) investing",
                  "net cash used in investing", "net cash from investing"]

    try:
        if hasattr(pdf_source, "seek"):
            pdf_source.seek(0)
        pdf = pdfplumber.open(pdf_source)
    except Exception:
        return results

    for page in pdf.pages:
        for table in (page.extract_tables() or []):
            if not table:
                continue
            for row in table:
                if not row or not row[0]:
                    continue
                label = str(row[0]).lower().strip()

                target = None
                if any(p in label for p in cash_labels):
                    target = "cash_at_end_quarter_aud"
                elif any(p in label for p in op_labels):
                    target = "operating_cashflow_aud"
                elif any(p in label for p in inv_labels):
                    target = "investing_cashflow_aud"

                if target and target not in results:
                    for cell in row[1:]:
                        if cell:
                            cell_s = str(cell).strip().replace(",", "").replace("$", "").replace(" ", "")
                            neg = cell_s.startswith("(") and cell_s.endswith(")")
                            if neg:
                                cell_s = cell_s[1:-1]
                            m = re.search(r"[\d]+\.?\d*", cell_s)
                            if m:
                                val = float(m.group())
                                results[target] = -val if neg else val
                                break

    pdf.close()
    return results


def _extract_with_regex(full_text: str) -> dict:
    """Extract common fields using regex patterns."""
    results = {}

    # Cash
    for pat in CASH_PATTERNS:
        m = pat.search(full_text)
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                # Check if "million" was in the match
                if any(w in m.group(0).lower() for w in ["million", " m"]):
                    val = val * 1_000_000
                results["cash_mentioned_aud"] = val
                break

    # Production
    for pat in PRODUCTION_PATTERNS:
        m = pat.search(full_text)
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                results["production"] = val
                results["production_unit"] = m.group(2)
                break

    # AISC
    for pat in AISC_PATTERNS:
        m = pat.search(full_text)
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                results["aisc_per_unit"] = val
                results["aisc_unit"] = f"A$/{m.group(2)}"
                break

    # Revenue
    for pat in REVENUE_PATTERNS:
        m = pat.search(full_text)
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                results["revenue_aud"] = val * 1_000_000  # convert from millions
                break

    # Resource
    for pat in RESOURCE_PATTERNS:
        m = pat.search(full_text)
        if m:
            results["resource_mt"] = _parse_number(m.group(1))
            results["grade"] = _parse_number(m.group(2))
            results["grade_unit"] = m.group(3)
            break

    # NPV
    for pat in NPV_PATTERNS:
        m = pat.search(full_text)
        if m:
            val = _parse_number(m.group(1))
            if val is not None:
                if "bn" in m.group(0).lower() or "b" == m.group(0)[-1].lower():
                    val = val * 1000
                results["npv_musd"] = val
                break

    # IRR
    for pat in IRR_PATTERNS:
        m = pat.search(full_text)
        if m:
            results["irr_pct"] = _parse_number(m.group(1))
            break

    # Shares
    for pat in SHARES_PATTERNS:
        m = pat.search(full_text)
        if m:
            val = _parse_number(m.group(1))
            if val and "million" in m.group(0).lower() or (m.lastindex and m.lastindex >= 2 and m.group(2)):
                val = val * 1_000_000
            results["shares_on_issue"] = val
            break

    return results


def _try_llm(schema, pages) -> dict | None:
    """Try LLM extraction, returning None if no API credits."""
    try:
        from pipeline.parsers.llm_extractor import extract_with_llm
        return extract_with_llm(schema, pages)
    except Exception as e:
        logger.info("LLM unavailable (%s), using regex-only", str(e)[:60])
        return None


def parse_generic(doc_id: str, pdf_source=None) -> dict:
    """
    Parse any document type. Tries table extraction + regex first,
    then LLM as optional enhancement.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT local_path, company_ticker, doc_type, header FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()

    if not row:
        logger.error("Document %s not found", doc_id)
        conn.close()
        return {}

    doc_type = row["doc_type"] or "other"
    header = row["header"] or ""

    if pdf_source is None:
        local_path = row["local_path"]
        if not local_path:
            logger.error("No pdf_source and no local_path for %s", doc_id)
            conn.close()
            return {}
        pdf_source = local_path

    logger.info("Generic parse [%s]: %s — %s", doc_type, doc_id, header[:60])

    # Extract all page texts
    all_pages = extract_page_texts(pdf_source)
    if not all_pages:
        logger.warning("Could not extract text from %s", doc_id)
        conn.execute("UPDATE documents SET parse_status = 'failed' WHERE id = ?", (doc_id,))
        conn.commit()
        conn.close()
        return {}

    full_text = "\n".join(all_pages)
    method = "rule_based"

    # Step 1: Try table extraction (for quarterly-type docs)
    if pdf_source:
        # Need to reset BytesIO position if it was already read
        if hasattr(pdf_source, "seek"):
            pdf_source.seek(0)
        results = _extract_from_tables(pdf_source)
    else:
        results = {}

    # Step 2: Regex extraction on full text
    regex_results = _extract_with_regex(full_text)
    for k, v in regex_results.items():
        if k not in results or results[k] is None:
            results[k] = v

    # Step 3: Try LLM if we're missing key data
    has_data = any(v is not None for v in results.values())
    if not has_data:
        schema = LLM_SCHEMAS.get(doc_type, LLM_SCHEMAS["other"])
        llm_results = _try_llm(schema, all_pages[:3])
        if llm_results:
            for k, v in llm_results.items():
                if v is not None and (k not in results or results[k] is None):
                    results[k] = v
            method = "llm"

    # Check what we got
    has_data = any(v is not None for k, v in results.items()
                   if k not in ("document_summary",))

    if not has_data:
        logger.info("No extractable data in %s, marking needs_review", doc_id)
        conn.execute("UPDATE documents SET parse_status = 'needs_review' WHERE id = ?", (doc_id,))
        conn.commit()
        conn.close()
        return results

    # Write to staging
    confidence = "high" if method == "rule_based" else "medium"
    for field, value in results.items():
        if value is None or value == "":
            continue
        if field.endswith("_unit"):
            continue  # unit descriptor, stored alongside the value

        unit = None
        if "aud" in field:
            unit = "AUD"
        elif "pct" in field:
            unit = "%"
        elif field == "production":
            unit = results.get("production_unit")
        elif field == "aisc_per_unit":
            unit = results.get("aisc_unit")
        elif field == "grade":
            unit = results.get("grade_unit")
        elif field == "resource_mt":
            unit = "Mt"
        elif field == "npv_musd":
            unit = "USD_M"

        norm_value = float(value) if isinstance(value, (int, float)) else None

        conn.execute(
            """INSERT INTO staging_extractions
               (document_id, field_name, raw_value, normalized_value, unit,
                extraction_method, confidence, needs_review)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
            (doc_id, f"{doc_type}_{field}", str(value), norm_value, unit, method, confidence),
        )

    conn.execute("UPDATE documents SET parse_status = 'done' WHERE id = ?", (doc_id,))
    conn.commit()
    conn.close()

    logger.info("Generic parse [%s] for %s: %s",
                method, doc_id, {k: v for k, v in results.items() if v is not None})
    return results

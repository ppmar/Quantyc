"""
Resource/Reserve Parser

Extracts JORC resource and reserve data from ASX announcements.
Strategy: try pdfplumber table extraction first, fall back to LLM.

Usage:
    python -m pipeline.parsers.resource --doc-id <id>
    python -m pipeline.parsers.resource --all
"""

import argparse
import logging
import re

import pdfplumber

from db import get_connection, init_db
from pipeline.section_finder import find_relevant_pages, extract_page_texts
from pipeline.parsers.llm_extractor import extract_with_llm, validate_extraction

logger = logging.getLogger(__name__)

# Schema for LLM extraction of resource data
RESOURCE_LLM_SCHEMA = {
    "commodity": "Primary commodity (e.g. gold, copper, lithium, silver, zinc)",
    "category": "JORC category: Measured, Indicated, Inferred, Proven, Probable, or Total",
    "tonnes_mt": "Tonnes in millions (Mt)",
    "grade": "Grade value (numeric)",
    "grade_unit": "Grade unit (g/t, %, ppm, Li2O%)",
    "contained_metal": "Contained metal (numeric)",
    "contained_unit": "Contained metal unit (koz, Moz, kt, Mlb, Mt)",
    "effective_date": "Date of the resource estimate (YYYY-MM-DD if available)",
    "cut_off_grade": "Cut-off grade used (numeric, or null)",
    "estimate_type": "Type: resource or reserve",
}

# LLM schema for when we want multiple rows extracted
RESOURCE_MULTI_LLM_SCHEMA = {
    "rows": "Array of resource/reserve rows. Each row has: commodity, category, tonnes_mt, grade, grade_unit, contained_metal, contained_unit, cut_off_grade, estimate_type. Use null for missing fields.",
    "effective_date": "Date of the resource/reserve estimate (YYYY-MM-DD if available)",
}

# Patterns for detecting JORC table headers
JORC_HEADER_PATTERNS = [
    re.compile(r"(measured|indicated|inferred|total)", re.I),
    re.compile(r"(tonnes|mt|grade|g/t|contained|metal)", re.I),
    re.compile(r"(proven|probable)", re.I),
]

CATEGORY_KEYWORDS = {
    "measured": "Measured",
    "indicated": "Indicated",
    "inferred": "Inferred",
    "proven": "Proven",
    "probable": "Probable",
    "total": "Total",
    "measured + indicated": "Measured+Indicated",
    "measured and indicated": "Measured+Indicated",
    "m+i": "Measured+Indicated",
    "m&i": "Measured+Indicated",
}

COMMODITY_KEYWORDS = {
    "gold": "gold", "au": "gold",
    "silver": "silver", "ag": "silver",
    "copper": "copper", "cu": "copper",
    "lithium": "lithium", "li": "lithium", "li2o": "lithium",
    "zinc": "zinc", "zn": "zinc",
    "nickel": "nickel", "ni": "nickel",
    "iron ore": "iron_ore", "fe": "iron_ore",
    "uranium": "uranium", "u3o8": "uranium",
    "cobalt": "cobalt", "co": "cobalt",
    "tin": "tin", "sn": "tin",
    "lead": "lead", "pb": "lead",
}


def _parse_number(text: str) -> float | None:
    """Parse a number from text, handling commas and parentheses."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "")
    if text in ("-", "–", "", "nil", "n/a"):
        return None
    # Handle parentheses as negative
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    match = re.search(r"-?\d+\.?\d*", text)
    if match:
        return float(match.group())
    return None


def _detect_category(text: str) -> str | None:
    """Detect JORC category from a cell or row label."""
    text_lower = text.lower().strip()
    for keyword, category in CATEGORY_KEYWORDS.items():
        if keyword in text_lower:
            return category
    return None


def _detect_commodity(text: str) -> str | None:
    """Detect commodity from text."""
    text_lower = text.lower().strip()
    for keyword, commodity in COMMODITY_KEYWORDS.items():
        if keyword in text_lower:
            return commodity
    return None


def _is_jorc_table(table: list[list]) -> bool:
    """Check if a table looks like a JORC resource/reserve table."""
    if not table or len(table) < 2:
        return False
    # Check first two rows for JORC-like headers
    header_text = " ".join(
        str(cell) for row in table[:2] for cell in row if cell
    ).lower()
    score = 0
    if any(w in header_text for w in ["measured", "indicated", "inferred", "proven", "probable"]):
        score += 2
    if any(w in header_text for w in ["tonnes", "mt", "million"]):
        score += 1
    if any(w in header_text for w in ["grade", "g/t", "%"]):
        score += 1
    if any(w in header_text for w in ["contained", "metal", "koz", "moz", "oz"]):
        score += 1
    return score >= 2


def extract_from_tables(pdf_source) -> list[dict]:
    """
    Try to extract resource/reserve rows from JORC tables in the PDF.
    Accepts a file path (str/Path) or in-memory BytesIO.
    Returns list of dicts with resource fields, or empty list if no tables found.
    """
    results = []
    try:
        if hasattr(pdf_source, "seek"):
            pdf_source.seek(0)
        pdf = pdfplumber.open(pdf_source)
    except Exception as e:
        logger.error("Failed to open PDF: %s", e)
        return results

    for page_num, page in enumerate(pdf.pages):
        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if not _is_jorc_table(table):
                continue

            logger.info("Found JORC table on page %d", page_num + 1)

            # Try to detect commodity from the page text
            page_text = page.extract_text() or ""
            commodity = _detect_commodity(page_text)

            # Detect grade unit from headers
            header_text = " ".join(
                str(c) for row in table[:2] for c in row if c
            ).lower()
            grade_unit = None
            if "g/t" in header_text:
                grade_unit = "g/t"
            elif "%" in header_text:
                grade_unit = "%"
            elif "ppm" in header_text:
                grade_unit = "ppm"

            # Detect contained unit from headers
            contained_unit = None
            for unit in ["moz", "koz", "kt", "mlb", "mt"]:
                if unit in header_text:
                    contained_unit = unit
                    break

            # Parse data rows (skip header rows)
            for row in table[1:]:
                if not row or not row[0]:
                    continue

                label = str(row[0])
                category = _detect_category(label)
                if not category:
                    continue

                # Extract numeric values from remaining columns
                numbers = []
                for cell in row[1:]:
                    n = _parse_number(str(cell)) if cell else None
                    numbers.append(n)

                # Heuristic: typical JORC table columns are
                # [Category | Tonnes (Mt) | Grade | Contained Metal]
                row_data = {
                    "commodity": commodity,
                    "category": category,
                    "estimate_type": _estimate_type_from_category(category),
                    "tonnes_mt": numbers[0] if len(numbers) > 0 else None,
                    "grade": numbers[1] if len(numbers) > 1 else None,
                    "grade_unit": grade_unit,
                    "contained_metal": numbers[2] if len(numbers) > 2 else None,
                    "contained_unit": contained_unit,
                    "cut_off_grade": None,
                    "effective_date": None,
                }

                # Only include rows that have at least tonnes or contained metal
                if row_data["tonnes_mt"] is not None or row_data["contained_metal"] is not None:
                    results.append(row_data)

    pdf.close()
    return results


def _estimate_type_from_category(category: str) -> str:
    """Determine if a category is a resource or reserve."""
    if category in ("Proven", "Probable"):
        return "reserve"
    return "resource"


def extract_with_llm_fallback(pdf_source) -> list[dict]:
    """
    Use LLM to extract resource data from the most relevant pages.
    Accepts a file path (str/Path) or in-memory BytesIO.
    Called when table extraction fails or finds nothing.
    """
    pages = find_relevant_pages(pdf_source, "resource", max_pages=3)
    if not pages:
        logger.warning("No resource-relevant pages found")
        return []

    # Use the multi-row schema
    schema = {
        "rows": (
            "Array of resource/reserve line items. Each item is an object with: "
            "commodity (string), category (Measured|Indicated|Inferred|Proven|Probable|Total), "
            "tonnes_mt (number, millions of tonnes), grade (number), "
            "grade_unit (g/t|%|ppm|Li2O%), contained_metal (number), "
            "contained_unit (koz|Moz|kt|Mlb|Mt), cut_off_grade (number or null), "
            "estimate_type (resource|reserve). Use null for missing fields."
        ),
        "effective_date": "Date of estimate (YYYY-MM-DD or null)",
    }

    result = extract_with_llm(schema, pages)
    if not result:
        return []

    rows = result.get("rows")
    if not rows or not isinstance(rows, list):
        logger.warning("LLM did not return rows array")
        return []

    effective_date = result.get("effective_date")

    # Normalize each row
    extracted = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row["effective_date"] = row.get("effective_date") or effective_date
        if not row.get("estimate_type") and row.get("category"):
            row["estimate_type"] = _estimate_type_from_category(row["category"])
        extracted.append(row)

    return extracted


def parse_resource(doc_id: str, pdf_source=None) -> list[dict]:
    """
    Parse a resource/reserve document and write results to staging_extractions.
    If pdf_source (BytesIO) is provided, use it instead of local_path from DB.
    Tries table extraction first, falls back to LLM.
    Returns list of extracted resource rows.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT local_path, company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()

    if not row:
        logger.error("Document %s not found", doc_id)
        conn.close()
        return []

    if pdf_source is None:
        local_path = row["local_path"]
        if not local_path:
            logger.error("No local_path for document %s", doc_id)
            conn.close()
            return []
        pdf_source = local_path

    logger.info("Parsing resource document: %s", doc_id)

    # Try table extraction first
    results = extract_from_tables(pdf_source)
    method = "rule_based"
    confidence = "high"

    if not results:
        logger.info("Table extraction found nothing, trying LLM fallback for %s", doc_id)
        results = extract_with_llm_fallback(pdf_source)
        method = "llm"
        confidence = "medium"

    if not results:
        logger.warning("No resource data extracted from %s", doc_id)
        conn.execute(
            "UPDATE documents SET parse_status = 'failed' WHERE id = ?",
            (doc_id,),
        )
        conn.commit()
        conn.close()
        return []

    # Write each row's fields to staging
    for res_row in results:
        for field, value in res_row.items():
            if value is not None:
                unit = None
                if field == "grade":
                    unit = res_row.get("grade_unit")
                elif field == "contained_metal":
                    unit = res_row.get("contained_unit")
                elif field == "tonnes_mt":
                    unit = "Mt"

                conn.execute(
                    """INSERT INTO staging_extractions
                       (document_id, field_name, raw_value, normalized_value, unit,
                        extraction_method, confidence, needs_review)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        doc_id,
                        f"resource_{field}",
                        str(value),
                        float(value) if isinstance(value, (int, float)) else None,
                        unit,
                        method,
                        confidence,
                        1 if confidence == "medium" else 0,
                    ),
                )

    conn.execute(
        "UPDATE documents SET parse_status = 'done' WHERE id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()

    logger.info("Extracted %d resource rows from %s (method=%s)", len(results), doc_id, method)
    return results


def parse_all_pending():
    """Parse all pending resource/reserve documents."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id FROM documents
           WHERE doc_type = 'resource_update' AND parse_status = 'pending'"""
    ).fetchall()
    conn.close()

    parsed = 0
    for row in rows:
        results = parse_resource(row["id"])
        if results:
            parsed += 1

    logger.info("Parsed %d / %d resource documents", parsed, len(rows))
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Parse resource/reserve documents")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Parse a specific document by ID")
    group.add_argument("--ticker", type=str, help="Parse all pending resource docs for a ticker")
    group.add_argument("--all", action="store_true", help="Parse all pending resource documents")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        results = parse_resource(args.doc_id)
        for r in results:
            print(f"  {r}")
    elif args.ticker:
        conn = get_connection()
        rows = conn.execute(
            """SELECT id FROM documents
               WHERE company_ticker = ? AND doc_type = 'resource_update' AND parse_status = 'pending'""",
            (args.ticker.upper(),),
        ).fetchall()
        conn.close()
        for row in rows:
            results = parse_resource(row["id"])
            print(f"  {row['id']}: {len(results)} rows")
    else:
        n = parse_all_pending()
        print(f"Parsed {n} resource documents")


if __name__ == "__main__":
    main()

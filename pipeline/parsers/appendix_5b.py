"""
Appendix 5B Parser

Rule-based parser for ASX Appendix 5B quarterly cash flow reports.
Extracts: cash at end of quarter, operating cashflow, investing cashflow.

The Appendix 5B is a standardised ASX form, so we can rely on table structure
and known row labels rather than LLM extraction.

Usage:
    python -m pipeline.parsers.appendix_5b --doc-id <id>
    python -m pipeline.parsers.appendix_5b --ticker DEG
"""

import argparse
import logging
import re

import pdfplumber

from db import get_connection, init_db

logger = logging.getLogger(__name__)

# Row labels we search for (lowercased). Map to our field names.
ROW_PATTERNS = {
    "cash_at_end_quarter": [
        "cash and cash equivalents at end of",
        "cash at end of quarter",
        "cash and cash equiv",
    ],
    "operating_cashflow": [
        "net cash from / (used in) operating activities",
        "net cash from/(used in) operating",
        "net cash used in operating",
        "net cash from operating",
    ],
    "investing_cashflow": [
        "net cash from / (used in) investing activities",
        "net cash from/(used in) investing",
        "net cash used in investing",
        "net cash from investing",
    ],
}


def _parse_amount(text: str) -> float | None:
    """Parse a dollar amount from text, handling parentheses for negatives."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace("$", "").replace(" ", "")
    if not text or text == "-" or text == "–":
        return None

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1]

    match = re.search(r"[-]?\d+\.?\d*", text)
    if not match:
        return None

    value = float(match.group())
    if negative:
        value = -value
    return value


def _match_row_label(cell_text: str, patterns: list[str]) -> bool:
    """Check if a cell's text matches any of the target patterns."""
    cell_lower = cell_text.lower().strip()
    return any(p in cell_lower for p in patterns)


def extract_from_tables(pdf_source) -> dict[str, float | None]:
    """
    Extract cash flow fields from Appendix 5B tables.
    Returns dict with keys: cash_at_end_quarter, operating_cashflow, investing_cashflow.
    Values are in A$'000 as reported (the standard unit in Appendix 5B).
    Accepts a file path (str/Path) or in-memory BytesIO.
    """
    results = {field: None for field in ROW_PATTERNS}

    try:
        if hasattr(pdf_source, "seek"):
            pdf_source.seek(0)
        pdf = pdfplumber.open(pdf_source)
    except Exception as e:
        logger.error("Failed to open PDF: %s", e)
        return results

    for page in pdf.pages:
        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if not table:
                continue
            for row in table:
                if not row or not row[0]:
                    continue

                label = str(row[0])
                for field, patterns in ROW_PATTERNS.items():
                    if results[field] is not None:
                        continue
                    if _match_row_label(label, patterns):
                        # The value is typically in the "current quarter" column
                        # which is usually column index 1 or the last non-empty column
                        for cell in row[1:]:
                            if cell:
                                val = _parse_amount(str(cell))
                                if val is not None:
                                    results[field] = val
                                    break

    pdf.close()
    return results


def parse_appendix_5b(doc_id: str, pdf_source=None) -> dict[str, float | None]:
    """
    Parse an Appendix 5B document and write results to staging_extractions.
    If pdf_source (BytesIO) is provided, use it instead of local_path from DB.
    Returns the extracted values.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT local_path, company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()

    if not row:
        logger.error("Document %s not found", doc_id)
        conn.close()
        return {}

    if pdf_source is None:
        local_path = row["local_path"]
        if not local_path:
            logger.error("No local_path for document %s", doc_id)
            conn.close()
            return {}
        pdf_source = local_path

    logger.info("Parsing Appendix 5B: %s", doc_id)
    results = extract_from_tables(pdf_source)

    # Write to staging
    for field, value in results.items():
        if value is not None:
            conn.execute(
                """INSERT INTO staging_extractions
                   (document_id, field_name, raw_value, normalized_value, unit,
                    extraction_method, confidence, needs_review)
                   VALUES (?, ?, ?, ?, 'AUD_000', 'rule_based', 'high', 0)""",
                (doc_id, field, str(value), value),
            )

    # Update parse status
    has_data = any(v is not None for v in results.values())
    status = "done" if has_data else "failed"
    conn.execute(
        "UPDATE documents SET parse_status = ? WHERE id = ?",
        (status, doc_id),
    )

    conn.commit()
    conn.close()

    logger.info("Appendix 5B results for %s: %s", doc_id, results)
    return results


def parse_all_pending():
    """Parse all pending Appendix 5B documents."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id FROM documents
           WHERE doc_type = 'appendix_5b' AND parse_status = 'pending'"""
    ).fetchall()
    conn.close()

    parsed = 0
    for row in rows:
        results = parse_appendix_5b(row["id"])
        if any(v is not None for v in results.values()):
            parsed += 1

    logger.info("Parsed %d / %d Appendix 5B documents", parsed, len(rows))
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Parse Appendix 5B documents")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Parse a specific document by ID")
    group.add_argument("--ticker", type=str, help="Parse all pending 5Bs for a ticker")
    group.add_argument("--all", action="store_true", help="Parse all pending 5B documents")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        results = parse_appendix_5b(args.doc_id)
        print(f"Results: {results}")
    elif args.ticker:
        conn = get_connection()
        rows = conn.execute(
            """SELECT id FROM documents
               WHERE company_ticker = ? AND doc_type = 'appendix_5b' AND parse_status = 'pending'""",
            (args.ticker.upper(),),
        ).fetchall()
        conn.close()
        for row in rows:
            results = parse_appendix_5b(row["id"])
            print(f"  {row['id']}: {results}")
    else:
        n = parse_all_pending()
        print(f"Parsed {n} Appendix 5B documents")


if __name__ == "__main__":
    main()

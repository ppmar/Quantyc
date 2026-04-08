"""
Capital Raise / Issue of Securities Parser

Extracts share issuance details from ASX announcements.
Strategy: regex-based extraction first, LLM fallback for complex terms.

Usage:
    python -m pipeline.parsers.capital_raise --doc-id <id>
    python -m pipeline.parsers.capital_raise --all
"""

import argparse
import logging
import re

from db import get_connection, init_db
from pipeline.section_finder import find_relevant_pages, extract_page_texts
from pipeline.parsers.llm_extractor import extract_with_llm

logger = logging.getLogger(__name__)

# Regex patterns for common capital raise fields
PATTERNS = {
    "total_raised_aud": [
        # "$X million" or "$X.Xm" or "A$X million"
        re.compile(r"(?:a?\$|aud\s*)\s*([\d,.]+)\s*(?:million|m)\b", re.I),
        # "raise of $X million" / "raising $X million"
        re.compile(r"rais(?:e|ing)\s+(?:of\s+)?(?:a?\$|aud\s*)\s*([\d,.]+)\s*(?:million|m)", re.I),
        # "gross proceeds of $X million"
        re.compile(r"(?:gross\s+)?proceeds\s+of\s+(?:a?\$|aud\s*)\s*([\d,.]+)\s*(?:million|m)", re.I),
    ],
    "price_per_share": [
        # "at $0.XX per share" / "at X cents per share"
        re.compile(r"at\s+\$([\d,.]+)\s+per\s+(?:share|new\s+share)", re.I),
        re.compile(r"at\s+([\d,.]+)\s+cents?\s+per\s+(?:share|new\s+share)", re.I),
        # "issue price of $0.XX"
        re.compile(r"issue\s+price\s+(?:of\s+)?\$([\d,.]+)", re.I),
        re.compile(r"issue\s+price\s+(?:of\s+)?([\d,.]+)\s+cents?", re.I),
    ],
    "new_shares": [
        # "X million shares" / "X,XXX,XXX shares"
        re.compile(r"([\d,.]+)\s*(?:million|m)\s+(?:new\s+)?(?:fully\s+paid\s+)?(?:ordinary\s+)?shares", re.I),
        re.compile(r"([\d,]+)\s+(?:new\s+)?(?:fully\s+paid\s+)?(?:ordinary\s+)?shares", re.I),
    ],
    "option_exercise_price": [
        # "exercise price of $X.XX" / "exercisable at $X.XX"
        re.compile(r"exercise\s+price\s+(?:of\s+)?\$([\d,.]+)", re.I),
        re.compile(r"exercisable\s+at\s+\$([\d,.]+)", re.I),
    ],
    "option_expiry": [
        # "expiring DD Month YYYY" / "expiry date of DD/MM/YYYY"
        re.compile(r"expir(?:ing|y)\s+(?:date\s+(?:of\s+)?)?(\d{1,2}\s+\w+\s+\d{4})", re.I),
        re.compile(r"expir(?:ing|y)\s+(?:date\s+(?:of\s+)?)?(\d{1,2}/\d{1,2}/\d{4})", re.I),
    ],
}

# LLM schema for fallback extraction
CAPITAL_RAISE_LLM_SCHEMA = {
    "new_shares": "Number of new shares issued (integer or float, NOT in millions unless stated)",
    "price_per_share": "Issue price per share in AUD (e.g. 0.05 for 5 cents)",
    "total_raised_aud": "Total amount raised in AUD millions (e.g. 5.0 for $5 million)",
    "options_attached": "Number of free-attaching options issued (null if none)",
    "option_exercise_price": "Exercise price of options in AUD (e.g. 0.10)",
    "option_expiry": "Option expiry date (YYYY-MM-DD or descriptive text)",
}


def _parse_amount(text: str) -> float | None:
    """Parse a numeric amount, stripping commas."""
    if not text:
        return None
    text = text.strip().replace(",", "")
    try:
        return float(text)
    except ValueError:
        return None


def extract_with_regex(text: str) -> dict[str, float | str | None]:
    """
    Extract capital raise fields from text using regex patterns.
    Returns dict with matched fields (None for unmatched).
    """
    results = {}

    for field, patterns in PATTERNS.items():
        results[field] = None
        for pat in patterns:
            match = pat.search(text)
            if match:
                raw = match.group(1)
                if field == "option_expiry":
                    results[field] = raw
                elif field == "price_per_share" and "cent" in match.group(0).lower():
                    # Convert cents to dollars
                    val = _parse_amount(raw)
                    results[field] = val / 100 if val else None
                elif field in ("total_raised_aud", "new_shares") and any(
                    w in match.group(0).lower() for w in ["million", "m"]
                ):
                    val = _parse_amount(raw)
                    results[field] = val  # Already in millions
                else:
                    results[field] = _parse_amount(raw)
                break

    return results


def extract_with_llm_fallback(pdf_source) -> dict:
    """Use LLM to extract capital raise data from relevant pages.
    Accepts a file path (str/Path) or in-memory BytesIO."""
    pages = find_relevant_pages(pdf_source, "capital_raise", max_pages=3)
    if not pages:
        pages = find_relevant_pages(pdf_source, "shares", max_pages=2)
    if not pages:
        all_pages = extract_page_texts(pdf_source)
        pages = all_pages[:3] if all_pages else []

    if not pages:
        return {}

    result = extract_with_llm(CAPITAL_RAISE_LLM_SCHEMA, pages)
    return result or {}


def parse_capital_raise(doc_id: str, pdf_source=None) -> dict:
    """
    Parse a capital raise document and write results to staging_extractions.
    If pdf_source (BytesIO) is provided, use it instead of local_path from DB.
    Tries regex first, falls back to LLM for missing fields.
    Returns dict of extracted values.
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

    logger.info("Parsing capital raise: %s", doc_id)

    # Extract full text for regex pass
    all_pages = extract_page_texts(pdf_source)
    full_text = "\n".join(all_pages)

    # Try regex first
    results = extract_with_regex(full_text)
    method = "rule_based"
    confidence = "high"

    # Count how many key fields we got
    key_fields = ["new_shares", "price_per_share", "total_raised_aud"]
    found_keys = sum(1 for f in key_fields if results.get(f) is not None)

    # If we're missing key fields, try LLM
    if found_keys < 2:
        logger.info("Regex found %d/3 key fields, trying LLM for %s", found_keys, doc_id)
        llm_results = extract_with_llm_fallback(pdf_source)
        if llm_results:
            # Merge: keep regex values where available, fill gaps with LLM
            for field, value in llm_results.items():
                if results.get(field) is None and value is not None:
                    results[field] = value
            method = "rule_based+llm"
            confidence = "medium"

    # Check if we got anything useful
    has_data = any(v is not None for v in results.values())

    if not has_data:
        logger.warning("No capital raise data extracted from %s", doc_id)
        conn.execute(
            "UPDATE documents SET parse_status = 'failed' WHERE id = ?",
            (doc_id,),
        )
        conn.commit()
        conn.close()
        return {}

    # Write to staging
    for field, value in results.items():
        if value is not None:
            unit = None
            if field == "total_raised_aud":
                unit = "AUD_M"
            elif field == "price_per_share":
                unit = "AUD"
            elif field == "option_exercise_price":
                unit = "AUD"

            conn.execute(
                """INSERT INTO staging_extractions
                   (document_id, field_name, raw_value, normalized_value, unit,
                    extraction_method, confidence, needs_review)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    doc_id,
                    f"raise_{field}",
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

    logger.info("Capital raise results for %s: %s", doc_id, results)
    return results


def parse_all_pending():
    """Parse all pending capital raise documents."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id FROM documents
           WHERE doc_type = 'capital_raise' AND parse_status = 'pending'"""
    ).fetchall()
    conn.close()

    parsed = 0
    for row in rows:
        results = parse_capital_raise(row["id"])
        if results:
            parsed += 1

    logger.info("Parsed %d / %d capital raise documents", parsed, len(rows))
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Parse capital raise documents")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Parse a specific document by ID")
    group.add_argument("--ticker", type=str, help="Parse all pending for a ticker")
    group.add_argument("--all", action="store_true", help="Parse all pending")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        results = parse_capital_raise(args.doc_id)
        for k, v in results.items():
            print(f"  {k}: {v}")
    elif args.ticker:
        conn = get_connection()
        rows = conn.execute(
            """SELECT id FROM documents
               WHERE company_ticker = ? AND doc_type = 'capital_raise' AND parse_status = 'pending'""",
            (args.ticker.upper(),),
        ).fetchall()
        conn.close()
        for row in rows:
            results = parse_capital_raise(row["id"])
            print(f"  {row['id']}: {results}")
    else:
        n = parse_all_pending()
        print(f"Parsed {n} capital raise documents")


if __name__ == "__main__":
    main()

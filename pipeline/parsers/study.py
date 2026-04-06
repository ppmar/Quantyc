"""
Study Parser

Extracts economic parameters from scoping study / PFS / DFS announcements.
These are narrative-heavy documents so we primarily use LLM extraction
on the most relevant pages (identified by section_finder).

Usage:
    python -m pipeline.parsers.study --doc-id <id>
    python -m pipeline.parsers.study --all
"""

import argparse
import logging
import re

from db import get_connection, init_db
from pipeline.section_finder import find_relevant_pages_multi, extract_page_texts
from pipeline.parsers.llm_extractor import extract_with_llm, validate_extraction

logger = logging.getLogger(__name__)

# LLM schema for study extraction
STUDY_LLM_SCHEMA = {
    "study_stage": "Study type: scoping, pfs, dfs, or production",
    "mine_life_years": "Mine life in years (numeric)",
    "annual_production": "Annual production rate (numeric)",
    "production_unit": "Unit of annual production (e.g. koz/yr, kt/yr, Mtpa)",
    "recovery_pct": "Processing recovery percentage (e.g. 92.5)",
    "initial_capex_musd": "Initial capital expenditure in millions USD (or AUD — specify in capex_currency)",
    "sustaining_capex_musd": "Sustaining/life-of-mine capital in millions (null if not stated)",
    "opex_per_unit": "Operating cost per unit (numeric)",
    "opex_unit": "Operating cost unit (e.g. $/oz, $/t, A$/oz, A$/t)",
    "post_tax_npv_musd": "Post-tax NPV in millions (numeric)",
    "irr_pct": "Post-tax IRR percentage (e.g. 35.2)",
    "assumed_commodity_price": "Commodity price assumed in the study (numeric)",
    "assumed_price_unit": "Unit of assumed price (e.g. US$/oz, A$/oz, US$/t, US$/lb)",
    "assumed_fx_audusd": "AUD/USD exchange rate assumed (e.g. 0.75, null if not stated)",
    "discount_rate_pct": "Discount rate used for NPV (e.g. 8.0)",
    "capex_currency": "Currency of capex figures: USD or AUD",
    "npv_currency": "Currency of NPV figure: USD or AUD",
}

# Study stage detection from document text
STUDY_STAGE_PATTERNS = {
    "dfs": [
        re.compile(r"definitive\s+feasibility\s+study", re.I),
        re.compile(r"\bDFS\b"),
        re.compile(r"bankable\s+feasibility", re.I),
        re.compile(r"\bBFS\b"),
    ],
    "pfs": [
        re.compile(r"pre-?feasibility\s+study", re.I),
        re.compile(r"\bPFS\b"),
    ],
    "scoping": [
        re.compile(r"scoping\s+study", re.I),
        re.compile(r"preliminary\s+economic\s+assessment", re.I),
        re.compile(r"\bPEA\b"),
    ],
}


def _detect_study_stage(text: str) -> str | None:
    """Detect study stage from text using patterns."""
    # Check in order of most advanced to least
    for stage in ["dfs", "pfs", "scoping"]:
        for pat in STUDY_STAGE_PATTERNS[stage]:
            if pat.search(text):
                return stage
    return None


def parse_study(doc_id: str) -> dict:
    """
    Parse a study announcement and write results to staging_extractions.
    Returns dict of extracted values.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT local_path, company_ticker, announcement_date, header FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()

    if not row:
        logger.error("Document %s not found", doc_id)
        conn.close()
        return {}

    local_path = row["local_path"]
    if not local_path:
        logger.error("No local_path for document %s", doc_id)
        conn.close()
        return {}

    logger.info("Parsing study document: %s", local_path)

    # Find relevant pages — studies need NPV, capex, and study keywords
    pages = find_relevant_pages_multi(
        local_path,
        section_types=["npv", "capex", "study"],
        max_pages=3,
    )

    if not pages:
        # Fallback: just use the first 3 pages (many studies have a summary upfront)
        logger.info("No high-scoring pages found, using first pages of %s", doc_id)
        all_pages = extract_page_texts(local_path)
        pages = all_pages[:3] if all_pages else []

    if not pages:
        logger.warning("Could not extract any text from %s", doc_id)
        conn.execute(
            "UPDATE documents SET parse_status = 'failed' WHERE id = ?",
            (doc_id,),
        )
        conn.commit()
        conn.close()
        return {}

    # Try to pre-detect the study stage from the header and first pages
    header = row["header"] or ""
    combined_text = header + "\n" + "\n".join(pages[:2])
    detected_stage = _detect_study_stage(combined_text)

    # LLM extraction
    results = extract_with_llm(STUDY_LLM_SCHEMA, pages)
    if not results:
        logger.warning("LLM extraction returned nothing for %s", doc_id)
        conn.execute(
            "UPDATE documents SET parse_status = 'failed' WHERE id = ?",
            (doc_id,),
        )
        conn.commit()
        conn.close()
        return {}

    # Override study_stage if our regex detection is more reliable
    if detected_stage and not results.get("study_stage"):
        results["study_stage"] = detected_stage

    # Validate
    is_valid, warnings = validate_extraction(results, STUDY_LLM_SCHEMA)
    if warnings:
        for w in warnings:
            logger.warning("Validation: %s (doc %s)", w, doc_id)

    needs_review = 0 if is_valid else 1

    # Check for red flags
    if results.get("post_tax_npv_musd") is None and results.get("irr_pct") is None:
        logger.warning("No NPV or IRR found in study %s — flagging for review", doc_id)
        needs_review = 1

    # Write to staging
    for field, value in results.items():
        if value is not None:
            unit = None
            if field in ("initial_capex_musd", "sustaining_capex_musd", "post_tax_npv_musd"):
                unit = results.get("npv_currency", "USD") + "_M"
            elif field == "opex_per_unit":
                unit = results.get("opex_unit")
            elif field == "assumed_commodity_price":
                unit = results.get("assumed_price_unit")
            elif field == "discount_rate_pct":
                unit = "%"
            elif field == "irr_pct":
                unit = "%"
            elif field == "recovery_pct":
                unit = "%"
            elif field == "mine_life_years":
                unit = "years"

            conn.execute(
                """INSERT INTO staging_extractions
                   (document_id, field_name, raw_value, normalized_value, unit,
                    extraction_method, confidence, needs_review)
                   VALUES (?, ?, ?, ?, ?, 'llm', 'medium', ?)""",
                (
                    doc_id,
                    f"study_{field}",
                    str(value),
                    float(value) if isinstance(value, (int, float)) else None,
                    unit,
                    needs_review,
                ),
            )

    conn.execute(
        "UPDATE documents SET parse_status = 'done' WHERE id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()

    logger.info("Study results for %s: %s", doc_id, {k: v for k, v in results.items() if v is not None})
    return results


def parse_all_pending():
    """Parse all pending study documents."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id FROM documents
           WHERE doc_type = 'study' AND parse_status = 'pending'"""
    ).fetchall()
    conn.close()

    parsed = 0
    for row in rows:
        results = parse_study(row["id"])
        if results:
            parsed += 1

    logger.info("Parsed %d / %d study documents", parsed, len(rows))
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Parse study documents")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Parse a specific document by ID")
    group.add_argument("--ticker", type=str, help="Parse all pending studies for a ticker")
    group.add_argument("--all", action="store_true", help="Parse all pending study documents")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        results = parse_study(args.doc_id)
        for k, v in results.items():
            if v is not None:
                print(f"  {k}: {v}")
    elif args.ticker:
        conn = get_connection()
        rows = conn.execute(
            """SELECT id FROM documents
               WHERE company_ticker = ? AND doc_type = 'study' AND parse_status = 'pending'""",
            (args.ticker.upper(),),
        ).fetchall()
        conn.close()
        for row in rows:
            results = parse_study(row["id"])
            print(f"  {row['id']}: {len([v for v in results.values() if v])} fields")
    else:
        n = parse_all_pending()
        print(f"Parsed {n} study documents")


if __name__ == "__main__":
    main()

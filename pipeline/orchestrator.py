"""
Pipeline Orchestrator

Week 1: pick pending docs → classify → update doc_type → set parse_status='classified'.
Week 2: classified docs → extract → normalize → flag.

Usage:
    from pipeline.orchestrator import run_orchestrator
"""

import logging

from db import get_connection
from pipeline.classify import classify, contains_standardized_form

logger = logging.getLogger(__name__)


def classify_pending() -> int:
    """
    Classify all documents with parse_status='pending'.
    Sets doc_type and advances parse_status to 'classified'.
    Returns count of classified documents.
    """
    conn = get_connection()
    docs = conn.execute(
        "SELECT document_id, header FROM documents WHERE parse_status = 'pending'"
    ).fetchall()
    conn.close()

    classified = 0
    for doc in docs:
        doc_id = doc["document_id"]
        headline = doc["header"] or ""

        # Classify from headline only (no PDF download for classification)
        doc_type = classify(headline=headline)

        conn = get_connection()
        conn.execute(
            "UPDATE documents SET doc_type = ?, parse_status = 'classified' WHERE document_id = ?",
            (doc_type, doc_id),
        )
        conn.commit()
        conn.close()

        classified += 1
        logger.info("Classified doc %d as '%s' (headline: %s)", doc_id, doc_type, headline[:60])

    logger.info("Classified %d pending documents", classified)
    return classified


def extract_classified() -> dict:
    """
    Extract data from classified documents.

    For each classified doc:
    1. If already a standardized type (appendix_5b, issue_of_securities), extract directly.
    2. Otherwise, download the PDF and scan for an embedded standardized form
       (e.g. a quarterly report with an Appendix 5B at the end).
       Only scans the 5 most recent non-standardized docs to avoid wasting downloads.
    3. If a standardized form is found, reclassify and extract.

    Returns stats dict.
    """
    from ingest.asx_poller import fetch_pdf_bytes
    from pipeline.extractors.appendix_5b import extract_appendix_5b
    from pipeline.extractors.issue_of_securities import extract_issue_of_securities
    from pipeline.normalize.company_financials import normalize_from_5b, normalize_from_securities

    STANDARDIZED_TYPES = {"appendix_5b", "issue_of_securities", "resource_update"}

    stats = {"extracted": 0, "skipped": 0, "failed": 0}

    conn = get_connection()
    docs = conn.execute(
        """SELECT document_id, doc_type, url, header, ticker, announcement_date
           FROM documents WHERE parse_status = 'classified'
           ORDER BY announcement_date DESC"""
    ).fetchall()
    conn.close()

    # Types that might contain an embedded standardized form (worth downloading to check)
    SCAN_TYPES = {"quarterly_activity"}

    for doc in docs:
        doc_id = doc["document_id"]
        doc_type = doc["doc_type"]
        url = doc["url"]

        if doc_type in STANDARDIZED_TYPES:
            # Direct extraction — download and parse
            pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
            if not pdf_bytes:
                _mark_failed(doc_id, "download_failed")
                stats["failed"] += 1
                continue
            _extract_doc(doc_id, doc_type, pdf_bytes, stats,
                         extract_appendix_5b, extract_issue_of_securities,
                         normalize_from_5b, normalize_from_securities,
                         ticker=doc["ticker"], announcement_date=doc["announcement_date"])
            del pdf_bytes

        elif doc_type in SCAN_TYPES:
            # Download and scan for embedded standardized forms (e.g. 5B at end of quarterly report)
            pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
            if not pdf_bytes:
                _mark_skipped(doc_id)
                stats["skipped"] += 1
                continue
            found_type = contains_standardized_form(pdf_bytes)
            if found_type:
                logger.info("Doc %d reclassified: %s → %s (embedded form)", doc_id, doc_type, found_type)
                _update_doc_type(doc_id, found_type)
                _extract_doc(doc_id, found_type, pdf_bytes, stats,
                             extract_appendix_5b, extract_issue_of_securities,
                             normalize_from_5b, normalize_from_securities,
                             ticker=doc["ticker"], announcement_date=doc["announcement_date"])
            else:
                _mark_skipped(doc_id)
                stats["skipped"] += 1
            del pdf_bytes

        else:
            # Not useful — skip without downloading
            _mark_skipped(doc_id)
            stats["skipped"] += 1

    logger.info("Extraction: %s", stats)
    return stats


def _extract_doc(doc_id, doc_type, pdf_bytes, stats,
                 extract_appendix_5b, extract_issue_of_securities,
                 normalize_from_5b, normalize_from_securities,
                 ticker=None, announcement_date=None):
    """Run the appropriate extractor + normalizer for a document."""
    if doc_type == "appendix_5b":
        result = extract_appendix_5b(doc_id, pdf_bytes)
        if result:
            normalize_from_5b(doc_id)
            _mark_parsed(doc_id)
            stats["extracted"] += 1
        else:
            _mark_failed(doc_id, "extraction_empty")
            stats["failed"] += 1

    elif doc_type == "issue_of_securities":
        result = extract_issue_of_securities(doc_id, pdf_bytes)
        if result:
            normalize_from_securities(doc_id)
            _mark_parsed(doc_id)
            stats["extracted"] += 1
        else:
            _mark_failed(doc_id, "extraction_empty")
            stats["failed"] += 1

    elif doc_type == "resource_update":
        _extract_resource_update(doc_id, pdf_bytes, ticker, announcement_date, stats)


def _extract_resource_update(doc_id, pdf_bytes, ticker, announcement_date, stats):
    """Parse a JORC resource update and persist to projects + resources."""
    from datetime import date as date_type, datetime, timezone
    from parsers.jorc_resource_estimate import detect_profile, parse
    from parsers.appendix_2a import ExtractionError, MalformedDocumentError

    if not ticker or not announcement_date:
        _mark_failed(doc_id, "missing_ticker_or_date")
        stats["failed"] += 1
        return

    if not detect_profile(pdf_bytes):
        _mark_skipped(doc_id)
        stats["skipped"] += 1
        return

    ann_date = announcement_date
    if isinstance(ann_date, str):
        ann_date = date_type.fromisoformat(ann_date)

    try:
        estimate = parse(pdf_bytes, ticker=ticker, doc_id=str(doc_id), announcement_date=ann_date)
    except (ExtractionError, MalformedDocumentError) as e:
        _mark_failed(doc_id, str(e))
        stats["failed"] += 1
        return

    conn = get_connection()
    try:
        # Look up company_id
        row = conn.execute("SELECT company_id FROM companies WHERE ticker = ?", (ticker,)).fetchone()
        if not row:
            _mark_failed(doc_id, "company_not_found")
            stats["failed"] += 1
            return
        company_id = row["company_id"]

        # Project bootstrap: look up or insert
        project_id = _get_or_create_project(conn, company_id, estimate.project_name)

        # Insert commodity association
        conn.execute(
            """INSERT OR IGNORE INTO project_commodities (project_id, commodity, is_primary)
               VALUES (?, ?, 1)""",
            (project_id, estimate.commodity),
        )

        # Insert resource rows (skip Total rows — they're derived)
        now = datetime.now(timezone.utc).isoformat()
        for row in estimate.rows:
            if row.category == "Total":
                continue
            conn.execute(
                """INSERT INTO resources
                   (project_id, document_id, effective_date, commodity,
                    resource_or_reserve, category, tonnes, grade, grade_unit,
                    contained_metal, contained_metal_unit,
                    cutoff_grade, cutoff_grade_unit,
                    attributable_contained_metal, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                (
                    project_id, doc_id, estimate.snapshot_date.isoformat(),
                    estimate.commodity, estimate.resource_or_reserve,
                    row.category,
                    float(row.tonnes_mt) if row.tonnes_mt is not None else None,
                    float(row.grade) if row.grade is not None else None,
                    row.grade_unit,
                    float(row.contained_metal) if row.contained_metal is not None else None,
                    row.contained_metal_unit,
                    float(estimate.cutoff_grade) if estimate.cutoff_grade is not None else None,
                    estimate.cutoff_grade_unit,
                    now,
                ),
            )

        conn.commit()
        _mark_parsed(doc_id)
        stats["extracted"] += 1
        logger.info(
            "Extracted JORC resource for %s — %s (%s): %d rows",
            ticker, estimate.project_name, estimate.commodity, len(estimate.rows),
        )
    except Exception as e:
        conn.rollback()
        _mark_failed(doc_id, f"resource_persist_error:{e}")
        stats["failed"] += 1
        logger.exception("Failed to persist resource update for doc %d", doc_id)
    finally:
        conn.close()


def _get_or_create_project(conn, company_id: int, project_name: str) -> int:
    """Look up a project by (company_id, project_name) or create it."""
    from datetime import datetime, timezone

    # Case-insensitive match, strip trailing Project/Deposit/Mine
    import re
    clean_name = re.sub(r"\s+(?:Project|Deposit|Mine|Operation)\s*$", "", project_name, flags=re.I).strip()

    row = conn.execute(
        """SELECT project_id FROM projects
           WHERE company_id = ? AND LOWER(project_name) = LOWER(?)
           ORDER BY created_at DESC LIMIT 1""",
        (company_id, clean_name),
    ).fetchone()

    if row:
        return row["project_id"]

    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """INSERT INTO projects (company_id, project_name, created_at)
           VALUES (?, ?, ?)""",
        (company_id, clean_name, now),
    )
    return cursor.lastrowid


def _update_doc_type(doc_id: int, doc_type: str) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET doc_type = ? WHERE document_id = ?",
        (doc_type, doc_id),
    )
    conn.commit()
    conn.close()


def _mark_skipped(doc_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET parse_status = 'skipped' WHERE document_id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()


def _mark_parsed(doc_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET parse_status = 'parsed' WHERE document_id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()


def _mark_failed(doc_id: int, error: str) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET parse_status = 'failed', parse_error = ? WHERE document_id = ?",
        (error, doc_id),
    )
    conn.commit()
    conn.close()


def run_orchestrator() -> dict:
    """Full pipeline run: classify → extract → normalize."""
    classified = classify_pending()
    stats = extract_classified()
    stats["classified"] = classified
    return stats

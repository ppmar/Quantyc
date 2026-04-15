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

    STANDARDIZED_TYPES = {"appendix_5b", "issue_of_securities"}

    stats = {"extracted": 0, "skipped": 0, "failed": 0}

    conn = get_connection()
    docs = conn.execute(
        """SELECT document_id, doc_type, url, header
           FROM documents WHERE parse_status = 'classified'
           ORDER BY announcement_date DESC"""
    ).fetchall()
    conn.close()

    # Split into already-standardized vs needs-scanning
    standard_docs = []
    other_docs = []
    for doc in docs:
        if doc["doc_type"] in STANDARDIZED_TYPES:
            standard_docs.append(doc)
        else:
            other_docs.append(doc)

    # Scan non-standardized docs for embedded standardized forms
    # (e.g. quarterly activity reports with an Appendix 5B at the end)
    for doc in other_docs:
        doc_id = doc["document_id"]
        url = doc["url"]

        pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
        if not pdf_bytes:
            _mark_skipped(doc_id)
            stats["skipped"] += 1
            continue

        found_type = contains_standardized_form(pdf_bytes)
        if found_type:
            logger.info("Doc %d reclassified: %s → %s (embedded form found)", doc_id, doc["doc_type"], found_type)
            _update_doc_type(doc_id, found_type)
            _extract_doc(doc_id, found_type, pdf_bytes, stats,
                         extract_appendix_5b, extract_issue_of_securities,
                         normalize_from_5b, normalize_from_securities)
        else:
            _mark_skipped(doc_id)
            stats["skipped"] += 1
        del pdf_bytes

    # Process already-standardized docs
    for doc in standard_docs:
        doc_id = doc["document_id"]
        url = doc["url"]
        doc_type = doc["doc_type"]

        pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
        if not pdf_bytes:
            _mark_failed(doc_id, "download_failed")
            stats["failed"] += 1
            continue

        _extract_doc(doc_id, doc_type, pdf_bytes, stats,
                     extract_appendix_5b, extract_issue_of_securities,
                     normalize_from_5b, normalize_from_securities)
        del pdf_bytes

    logger.info("Extraction: %s", stats)
    return stats


def _extract_doc(doc_id, doc_type, pdf_bytes, stats,
                 extract_appendix_5b, extract_issue_of_securities,
                 normalize_from_5b, normalize_from_securities):
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

"""
Pipeline Orchestrator

Week 1: pick pending docs → classify → update doc_type → set parse_status='classified'.
Week 2: classified docs → extract → normalize → flag.

Usage:
    from pipeline.orchestrator import run_orchestrator
"""

import logging

from db import get_connection
from pipeline.classify import classify

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
    Week 2: routes to type-specific extractors.
    Returns stats dict.
    """
    from ingest.asx_poller import fetch_pdf_bytes
    from pipeline.extractors.appendix_5b import extract_appendix_5b
    from pipeline.extractors.issue_of_securities import extract_issue_of_securities
    from pipeline.normalize.company_financials import normalize_from_5b, normalize_from_securities

    stats = {"extracted": 0, "skipped": 0, "failed": 0}

    conn = get_connection()
    docs = conn.execute(
        "SELECT document_id, doc_type, url, header FROM documents WHERE parse_status = 'classified'"
    ).fetchall()
    conn.close()

    for doc in docs:
        doc_id = doc["document_id"]
        doc_type = doc["doc_type"]
        url = doc["url"]

        if doc_type == "appendix_5b":
            pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
            if not pdf_bytes:
                _mark_failed(doc_id, "download_failed")
                stats["failed"] += 1
                continue

            result = extract_appendix_5b(doc_id, pdf_bytes)
            del pdf_bytes
            if result:
                normalize_from_5b(doc_id)
                _mark_parsed(doc_id)
                stats["extracted"] += 1
            else:
                _mark_failed(doc_id, "extraction_empty")
                stats["failed"] += 1

        elif doc_type == "issue_of_securities":
            pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
            if not pdf_bytes:
                _mark_failed(doc_id, "download_failed")
                stats["failed"] += 1
                continue

            result = extract_issue_of_securities(doc_id, pdf_bytes)
            del pdf_bytes
            if result:
                normalize_from_securities(doc_id)
                _mark_parsed(doc_id)
                stats["extracted"] += 1
            else:
                _mark_failed(doc_id, "extraction_empty")
                stats["failed"] += 1

        else:
            # Not handled in Week 2 — skip
            conn = get_connection()
            conn.execute(
                "UPDATE documents SET parse_status = 'skipped' WHERE document_id = ?",
                (doc_id,),
            )
            conn.commit()
            conn.close()
            stats["skipped"] += 1

    logger.info("Extraction: %s", stats)
    return stats


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

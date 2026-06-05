#!/usr/bin/env python3
"""
Re-extract specific studies whose NPV extraction was incomplete/suspect.

Deletes the existing (broken) study row and its revaluations, re-fetches the
source PDF, and re-runs the LLM study extractor through the orchestrator path
(which now applies check_study_review_flags on persist).

Usage:
    GOOGLE_API_KEY=... python scripts/reextract_studies.py 8 10 13 14
    # no args => re-extract every study currently flagged needs_review with a
    # missing-NPV reason.
"""
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from db import get_connection
from ingest.asx_poller import fetch_pdf_bytes, _extract_study


def _target_study_ids(conn, argv):
    if argv:
        return [int(a) for a in argv]
    rows = conn.execute(
        """SELECT study_id FROM studies
           WHERE needs_review = 1
             AND (review_reason LIKE '%missing_pre_tax_npv%'
                  OR review_reason LIKE '%missing_post_tax_npv%')"""
    ).fetchall()
    return [r["study_id"] for r in rows]


def reextract(conn, study_id: int) -> bool:
    row = conn.execute(
        """SELECT s.study_id, s.document_id, d.doc_type, d.url
           FROM studies s JOIN documents d ON s.document_id = d.document_id
           WHERE s.study_id = ?""",
        (study_id,),
    ).fetchone()
    if not row:
        logger.warning("study %d not found or has no document", study_id)
        return False
    if not row["url"]:
        logger.warning("study %d doc %d has no URL — cannot re-fetch", study_id, row["document_id"])
        return False

    doc_id = row["document_id"]
    doc_type = row["doc_type"]

    pdf_bytes = fetch_pdf_bytes(row["url"])
    if not pdf_bytes:
        logger.error("study %d: failed to fetch PDF %s", study_id, row["url"])
        return False

    # Remove the broken row + dependent revaluations so re-extraction replaces it
    # (dedup is on project_id+stage+post_tax_npv; a changed NPV would otherwise
    # leave the broken row behind).
    conn.execute("DELETE FROM revaluations WHERE study_id = ?", (study_id,))
    conn.execute("DELETE FROM studies WHERE study_id = ?", (study_id,))
    conn.commit()

    _extract_study(doc_id, doc_type, pdf_bytes)  # orchestrator path; re-applies guard
    logger.info("re-extracted study (old id %d, doc %d, %s)", study_id, doc_id, doc_type)
    return True


def main():
    conn = get_connection()
    targets = _target_study_ids(conn, sys.argv[1:])
    if not targets:
        logger.info("no target studies")
        return
    logger.info("re-extracting studies: %s", targets)
    ok = sum(1 for sid in targets if reextract(conn, sid))
    logger.info("done: %d/%d re-extracted", ok, len(targets))


if __name__ == "__main__":
    main()

"""
Document Classifier

Classifies ASX announcement doc_type from the announcement title/header.
Called after the collector has downloaded documents.

Usage:
    python -m pipeline.classifier
"""

import logging

from db import get_connection

logger = logging.getLogger(__name__)

# Keywords mapped to doc_type — checked in priority order
TYPE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("appendix_5b", ["appendix 5b", "quarterly cash flow"]),
    ("study", [
        "scoping study", "pfs", "dfs", "pre-feasibility", "feasibility study",
        "definitive feasibility", "preliminary economic assessment",
    ]),
    ("resource_update", [
        "resource estimate", "reserve estimate", "jorc resource",
        "mineral resource estimate", "ore reserve estimate",
        "resource update", "maiden resource",
    ]),
    ("drill_results", [
        "drill", "drilling", "assay", "intercept", "intersection",
        "high-grade", "high grade", "metres @", "meters @", "m @",
        "g/t", "exploration results", "drill hole", "drill program",
    ]),
    ("capital_raise", [
        "placement", "entitlement offer", "rights issue", "issue of securities",
        "capital raising", "share purchase plan", "spp",
    ]),
    ("annual_report", ["annual report", "annual financial"]),
    ("quarterly_report", [
        "quarterly activity", "quarterly report", "operations update",
        "quarterly activities",
    ]),
]


def classify_title(title: str) -> str:
    """Classify a document type from its announcement title."""
    title_lower = title.lower()
    for doc_type, keywords in TYPE_KEYWORDS:
        for kw in keywords:
            if kw in title_lower:
                return doc_type
    return "other"


def classify_unclassified():
    """
    Classify all documents that don't yet have a doc_type set.
    Uses the stored header, falling back to URL path.
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, header, url FROM documents WHERE doc_type IS NULL"
    ).fetchall()

    if not rows:
        logger.info("No unclassified documents found")
        return 0

    classified = 0
    for row in rows:
        title = row["header"] or ""
        if not title:
            url = row["url"] or ""
            title = url.rsplit("/", 1)[-1].replace("-", " ").replace("_", " ").replace("+", " ")
        doc_type = classify_title(title)

        conn.execute(
            "UPDATE documents SET doc_type = ? WHERE id = ?",
            (doc_type, row["id"]),
        )
        classified += 1

    conn.commit()
    conn.close()
    logger.info("Classified %d documents", classified)
    return classified


def classify_from_header(doc_id: str, header: str):
    """Classify a single document given its announcement header text."""
    doc_type = classify_title(header)
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET doc_type = ? WHERE id = ?",
        (doc_type, doc_id),
    )
    conn.commit()
    conn.close()
    return doc_type


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    n = classify_unclassified()
    print(f"Classified {n} documents")

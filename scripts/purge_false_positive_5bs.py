"""
Purge company_financials rows whose effective_date is not a fiscal quarter-end.

These rows are false positives from the pre-gate ingestion era, where
normalize_from_5b fell back to announcement_date when the extractor
failed to determine a real effective_date.

Usage:
    python -m scripts.purge_false_positive_5bs --dry-run
    python -m scripts.purge_false_positive_5bs --execute
"""

import argparse
import logging
from db import get_connection

logger = logging.getLogger(__name__)

VALID_MMDD = ("03-31", "06-30", "09-30", "12-31")


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true")
    group.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conn = get_connection()

    rows = conn.execute(
        """SELECT cf.financial_id, cf.document_id, c.ticker, cf.effective_date
           FROM company_financials cf
           JOIN companies c ON c.company_id = cf.company_id
           WHERE substr(cf.effective_date, 6, 5) NOT IN (?, ?, ?, ?)
           ORDER BY c.ticker, cf.effective_date""",
        VALID_MMDD,
    ).fetchall()

    print(f"Found {len(rows)} rows with invalid effective_date")
    for r in rows:
        print(f"  {r['ticker']}  {r['effective_date']}  financial_id={r['financial_id']}  doc={r['document_id']}")

    if args.dry_run:
        print("\nDry run — no changes made.")
        conn.close()
        return

    if not rows:
        print("\nNothing to purge.")
        conn.close()
        return

    # Execute: delete from company_financials and mark the source documents
    financial_ids = [r["financial_id"] for r in rows]
    document_ids = [r["document_id"] for r in rows]

    conn.execute(
        f"DELETE FROM company_financials "
        f"WHERE financial_id IN ({','.join('?' * len(financial_ids))})",
        financial_ids,
    )
    # Mark source documents so they won't be re-normalized
    conn.execute(
        f"UPDATE documents SET parse_status = 'failed', "
        f"parse_error = 'purged_by_migration:invalid_quarter_end' "
        f"WHERE document_id IN ({','.join('?' * len(document_ids))})",
        document_ids,
    )

    # Also drop the related staging rows so a future reprocess can't resurrect them
    conn.execute(
        f"DELETE FROM _stg_appendix_5b "
        f"WHERE document_id IN ({','.join('?' * len(document_ids))})",
        document_ids,
    )

    conn.commit()
    conn.close()
    print(f"\nDeleted {len(rows)} rows from company_financials and marked {len(document_ids)} documents as failed.")


if __name__ == "__main__":
    main()

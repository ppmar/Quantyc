#!/usr/bin/env python3
"""
One-off backfill after the burn-sign / needs_review / reval-warning fixes.

1. Re-extract Appendix 5B documents whose company_financials rows carry burn
   figures. The old extractor stored quarterly_opex_burn as abs(), so a
   producer's operating INFLOW (CMM ~A$125M/quarter) was recorded as burn and
   now displays as near-zero runway. The sign is unrecoverable from the DB:
   delete the affected rows, reset the docs to 'classified', re-run extraction
   (PDFs are re-fetched stateless by URL).
2. Clean review_reason NA-noise (old _check_review_flags flagged fields the
   doc type can never provide) and set needs_review on genuine reasons.
3. Recompute revaluations so the new persisted warnings exist on every row.

Usage:
    python scripts/backfill_burn_sign_and_review.py [--dry-run]

The DB file is backed up next to itself before any write.
"""
import argparse
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)

# Reasons the old check emitted for fields the document type can never
# provide. Stripped by the cleanup; whatever remains is a genuine flag.
_NA_REASONS_BY_DOC_TYPE: dict[str, set[str]] = {
    "appendix_5b": {"missing_shares_fd"},
    "quarterly_activity": {"missing_shares_fd"},
    "appendix_2a": {"missing_cash", "missing_opex_burn"},
    "appendix_3g": {"missing_cash", "missing_opex_burn"},
    "appendix_3h": {"missing_cash", "missing_opex_burn"},
    "issue_of_securities": {"missing_cash", "missing_opex_burn"},
    "presentation": {"missing_opex_burn"},
}


def clean_review_reason(
    reason: str | None, doc_type: str | None
) -> tuple[bool, str | None]:
    """Strip NA-reasons for this doc_type; flag iff genuine reasons remain."""
    if not reason:
        return False, None
    na = _NA_REASONS_BY_DOC_TYPE.get(doc_type or "", set())
    kept = [part for part in (p.strip() for p in reason.split(";")) if part and part not in na]
    if kept:
        return True, "; ".join(kept)
    return False, None


def _backup_db(db_path: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = db_path.with_suffix(f".db.bak-{stamp}")
    shutil.copy2(db_path, backup)
    return backup


def step1_reset_5b_docs(conn, dry_run: bool) -> list[int]:
    """Delete burn-bearing 5B rows and reset their docs for re-extraction."""
    doc_ids = [
        r[0]
        for r in conn.execute(
            """SELECT DISTINCT cf.document_id
               FROM company_financials cf
               JOIN documents d ON d.document_id = cf.document_id
               WHERE d.doc_type IN ('appendix_5b', 'quarterly_activity')
                 AND (cf.quarterly_opex_burn IS NOT NULL
                      OR cf.quarterly_invest_burn IS NOT NULL)"""
        ).fetchall()
    ]
    logger.info("Step 1: %d documents to re-extract: %s", len(doc_ids), doc_ids)
    if dry_run or not doc_ids:
        return doc_ids

    qmarks = ",".join("?" * len(doc_ids))
    conn.execute(f"DELETE FROM company_financials WHERE document_id IN ({qmarks})", doc_ids)
    conn.execute(f"DELETE FROM _stg_appendix_5b WHERE document_id IN ({qmarks})", doc_ids)
    conn.execute(
        f"""UPDATE documents SET parse_status = 'classified', parse_error = NULL
            WHERE document_id IN ({qmarks})""",
        doc_ids,
    )
    conn.commit()
    return doc_ids


def step2_clean_review_reasons(conn, dry_run: bool) -> int:
    rows = conn.execute(
        """SELECT cf.financial_id, cf.review_reason, d.doc_type
           FROM company_financials cf
           LEFT JOIN documents d ON d.document_id = cf.document_id
           WHERE cf.review_reason IS NOT NULL"""
    ).fetchall()
    changed = 0
    for fid, reason, doc_type in rows:
        needs_review, cleaned = clean_review_reason(reason, doc_type)
        if cleaned != reason or needs_review:
            changed += 1
            logger.info(
                "Step 2: financial_id=%s doc_type=%s: %r -> needs_review=%d reason=%r",
                fid, doc_type, reason, int(needs_review), cleaned,
            )
            if not dry_run:
                conn.execute(
                    "UPDATE company_financials SET needs_review = ?, review_reason = ? "
                    "WHERE financial_id = ?",
                    (1 if needs_review else 0, cleaned, fid),
                )
    if not dry_run:
        conn.commit()
    return changed


def step3_rerun_revaluations(conn, dry_run: bool) -> dict:
    from revaluation.pipeline import revalue_study
    from revaluation.math import RevaluationError
    from revaluation.prices import PriceFetchError

    study_ids = [
        r[0] for r in conn.execute("SELECT DISTINCT study_id FROM revaluations").fetchall()
    ]
    logger.info("Step 3: %d studies to revalue: %s", len(study_ids), study_ids)
    stats = {"ok": 0, "skipped": 0, "failed": 0}
    if dry_run:
        return stats
    for study_id in study_ids:
        try:
            reval_id = revalue_study(conn, study_id)
            stats["ok" if reval_id else "skipped"] += 1
        except (RevaluationError, PriceFetchError) as e:
            logger.warning("Step 3: study %d failed: %s", study_id, e)
            stats["failed"] += 1
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report, write nothing")
    parser.add_argument(
        "--no-extract", action="store_true",
        help="skip in-process re-extraction; leave reset docs 'classified' for "
             "the orchestrator (use POST /api/orchestrate — survives SSH disconnect)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from db import DB_PATH, get_connection

    if not args.dry_run:
        backup = _backup_db(Path(DB_PATH))
        logger.info("DB backed up to %s", backup)

    conn = get_connection()
    doc_ids = step1_reset_5b_docs(conn, args.dry_run)
    cleaned = step2_clean_review_reasons(conn, args.dry_run)
    conn.close()

    # Re-extract AFTER the cleanup pass so freshly written rows (with correct
    # reasons from the fixed normalizer) are not touched by step 2.
    extract_stats = None
    if doc_ids and not args.dry_run and not args.no_extract:
        from pipeline.orchestrator import extract_classified
        extract_stats = extract_classified()

    conn = get_connection()
    reval_stats = step3_rerun_revaluations(conn, args.dry_run)
    conn.close()

    print(f"\nStep 1 (5B re-extract): {len(doc_ids)} docs reset; extraction: {extract_stats}")
    print(f"Step 2 (review reasons): {cleaned} rows updated")
    print(f"Step 3 (revaluations):  {reval_stats}")
    if args.dry_run:
        print("(dry run — nothing written)")


if __name__ == "__main__":
    main()

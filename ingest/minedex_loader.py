"""
MINEDEX Bootstrap Loader — populate projects from WA DMIRS MINEDEX database.

Reads a pre-downloaded MINEDEX CSV extract, matches operator names to ASX tickers,
and upserts into the projects + project_commodities tables.

Usage:
    from ingest.minedex_loader import load_minedex
    stats = load_minedex(csv_path="data/minedex_extract.csv", dry_run=False)
"""

import csv
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from db import get_connection
from ingest.ozmin_loader import normalize_operator, load_operator_mapping, _normalize_stage, _parse_commodities

logger = logging.getLogger(__name__)

DEFAULT_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "minedex_extract.csv"


def load_minedex(
    csv_path: str | Path = DEFAULT_CSV_PATH,
    dry_run: bool = False,
    rows: list[dict] | None = None,
) -> dict:
    """
    Load MINEDEX records into projects + project_commodities.

    Args:
        csv_path: Path to MINEDEX CSV extract.
        dry_run: If True, print what would be inserted but don't write.
        rows: Pre-loaded rows (for testing). If None, reads from CSV.

    Returns:
        Stats dict with counts.
    """
    if rows is None:
        csv_path = Path(csv_path)
        if not csv_path.exists():
            logger.error("MINEDEX CSV not found: %s", csv_path)
            return {"error": f"CSV not found: {csv_path}"}

        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

    logger.info("MINEDEX: %d records loaded", len(rows))

    operator_map = load_operator_mapping()
    conn = get_connection()

    stats = {
        "fetched": len(rows),
        "matched": 0,
        "inserted": 0,
        "updated": 0,
        "commodities_inserted": 0,
        "skipped_unmapped": 0,
        "skipped_no_company": 0,
        "skipped_duplicate": 0,
    }

    for record in rows:
        # Flexible column name matching (MINEDEX schema varies between releases)
        deposit_name = (
            record.get("DEPOSIT_NAME")
            or record.get("deposit_name")
            or record.get("Name")
            or record.get("PROJECT_NAME")
            or ""
        ).strip()

        operator_raw = (
            record.get("OPERATOR")
            or record.get("operator")
            or record.get("Operator")
            or ""
        ).strip()

        commodities_raw = (
            record.get("COMMODITIES")
            or record.get("commodities")
            or record.get("Commodities")
            or record.get("COMMODITY")
            or ""
        ).strip()

        operating_status = (
            record.get("OPERATING_STATUS")
            or record.get("operating_status")
            or record.get("Status")
            or ""
        ).strip()

        if not deposit_name or not operator_raw:
            stats["skipped_unmapped"] += 1
            continue

        operator_norm = normalize_operator(operator_raw)
        ticker = operator_map.get(operator_norm)

        if not ticker:
            logger.warning("MINEDEX: unmapped operator '%s' (deposit: %s)", operator_raw, deposit_name)
            stats["skipped_unmapped"] += 1
            continue

        row = conn.execute(
            "SELECT company_id FROM companies WHERE ticker = ?", (ticker,)
        ).fetchone()

        if not row:
            logger.info("MINEDEX: ticker %s mapped but no companies row (deposit: %s)", ticker, deposit_name)
            stats["skipped_no_company"] += 1
            continue

        company_id = row["company_id"]
        stats["matched"] += 1

        project_name = re.sub(
            r"\s+(?:Project|Deposit|Mine|Operation)\s*$", "", deposit_name, flags=re.I
        ).strip()

        stage = _normalize_stage(operating_status)

        if dry_run:
            logger.info("DRY RUN: would insert project '%s' for %s (stage=%s)",
                        project_name, ticker, stage)
            stats["inserted"] += 1
            continue

        now = datetime.now(timezone.utc).isoformat()
        existing = conn.execute(
            """SELECT project_id, stage, state, source FROM projects
               WHERE company_id = ? AND LOWER(project_name) = LOWER(?)
               ORDER BY created_at DESC LIMIT 1""",
            (company_id, project_name),
        ).fetchone()

        if existing:
            # Only update NULL fields; never overwrite existing data
            updates = []
            params = []
            if existing["stage"] is None and stage:
                updates.append("stage = ?")
                params.append(stage)
            if existing["state"] is None:
                updates.append("state = ?")
                params.append("WA")
            if existing["source"] is None:
                updates.append("source = ?")
                params.append("minedex")
            if updates:
                params.append(existing["project_id"])
                conn.execute(
                    f"UPDATE projects SET {', '.join(updates)} WHERE project_id = ?",
                    params,
                )
                stats["updated"] += 1
            else:
                stats["skipped_duplicate"] += 1
            project_id = existing["project_id"]
        else:
            cursor = conn.execute(
                """INSERT INTO projects (company_id, project_name, country, state, stage, source, created_at)
                   VALUES (?, ?, 'Australia', 'WA', ?, 'minedex', ?)""",
                (company_id, project_name, stage, now),
            )
            project_id = cursor.lastrowid
            stats["inserted"] += 1

        commodities = _parse_commodities(commodities_raw)
        for i, commodity in enumerate(commodities):
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO project_commodities (project_id, commodity, is_primary)
                       VALUES (?, ?, ?)""",
                    (project_id, commodity, 1 if i == 0 else 0),
                )
                stats["commodities_inserted"] += 1
            except Exception:
                pass

    if not dry_run:
        conn.commit()
    conn.close()

    return stats

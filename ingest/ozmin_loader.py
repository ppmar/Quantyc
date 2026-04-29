"""
OZMIN Bootstrap Loader — populate projects from Geoscience Australia's OZMIN database.

Fetches mineral deposit features via WFS, matches operator names to ASX tickers
using a hand-maintained CSV, and upserts into the projects + project_commodities tables.

Usage:
    from ingest.ozmin_loader import load_ozmin
    stats = load_ozmin(dry_run=False)
"""

import csv
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import requests

from db import get_connection

logger = logging.getLogger(__name__)

OZMIN_WFS_URL = (
    "https://services.ga.gov.au/gis/services/ProvinceMineralResourcesMines/MapServer/WFSServer"
)

OPERATOR_CSV = Path(__file__).resolve().parent.parent / "data" / "ozmin_operator_to_ticker.csv"

_COMPANY_SUFFIXES = re.compile(
    r"\s+(?:limited|ltd\.?|pty|plc|corp|corporation|inc|n\.?l\.?)\s*$",
    re.I,
)

# OZMIN operating_status → projects.stage mapping
_STAGE_MAP = {
    "operating mine": "production",
    "producer": "production",
    "production": "production",
    "care and maintenance": "care_and_maintenance",
    "construction": "development",
    "development": "development",
    "feasibility": "feasibility",
    "pfs": "feasibility",
    "dfs": "feasibility",
    "resource definition": "advanced_exploration",
    "advanced exploration": "advanced_exploration",
    "exploration": "exploration",
    "prospect": "exploration",
}


def normalize_operator(name: str) -> str:
    """Normalize an operator company name for matching."""
    n = name.strip().lower()
    n = _COMPANY_SUFFIXES.sub("", n).strip()
    n = re.sub(r"\s+", " ", n)
    return n


def load_operator_mapping(csv_path: Path = OPERATOR_CSV) -> dict[str, str]:
    """Load operator_name_normalized → ticker mapping from CSV."""
    mapping = {}
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["operator_name_normalized"].strip().lower()
            mapping[key] = row["ticker"].strip().upper()
    return mapping


def fetch_ozmin_features() -> list[dict]:
    """Fetch features from OZMIN WFS endpoint."""
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "TYPENAME": "MineralResources_MineralResourceView",
        "OUTPUTFORMAT": "application/json",
    }
    resp = requests.get(OZMIN_WFS_URL, params=params, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data.get("features", [])


def _extract_feature_fields(feature: dict) -> dict:
    """Extract relevant fields from an OZMIN GeoJSON feature."""
    props = feature.get("properties", {})
    return {
        "deposit_name": props.get("deposit_name") or props.get("name") or props.get("DEPOSIT_NAME") or "",
        "operator_name": (
            props.get("operator_name")
            or props.get("holder_name")
            or props.get("tenement_holder")
            or props.get("OPERATOR")
            or ""
        ),
        "commodities": props.get("commodities") or props.get("commodity") or props.get("COMMODITIES") or "",
        "state": props.get("state") or props.get("STATE") or "",
        "operating_status": props.get("operating_status") or props.get("mining_status") or "",
    }


def _normalize_stage(status: str) -> str | None:
    """Map OZMIN operating_status to projects.stage."""
    return _STAGE_MAP.get(status.strip().lower())


def _parse_commodities(raw: str) -> list[str]:
    """Split commodity string into list."""
    if not raw:
        return []
    # OZMIN uses comma or semicolon separation
    parts = re.split(r"[,;/]", raw)
    return [p.strip() for p in parts if p.strip()]


def load_ozmin(dry_run: bool = False, features: list[dict] | None = None) -> dict:
    """
    Load OZMIN features into projects + project_commodities.

    Args:
        dry_run: If True, print what would be inserted but don't write.
        features: Pre-fetched features (for testing). If None, fetches from WFS.

    Returns:
        Stats dict with counts.
    """
    if features is None:
        logger.info("Fetching OZMIN features from WFS...")
        features = fetch_ozmin_features()

    logger.info("OZMIN: %d features fetched", len(features))

    operator_map = load_operator_mapping()
    conn = get_connection()

    stats = {
        "fetched": len(features),
        "matched": 0,
        "inserted": 0,
        "updated": 0,
        "commodities_inserted": 0,
        "skipped_unmapped": 0,
        "skipped_no_company": 0,
    }

    for feature in features:
        fields = _extract_feature_fields(feature)
        deposit_name = fields["deposit_name"].strip()
        operator_raw = fields["operator_name"]

        if not deposit_name or not operator_raw:
            stats["skipped_unmapped"] += 1
            continue

        operator_norm = normalize_operator(operator_raw)
        ticker = operator_map.get(operator_norm)

        if not ticker:
            logger.warning("OZMIN: unmapped operator '%s' (deposit: %s)", operator_raw, deposit_name)
            stats["skipped_unmapped"] += 1
            continue

        # Look up company_id
        row = conn.execute(
            "SELECT company_id FROM companies WHERE ticker = ?", (ticker,)
        ).fetchone()

        if not row:
            logger.info("OZMIN: ticker %s mapped but no companies row (deposit: %s)", ticker, deposit_name)
            stats["skipped_no_company"] += 1
            continue

        company_id = row["company_id"]
        stats["matched"] += 1

        # Normalize project name
        project_name = re.sub(
            r"\s+(?:Project|Deposit|Mine|Operation)\s*$", "", deposit_name, flags=re.I
        ).strip()

        stage = _normalize_stage(fields["operating_status"])
        state = fields["state"].strip() or None

        if dry_run:
            logger.info("DRY RUN: would insert project '%s' for %s (stage=%s, state=%s)",
                        project_name, ticker, stage, state)
            stats["inserted"] += 1
            continue

        # Upsert project: insert if not exists, update only NULL fields if exists
        now = datetime.now(timezone.utc).isoformat()
        existing = conn.execute(
            """SELECT project_id, stage, state, country, source FROM projects
               WHERE company_id = ? AND LOWER(project_name) = LOWER(?)
               ORDER BY created_at DESC LIMIT 1""",
            (company_id, project_name),
        ).fetchone()

        if existing:
            # Update only NULL fields
            updates = []
            params = []
            if existing["stage"] is None and stage:
                updates.append("stage = ?")
                params.append(stage)
            if existing["state"] is None and state:
                updates.append("state = ?")
                params.append(state)
            if existing["country"] is None:
                updates.append("country = ?")
                params.append("Australia")
            if existing["source"] is None:
                updates.append("source = ?")
                params.append("ozmin")
            if updates:
                params.append(existing["project_id"])
                conn.execute(
                    f"UPDATE projects SET {', '.join(updates)} WHERE project_id = ?",
                    params,
                )
                stats["updated"] += 1
            project_id = existing["project_id"]
        else:
            cursor = conn.execute(
                """INSERT INTO projects (company_id, project_name, country, state, stage, source, created_at)
                   VALUES (?, ?, 'Australia', ?, ?, 'ozmin', ?)""",
                (company_id, project_name, state, stage, now),
            )
            project_id = cursor.lastrowid
            stats["inserted"] += 1

        # Commodities
        commodities = _parse_commodities(fields["commodities"])
        for i, commodity in enumerate(commodities):
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO project_commodities (project_id, commodity, is_primary)
                       VALUES (?, ?, ?)""",
                    (project_id, commodity, 1 if i == 0 else 0),
                )
                stats["commodities_inserted"] += 1
            except Exception:
                pass  # duplicate, skip

    if not dry_run:
        conn.commit()
    conn.close()

    return stats

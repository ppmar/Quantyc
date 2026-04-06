"""
Normalizer

Resolves units, currencies, gross→attributable, and dilution calculations.
Takes raw staging_extractions and produces normalized values ready for
loading into core tables.

Usage:
    python -m pipeline.normalizer --doc-id <id>
    python -m pipeline.normalizer --all
"""

import argparse
import logging

from db import get_connection, init_db

logger = logging.getLogger(__name__)

# Unit conversion factors to standard units
# Gold: standard unit is koz (thousands of ounces)
# Copper: standard unit is kt (thousands of tonnes)
# Lithium: standard unit is kt Li2O
# Cash: standard unit is AUD (dollars, not thousands)

METAL_UNIT_CONVERSIONS = {
    # Gold / Silver
    ("oz", "koz"): 0.001,
    ("koz", "koz"): 1.0,
    ("moz", "koz"): 1000.0,
    ("g", "koz"): 0.0000321507,  # 1 gram = 0.0321507 troy oz, /1000 for koz
    # Base metals
    ("t", "kt"): 0.001,
    ("kt", "kt"): 1.0,
    ("mt", "kt"): 1000.0,
    ("lb", "mlb"): 0.000001,
    ("klb", "mlb"): 0.001,
    ("mlb", "mlb"): 1.0,
    # Tonnes
    ("t", "mt"): 0.000001,
    ("kt", "mt"): 0.001,
    ("mt", "mt"): 1.0,
}

# Cash unit conversions to AUD
CASH_UNIT_CONVERSIONS = {
    "AUD_000": 1000.0,        # A$'000 → AUD
    "AUD_M": 1_000_000.0,     # A$ million → AUD
    "AUD": 1.0,
    "USD_000": None,           # Needs FX rate
    "USD_M": None,             # Needs FX rate
    "USD": None,               # Needs FX rate
}


def get_latest_fx_rate(conn) -> float | None:
    """Get the most recent AUD/USD rate from macro_assumptions."""
    row = conn.execute(
        "SELECT aud_usd FROM macro_assumptions ORDER BY date DESC LIMIT 1"
    ).fetchone()
    if row and row["aud_usd"]:
        return row["aud_usd"]
    return None


def normalize_cash_to_aud(value: float, unit: str, fx_rate: float | None = None) -> float | None:
    """
    Normalize a cash value to AUD.
    Returns None if conversion is not possible (e.g., USD without FX rate).
    """
    if unit in ("AUD_000", "AUD_M", "AUD"):
        factor = CASH_UNIT_CONVERSIONS[unit]
        return value * factor

    if unit in ("USD_000", "USD_M", "USD"):
        if fx_rate is None or fx_rate == 0:
            logger.warning("Cannot convert %s to AUD: no FX rate available", unit)
            return None
        # Convert USD to AUD: AUD = USD / aud_usd_rate
        usd_factors = {"USD_000": 1000.0, "USD_M": 1_000_000.0, "USD": 1.0}
        usd_value = value * usd_factors[unit]
        return usd_value / fx_rate

    logger.warning("Unknown cash unit: %s", unit)
    return None


def normalize_contained_metal(value: float, from_unit: str, commodity: str) -> tuple[float | None, str | None]:
    """
    Normalize contained metal to standard units per commodity.
    Returns (normalized_value, standard_unit) or (None, None).
    """
    from_unit_lower = from_unit.lower().strip()

    # Determine target unit by commodity
    if commodity in ("gold", "silver"):
        target = "koz"
    elif commodity in ("copper", "zinc", "nickel", "lead", "tin", "cobalt"):
        target = "kt"
    elif commodity in ("lithium",):
        target = "kt"
    elif commodity in ("iron_ore",):
        target = "mt"
    elif commodity in ("uranium",):
        target = "mlb"
    else:
        target = "kt"

    key = (from_unit_lower, target)
    if key in METAL_UNIT_CONVERSIONS:
        return value * METAL_UNIT_CONVERSIONS[key], target

    # If already in target unit
    if from_unit_lower == target:
        return value, target

    logger.warning("Cannot convert %s → %s for %s", from_unit, target, commodity)
    return None, None


def compute_attributable(gross_value: float, ownership_pct: float | None) -> float:
    """Compute attributable value: gross × ownership percentage."""
    if ownership_pct is None:
        # Default to 100% if ownership not specified
        return gross_value
    return gross_value * (ownership_pct / 100.0)


def compute_cash_runway(cash_aud: float | None, quarterly_burn: float | None) -> float | None:
    """
    Compute cash runway in months.
    quarterly_burn should be positive (absolute spend per quarter).
    """
    if cash_aud is None or quarterly_burn is None or quarterly_burn == 0:
        return None
    # burn is typically negative in reports (cash outflow), but we want positive months
    burn = abs(quarterly_burn)
    quarters = cash_aud / burn
    return quarters * 3  # Convert quarters to months


def compute_fully_diluted_shares(
    basic: float | None,
    options: float | None = None,
    warrants: float | None = None,
    rights: float | None = None,
    convertibles: float | None = None,
) -> float | None:
    """Compute fully diluted share count."""
    if basic is None:
        return None
    fd = basic
    for component in (options, warrants, rights, convertibles):
        if component is not None:
            fd += component
    return fd


def normalize_document_extractions(doc_id: str) -> int:
    """
    Normalize all staging extractions for a document.
    Updates normalized_value and unit in staging_extractions.
    Returns count of normalized fields.
    """
    conn = get_connection()

    rows = conn.execute(
        """SELECT id, field_name, raw_value, normalized_value, unit
           FROM staging_extractions
           WHERE document_id = ? AND reviewed = 0""",
        (doc_id,),
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    fx_rate = get_latest_fx_rate(conn)
    normalized = 0

    for row in rows:
        field = row["field_name"]
        raw = row["raw_value"]
        unit = row["unit"]
        current_norm = row["normalized_value"]

        # Skip if already normalized with a value
        if current_norm is not None:
            normalized += 1
            continue

        # Try to parse raw value as float
        try:
            value = float(raw)
        except (ValueError, TypeError):
            continue

        new_value = None
        new_unit = unit

        # Normalize cash fields
        if field.startswith("cash_") or field.startswith("raise_total"):
            if unit and unit in CASH_UNIT_CONVERSIONS:
                new_value = normalize_cash_to_aud(value, unit, fx_rate)
                new_unit = "AUD"

        # Normalize study capex/NPV
        elif field.startswith("study_") and unit:
            if unit.endswith("_M"):
                # Keep in millions for study fields
                new_value = value
                new_unit = unit

        # Default: just store the parsed float
        else:
            new_value = value

        if new_value is not None:
            conn.execute(
                """UPDATE staging_extractions
                   SET normalized_value = ?, unit = ?
                   WHERE id = ?""",
                (new_value, new_unit, row["id"]),
            )
            normalized += 1

    conn.commit()
    conn.close()
    logger.info("Normalized %d / %d extractions for document %s", normalized, len(rows), doc_id)
    return normalized


def normalize_all_pending() -> int:
    """Normalize extractions for all documents that have been parsed."""
    conn = get_connection()
    doc_ids = conn.execute(
        """SELECT DISTINCT document_id FROM staging_extractions
           WHERE normalized_value IS NULL AND reviewed = 0"""
    ).fetchall()
    conn.close()

    total = 0
    for row in doc_ids:
        total += normalize_document_extractions(row["document_id"])

    logger.info("Normalized %d fields across %d documents", total, len(doc_ids))
    return total


def main():
    parser = argparse.ArgumentParser(description="Normalize staging extractions")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Normalize a specific document")
    group.add_argument("--all", action="store_true", help="Normalize all pending")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        n = normalize_document_extractions(args.doc_id)
        print(f"Normalized {n} fields for {args.doc_id}")
    else:
        n = normalize_all_pending()
        print(f"Normalized {n} fields total")


if __name__ == "__main__":
    main()

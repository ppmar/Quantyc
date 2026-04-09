"""
Loader

Moves validated data from staging_extractions into core tables
(company_financials, resources, studies, projects).

Handles conflict resolution: newer documents supersede older ones
for the same company/project/field.

Usage:
    python -m pipeline.loader --ticker DEG
    python -m pipeline.loader --all
"""

import argparse
import logging
import re

from db import get_connection, init_db

logger = logging.getLogger(__name__)


def _slugify(text: str) -> str:
    """Create a URL-safe slug from text."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _get_staging_fields(conn, doc_id: str, prefix: str) -> dict:
    """
    Collect all staging_extractions for a document matching a field prefix.
    Returns dict of {field_name_without_prefix: normalized_value}.
    """
    rows = conn.execute(
        """SELECT field_name, normalized_value, raw_value, unit
           FROM staging_extractions
           WHERE document_id = ? AND field_name LIKE ?
           ORDER BY extracted_at DESC""",
        (doc_id, prefix + "%"),
    ).fetchall()

    fields = {}
    for row in rows:
        name = row["field_name"][len(prefix):]
        value = row["normalized_value"]
        if value is None:
            # Try raw_value as fallback for string fields
            value = row["raw_value"]
        fields[name] = value

    return fields


def load_financials_from_5b(doc_id: str) -> bool:
    """
    Load Appendix 5B data from staging into company_financials.
    """
    conn = get_connection()

    doc = conn.execute(
        "SELECT company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()
    if not doc:
        conn.close()
        return False

    ticker = doc["company_ticker"]
    effective_date = doc["announcement_date"]

    fields = _get_staging_fields(conn, doc_id, "")
    cash = fields.get("cash_at_end_quarter")
    operating = fields.get("operating_cashflow")
    investing = fields.get("investing_cashflow")

    if cash is None and operating is None:
        logger.warning("No usable 5B data for %s", doc_id)
        conn.close()
        return False

    # Convert from AUD'000 to AUD
    if cash is not None:
        cash = cash * 1000
    quarterly_burn = None
    if operating is not None and investing is not None:
        quarterly_burn = abs(operating * 1000) + abs(investing * 1000)
    elif operating is not None:
        quarterly_burn = abs(operating * 1000)

    cash_runway = None
    if cash is not None and quarterly_burn and quarterly_burn > 0:
        cash_runway = (cash / quarterly_burn) * 3  # quarters to months

    # Check for existing record — update if this is newer
    existing = conn.execute(
        """SELECT id, effective_date FROM company_financials
           WHERE ticker = ? AND source_doc_id = ?""",
        (ticker, doc_id),
    ).fetchone()

    if existing:
        logger.info("Updating existing financials record for %s from %s", ticker, doc_id)
        conn.execute(
            """UPDATE company_financials
               SET cash_aud = ?, quarterly_burn = ?, cash_runway_months = ?,
                   effective_date = ?, confidence = 'high', needs_review = 0
               WHERE id = ?""",
            (cash, quarterly_burn, cash_runway, effective_date, existing["id"]),
        )
    else:
        conn.execute(
            """INSERT INTO company_financials
               (ticker, effective_date, cash_aud, quarterly_burn, cash_runway_months,
                source_doc_id, extraction_method, confidence, needs_review)
               VALUES (?, ?, ?, ?, ?, ?, 'rule_based', 'high', ?)""",
            (
                ticker, effective_date, cash, quarterly_burn, cash_runway,
                doc_id,
                1 if cash_runway is not None and cash_runway < 6 else 0,
            ),
        )

    # Ensure company exists
    conn.execute(
        "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
        (ticker,),
    )

    conn.commit()
    conn.close()
    logger.info("Loaded 5B financials for %s (cash=%s, burn=%s, runway=%s)",
                ticker, cash, quarterly_burn, cash_runway)
    return True


def load_resource(doc_id: str) -> int:
    """
    Load resource data from staging into resources table.
    Returns number of resource rows loaded.
    """
    conn = get_connection()

    # Dedup: skip if already loaded from this document
    already = conn.execute(
        "SELECT COUNT(*) as n FROM resources WHERE source_doc_id = ?", (doc_id,)
    ).fetchone()
    if already and already["n"] > 0:
        logger.info("Resources already loaded for doc %s, skipping", doc_id)
        conn.close()
        return already["n"]

    doc = conn.execute(
        "SELECT company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()
    if not doc:
        conn.close()
        return 0

    ticker = doc["company_ticker"]
    ann_date = doc["announcement_date"]

    # Get all resource rows from staging — they have prefix "resource_"
    rows = conn.execute(
        """SELECT field_name, raw_value, normalized_value, unit, extraction_method, confidence
           FROM staging_extractions
           WHERE document_id = ? AND field_name LIKE 'resource_%'
           ORDER BY id""",
        (doc_id,),
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    # Group fields into resource rows
    # Each set of resource fields (category, tonnes_mt, grade, etc.) belongs to a row
    # We detect row boundaries by seeing a new "resource_category" field
    resource_rows = []
    current_row = {}

    for r in rows:
        field = r["field_name"][len("resource_"):]
        value = r["normalized_value"]
        if value is None:
            value = r["raw_value"]

        if field == "category" and current_row.get("category"):
            resource_rows.append(current_row)
            current_row = {}

        current_row[field] = value
        current_row["_method"] = r["extraction_method"]
        current_row["_confidence"] = r["confidence"]

    if current_row:
        resource_rows.append(current_row)

    # Determine/create a project
    # Use the first commodity found or default
    primary_commodity = None
    for rr in resource_rows:
        c = rr.get("commodity")
        if c and isinstance(c, str):
            primary_commodity = c.lower()
            break

    project_id = f"{ticker.lower()}_main"
    conn.execute(
        """INSERT OR IGNORE INTO projects
           (id, ticker, project_name, stage, ownership_pct, is_primary, updated_at)
           VALUES (?, ?, 'Main Project', 'discovery', 100.0, 1, CURRENT_TIMESTAMP)""",
        (project_id, ticker),
    )

    # Ensure company exists
    conn.execute(
        """INSERT OR IGNORE INTO companies (ticker, primary_commodity, updated_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)""",
        (ticker, primary_commodity),
    )

    # Get ownership for attributable calculation
    project = conn.execute(
        "SELECT ownership_pct FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    ownership_pct = project["ownership_pct"] if project else 100.0

    loaded = 0
    for rr in resource_rows:
        category = rr.get("category")
        if not category:
            continue

        commodity = rr.get("commodity", primary_commodity) or "unknown"
        if isinstance(commodity, str):
            commodity = commodity.lower()

        contained = rr.get("contained_metal")
        if contained is not None:
            try:
                contained = float(contained)
            except (ValueError, TypeError):
                contained = None

        attributable = None
        if contained is not None:
            attributable = contained * (ownership_pct / 100.0)

        tonnes = rr.get("tonnes_mt")
        if tonnes is not None:
            try:
                tonnes = float(tonnes)
            except (ValueError, TypeError):
                tonnes = None

        grade = rr.get("grade")
        if grade is not None:
            try:
                grade = float(grade)
            except (ValueError, TypeError):
                grade = None

        cut_off = rr.get("cut_off_grade")
        if cut_off is not None:
            try:
                cut_off = float(cut_off)
            except (ValueError, TypeError):
                cut_off = None

        estimate_type = rr.get("estimate_type", "resource")

        conn.execute(
            """INSERT INTO resources
               (project_id, commodity, effective_date, estimate_type, category,
                tonnes_mt, grade, grade_unit, contained_metal, contained_unit,
                attributable_contained, cut_off_grade,
                source_doc_id, extraction_method, confidence, needs_review)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                project_id, commodity, ann_date, estimate_type, category,
                tonnes, grade, rr.get("grade_unit"),
                contained, rr.get("contained_unit"),
                attributable, cut_off,
                doc_id, rr.get("_method", "unknown"), rr.get("_confidence", "medium"),
                0,
            ),
        )
        loaded += 1

    conn.commit()
    conn.close()
    logger.info("Loaded %d resource rows for %s from %s", loaded, ticker, doc_id)
    return loaded


def load_study(doc_id: str) -> bool:
    """Load study data from staging into studies table."""
    conn = get_connection()

    # Dedup: skip if already loaded from this document
    already = conn.execute(
        "SELECT COUNT(*) as n FROM studies WHERE source_doc_id = ?", (doc_id,)
    ).fetchone()
    if already and already["n"] > 0:
        logger.info("Study already loaded for doc %s, skipping", doc_id)
        conn.close()
        return True

    doc = conn.execute(
        "SELECT company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()
    if not doc:
        conn.close()
        return False

    ticker = doc["company_ticker"]
    ann_date = doc["announcement_date"]

    fields = _get_staging_fields(conn, doc_id, "study_")
    if not fields:
        conn.close()
        return False

    # Ensure project exists
    project_id = f"{ticker.lower()}_main"
    conn.execute(
        """INSERT OR IGNORE INTO projects
           (id, ticker, project_name, stage, ownership_pct, is_primary, updated_at)
           VALUES (?, ?, 'Main Project', 'feasibility', 100.0, 1, CURRENT_TIMESTAMP)""",
        (project_id, ticker),
    )

    # Update project stage based on study type
    study_stage = fields.get("study_stage")
    if study_stage:
        conn.execute(
            "UPDATE projects SET stage = 'feasibility', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (project_id,),
        )

    def _float_or_none(v):
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    # Flag for review if study assumptions look stale
    needs_review = 0
    npv = _float_or_none(fields.get("post_tax_npv_musd"))
    irr = _float_or_none(fields.get("irr_pct"))
    if npv is None and irr is None:
        needs_review = 1

    conn.execute(
        """INSERT INTO studies
           (project_id, study_stage, study_date, mine_life_years, annual_production,
            production_unit, recovery_pct, initial_capex_musd, sustaining_capex_musd,
            opex_per_unit, opex_unit, post_tax_npv_musd, irr_pct,
            assumed_commodity_price, assumed_price_unit, assumed_fx_audusd,
            discount_rate_pct, source_doc_id, extraction_method, confidence, needs_review)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'llm', 'medium', ?)""",
        (
            project_id,
            study_stage,
            ann_date,
            _float_or_none(fields.get("mine_life_years")),
            _float_or_none(fields.get("annual_production")),
            fields.get("production_unit"),
            _float_or_none(fields.get("recovery_pct")),
            _float_or_none(fields.get("initial_capex_musd")),
            _float_or_none(fields.get("sustaining_capex_musd")),
            _float_or_none(fields.get("opex_per_unit")),
            fields.get("opex_unit"),
            npv,
            irr,
            _float_or_none(fields.get("assumed_commodity_price")),
            fields.get("assumed_price_unit"),
            _float_or_none(fields.get("assumed_fx_audusd")),
            _float_or_none(fields.get("discount_rate_pct")),
            doc_id,
            needs_review,
        ),
    )

    conn.commit()
    conn.close()
    logger.info("Loaded study for %s from %s (stage=%s, NPV=%s, IRR=%s)",
                ticker, doc_id, study_stage, npv, irr)
    return True


def load_capital_raise(doc_id: str) -> bool:
    """Load capital raise data from staging into company_financials."""
    conn = get_connection()

    doc = conn.execute(
        "SELECT company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()
    if not doc:
        conn.close()
        return False

    ticker = doc["company_ticker"]
    ann_date = doc["announcement_date"]

    fields = _get_staging_fields(conn, doc_id, "raise_")
    if not fields:
        conn.close()
        return False

    def _float_or_none(v):
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    price = _float_or_none(fields.get("price_per_share"))
    new_shares = _float_or_none(fields.get("new_shares"))

    if price is None and new_shares is None:
        conn.close()
        return False

    # Ensure company exists
    conn.execute(
        "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
        (ticker,),
    )

    # Update or insert financials with raise info
    existing = conn.execute(
        """SELECT id FROM company_financials
           WHERE ticker = ? AND source_doc_id = ?""",
        (ticker, doc_id),
    ).fetchone()

    if existing:
        conn.execute(
            """UPDATE company_financials
               SET last_raise_date = ?, last_raise_price = ?, last_raise_shares = ?
               WHERE id = ?""",
            (ann_date, price, new_shares, existing["id"]),
        )
    else:
        conn.execute(
            """INSERT INTO company_financials
               (ticker, effective_date, last_raise_date, last_raise_price, last_raise_shares,
                source_doc_id, extraction_method, confidence, needs_review)
               VALUES (?, ?, ?, ?, ?, ?, 'rule_based', 'medium', 0)""",
            (ticker, ann_date, ann_date, price, new_shares, doc_id),
        )

    conn.commit()
    conn.close()
    logger.info("Loaded capital raise for %s: price=%s, shares=%s", ticker, price, new_shares)
    return True


def load_generic_financials(doc_id: str) -> bool:
    """
    Load financial data from generic-parsed documents (quarterly_report, annual_report, other).
    The generic parser writes staging fields prefixed with '{doc_type}_'.
    """
    conn = get_connection()

    # Dedup: skip if already loaded
    already = conn.execute(
        "SELECT COUNT(*) as n FROM company_financials WHERE source_doc_id = ?", (doc_id,)
    ).fetchone()
    if already and already["n"] > 0:
        logger.info("Generic financials already loaded for doc %s, skipping", doc_id)
        conn.close()
        return True

    doc = conn.execute(
        "SELECT company_ticker, announcement_date, doc_type FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()
    if not doc:
        conn.close()
        return False

    ticker = doc["company_ticker"]
    effective_date = doc["announcement_date"]
    doc_type = doc["doc_type"] or "other"

    fields = _get_staging_fields(conn, doc_id, f"{doc_type}_")

    def _float_or_none(v):
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    # Map generic field names to financials columns
    cash = (
        _float_or_none(fields.get("cash_at_end_quarter_aud"))
        or _float_or_none(fields.get("cash_at_year_end_aud"))
        or _float_or_none(fields.get("cash_mentioned_aud"))
    )

    operating = _float_or_none(fields.get("operating_cashflow_aud"))
    investing = _float_or_none(fields.get("investing_cashflow_aud"))

    quarterly_burn = None
    if operating is not None and investing is not None:
        quarterly_burn = abs(operating) + abs(investing)
    elif operating is not None:
        quarterly_burn = abs(operating)

    cash_runway = None
    if cash is not None and quarterly_burn and quarterly_burn > 0:
        cash_runway = (cash / quarterly_burn) * 3

    shares = _float_or_none(fields.get("shares_on_issue"))

    if cash is None and shares is None and quarterly_burn is None:
        # No core financial data, but staging extractions may have production/revenue.
        # Ensure company exists so the data is still queryable.
        conn.execute(
            "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
            (ticker,),
        )
        conn.commit()
        conn.close()
        logger.info("No cash/shares data in %s (staging has other fields)", doc_id)
        return True  # data is in staging, not a failure

    # Check for existing record
    existing = conn.execute(
        "SELECT id FROM company_financials WHERE ticker = ? AND source_doc_id = ?",
        (ticker, doc_id),
    ).fetchone()

    if existing:
        conn.execute(
            """UPDATE company_financials
               SET cash_aud = COALESCE(?, cash_aud),
                   quarterly_burn = COALESCE(?, quarterly_burn),
                   cash_runway_months = COALESCE(?, cash_runway_months),
                   shares_basic = COALESCE(?, shares_basic),
                   effective_date = ?
               WHERE id = ?""",
            (cash, quarterly_burn, cash_runway, shares, effective_date, existing["id"]),
        )
    else:
        conn.execute(
            """INSERT INTO company_financials
               (ticker, effective_date, cash_aud, quarterly_burn, cash_runway_months,
                shares_basic, source_doc_id, extraction_method, confidence, needs_review)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'rule_based', 'medium', ?)""",
            (
                ticker, effective_date, cash, quarterly_burn, cash_runway,
                shares, doc_id,
                1 if cash_runway is not None and cash_runway < 6 else 0,
            ),
        )

    conn.execute(
        "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
        (ticker,),
    )

    conn.commit()
    conn.close()
    logger.info("Loaded generic financials for %s (cash=%s, burn=%s, shares=%s)",
                ticker, cash, quarterly_burn, shares)
    return True


def load_document(doc_id: str) -> bool:
    """Load a single document's staging data into core tables based on doc_type."""
    conn = get_connection()
    doc = conn.execute(
        "SELECT doc_type, parse_status FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()
    conn.close()

    if not doc:
        logger.error("Document %s not found", doc_id)
        return False

    if doc["parse_status"] != "done":
        logger.info("Skipping %s — parse_status is '%s'", doc_id, doc["parse_status"])
        return False

    doc_type = doc["doc_type"]

    if doc_type == "appendix_5b":
        return load_financials_from_5b(doc_id)
    elif doc_type == "resource_update":
        return load_resource(doc_id) > 0
    elif doc_type == "study":
        return load_study(doc_id)
    elif doc_type == "capital_raise":
        return load_capital_raise(doc_id)
    elif doc_type in ("drill_results", "exploration_results"):
        # Drill results are loaded directly by the parser — nothing to do here
        return True
    elif doc_type in ("quarterly_report", "annual_report", "other"):
        return load_generic_financials(doc_id)
    else:
        logger.info("No loader for doc_type '%s' (doc %s)", doc_type, doc_id)
        return False


def load_all_parsed() -> int:
    """Load all parsed documents into core tables."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT id FROM documents WHERE parse_status = 'done'"
    ).fetchall()
    conn.close()

    loaded = 0
    for row in rows:
        if load_document(row["id"]):
            loaded += 1

    logger.info("Loaded %d / %d parsed documents", loaded, len(rows))
    return loaded


def load_ticker(ticker: str) -> int:
    """Load all parsed documents for a specific ticker."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT id FROM documents WHERE company_ticker = ? AND parse_status = 'done'",
        (ticker.upper(),),
    ).fetchall()
    conn.close()

    loaded = 0
    for row in rows:
        if load_document(row["id"]):
            loaded += 1

    logger.info("Loaded %d / %d documents for %s", loaded, len(rows), ticker)
    return loaded


def main():
    parser = argparse.ArgumentParser(description="Load staging data into core tables")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Load a specific document")
    group.add_argument("--ticker", type=str, help="Load all parsed docs for a ticker")
    group.add_argument("--all", action="store_true", help="Load all parsed documents")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        ok = load_document(args.doc_id)
        print(f"Loaded: {ok}")
    elif args.ticker:
        n = load_ticker(args.ticker)
        print(f"Loaded {n} documents for {args.ticker.upper()}")
    else:
        n = load_all_parsed()
        print(f"Loaded {n} documents total")


if __name__ == "__main__":
    main()

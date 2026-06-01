"""
Orchestrates: study row -> price fetch -> math -> persist to revaluations.

Called by scripts/run_revaluation_poc.py and (later) by the orchestrator on
each new DFS parsed.
"""
import json
import logging
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from revaluation.math import (
    RevaluationInput,
    SUPPORTED_COMMODITIES,
    revalue,
    RevaluationError,
)
from revaluation.prices import get_or_fetch_price, PriceFetchError

logger = logging.getLogger(__name__)


def revalue_study(conn: sqlite3.Connection, study_id: int) -> Optional[int]:
    """
    Revalue a single study row. Returns revaluation_id on success, None on skip.
    Raises RevaluationError or PriceFetchError on hard failure.
    """
    study = conn.execute("""
        SELECT s.study_id, s.project_id, s.mine_life_years, s.annual_production,
               s.recovery_pct, s.post_tax_npv, s.discount_rate_pct, s.tax_rate_pct,
               s.assumed_price_deck, s.reporting_currency, s.study_stage,
               s.study_confidence_tier,
               p.project_id, p.company_id,
               pc.commodity, pc.is_primary
        FROM studies s
        JOIN projects p ON p.project_id = s.project_id
        LEFT JOIN project_commodities pc ON pc.project_id = p.project_id AND pc.is_primary = 1
        WHERE s.study_id = ?
    """, (study_id,)).fetchone()

    if not study:
        raise RevaluationError(f"study_not_found:{study_id}")

    commodity = study["commodity"]
    if commodity not in SUPPORTED_COMMODITIES:
        logger.info("Skipping study %d: commodity %s not supported by POC", study_id, commodity)
        return None

    # Required fields for math
    required = {
        "annual_production": study["annual_production"],
        "mine_life_years": study["mine_life_years"],
        "discount_rate_pct": study["discount_rate_pct"],
        "post_tax_npv": study["post_tax_npv"],
        "reporting_currency": study["reporting_currency"],
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise RevaluationError(f"missing_fields:{','.join(missing)}")

    # Production unit: oz for Au/Ag, t for Cu (per LLM prompt instructions)
    production_unit = "oz" if commodity in ("Au", "Ag") else "t"

    # Sanity check: if Au production < 1000, Gemini likely reported in koz.
    # Silver is excluded — Moz-scale output makes the koz heuristic unsafe.
    annual_prod = study["annual_production"]
    if commodity == "Au" and annual_prod is not None and annual_prod < 1000:
        logger.warning("Study %d: annual_production=%.1f oz looks like koz, multiplying by 1000",
                        study_id, annual_prod)
        annual_prod = annual_prod * 1000

    # Extract DFS price assumption for primary commodity
    price_deck = json.loads(study["assumed_price_deck"] or "[]")
    price_dfs = None
    for entry in price_deck:
        if entry.get("commodity") == commodity:
            price_dfs = Decimal(str(entry["price"]))
            break
    if price_dfs is None:
        raise RevaluationError(f"no_dfs_price_for_commodity:{commodity}")

    # Spot price
    try:
        price_spot, price_spot_id = get_or_fetch_price(conn, commodity)
    except PriceFetchError as e:
        raise RevaluationError(f"spot_fetch_failed:{e}")

    # FX if needed
    fx_rate = None
    fx_price_id = None
    if study["reporting_currency"] == "AUD":
        fx_rate, fx_price_id = get_or_fetch_price(conn, "AUDUSD")
    elif study["reporting_currency"] != "USD":
        raise RevaluationError(f"reporting_currency_not_supported:{study['reporting_currency']}")

    inp = RevaluationInput(
        commodity=commodity,
        price_dfs_usd=price_dfs,
        price_spot_usd=price_spot,
        annual_production=Decimal(str(annual_prod)),
        annual_production_unit=production_unit,
        mine_life_years=Decimal(str(study["mine_life_years"])),
        discount_rate_pct=Decimal(str(study["discount_rate_pct"])),
        tax_rate_pct=Decimal(str(study["tax_rate_pct"])) if study["tax_rate_pct"] else None,
        npv_dfs=Decimal(str(study["post_tax_npv"])),
        reporting_currency=study["reporting_currency"],
        fx_rate=fx_rate,
    )

    result = revalue(inp)

    cur = conn.execute("""
        INSERT INTO revaluations (
            study_id, project_id, company_id, computed_at,
            commodity, price_dfs, price_spot, price_spot_id,
            fx_rate, fx_rate_price_id,
            annual_production, annual_production_unit,
            mine_life_years, discount_rate_pct, tax_rate_pct, annuity_factor,
            npv_dfs, npv_spot, npv_uplift, npv_uplift_pct,
            method_version, warnings, study_confidence_tier
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        study_id, study["project_id"], study["company_id"],
        datetime.now(timezone.utc).isoformat(),
        commodity, float(price_dfs), float(price_spot), price_spot_id,
        float(fx_rate) if fx_rate else None, fx_price_id,
        float(inp.annual_production), production_unit,
        float(inp.mine_life_years), float(inp.discount_rate_pct),
        float(result.tax_rate_used), float(result.annuity_factor),
        float(result.npv_dfs), float(result.npv_spot),
        float(result.npv_uplift), float(result.npv_uplift_pct),
        result.method_version,
        json.dumps(result.warnings),
        study["study_confidence_tier"],
    ))
    conn.commit()
    return cur.lastrowid

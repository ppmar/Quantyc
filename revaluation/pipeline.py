"""
Orchestrates: study row -> price fetch -> math -> persist to revaluations.

Called by scripts/run_revaluation_poc.py and (later) by the orchestrator on
each new DFS parsed.
"""
import json
import logging
import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from revaluation.math import (
    RevaluationInput,
    SUPPORTED_COMMODITIES,
    revalue,
    normalize_cu_price_to_per_lb,
    RevaluationError,
)
from parsers.dfs_study_schemas import _TIER_BY_TYPE
from revaluation.prices import get_or_fetch_price, PriceFetchError

logger = logging.getLogger(__name__)

# A producing mine whose study is older than this likely has extended reserves
# (mine-life upgrades, new deposits) not in the study, so depletion off the old
# mine_life understates remaining life. Surface a warning past this age.
STALE_STUDY_YEARS = 3.0


def revalue_study(conn: sqlite3.Connection, study_id: int) -> Optional[int]:
    """
    Revalue a single study row. Returns revaluation_id on success, None on skip.
    Raises RevaluationError or PriceFetchError on hard failure.
    """
    study = conn.execute("""
        SELECT s.study_id, s.project_id, s.study_date, s.mine_life_years, s.annual_production,
               s.recovery_pct, s.post_tax_npv, s.discount_rate_pct, s.tax_rate_pct,
               s.assumed_price_deck, s.reporting_currency, s.study_stage,
               s.study_confidence_tier,
               p.project_id, p.company_id, p.production_start_date,
               pc.commodity, pc.is_primary
        FROM studies s
        JOIN projects p ON p.project_id = s.project_id
        LEFT JOIN project_commodities pc ON pc.project_id = p.project_id AND pc.is_primary = 1
        WHERE s.study_id = ?
    """, (study_id,)).fetchone()

    if not study:
        raise RevaluationError(f"study_not_found:{study_id}")

    # --- Tier gate (chokepoint; do NOT rely on callers) ---------------
    # Only definitive/indicative studies are revaluable. Scoping/PEA
    # (conceptual) and anything unmappable must never produce a reval row.
    # This is the single enforcement point: the orchestrator, asx_poller,
    # and /api/revalue/backfill also gate, but run_revaluation_poc.py and
    # any direct caller do not — so the invariant lives here.
    tier = study["study_confidence_tier"]
    if tier is None:
        tier = _TIER_BY_TYPE.get(study["study_stage"])  # legacy rows: derive from stage
    if tier not in ("definitive", "indicative"):
        raise RevaluationError(f"not_revaluable_tier:{tier}")

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
    annual_prod = study["annual_production"]
    if commodity == "Au" and annual_prod is not None and annual_prod < 1000:
        logger.warning("Study %d: annual_production=%.1f oz looks like koz, multiplying by 1000",
                        study_id, annual_prod)
        annual_prod = annual_prod * 1000

    # Silver DFSs report production in Moz; the math needs absolute oz. Any
    # silver figure < 1000 is Moz (no mine produces sub-1000 oz/yr) -> x1e6.
    if commodity == "Ag" and annual_prod is not None and annual_prod < 1000:
        logger.warning("Study %d: annual_production=%.3f oz looks like Moz, multiplying by 1e6",
                        study_id, annual_prod)
        annual_prod = annual_prod * 1_000_000

    # Extract DFS price assumption for primary commodity
    price_deck = json.loads(study["assumed_price_deck"] or "[]")
    price_dfs = None
    price_dfs_unit = None
    cu_price_warning = None
    for entry in price_deck:
        if entry.get("commodity") == commodity:
            price_dfs = Decimal(str(entry["price"]))
            price_dfs_unit = entry.get("unit")
            break
    if price_dfs is None:
        raise RevaluationError(f"no_dfs_price_for_commodity:{commodity}")

    # Copper decks are often quoted in USD/tonne while spot (HG=F) is USD/lb;
    # reconcile to USD/lb so the price delta isn't off by ~2204x (CYM Nifty bug).
    if commodity == "Cu":
        price_dfs, cu_price_warning = normalize_cu_price_to_per_lb(price_dfs, price_dfs_unit)

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

    # Convert an AUD-denominated price deck to USD so it is comparable to USD spot.
    # The deck unit (e.g. "AUD/oz", "A$/oz", "AUD/t") carries the currency; spot is
    # always USD. Without this an AUD deck is compared raw against USD spot
    # (BTR: A$5000/oz deck vs US$4365 spot -> bogus -54% uplift).
    aud_price_warning = None
    if price_dfs_unit and ("AUD" in price_dfs_unit.upper() or "A$" in price_dfs_unit.upper()):
        if fx_rate is None:
            fx_rate, fx_price_id = get_or_fetch_price(conn, "AUDUSD")
        price_dfs = price_dfs * fx_rate  # fx_rate = USD per AUD
        aud_price_warning = f"price_deck_aud_to_usd:{price_dfs_unit}@fx{fx_rate}"

    # Years already in production at the valuation date. None when the project has
    # no production_start_date (developer / pre-production) -> annuity over full life.
    # Set for producers -> annuity over remaining life only (see math.remaining_life_years).
    production_elapsed_years = None
    start_raw = study["production_start_date"]
    if start_raw:
        try:
            start_date = date.fromisoformat(start_raw)
            elapsed_days = (date.today() - start_date).days
            if elapsed_days > 0:
                production_elapsed_years = Decimal(str(elapsed_days)) / Decimal("365.25")
        except ValueError:
            logger.warning("Study %d: unparseable production_start_date=%r, treating as developer",
                           study_id, start_raw)

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
        production_elapsed_years=production_elapsed_years,
    )

    result = revalue(inp)

    # Stale-study guard. Remaining life is derived from THIS study's mine_life.
    # For a producer whose study is years old, reserves have often been extended
    # (new deposits, mine-life upgrades) since, so `mine_life - elapsed` understates
    # the real remaining life and the depletion-adjusted uplift is too low. Flag it.
    warnings = list(result.warnings)
    if cu_price_warning:
        warnings.append(cu_price_warning)
    if aud_price_warning:
        warnings.append(aud_price_warning)
    if production_elapsed_years is not None and study["study_date"]:
        try:
            study_age_years = (date.today() - date.fromisoformat(study["study_date"])).days / 365.25
            if study_age_years > STALE_STUDY_YEARS:
                warnings.append(
                    f"depletion_from_stale_study:study_date={study['study_date']}_"
                    f"age={study_age_years:.1f}y_remaining_life_may_be_understated"
                )
        except ValueError:
            logger.warning("Study %d: unparseable study_date=%r", study_id, study["study_date"])

    cur = conn.execute("""
        INSERT INTO revaluations (
            study_id, project_id, company_id, computed_at,
            commodity, price_dfs, price_spot, price_spot_id,
            fx_rate, fx_rate_price_id,
            annual_production, annual_production_unit,
            mine_life_years, remaining_life_years, discount_rate_pct, tax_rate_pct, annuity_factor,
            npv_dfs, npv_spot, npv_uplift, npv_uplift_pct,
            method_version, warnings, study_confidence_tier
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        study_id, study["project_id"], study["company_id"],
        datetime.now(timezone.utc).isoformat(),
        commodity, float(price_dfs), float(price_spot), price_spot_id,
        float(fx_rate) if fx_rate else None, fx_price_id,
        float(inp.annual_production), production_unit,
        float(inp.mine_life_years), float(result.remaining_life_years),
        float(inp.discount_rate_pct),
        float(result.tax_rate_used), float(result.annuity_factor),
        float(result.npv_dfs), float(result.npv_spot),
        float(result.npv_uplift), float(result.npv_uplift_pct),
        result.method_version,
        json.dumps(warnings),
        study["study_confidence_tier"],
    ))
    conn.commit()
    return cur.lastrowid

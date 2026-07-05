"""
Orchestrates: study row -> price fetch -> math -> persist to revaluations.

Called by scripts/run_revaluation_poc.py and (later) by the orchestrator on
each new DFS parsed.
"""
import json
import logging
import re
import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

from revaluation.math import (
    BasketRevaluationInput,
    CommodityLeg,
    SUPPORTED_COMMODITIES,
    revalue_basket,
    apply_production_magnitude_heuristic,
    normalize_cu_price_to_per_lb,
    normalize_production_to_unit_price_basis,
    normalize_tax_rate_pct,
    RevaluationError,
)
from parsers.dfs_study_schemas import _TIER_BY_TYPE
from revaluation.prices import get_or_fetch_price, PriceFetchError

logger = logging.getLogger(__name__)

# A producing mine whose study is older than this likely has extended reserves
# (mine-life upgrades, new deposits) not in the study, so depletion off the old
# mine_life understates remaining life. Surface a warning past this age.
STALE_STUDY_YEARS = 3.0


_AUD_UNIT_RE = re.compile(r"AUD|(?<![CU])A\$|\$A(?![A-Z$])", re.I)


def is_aud_price_unit(unit: Optional[str]) -> bool:
    """True when a price-deck unit string is Australian dollars.

    Covers the three spellings ASX studies use — "AUD", "A$", "$A" — while
    not matching US$/CA$/CAD. Spot is always USD, so an AUD deck must be
    FX-converted before the price delta is taken.
    """
    if not unit:
        return False
    return bool(_AUD_UNIT_RE.search(unit.upper()))


class _ResolvedLeg:
    """One metal's resolved revaluation inputs (per-leg v3 logic applied). Carries both
    the supported-leg math inputs and the coverage bookkeeping for revaluation_legs."""
    __slots__ = ("commodity", "supported", "price_dfs_usd", "price_spot", "price_spot_id",
                 "annual_production", "annual_production_unit", "dfs_metal_revenue_usd",
                 "warnings")

    def __init__(self, commodity, supported, price_dfs_usd, price_spot, price_spot_id,
                 annual_production, annual_production_unit, dfs_metal_revenue_usd, warnings):
        self.commodity = commodity
        self.supported = supported
        self.price_dfs_usd = price_dfs_usd
        self.price_spot = price_spot
        self.price_spot_id = price_spot_id
        self.annual_production = annual_production
        self.annual_production_unit = annual_production_unit
        self.dfs_metal_revenue_usd = dfs_metal_revenue_usd
        self.warnings = warnings


def _resolve_leg(conn, study, leg_row, price_deck, get_fx) -> _ResolvedLeg:
    """Per-leg carry-over of the v3 single-commodity logic (I9): magnitude heuristic,
    Cu /t→/lb, AUD-deck→USD, spot fetch. A leg is *supported* (contributes ΔNPV) only
    when its commodity is in SUPPORTED_COMMODITIES AND both a DFS price and a production
    volume resolve. Otherwise it is recorded with supported=0 and 0 ΔNPV, but its DFS
    metal revenue still counts toward the coverage denominator (I3, I4)."""
    commodity = leg_row["commodity"]
    warnings: list[str] = []
    prod_raw = leg_row["annual_production"]
    # Canonical production label per commodity: troy-oz metals in oz, U3O8 in lb
    # (its magnitude heuristic resolves legacy mislabels to absolute lb), rest in t.
    if commodity in ("Au", "Ag", "Pd", "Pt"):
        basis_unit = "oz"
    elif commodity == "U3O8":
        basis_unit = "lb"
    else:
        basis_unit = "t"

    # DFS price from the deck (per commodity). Decks carry currency+unit verbatim.
    price_dfs = None
    price_dfs_unit = None
    for entry in price_deck:
        if entry.get("commodity") == commodity:
            price_dfs = Decimal(str(entry["price"]))
            price_dfs_unit = entry.get("unit")
            break

    # Cu deck /t→/lb (CYM Nifty) — supported metals only; the heuristic is Cu-specific.
    if commodity == "Cu" and price_dfs is not None:
        price_dfs, w = normalize_cu_price_to_per_lb(price_dfs, price_dfs_unit)
        if w:
            warnings.append(w)
    # AUD deck → USD (BTR). The deck unit carries the currency; spot is always USD.
    if price_dfs is not None and is_aud_price_unit(price_dfs_unit):
        price_dfs = price_dfs * get_fx()
        warnings.append(f"price_deck_aud_to_usd_{commodity}:{price_dfs_unit}")

    # Magnitude heuristic (koz/Moz/kt mislabels) — supported metals only (keys on Au/Ag/Cu).
    prod_scaled = None
    if prod_raw is not None:
        if commodity in SUPPORTED_COMMODITIES:
            prod_scaled, w = apply_production_magnitude_heuristic(commodity, Decimal(str(prod_raw)))
            if w:
                warnings.append(w)
        else:
            prod_scaled = Decimal(str(prod_raw))

    supported = (commodity in SUPPORTED_COMMODITIES
                 and price_dfs is not None and prod_scaled is not None)

    # Spot — only needed (and only meaningful) for supported metals.
    price_spot = None
    price_spot_id = None
    if supported:
        try:
            price_spot, price_spot_id = get_or_fetch_price(conn, commodity)
        except PriceFetchError as e:
            raise RevaluationError(f"spot_fetch_failed:{commodity}:{e}")

    # DFS metal revenue for the coverage denominator. Supported legs use the price-basis
    # (oz/lb) so the weight is comparable to the modeled ΔNPV; unsupported legs use the
    # raw stored production × deck price (best-effort proxy — no unit rules exist for
    # Ni/Co/Zn, and these only affect the denominator).
    dfs_rev = Decimal("0")
    if prod_scaled is not None and price_dfs is not None:
        if supported:
            norm_prod, _w = normalize_production_to_unit_price_basis(
                prod_scaled, basis_unit, ("oz" if commodity in ("Au", "Ag", "Pd", "Pt") else "lb"), commodity)
            dfs_rev = norm_prod * price_dfs
        else:
            dfs_rev = prod_scaled * price_dfs

    return _ResolvedLeg(
        commodity=commodity,
        supported=supported,
        price_dfs_usd=price_dfs,
        price_spot=price_spot,
        price_spot_id=price_spot_id,
        annual_production=prod_scaled,
        annual_production_unit=basis_unit,
        dfs_metal_revenue_usd=dfs_rev,
        warnings=warnings,
    )


def revalue_study(conn: sqlite3.Connection, study_id: int) -> Optional[int]:
    """
    Revalue a single study row. Returns revaluation_id on success, None on skip.
    Raises RevaluationError or PriceFetchError on hard failure.
    """
    study = conn.execute("""
        SELECT s.study_id, s.project_id, s.study_date, s.mine_life_years, s.annual_production,
               s.recovery_pct, s.post_tax_npv, s.discount_rate_pct, s.tax_rate_pct,
               s.assumed_price_deck, s.reporting_currency, s.study_stage,
               s.study_confidence_tier, s.header_tier, s.needs_review, s.review_reason,
               p.project_id, p.company_id, p.production_start_date, p.stage,
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
    # Header overrides TOWARD conceptual: never revalue a study the announcement
    # title calls Scoping/PEA, even if the LLM mislabelled it DFS (AZY bypass).
    if study["header_tier"] == "conceptual":
        raise RevaluationError("not_revaluable_tier:conceptual_by_header")
    if tier not in ("definitive", "indicative"):
        raise RevaluationError(f"not_revaluable_tier:{tier}")

    # NOTE: the PR0 multi-commodity hard guard is gone — first_order_v4 values the basket
    # leg-by-leg (below) and reports coverage. Distinct project commodities are still read,
    # but only to WARN when study_commodities under-represents the basket (a legacy study
    # backfilled with just its primary leg, not yet re-extracted into a full basket): such a
    # row is valued on what it has, but the warning keeps the coverage signal honest (I4).
    project_distinct = conn.execute(
        "SELECT COUNT(DISTINCT commodity) FROM ("
        "  SELECT commodity FROM project_commodities WHERE project_id = ?"
        "  UNION SELECT commodity FROM resources WHERE project_id = ?)",
        (study["project_id"], study["project_id"]),
    ).fetchone()[0]

    # Required study-level (shared) fields for the basket math.
    required = {
        "mine_life_years": study["mine_life_years"],
        "discount_rate_pct": study["discount_rate_pct"],
        "post_tax_npv": study["post_tax_npv"],
        "reporting_currency": study["reporting_currency"],
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise RevaluationError(f"missing_fields:{','.join(missing)}")

    if study["reporting_currency"] not in ("AUD", "USD"):
        raise RevaluationError(f"reporting_currency_not_supported:{study['reporting_currency']}")

    # Per-metal basket — the canonical production input (study_commodities). The 0015
    # backfill guarantees ≥1 leg per existing study; new studies get legs at ingest.
    legs = conn.execute(
        "SELECT commodity, annual_production, annual_production_unit, is_primary "
        "FROM study_commodities WHERE study_id = ?", (study_id,)
    ).fetchall()
    if not legs:
        raise RevaluationError("no_study_commodities")

    price_deck = json.loads(study["assumed_price_deck"] or "[]")

    # FX (USD per AUD) is shared: needed for AUD reporting (final NPV conversion) and for
    # any AUD-denominated deck leg. Fetched at most once, memoized.
    _fx_cache: dict = {}
    fx_price_id = None
    def get_fx():
        nonlocal fx_price_id
        if "rate" not in _fx_cache:
            rate, pid = get_or_fetch_price(conn, "AUDUSD")
            _fx_cache["rate"] = rate
            fx_price_id = pid
        return _fx_cache["rate"]

    fx_rate = None
    if study["reporting_currency"] == "AUD":
        fx_rate = get_fx()

    # Resolve every leg (per-leg v3 logic), split supported from not.
    resolved = [_resolve_leg(conn, study, lr, price_deck, get_fx) for lr in legs]
    dfs_rev_total = sum((r.dfs_metal_revenue_usd for r in resolved), Decimal("0"))
    supported = [r for r in resolved if r.supported]
    if not supported:
        raise RevaluationError("not_revaluable_no_supported_commodity")

    # An AUD-deck conversion inside a leg may have populated fx; reflect it for reporting.
    if fx_rate is None and "rate" in _fx_cache:
        fx_rate = _fx_cache["rate"]

    # Coverage (I4): share of DFS metal revenue that the supported legs represent.
    supported_rev = sum((r.dfs_metal_revenue_usd for r in supported), Decimal("0"))
    coverage_pct = (supported_rev / dfs_rev_total * Decimal("100")) if dfs_rev_total > 0 else Decimal("100")

    # Years already in production at the valuation date. None when the project has
    # no production_start_date (developer / pre-production) -> annuity over full life.
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

    # Tax rate: `is not None` (0 is a real rate, not "missing") + fraction guard.
    tax_raw = study["tax_rate_pct"]
    tax_rate_pct, tax_warning = normalize_tax_rate_pct(
        Decimal(str(tax_raw)) if tax_raw is not None else None
    )

    inp = BasketRevaluationInput(
        legs=tuple(
            CommodityLeg(
                commodity=r.commodity,
                price_dfs_usd=r.price_dfs_usd,
                price_spot_usd=r.price_spot,
                annual_production=r.annual_production,
                annual_production_unit=r.annual_production_unit,
            )
            for r in supported
        ),
        mine_life_years=Decimal(str(study["mine_life_years"])),
        discount_rate_pct=Decimal(str(study["discount_rate_pct"])),
        tax_rate_pct=tax_rate_pct,
        npv_dfs=Decimal(str(study["post_tax_npv"])),
        reporting_currency=study["reporting_currency"],
        fx_rate=fx_rate,
        production_elapsed_years=production_elapsed_years,
    )

    result = revalue_basket(inp)

    warnings = list(result.warnings)
    for r in resolved:
        warnings.extend(r.warnings)
    if tax_warning:
        warnings.append(tax_warning)
    warnings.append(f"coverage_pct:{coverage_pct.quantize(Decimal('0.1'))}")
    if coverage_pct < Decimal("100"):
        warnings.append(f"partial_basket_coverage:{coverage_pct.quantize(Decimal('0.1'))}pct")
    # Legacy safety net: the project spans more distinct commodities than study_commodities
    # has legs → the basket is under-extracted (not yet re-run through commodity_production).
    if project_distinct > len(resolved):
        warnings.append(f"basket_legs_incomplete:{len(resolved)}of{project_distinct}_metals_modeled")

    # Stale-study guards. An old study's deck AND cost base are both fantasy at today's
    # prices — a huge uplift off a 2017-2020 deck flags "restudy needed", not "buy".
    if study["study_date"]:
        try:
            study_age_years = (date.today() - date.fromisoformat(study["study_date"])).days / 365.25
            if study_age_years > STALE_STUDY_YEARS:
                warnings.append(
                    f"stale_study:age={study_age_years:.1f}y_deck_and_costs_outdated"
                )
                if production_elapsed_years is not None:
                    warnings.append(
                        f"depletion_from_stale_study:study_date={study['study_date']}_"
                        f"age={study_age_years:.1f}y_remaining_life_may_be_understated"
                    )
        except ValueError:
            logger.warning("Study %d: unparseable study_date=%r", study_id, study["study_date"])

    # Deck-vs-spot divergence: when the deck price is under half of spot for any supported
    # leg, the study predates the price regime entirely — same restudy-needed signal.
    for r in supported:
        if r.price_spot and r.price_dfs_usd and r.price_dfs_usd < r.price_spot / 2:
            warnings.append(
                f"deck_far_below_spot_{r.commodity}:{r.price_dfs_usd}_vs_{r.price_spot}"
            )

    # A producing project with no production_start_date gets NO depletion — the annuity
    # runs over the study's full life, overstating uplift for a mine already part-mined.
    if study["stage"] == "production" and production_elapsed_years is None:
        warnings.append("producer_missing_start_date_no_depletion_uplift_overstated")

    # The study itself is review-flagged (e.g. post_tax == pre_tax NPV): the reval base
    # may be wrong, so the result is a weak signal and must carry the flag.
    if study["needs_review"]:
        warnings.append(f"study_needs_review:{study['review_reason'] or 'unspecified'}")

    # Per-metal columns hold the REPRESENTATIVE leg (I7): the project-primary leg if it is
    # supported, else the first supported leg (columns are NOT NULL). Aggregate columns
    # (npv_*) hold the whole-basket result.
    primary_commodity = study["commodity"]
    rep = next((r for r in supported if r.commodity == primary_commodity), supported[0])

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
        rep.commodity, float(rep.price_dfs_usd), float(rep.price_spot), rep.price_spot_id,
        float(fx_rate) if fx_rate else None, fx_price_id,
        float(rep.annual_production), rep.annual_production_unit,
        float(inp.mine_life_years), float(result.remaining_life_years),
        float(inp.discount_rate_pct),
        float(result.tax_rate_used), float(result.annuity_factor),
        float(result.npv_dfs), float(result.npv_spot),
        float(result.npv_uplift), float(result.npv_uplift_pct),
        result.method_version,
        json.dumps(warnings),
        study["study_confidence_tier"],
    ))
    revaluation_id = cur.lastrowid

    # Full per-leg breakdown (supported + unsupported) → revaluation_legs.
    leg_delta = dict(result.leg_delta_revenue_usd)
    for r in resolved:
        conn.execute("""
            INSERT INTO revaluation_legs (
                revaluation_id, commodity, supported,
                price_dfs, price_spot, price_spot_id,
                annual_production, annual_production_unit,
                delta_revenue_annual_usd, dfs_metal_revenue_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            revaluation_id, r.commodity, 1 if r.supported else 0,
            float(r.price_dfs_usd) if r.price_dfs_usd is not None else None,
            float(r.price_spot) if r.price_spot is not None else None,
            r.price_spot_id,
            float(r.annual_production) if r.annual_production is not None else None,
            r.annual_production_unit,
            float(leg_delta.get(r.commodity, Decimal("0"))),
            float(r.dfs_metal_revenue_usd),
        ))

    conn.commit()
    return revaluation_id

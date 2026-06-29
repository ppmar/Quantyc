"""
Pure-function math for first-order DFS revaluation at current spot prices.

NO database access, NO network calls, NO state. All inputs explicit.
Tested with hardcoded values in tests/test_revaluation_math.py.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

METHOD_VERSION = "first_order_v3"
METHOD_VERSION_V4 = "first_order_v4"
DEFAULT_TAX_RATE = Decimal("0.30")
# |uplift| beyond this ratio (500%) is almost always a bad input (unit mismatch,
# stale deck far from spot) rather than real leverage — flag it, don't trust it.
EXTREME_UPLIFT_RATIO = Decimal("5")

SUPPORTED_COMMODITIES = {"Au", "Ag", "Cu"}

# Commodities priced and produced per troy ounce (no unit conversion needed).
_OZ_COMMODITIES = {"Au", "Ag"}


@dataclass(frozen=True)
class RevaluationInput:
    commodity: str
    price_dfs_usd: Decimal           # USD per oz (Au) or per lb (Cu)
    price_spot_usd: Decimal          # same unit as price_dfs_usd
    annual_production: Decimal       # in oz (Au) or in tonnes contained Cu
    annual_production_unit: str      # 'oz' or 't'
    mine_life_years: Decimal
    discount_rate_pct: Decimal       # e.g., Decimal("8.0") for 8%
    tax_rate_pct: Optional[Decimal]  # None falls back to DEFAULT_TAX_RATE
    npv_dfs: Decimal                 # in reporting_currency millions
    reporting_currency: str          # 'AUD', 'USD', etc.
    fx_rate: Optional[Decimal]       # USD per 1 AUD, e.g. 0.6452 (Yahoo AUDUSD=X convention).
                                     # Required when reporting_currency == "AUD". Used as:
                                     # amount_aud = amount_usd / fx_rate
    production_elapsed_years: Optional[Decimal] = None
                                     # Years already in production at the valuation date.
                                     # None => not yet producing (developer): annuity runs
                                     # over the FULL mine_life. Set => annuity runs over the
                                     # REMAINING life = mine_life - elapsed, so a producer is
                                     # not credited price uplift on ounces already mined.


@dataclass(frozen=True)
class CommodityLeg:
    """One supported metal in a basket revaluation (first_order_v4). Prices are USD on
    the metal's own basis (USD/oz for Au/Ag, USD/lb for Cu); production is in canonical
    absolute units (oz / t) with annual_production_unit naming that basis."""
    commodity: str
    price_dfs_usd: Decimal
    price_spot_usd: Decimal
    annual_production: Decimal
    annual_production_unit: str


@dataclass(frozen=True)
class BasketRevaluationInput:
    """first_order_v4 input. Annuity, tax, FX, npv_dfs are shared project-level terms;
    only the revenue delta is per-metal. >= 1 supported leg."""
    legs: tuple[CommodityLeg, ...]
    mine_life_years: Decimal
    discount_rate_pct: Decimal
    tax_rate_pct: Optional[Decimal]
    npv_dfs: Decimal
    reporting_currency: str
    fx_rate: Optional[Decimal] = None
    production_elapsed_years: Optional[Decimal] = None


@dataclass(frozen=True)
class RevaluationResult:
    annuity_factor: Decimal
    remaining_life_years: Decimal    # life used for the annuity (== mine_life for developers)
    npv_dfs: Decimal
    npv_spot: Decimal
    npv_uplift: Decimal
    npv_uplift_pct: Decimal
    delta_revenue_annual_usd: Decimal
    delta_npv_reporting_currency: Decimal
    tax_rate_used: Decimal
    method_version: str
    warnings: list[str]
    # v4 only: per-leg annual revenue delta in USD (commodity -> delta). Empty for v3.
    leg_delta_revenue_usd: tuple[tuple[str, Decimal], ...] = ()


class RevaluationError(ValueError):
    """Inputs are invalid for revaluation."""


def apply_production_magnitude_heuristic(
    commodity: str,
    annual_production: Decimal,
) -> tuple[Decimal, Optional[str]]:
    """Rescue magnitude-mislabelled annual production (koz/Moz/kt reported as
    if absolute). Returns (value, warning_or_None); the warning MUST be
    persisted with the revaluation row — a silent x1000 is an audit hole.

    Au: no gold DFS produces < 1,000 oz/yr -> a figure < 1000 is koz, x1000.
    Ag: figures < 100 are Moz (silver Moz decks run ~0.5–50), x1e6. The
        100–999 band is ambiguous — koz reporters (100–999 koz) and Moz both
        land there; a guess can be 1000x wrong, so refuse.
    Cu: a DFS/PFS project is never below ~thousands of t/yr contained Cu ->
        a figure < 100 is kt, x1000.
    """
    if commodity == "Au" and annual_production < 1000:
        scaled = annual_production * 1000
        return scaled, f"production_magnitude_scaled_Au_{annual_production}_x1000"
    if commodity == "Ag":
        if annual_production < 100:
            scaled = annual_production * 1_000_000
            return scaled, f"production_magnitude_scaled_Ag_{annual_production}_x1000000"
        if annual_production < 1000:
            raise RevaluationError(
                f"ag_production_unit_ambiguous:{annual_production}_koz_or_moz"
            )
    if commodity == "Cu" and annual_production < 100:
        scaled = annual_production * 1000
        return scaled, f"production_magnitude_scaled_Cu_{annual_production}_x1000"
    return annual_production, None


def normalize_tax_rate_pct(
    raw: Optional[Decimal],
) -> tuple[Optional[Decimal], Optional[str]]:
    """Guard an extracted tax rate before it reaches the math.

    - None passes through (revalue() applies the default).
    - 0 is kept (not defaulted): a genuine 0% must not be falsy-coerced to 30%.
    - 0 < raw < 1 is a fraction mislabelled as percent (LLM returning 0.30 for
      "30%"): scale x100. Real sub-1% corporate tax rates do not exist.
    Returns (rate_pct, warning_or_None).
    """
    if raw is None:
        return None, None
    if raw == 0:
        return Decimal("0"), "tax_rate_zero_pct_kept"
    if raw < 1:
        scaled = (raw * 100).quantize(Decimal("0.1"))
        return scaled, f"tax_rate_fraction_scaled_{raw}_to_{scaled}pct"
    return raw, None


def annuity_factor(discount_rate_pct: Decimal, mine_life_years: Decimal) -> Decimal:
    """Standard annuity factor: A = (1 - (1+r)^-n) / r."""
    if discount_rate_pct <= 0:
        raise RevaluationError(f"discount_rate_pct must be positive, got {discount_rate_pct}")
    if mine_life_years <= 0:
        raise RevaluationError(f"mine_life_years must be positive, got {mine_life_years}")
    r = discount_rate_pct / Decimal("100")
    n = mine_life_years
    one_plus_r = Decimal("1") + r
    factor = (Decimal("1") - one_plus_r ** (-n)) / r
    return factor.quantize(Decimal("0.0001"))


def remaining_life_years(
    mine_life_years: Decimal,
    production_elapsed_years: Optional[Decimal],
) -> Decimal:
    """
    Life still to run, for the annuity.

        vie_restante = duree_de_vie - (annee_courante - annee_debut_production)

    `production_elapsed_years` is (annee_courante - annee_debut_production), i.e.
    years already in production. None means the mine is not yet producing
    (developer / pre-production), so the full mine_life remains. The result is
    clamped to [0, mine_life_years]: a mine cannot have negative remaining life,
    and elapsed cannot make it longer than the original plan.
    """
    if production_elapsed_years is None:
        return mine_life_years
    if production_elapsed_years < 0:
        # Production starts in the future relative to the valuation date.
        return mine_life_years
    rem = mine_life_years - production_elapsed_years
    if rem < 0:
        return Decimal("0")
    return rem


_LB_PER_TONNE = Decimal("2204.62262")
# Above this, a "USD/lb" copper price is implausible and is really USD/tonne.
# Cu/lb has historically topped out well under $10; LME $/tonne is >$1500.
_CU_PER_LB_MAX = Decimal("100")


def normalize_cu_price_to_per_lb(price: Decimal, unit: Optional[str]) -> tuple[Decimal, Optional[str]]:
    """
    Reconcile a copper DFS price to USD/lb (the basis spot is fetched in, HG=F).

    Studies frequently report copper NPV decks in USD/tonne (LME convention) while
    the deck's unit string is mislabelled "USD/lb" (e.g. CYM Nifty: "13000 USD/lb").
    Use the unit when it clearly says per-tonne, and fall back to magnitude: any
    copper "per lb" price above _CU_PER_LB_MAX is really per-tonne.

    Returns (price_per_lb, warning_or_None).
    """
    u = (unit or "").lower().replace(" ", "")
    per_tonne = any(tok in u for tok in ("/t", "/tonne", "/mt", "pertonne"))
    per_lb = "/lb" in u or u.endswith("lb")
    if per_tonne and not per_lb:
        return price / _LB_PER_TONNE, f"cu_price_unit_t_to_lb:{price}{unit}"
    if price > _CU_PER_LB_MAX:
        # Unit says lb (or is blank) but the magnitude is a per-tonne figure.
        return price / _LB_PER_TONNE, f"cu_price_magnitude_t_to_lb:{price}{unit}"
    return price, None


def normalize_production_to_unit_price_basis(
    annual_production: Decimal,
    production_unit: str,
    price_unit_basis: str,
    commodity: str,
) -> tuple[Decimal, list[str]]:
    """
    Reconcile production unit with price unit.

    Gold: production in oz, price in USD/oz -> no conversion.
    Copper: production typically in 't' (contained Cu tonnes), price in USD/lb.
            Convert tonnes -> lb: 1 t = 2204.62262 lb.
    """
    warnings = []
    if commodity in _OZ_COMMODITIES:
        if production_unit != "oz":
            raise RevaluationError(
                f"{commodity} production must be in 'oz', got '{production_unit}'. "
                f"Check DFS extraction."
            )
        return annual_production, warnings
    elif commodity == "Cu":
        if production_unit == "t":
            converted = annual_production * Decimal("2204.62262")
            warnings.append(f"converted_production_{annual_production}t_to_{converted}lb")
            return converted, warnings
        elif production_unit == "lb":
            return annual_production, warnings
        else:
            raise RevaluationError(
                f"Cu production must be in 't' or 'lb', got '{production_unit}'"
            )
    else:
        raise RevaluationError(f"unsupported_commodity:{commodity}")


def revalue(inp: RevaluationInput) -> RevaluationResult:
    """First-order revaluation at spot. See spec_llm_extract.md math section."""
    warnings: list[str] = []

    if inp.commodity not in SUPPORTED_COMMODITIES:
        raise RevaluationError(f"unsupported_commodity:{inp.commodity}")

    tax_rate = inp.tax_rate_pct if inp.tax_rate_pct is not None else DEFAULT_TAX_RATE * 100
    if inp.tax_rate_pct is None:
        warnings.append(f"tax_rate_defaulted_to_{DEFAULT_TAX_RATE * 100}pct")

    # Normalize production units to match price unit basis
    price_unit_basis = "oz" if inp.commodity in _OZ_COMMODITIES else "lb"
    normalized_production, conv_warnings = normalize_production_to_unit_price_basis(
        inp.annual_production,
        inp.annual_production_unit,
        price_unit_basis,
        inp.commodity,
    )
    warnings.extend(conv_warnings)

    # Annuity over REMAINING life, not full mine life. A producer already mined
    # part of its plan; price uplift only applies to ounces still to come.
    life = remaining_life_years(inp.mine_life_years, inp.production_elapsed_years)
    if inp.production_elapsed_years is not None:
        warnings.append(
            f"remaining_life_{life}y_of_{inp.mine_life_years}y_"
            f"elapsed_{inp.production_elapsed_years}y"
        )
    if life <= 0:
        # Mine is fully depleted at the valuation date: no go-forward production,
        # so the first-order price uplift is zero.
        warnings.append("mine_depleted_no_remaining_life")
        a = Decimal("0.0000")
    else:
        a = annuity_factor(inp.discount_rate_pct, life)

    # Both prices are USD per invariant I2/I3. Compute uplift in USD.
    delta_price_usd = inp.price_spot_usd - inp.price_dfs_usd
    delta_revenue_annual_usd = normalized_production * delta_price_usd

    return _finalize_revaluation(
        delta_revenue_annual_usd=delta_revenue_annual_usd,
        a=a,
        tax_rate=tax_rate,
        life=life,
        npv_dfs=inp.npv_dfs,
        reporting_currency=inp.reporting_currency,
        fx_rate=inp.fx_rate,
        production_elapsed_years=inp.production_elapsed_years,
        method_version=METHOD_VERSION,
        warnings=warnings,
    )


def _finalize_revaluation(
    *,
    delta_revenue_annual_usd: Decimal,
    a: Decimal,
    tax_rate: Decimal,
    life: Decimal,
    npv_dfs: Decimal,
    reporting_currency: str,
    fx_rate: Optional[Decimal],
    production_elapsed_years: Optional[Decimal],
    method_version: str,
    warnings: list[str],
    leg_delta_revenue_usd: tuple[tuple[str, Decimal], ...] = (),
) -> RevaluationResult:
    """Shared tail of v3 and v4: turn an annual USD revenue delta into the NPV uplift.
    Everything here is project-level (annuity, tax, FX, npv_dfs), so v4's only divergence
    from v3 is how delta_revenue_annual_usd was summed — guaranteeing I1 by construction."""
    delta_npv_usd = delta_revenue_annual_usd * a * (Decimal("1") - tax_rate / Decimal("100"))
    delta_npv_usd_millions = delta_npv_usd / Decimal("1000000")

    # Convert to reporting currency per invariant I4.
    if reporting_currency == "USD":
        delta_npv_reporting_currency = delta_npv_usd_millions
    elif reporting_currency == "AUD":
        if fx_rate is None:
            raise RevaluationError("fx_rate_required_for_aud_reporting")
        if fx_rate <= 0:
            raise RevaluationError(f"fx_rate_must_be_positive:{fx_rate}")
        # fx_rate = USD per AUD (~0.65). amount_aud = amount_usd / fx_rate.
        delta_npv_reporting_currency = delta_npv_usd_millions / fx_rate
    else:
        raise RevaluationError(f"unsupported_reporting_currency:{reporting_currency}")

    npv_spot = npv_dfs + delta_npv_reporting_currency
    npv_uplift = npv_spot - npv_dfs
    # Denominator |npv_dfs|: a negative study NPV must not flip the sign of
    # the percentage. Zero NPV makes the % undefined — flag, don't hide.
    if npv_dfs == 0:
        npv_uplift_pct = Decimal("0")
        warnings.append("npv_dfs_zero_pct_undefined")
    else:
        npv_uplift_pct = npv_uplift / abs(npv_dfs)
        if npv_dfs < 0:
            warnings.append("npv_dfs_negative_pct_vs_abs")

    if abs(npv_uplift_pct) > EXTREME_UPLIFT_RATIO:
        warnings.append(f"extreme_uplift_check_inputs:{npv_uplift_pct.quantize(Decimal('0.1'))}")

    # Known first-order biases — declared on every result so downstream signal
    # confidence is honest (royalty/stream terms are not extracted yet, and a
    # developer's annuity starts at year 1 while the DFS schedules production
    # after a multi-year build: uplift overstated ~(1+r)^build_years).
    warnings.append("royalty_not_netted_uplift_gross_of_royalties")
    if production_elapsed_years is None:
        warnings.append("developer_pre_production_timing_not_discounted")

    return RevaluationResult(
        annuity_factor=a,
        remaining_life_years=life,
        npv_dfs=npv_dfs,
        npv_spot=npv_spot.quantize(Decimal("0.01")),
        npv_uplift=npv_uplift.quantize(Decimal("0.01")),
        npv_uplift_pct=npv_uplift_pct.quantize(Decimal("0.0001")),
        delta_revenue_annual_usd=delta_revenue_annual_usd.quantize(Decimal("0.01")),
        delta_npv_reporting_currency=delta_npv_reporting_currency.quantize(Decimal("0.01")),
        tax_rate_used=tax_rate,
        method_version=method_version,
        warnings=warnings,
        leg_delta_revenue_usd=leg_delta_revenue_usd,
    )


def revalue_basket(inp: BasketRevaluationInput) -> RevaluationResult:
    """first_order_v4: multi-commodity basket revaluation. ΔNPV is linear in each price
    (no cross terms), so the basket ΔNPV = Σ per-metal ΔNPV. Annuity, tax, FX and npv_dfs
    are computed once and shared (I2). A single-leg input reproduces v3 exactly (I1)."""
    warnings: list[str] = []

    if not inp.legs:
        raise RevaluationError("no_supported_legs")
    for leg in inp.legs:
        if leg.commodity not in SUPPORTED_COMMODITIES:
            raise RevaluationError(f"unsupported_commodity:{leg.commodity}")

    tax_rate = inp.tax_rate_pct if inp.tax_rate_pct is not None else DEFAULT_TAX_RATE * 100
    if inp.tax_rate_pct is None:
        warnings.append(f"tax_rate_defaulted_to_{DEFAULT_TAX_RATE * 100}pct")

    # Annuity over REMAINING life, shared across legs.
    life = remaining_life_years(inp.mine_life_years, inp.production_elapsed_years)
    if inp.production_elapsed_years is not None:
        warnings.append(
            f"remaining_life_{life}y_of_{inp.mine_life_years}y_"
            f"elapsed_{inp.production_elapsed_years}y"
        )
    if life <= 0:
        warnings.append("mine_depleted_no_remaining_life")
        a = Decimal("0.0000")
    else:
        a = annuity_factor(inp.discount_rate_pct, life)

    # Sum revenue deltas across legs; each normalized to its own price basis.
    delta_revenue_annual_usd = Decimal("0")
    leg_deltas: list[tuple[str, Decimal]] = []
    for leg in inp.legs:
        basis = "oz" if leg.commodity in _OZ_COMMODITIES else "lb"
        prod_norm, conv_w = normalize_production_to_unit_price_basis(
            leg.annual_production, leg.annual_production_unit, basis, leg.commodity)
        warnings.extend(conv_w)
        leg_delta = prod_norm * (leg.price_spot_usd - leg.price_dfs_usd)
        delta_revenue_annual_usd += leg_delta
        leg_deltas.append((leg.commodity, leg_delta.quantize(Decimal("0.01"))))

    return _finalize_revaluation(
        delta_revenue_annual_usd=delta_revenue_annual_usd,
        a=a,
        tax_rate=tax_rate,
        life=life,
        npv_dfs=inp.npv_dfs,
        reporting_currency=inp.reporting_currency,
        fx_rate=inp.fx_rate,
        production_elapsed_years=inp.production_elapsed_years,
        method_version=METHOD_VERSION_V4,
        warnings=warnings,
        leg_delta_revenue_usd=tuple(leg_deltas),
    )

"""
Pure-function math for first-order DFS revaluation at current spot prices.

NO database access, NO network calls, NO state. All inputs explicit.
Tested with hardcoded values in tests/test_revaluation_math.py.
"""
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

METHOD_VERSION = "first_order_v2"
DEFAULT_TAX_RATE = Decimal("0.30")

SUPPORTED_COMMODITIES = {"Au", "Cu"}


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


@dataclass(frozen=True)
class RevaluationResult:
    annuity_factor: Decimal
    npv_dfs: Decimal
    npv_spot: Decimal
    npv_uplift: Decimal
    npv_uplift_pct: Decimal
    delta_revenue_annual_usd: Decimal
    delta_npv_reporting_currency: Decimal
    tax_rate_used: Decimal
    method_version: str
    warnings: list[str]


class RevaluationError(ValueError):
    """Inputs are invalid for revaluation."""


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
    if commodity == "Au":
        if production_unit != "oz":
            raise RevaluationError(
                f"Au production must be in 'oz', got '{production_unit}'. "
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
    price_unit_basis = "oz" if inp.commodity == "Au" else "lb"
    normalized_production, conv_warnings = normalize_production_to_unit_price_basis(
        inp.annual_production,
        inp.annual_production_unit,
        price_unit_basis,
        inp.commodity,
    )
    warnings.extend(conv_warnings)

    # Annuity factor
    a = annuity_factor(inp.discount_rate_pct, inp.mine_life_years)

    # Both prices are USD per invariant I2/I3. Compute uplift in USD.
    delta_price_usd = inp.price_spot_usd - inp.price_dfs_usd
    delta_revenue_annual_usd = normalized_production * delta_price_usd
    delta_npv_usd = delta_revenue_annual_usd * a * (Decimal("1") - tax_rate / Decimal("100"))
    delta_npv_usd_millions = delta_npv_usd / Decimal("1000000")

    # Convert to reporting currency per invariant I4.
    if inp.reporting_currency == "USD":
        delta_npv_reporting_currency = delta_npv_usd_millions
    elif inp.reporting_currency == "AUD":
        if inp.fx_rate is None:
            raise RevaluationError("fx_rate_required_for_aud_reporting")
        if inp.fx_rate <= 0:
            raise RevaluationError(f"fx_rate_must_be_positive:{inp.fx_rate}")
        # fx_rate = USD per AUD (~0.65). amount_aud = amount_usd / fx_rate.
        delta_npv_reporting_currency = delta_npv_usd_millions / inp.fx_rate
    else:
        raise RevaluationError(f"unsupported_reporting_currency:{inp.reporting_currency}")

    npv_spot = inp.npv_dfs + delta_npv_reporting_currency
    npv_uplift = npv_spot - inp.npv_dfs
    npv_uplift_pct = (npv_uplift / inp.npv_dfs) if inp.npv_dfs != 0 else Decimal("0")

    return RevaluationResult(
        annuity_factor=a,
        npv_dfs=inp.npv_dfs,
        npv_spot=npv_spot.quantize(Decimal("0.01")),
        npv_uplift=npv_uplift.quantize(Decimal("0.01")),
        npv_uplift_pct=npv_uplift_pct.quantize(Decimal("0.0001")),
        delta_revenue_annual_usd=delta_revenue_annual_usd.quantize(Decimal("0.01")),
        delta_npv_reporting_currency=delta_npv_reporting_currency.quantize(Decimal("0.01")),
        tax_rate_used=tax_rate,
        method_version=METHOD_VERSION,
        warnings=warnings,
    )

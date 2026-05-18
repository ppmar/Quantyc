"""Tests for revaluation math layer — all values hand-checked."""
from decimal import Decimal

import pytest

from revaluation.math import (
    RevaluationInput,
    RevaluationError,
    annuity_factor,
    revalue,
)


# ── Annuity factor tests ──────────────────────────────────────────


def test_annuity_factor_10y_8pct():
    """Standard textbook: A(8%, 10) = 6.7101"""
    result = annuity_factor(Decimal("8.0"), Decimal("10"))
    assert abs(result - Decimal("6.7101")) < Decimal("0.0001")


def test_annuity_factor_15y_5pct():
    """A(5%, 15) = 10.3797"""
    result = annuity_factor(Decimal("5.0"), Decimal("15"))
    assert abs(result - Decimal("10.3797")) < Decimal("0.0001")


def test_annuity_factor_zero_rate_raises():
    with pytest.raises(RevaluationError, match="positive"):
        annuity_factor(Decimal("0"), Decimal("10"))


def test_annuity_factor_negative_years_raises():
    with pytest.raises(RevaluationError, match="positive"):
        annuity_factor(Decimal("8.0"), Decimal("-1"))


# ── Gold AUD revaluation ──────────────────────────────────────────


def test_revalue_gold_aud_reporting():
    """Hemi-like scenario: DFS price in USD, NPV reported in AUD,
    FX is Yahoo AUDUSD=X convention (USD per AUD)."""
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),       # USD per invariant I2
        price_spot_usd=Decimal("3500"),      # USD per invariant I3
        annual_production=Decimal("180000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"),
        npv_dfs=Decimal("985"),              # AUD M
        reporting_currency="AUD",
        fx_rate=Decimal("0.6452"),           # USD per AUD (Yahoo convention)
    )
    result = revalue(inp)
    # ΔPrice_USD = 1600 USD/oz
    # ΔRev_USD  = 180,000 * 1600 = 288,000,000 USD/yr
    # A(5%,10)  = 7.7217
    # ΔNPV_USD  = 288e6 * 7.7217 * 0.70 / 1e6 = 1556.70 USD M
    # ΔNPV_AUD  = 1556.70 / 0.6452              = 2412.74 AUD M
    # NPV_spot  = 985 + 2412.74                 = 3397.74 AUD M
    assert abs(result.npv_spot - Decimal("3397.74")) < Decimal("0.10")
    assert abs(result.npv_uplift - Decimal("2412.74")) < Decimal("0.10")
    assert abs(result.delta_revenue_annual_usd - Decimal("288000000")) < Decimal("1")
    assert result.method_version == "first_order_v2"
    assert result.warnings == []


def test_revalue_gold_usd_reporting_magnitude():
    """Same scenario but reporting in USD: no FX conversion."""
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"),
        npv_dfs=Decimal("985"),
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    # NPV_spot = 985 + 1556.70 = 2541.70 USD M
    assert abs(result.npv_spot - Decimal("2541.70")) < Decimal("0.10")
    assert abs(result.npv_uplift - Decimal("1556.70")) < Decimal("0.10")


def test_revalue_sanbrado_au_usd():
    """Sanbrado Gold (West African Resources) — USD-reporting DFS.
    Production back-solved from displayed uplift = 4063 M at 3384 USD/oz uplift,
    11yr life, 5% rate, 32% tax: production ~ 212,580 oz/yr.
    """
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1300"),
        price_spot_usd=Decimal("4684"),
        annual_production=Decimal("212580"),
        annual_production_unit="oz",
        mine_life_years=Decimal("11"),
        discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("32.0"),
        npv_dfs=Decimal("405"),
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    assert abs(result.npv_spot - Decimal("4468")) < Decimal("5")
    assert abs(result.npv_uplift - Decimal("4063")) < Decimal("5")


def test_aud_reporting_zero_fx_raises():
    inp = RevaluationInput(
        commodity="Au", price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"), annual_production_unit="oz",
        mine_life_years=Decimal("10"), discount_rate_pct=Decimal("5"),
        tax_rate_pct=Decimal("30"), npv_dfs=Decimal("985"),
        reporting_currency="AUD", fx_rate=Decimal("0"),
    )
    with pytest.raises(RevaluationError, match="fx_rate_must_be_positive"):
        revalue(inp)


# ── Copper USD revaluation ────────────────────────────────────────


def test_revalue_copper_usd_reporting():
    """Copper: production in tonnes, price in USD/lb."""
    inp = RevaluationInput(
        commodity="Cu",
        price_dfs_usd=Decimal("3.50"),
        price_spot_usd=Decimal("4.80"),
        annual_production=Decimal("25000"),
        annual_production_unit="t",
        mine_life_years=Decimal("15"),
        discount_rate_pct=Decimal("8.0"),
        tax_rate_pct=Decimal("30.0"),
        npv_dfs=Decimal("450"),
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    # 25000 t * 2204.62262 lb/t = 55,115,565 lb
    # ΔRev = 55,115,565 * 1.30 = 71,650,234 USD/year
    # A(8%, 15) = 8.5595
    # ΔNPV = 71,650,234 * 8.5595 * 0.70 / 1,000,000 = 429.31 USD M
    # NPV_spot = 450 + 429.31 = 879.31 USD M
    assert abs(result.npv_spot - Decimal("879.31")) < Decimal("2.0")
    assert any("converted_production" in w for w in result.warnings)


# ── Error cases ───────────────────────────────────────────────────


def test_unsupported_commodity_raises():
    inp = RevaluationInput(
        commodity="Li2O",
        price_dfs_usd=Decimal("1500"),
        price_spot_usd=Decimal("800"),
        annual_production=Decimal("500000"),
        annual_production_unit="t",
        mine_life_years=Decimal("20"),
        discount_rate_pct=Decimal("8"),
        tax_rate_pct=None,
        npv_dfs=Decimal("2000"),
        reporting_currency="AUD",
        fx_rate=Decimal("0.6452"),
    )
    with pytest.raises(RevaluationError, match="unsupported_commodity"):
        revalue(inp)


def test_au_production_wrong_unit_raises():
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("5600"),
        annual_production_unit="kg",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("8"),
        tax_rate_pct=Decimal("30"),
        npv_dfs=Decimal("500"),
        reporting_currency="USD",
        fx_rate=None,
    )
    with pytest.raises(RevaluationError, match="Au production must be in 'oz'"):
        revalue(inp)


def test_aud_reporting_without_fx_raises():
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5"),
        tax_rate_pct=Decimal("30"),
        npv_dfs=Decimal("985"),
        reporting_currency="AUD",
        fx_rate=None,
    )
    with pytest.raises(RevaluationError, match="fx_rate_required"):
        revalue(inp)


# ── Tax default ───────────────────────────────────────────────────


def test_tax_rate_defaults_when_none():
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5"),
        tax_rate_pct=None,
        npv_dfs=Decimal("985"),
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    assert any("defaulted" in w for w in result.warnings)
    assert result.tax_rate_used == Decimal("30.0")


# ── Edge cases ────────────────────────────────────────────────────


def test_negative_price_change_lowers_npv():
    """If spot < DFS price, NPV decreases."""
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("3500"),
        price_spot_usd=Decimal("1900"),
        annual_production=Decimal("180000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5"),
        tax_rate_pct=Decimal("30"),
        npv_dfs=Decimal("2000"),
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    assert result.npv_spot < result.npv_dfs
    assert result.npv_uplift < 0
    assert result.npv_uplift_pct < 0


def test_zero_npv_dfs_no_division_by_zero():
    """Edge case: DFS NPV is zero."""
    inp = RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"),
        price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5"),
        tax_rate_pct=Decimal("30"),
        npv_dfs=Decimal("0"),
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    assert result.npv_uplift_pct == Decimal("0")
    assert result.npv_spot > 0

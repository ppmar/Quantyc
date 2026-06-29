"""Tests for revaluation math layer — all values hand-checked."""
from decimal import Decimal

import pytest

from revaluation.math import (
    RevaluationInput,
    RevaluationError,
    BasketRevaluationInput,
    CommodityLeg,
    annuity_factor,
    remaining_life_years,
    revalue,
    revalue_basket,
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


# ── Silver USD revaluation ────────────────────────────────────────


def test_revalue_silver_usd_reporting():
    """Silver behaves like gold: oz production, USD/oz price, no FX."""
    inp = RevaluationInput(
        commodity="Ag",
        price_dfs_usd=Decimal("20"),
        price_spot_usd=Decimal("30"),
        annual_production=Decimal("5000000"),  # 5 Moz/yr
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"),
        npv_dfs=Decimal("200"),                # USD M
        reporting_currency="USD",
        fx_rate=None,
    )
    result = revalue(inp)
    # A(5%,10)=7.7217; ΔRev=5e6*10=50e6; ΔNPV=50e6*7.7217*0.7=270.26M
    assert result.annuity_factor == Decimal("7.7217")
    assert abs(result.npv_spot - Decimal("470.26")) < Decimal("0.5")
    assert abs(result.npv_uplift_pct - Decimal("1.3513")) < Decimal("0.001")


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
    assert result.method_version == "first_order_v3"
    # Only the always-on honesty warnings; no data-quality warnings.
    assert result.warnings == [
        "royalty_not_netted_uplift_gross_of_royalties",
        "developer_pre_production_timing_not_discounted",
    ]


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


# ── Remaining-life annuity (producer depletion fix) ───────────────


def test_remaining_life_developer_is_full_life():
    """No elapsed production => full mine life remains."""
    assert remaining_life_years(Decimal("11"), None) == Decimal("11")


def test_remaining_life_producer_subtracts_elapsed():
    """vie_restante = duree - elapsed."""
    assert remaining_life_years(Decimal("11"), Decimal("6")) == Decimal("5")


def test_remaining_life_clamps_at_zero():
    """A mine past its life has no remaining life, never negative."""
    assert remaining_life_years(Decimal("11"), Decimal("13")) == Decimal("0")


def test_remaining_life_future_start_uses_full_life():
    """Negative elapsed (production starts after valuation date) => full life."""
    assert remaining_life_years(Decimal("11"), Decimal("-2")) == Decimal("11")


def _producer_input(elapsed):
    """Sanbrado-shaped gold producer: 11y life, 211koz/yr, 5% disc."""
    return RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1300"),
        price_spot_usd=Decimal("4500"),
        annual_production=Decimal("211000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("11"),
        discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("31.5"),
        npv_dfs=Decimal("405"),
        reporting_currency="USD",
        fx_rate=None,
        production_elapsed_years=elapsed,
    )


def test_producer_remaining_life_shrinks_annuity_and_uplift():
    """6 years into an 11y mine: annuity runs over 5y, not 11y, so the spot
    uplift is far smaller than the full-life (buggy) figure."""
    full = revalue(_producer_input(None))          # old behaviour: full 11y
    depleted = revalue(_producer_input(Decimal("6")))  # fixed: 5y remaining

    assert full.remaining_life_years == Decimal("11")
    assert depleted.remaining_life_years == Decimal("5")
    # A(5%,11)=8.3064 vs A(5%,5)=4.3295
    assert depleted.annuity_factor == annuity_factor(Decimal("5.0"), Decimal("5"))
    assert depleted.annuity_factor < full.annuity_factor
    # Uplift scales with the annuity factor -> materially smaller.
    assert depleted.npv_uplift < full.npv_uplift
    assert any("remaining_life" in w for w in depleted.warnings)


def test_fully_depleted_producer_has_zero_uplift():
    """Past end of life: no go-forward ounces => no first-order uplift."""
    result = revalue(_producer_input(Decimal("12")))
    assert result.remaining_life_years == Decimal("0")
    assert result.annuity_factor == Decimal("0.0000")
    assert result.npv_uplift == Decimal("0.00")
    assert result.npv_spot == result.npv_dfs
    assert "mine_depleted_no_remaining_life" in result.warnings


# ── Copper price unit normalization (CYM Nifty bug) ───────────────

from revaluation.math import normalize_cu_price_to_per_lb, EXTREME_UPLIFT_RATIO


def test_cu_price_per_tonne_unit_converts():
    p, w = normalize_cu_price_to_per_lb(Decimal("9370"), "USD/t")
    assert abs(p - Decimal("9370") / Decimal("2204.62262")) < Decimal("0.001")
    assert w and "t_to_lb" in w


def test_cu_price_mislabeled_lb_magnitude_converts():
    # CYM case: "13000 USD/lb" is really USD/tonne.
    p, w = normalize_cu_price_to_per_lb(Decimal("13000"), "USD/lb")
    assert abs(p - Decimal("13000") / Decimal("2204.62262")) < Decimal("0.001")
    assert w and "magnitude_t_to_lb" in w


def test_cu_price_genuine_per_lb_unchanged():
    p, w = normalize_cu_price_to_per_lb(Decimal("4.20"), "USD/lb")
    assert p == Decimal("4.20")
    assert w is None


def test_cu_price_blank_unit_small_value_unchanged():
    p, w = normalize_cu_price_to_per_lb(Decimal("5.5"), None)
    assert p == Decimal("5.5")
    assert w is None


def test_extreme_uplift_flagged():
    # Tiny base, large spot move -> >500% uplift -> flagged.
    inp = RevaluationInput(
        commodity="Au", price_dfs_usd=Decimal("1250"), price_spot_usd=Decimal("4500"),
        annual_production=Decimal("100000"), annual_production_unit="oz",
        mine_life_years=Decimal("10"), discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"), npv_dfs=Decimal("18"),
        reporting_currency="USD", fx_rate=None,
    )
    result = revalue(inp)
    assert any(w.startswith("extreme_uplift_check_inputs") for w in result.warnings)


# ── first_order_v4 basket revaluation ─────────────────────────────


def test_v4_single_leg_matches_v3_regression():
    """I1: first_order_v4 with one leg reproduces v3 numbers exactly. Same inputs as
    test_revalue_gold_aud_reporting."""
    v3 = revalue(RevaluationInput(
        commodity="Au",
        price_dfs_usd=Decimal("1900"), price_spot_usd=Decimal("3500"),
        annual_production=Decimal("180000"), annual_production_unit="oz",
        mine_life_years=Decimal("10"), discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"), npv_dfs=Decimal("985"),
        reporting_currency="AUD", fx_rate=Decimal("0.6452"),
    ))
    v4 = revalue_basket(BasketRevaluationInput(
        legs=(CommodityLeg("Au", Decimal("1900"), Decimal("3500"),
                           Decimal("180000"), "oz"),),
        mine_life_years=Decimal("10"), discount_rate_pct=Decimal("5.0"),
        tax_rate_pct=Decimal("30.0"), npv_dfs=Decimal("985"),
        reporting_currency="AUD", fx_rate=Decimal("0.6452"),
    ))
    assert v4.npv_spot == v3.npv_spot
    assert v4.npv_uplift == v3.npv_uplift
    assert v4.npv_uplift_pct == v3.npv_uplift_pct
    assert v4.delta_revenue_annual_usd == v3.delta_revenue_annual_usd
    assert v4.method_version == "first_order_v4"


def test_v4_basket_au_plus_cu():
    """Basket ΔNPV = Σ per-metal ΔNPV (I2). Hand-computed Au + Cu, USD reporting.

    Au: 180,000 oz * (3500 - 1900)      = 288,000,000 USD/yr
    Cu: 25,000 t * 2204.62262 lb/t = 55,115,565.5 lb
        * (4.80 - 3.50)                 = 71,650,235.15 USD/yr
    ΔRev_total                          = 359,650,235.15 USD/yr
    A(8%, 15) = 8.5595 ; (1 - 0.30) = 0.70
    ΔNPV_USD = 359,650,235.15 * 8.5595 * 0.70 / 1e6 = 2,154.86 USD M
    NPV_spot = 700 + 2154.86 = 2854.86 USD M
    """
    result = revalue_basket(BasketRevaluationInput(
        legs=(
            CommodityLeg("Au", Decimal("1900"), Decimal("3500"), Decimal("180000"), "oz"),
            CommodityLeg("Cu", Decimal("3.50"), Decimal("4.80"), Decimal("25000"), "t"),
        ),
        mine_life_years=Decimal("15"), discount_rate_pct=Decimal("8.0"),
        tax_rate_pct=Decimal("30.0"), npv_dfs=Decimal("700"),
        reporting_currency="USD", fx_rate=None,
    ))
    assert abs(result.delta_revenue_annual_usd - Decimal("359650235.15")) < Decimal("1")
    assert abs(result.npv_spot - Decimal("2854.86")) < Decimal("2.0")
    # Per-leg breakdown is carried for persistence (revaluation_legs).
    legs = dict(result.leg_delta_revenue_usd)
    assert abs(legs["Au"] - Decimal("288000000")) < Decimal("1")
    assert abs(legs["Cu"] - Decimal("71650235.15")) < Decimal("1")


def test_v4_each_leg_uses_its_own_unit_basis():
    """Au leg stays in oz; Cu leg converts t→lb. The conversion warning fires for Cu
    only, proving each leg normalizes to its own basis."""
    result = revalue_basket(BasketRevaluationInput(
        legs=(
            CommodityLeg("Au", Decimal("1900"), Decimal("2000"), Decimal("100000"), "oz"),
            CommodityLeg("Cu", Decimal("3.50"), Decimal("4.00"), Decimal("10000"), "t"),
        ),
        mine_life_years=Decimal("10"), discount_rate_pct=Decimal("8.0"),
        tax_rate_pct=Decimal("30.0"), npv_dfs=Decimal("500"),
        reporting_currency="USD", fx_rate=None,
    ))
    conv = [w for w in result.warnings if "converted_production" in w]
    assert len(conv) == 1 and "10000t" in conv[0]


def test_v4_unsupported_leg_raises():
    with pytest.raises(RevaluationError, match="unsupported_commodity:Ni"):
        revalue_basket(BasketRevaluationInput(
            legs=(CommodityLeg("Ni", Decimal("8"), Decimal("9"), Decimal("5000"), "t"),),
            mine_life_years=Decimal("10"), discount_rate_pct=Decimal("8.0"),
            tax_rate_pct=Decimal("30.0"), npv_dfs=Decimal("500"),
            reporting_currency="USD", fx_rate=None,
        ))

"""npv_uplift_pct edge cases.

- Negative npv_dfs flipped the sign of the percentage (positive uplift showed
  as negative %). Denominator must be |npv_dfs|.
- npv_dfs == 0 hid any uplift as 0% with no signal that the % is undefined.
"""
from decimal import Decimal

from revaluation.math import RevaluationInput, revalue


def _inp(**overrides) -> RevaluationInput:
    base = dict(
        commodity="Au",
        price_dfs_usd=Decimal("1800"),
        price_spot_usd=Decimal("2400"),
        annual_production=Decimal("100000"),
        annual_production_unit="oz",
        mine_life_years=Decimal("10"),
        discount_rate_pct=Decimal("8.0"),
        tax_rate_pct=Decimal("30"),
        npv_dfs=Decimal("500"),
        reporting_currency="USD",
        fx_rate=None,
        production_elapsed_years=None,
    )
    base.update(overrides)
    return RevaluationInput(**base)


def test_negative_npv_dfs_keeps_uplift_sign():
    result = revalue(_inp(npv_dfs=Decimal("-100")))
    assert result.npv_uplift > 0
    assert result.npv_uplift_pct > 0  # was negative: uplift / (-100)
    assert "npv_dfs_negative_pct_vs_abs" in result.warnings


def test_zero_npv_dfs_flagged_undefined():
    result = revalue(_inp(npv_dfs=Decimal("0")))
    assert result.npv_uplift_pct == Decimal("0")
    assert "npv_dfs_zero_pct_undefined" in result.warnings


def test_positive_npv_dfs_unchanged():
    result = revalue(_inp())
    assert result.npv_uplift_pct > 0
    assert "npv_dfs_negative_pct_vs_abs" not in result.warnings
    assert "npv_dfs_zero_pct_undefined" not in result.warnings

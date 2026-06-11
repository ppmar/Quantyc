"""First-order revaluation must declare its known overstatement biases.

1. Royalties/streams are not netted (inputs not extracted yet): the uplift is
   gross of royalties — every result carries that warning.
2. For a developer (not yet producing), the annuity starts at year 1, but the
   DFS schedules production after a multi-year build; the uplift is overstated
   by ~(1+r)^build_years. Flag every developer revaluation.
"""
from decimal import Decimal

from revaluation.math import RevaluationInput, revalue


def _inp(**overrides) -> RevaluationInput:
    base = dict(
        commodity="Au",
        price_dfs_usd=Decimal("1800"),
        price_spot_usd=Decimal("2400"),
        annual_production=Decimal("150000"),
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


def test_royalty_warning_always_present():
    result = revalue(_inp())
    assert "royalty_not_netted_uplift_gross_of_royalties" in result.warnings


def test_developer_gets_timing_warning():
    result = revalue(_inp(production_elapsed_years=None))
    assert "developer_pre_production_timing_not_discounted" in result.warnings


def test_producer_no_timing_warning():
    result = revalue(_inp(production_elapsed_years=Decimal("3")))
    assert "developer_pre_production_timing_not_discounted" not in result.warnings

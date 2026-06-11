"""Burn sign convention, end to end.

Convention: quarterly_opex_burn = -(net operating cashflow), in dollars.
    positive  => company is burning cash (the normal explorer case)
    negative  => net operating inflow ("net cash positive")

The old extractor stored abs(operating) so every company looked identical,
and the snapshot read-path treated positive burn as a net inflow — every
burning explorer displayed "Net cash positive" and runway never computed.
"""
from api.snapshot import compute_runway_display
from pipeline.extractors.appendix_5b import finalize_5b_amounts


# ── read path: runway from signed burn ──────────────────────────────

def test_runway_computed_for_burning_company():
    # 4M cash, burning 2M/quarter -> ~2 quarters
    assert compute_runway_display(4_000_000, 2_000_000) == "~2 quarters of runway"


def test_net_inflow_shows_net_cash_positive():
    assert compute_runway_display(4_000_000, -500_000) == "Net cash positive"


def test_missing_inputs_give_no_display():
    assert compute_runway_display(None, 2_000_000) is None
    assert compute_runway_display(4_000_000, None) is None
    assert compute_runway_display(4_000_000, 0) is None


# ── write path: extractor preserves operating sign ──────────────────

def test_operating_outflow_becomes_positive_burn():
    # 5B reports A$'000; operating -2,000 (outflow) -> burn +2,000,000
    amounts = finalize_5b_amounts({"operating": -2000.0, "cash": 5000.0})
    assert amounts["quarterly_opex_burn"] == 2_000_000.0
    assert amounts["cash"] == 5_000_000.0


def test_operating_inflow_becomes_negative_burn():
    # Positive operating cashflow (rare; misparse red flag or true inflow)
    # must keep its sign so downstream can tell inflow from burn.
    amounts = finalize_5b_amounts({"operating": 500.0})
    assert amounts["quarterly_opex_burn"] == -500_000.0


def test_investing_sign_preserved_same_convention():
    amounts = finalize_5b_amounts({"investing": -300.0})
    assert amounts["quarterly_invest_burn"] == 300_000.0

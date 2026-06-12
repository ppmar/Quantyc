"""Cash-section prose must not call an operating inflow "Burn".

Burn convention: positive = outflow (burning), negative = net inflow.
CMM's A$125M/quarter inflow read "Burn A$125.0M per quarter".
"""
from api.snapshot import compute_burn_prose


def test_burning_company_says_burn():
    assert compute_burn_prose(2_000_000, None) == "Burn A$2.0M per quarter"


def test_inflow_says_operating_inflow():
    assert compute_burn_prose(-125_000_000, None) == "Operating inflow A$125.0M per quarter"


def test_burn_up_from_prior():
    assert compute_burn_prose(2_000_000, 1_000_000) == (
        "Burn A$2.0M per quarter, up from A$1.0M prior"
    )


def test_burn_down_from_prior():
    assert compute_burn_prose(1_000_000, 2_000_000) == (
        "Burn A$1.0M per quarter, down from A$2.0M prior"
    )


def test_inflow_up_from_prior():
    assert compute_burn_prose(-125_000_000, -122_600_000) == (
        "Operating inflow A$125.0M per quarter, up from A$122.6M prior"
    )


def test_sign_flip_no_comparator():
    # Prior was a burn, now an inflow (or vice versa): "up/down from" would
    # compare different things — drop the comparator.
    assert compute_burn_prose(-1_000_000, 2_000_000) == (
        "Operating inflow A$1.0M per quarter"
    )


def test_none_burn():
    assert compute_burn_prose(None, None) is None

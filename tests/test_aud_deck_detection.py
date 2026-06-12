"""AUD price-deck detection must catch every Australian-dollar spelling.

ASX studies write the currency as "AUD/oz", "A$/oz", and also "$A/oz" —
the last was treated as USD and compared raw against USD spot (bogus uplift,
same class as the BTR A$5000-deck bug).
"""
from revaluation.pipeline import is_aud_price_unit


def test_aud_token():
    assert is_aud_price_unit("AUD/oz") is True


def test_a_dollar_prefix():
    assert is_aud_price_unit("A$/oz") is True


def test_dollar_a_spelling():
    assert is_aud_price_unit("$A/oz") is True
    assert is_aud_price_unit("$A per ounce") is True


def test_usd_not_matched():
    assert is_aud_price_unit("USD/oz") is False
    assert is_aud_price_unit("US$/lb") is False


def test_canadian_dollar_not_matched():
    assert is_aud_price_unit("CA$/oz") is False
    assert is_aud_price_unit("CAD/oz") is False


def test_blank_and_none():
    assert is_aud_price_unit(None) is False
    assert is_aud_price_unit("") is False

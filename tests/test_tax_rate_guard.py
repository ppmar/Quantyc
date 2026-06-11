"""Tax-rate input guards for revaluation.

Bug 1: `if study["tax_rate_pct"]` — an extracted 0 is falsy, silently became
the 30% default.
Bug 2: no fraction-vs-percent guard — an LLM returning 0.30 ("30%") passed the
0–100 schema bound and produced (1 - 0.30/100) = 0.997, i.e. almost no tax.
"""
from decimal import Decimal

from revaluation.math import normalize_tax_rate_pct


def test_none_passes_through():
    assert normalize_tax_rate_pct(None) == (None, None)


def test_zero_kept_not_defaulted():
    rate, warning = normalize_tax_rate_pct(Decimal("0"))
    assert rate == Decimal("0")
    assert warning == "tax_rate_zero_pct_kept"


def test_fraction_scaled_to_percent():
    rate, warning = normalize_tax_rate_pct(Decimal("0.30"))
    assert rate == Decimal("30.0")
    assert warning == "tax_rate_fraction_scaled_0.30_to_30.0pct"


def test_normal_percent_unchanged():
    assert normalize_tax_rate_pct(Decimal("30")) == (Decimal("30"), None)

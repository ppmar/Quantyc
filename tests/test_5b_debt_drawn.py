"""Debt must be the AMOUNT DRAWN (section 7 column 2), never the facility total.

An undrawn facility is not debt; treating the total as debt overstates EV.
When only one number is present it is ambiguous (usually the total) — refuse.
"""
from pipeline.extractors.appendix_5b import _debt_from_facility_values, _extract_from_text


# ── table path ────────────────────────────────────────────────────────

def test_two_values_returns_drawn():
    assert _debt_from_facility_values([10_000.0, 4_000.0]) == 4_000.0


def test_single_value_ambiguous_returns_none():
    assert _debt_from_facility_values([10_000.0]) is None


def test_no_values_returns_none():
    assert _debt_from_facility_values([]) is None


# ── regex fallback path ───────────────────────────────────────────────

def test_regex_debt_uses_drawn_column():
    text = "7.4 Total financing facilities 10,000 4,000"
    results = _extract_from_text(text)
    assert results["debt"] == 4_000.0


def test_regex_single_number_does_not_set_debt():
    text = "7.4 Total financing facilities 10,000"
    results = _extract_from_text(text)
    assert "debt" not in results

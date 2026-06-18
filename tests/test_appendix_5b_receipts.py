"""Appendix 5B line 1.1 'Receipts from customers' extraction (production signal)."""
from pipeline.extractors.appendix_5b import finalize_5b_amounts, _extract_from_text


def test_finalize_converts_receipts_thousands_to_dollars():
    out = finalize_5b_amounts({"receipts": 4500, "cash": 10000, "effective_date": "2026-03-31"})
    assert out["receipts_from_customers"] == 4_500_000  # A$'000 -> dollars, no sign flip


def test_finalize_receipts_none_passthrough():
    out = finalize_5b_amounts({"cash": 10000})
    assert out["receipts_from_customers"] is None


def test_extract_from_text_captures_receipts():
    text = (
        "1. Cash flows from operating activities\n"
        "1.1 Receipts from customers 4,500 9,000\n"
        "1.9 Net cash from operating activities (3,000) (6,000)\n"
    )
    res = _extract_from_text(text)
    assert res["receipts"] == 4500.0
    # the producer tell is positive even while net operating (1.9) is negative
    assert res["operating"] == -3000.0

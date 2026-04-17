import pytest

from pipeline.extractors.appendix_5b import _gate2_quarter_end_check


@pytest.mark.parametrize("date_str", [
    "2025-03-31", "2025-06-30", "2025-09-30", "2025-12-31",
    "2024-03-31", "2024-06-30",
])
def test_gate2_accepts_fiscal_quarter_ends(date_str):
    ok, reason = _gate2_quarter_end_check(date_str)
    assert ok is True


@pytest.mark.parametrize("date_str", [
    "2025-12-24", "2025-10-31", "2025-07-31",
    "2025-04-30", "2025-01-31", "2024-10-31",
])
def test_gate2_rejects_non_quarter_ends(date_str):
    ok, reason = _gate2_quarter_end_check(date_str)
    assert ok is False
    assert "not_quarter_end" in reason


def test_gate2_rejects_none():
    ok, reason = _gate2_quarter_end_check(None)
    assert ok is False
    assert reason == "missing_effective_date"


def test_gate2_rejects_empty_string():
    ok, reason = _gate2_quarter_end_check("")
    assert ok is False
    assert reason == "missing_effective_date"

"""5B amount-parsing edge cases.

Bug 1: _get_numeric_cells parsed the item-reference cell ("1.9") as a number,
so a row with a single amount column returned the line-item number as the value
(non_none[-2] picked 1.9 instead of the amount).

Bug 2: the regex fallback only handled parenthesised negatives; a minus-sign
negative ("-4,069") was captured without its sign.
"""
from pipeline.extractors.appendix_5b import (
    _get_numeric_cells,
    _extract_from_text,
)


# ── Bug 1: item-ref cell must not be read as an amount ───────────────

def test_ref_cell_not_parsed_as_value_single_amount_column():
    row = ["1.9", "Net cash used in operating activities", "(2,069)"]
    values = _get_numeric_cells(row)
    non_none = [v for v in values if v is not None]
    assert non_none == [-2069.0]


def test_ref_cell_skipped_with_quarter_and_ytd_columns():
    row = ["1.9", "Net cash from operating activities", "(2,069)", "(4,138)"]
    values = _get_numeric_cells(row)
    non_none = [v for v in values if v is not None]
    assert non_none == [-2069.0, -4138.0]


# ── Bug 2: minus-sign negatives keep their sign ──────────────────────

def test_regex_minus_sign_negative_operating():
    text = "1.9 Net cash from / (used in) operating activities -4,069 -8,123"
    results = _extract_from_text(text)
    assert results["operating"] == -4069.0


def test_regex_paren_negative_still_works():
    text = "1.9 Net cash from / (used in) operating activities (4,069) (8,123)"
    results = _extract_from_text(text)
    assert results["operating"] == -4069.0

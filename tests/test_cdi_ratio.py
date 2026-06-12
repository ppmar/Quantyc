"""CDI counts are not share counts unless the ratio is 1:1.

ASX CDIs exist at 10:1, 5:1 etc. (N CDIs represent 1 underlying share).
Counting raw CDIs as shares overstates shares_on_issue by the ratio.
"""
from pipeline.extractors.issue_of_securities import cdi_underlying_shares


def test_one_to_one_unchanged():
    shares, warning = cdi_underlying_shares("CHESS DEPOSITARY INTERESTS 1:1", 137_886_534)
    assert shares == 137_886_534
    assert warning is None


def test_ten_to_one_divides():
    shares, warning = cdi_underlying_shares("CHESS DEPOSITARY INTERESTS 10:1", 100_000_000)
    assert shares == 10_000_000
    assert warning == "cdi_ratio_10:1_count_100000000_shares_10000000"


def test_no_ratio_assumes_1_to_1_with_flag():
    shares, warning = cdi_underlying_shares("CHESS DEPOSITARY INTERESTS", 50_000_000)
    assert shares == 50_000_000
    assert warning == "cdi_ratio_unknown_assumed_1:1"


def test_non_cdi_description_passthrough():
    shares, warning = cdi_underlying_shares("ORDINARY FULLY PAID SHARES", 90_000_000)
    assert shares == 90_000_000
    assert warning is None

"""Targeted review_reason cleanup for the needs_review backfill.

Old _check_review_flags flagged fields the document type can never provide
(missing_shares_fd on every 5B, missing_cash/opex on every securities doc).
The cleanup strips those NA-reasons per doc_type and sets needs_review only
when genuine reasons remain (deviations, fields the doc SHOULD have had).
"""
from scripts.backfill_burn_sign_and_review import clean_review_reason


def test_5b_na_noise_stripped_entirely():
    needs_review, reason = clean_review_reason("missing_shares_fd", "appendix_5b")
    assert needs_review is False
    assert reason is None


def test_securities_na_noise_stripped():
    needs_review, reason = clean_review_reason(
        "missing_cash; missing_opex_burn", "appendix_2a"
    )
    assert needs_review is False
    assert reason is None


def test_genuine_deviation_kept_and_flagged():
    needs_review, reason = clean_review_reason(
        "missing_shares_fd; cash_50pct_deviation; quarterly_opex_burn_50pct_deviation",
        "appendix_5b",
    )
    assert needs_review is True
    assert reason == "cash_50pct_deviation; quarterly_opex_burn_50pct_deviation"


def test_missing_cash_on_5b_is_genuine():
    # A 5B should always carry cash — missing_cash on a 5B is a real flag.
    needs_review, reason = clean_review_reason("missing_cash", "appendix_5b")
    assert needs_review is True
    assert reason == "missing_cash"


def test_null_reason_passthrough():
    assert clean_review_reason(None, "appendix_5b") == (False, None)

"""Tests for the study NPV/tax extraction guard (pipeline.orchestrator)."""
from pipeline.orchestrator import check_study_review_flags


def test_clean_study_not_flagged():
    # WAF Kiaka shape: ~29% gap, tax rate present.
    needs, reason = check_study_review_flags(1675.0, 1183.0, 35.5)
    assert needs is False
    assert reason is None


def test_missing_post_tax_npv_flagged():
    # CMM Karlawinda shape — this is what broke the revaluation.
    needs, reason = check_study_review_flags(144.0, None, None)
    assert needs is True
    assert "missing_post_tax_npv" in reason


def test_missing_pre_tax_npv_flagged():
    needs, reason = check_study_review_flags(None, 259.0, None)
    assert needs is True
    assert "missing_pre_tax_npv" in reason


def test_inverted_npv_flagged():
    # Post-tax cannot exceed pre-tax — tax never adds value.
    needs, reason = check_study_review_flags(100.0, 105.0, 30.0)
    assert needs is True
    assert "post_tax_npv_ge_pre_tax_npv" in reason


def test_equal_npv_flagged():
    needs, reason = check_study_review_flags(100.0, 100.0, 30.0)
    assert needs is True
    assert "post_tax_npv_ge_pre_tax_npv" in reason


def test_narrow_gap_out_of_band_flagged():
    # RMS Never Never shape: 17.4% implied tax — too low for AU gold.
    needs, reason = check_study_review_flags(4190.0, 3459.0, None)
    assert needs is True
    assert "implied_tax_gap_17.4pct_out_of_band" in reason


def test_wide_gap_out_of_band_flagged():
    # 50% gap — too high; one NPV likely mislabelled.
    needs, reason = check_study_review_flags(1000.0, 500.0, None)
    assert needs is True
    assert "out_of_band" in reason


def test_in_band_gap_only_missing_tax():
    # ~28% gap is fine; only the missing tax rate is flagged.
    needs, reason = check_study_review_flags(1000.0, 720.0, None)
    assert needs is True
    assert reason == "missing_tax_rate"


def test_missing_tax_rate_alone_flags():
    needs, reason = check_study_review_flags(1000.0, 700.0, None)
    assert needs is True
    assert "missing_tax_rate" in reason


def test_nonpositive_discount_flagged():
    needs, reason = check_study_review_flags(750.0, 540.0, 30.0, discount_rate_pct=0.0)
    assert needs is True
    assert "discount_rate_nonpositive" in reason


def test_negative_aisc_flagged():
    needs, reason = check_study_review_flags(750.0, 540.0, 30.0, aisc_per_unit=-0.24)
    assert needs is True
    assert "aisc_negative" in reason


def test_positive_discount_and_aisc_clean():
    needs, reason = check_study_review_flags(750.0, 540.0, 30.0, discount_rate_pct=8.0, aisc_per_unit=1200.0)
    assert needs is False
    assert reason is None


def test_new_params_default_none_backcompat():
    # Old 3-arg call still works (discount/aisc unchecked).
    needs, reason = check_study_review_flags(750.0, 540.0, 30.0)
    assert needs is False

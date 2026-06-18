"""Deterministic production floor (pure)."""
from pipeline.stage_floor import production_floor


def test_material_receipts_with_study_is_production():
    assert production_floor(5_000_000, None, "2026-06-18", has_revaluable_study=True) is True


def test_passed_production_date_with_study_is_production():
    assert production_floor(None, "2025-01-01", "2026-06-18", has_revaluable_study=True) is True


def test_future_production_date_is_not_production():
    assert production_floor(None, "2027-01-01", "2026-06-18", has_revaluable_study=True) is False


def test_material_receipts_without_study_is_production():
    # Receipts self-prove a producing mine; our study coverage is incomplete, so
    # the receipts path must not require a DFS on file.
    assert production_floor(9_000_000, None, "2026-06-18", has_revaluable_study=False) is True


def test_implausible_receipts_rejected_as_misparse():
    # A$26B/qtr is not a junior's receipts — it's a misparse (sanity bound).
    assert production_floor(26_000_000_000, None, "2026-06-18", has_revaluable_study=True) is False


def test_passed_date_still_needs_study():
    # The date path comes from a DFS, so it keeps the study requirement.
    assert production_floor(None, "2025-01-01", "2026-06-18", has_revaluable_study=False) is False


def test_immaterial_receipts_no_date_is_not_production():
    assert production_floor(500_000, None, "2026-06-18", has_revaluable_study=True) is False


def test_no_signal_is_not_production():
    assert production_floor(None, None, "2026-06-18", has_revaluable_study=True) is False

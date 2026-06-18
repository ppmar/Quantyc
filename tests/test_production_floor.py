"""Deterministic production floor (pure)."""
from pipeline.stage_floor import production_floor


def test_material_receipts_with_study_is_production():
    assert production_floor(5_000_000, None, "2026-06-18", has_revaluable_study=True) is True


def test_passed_production_date_with_study_is_production():
    assert production_floor(None, "2025-01-01", "2026-06-18", has_revaluable_study=True) is True


def test_future_production_date_is_not_production():
    assert production_floor(None, "2027-01-01", "2026-06-18", has_revaluable_study=True) is False


def test_no_study_never_production_even_with_receipts():
    # An explorer with a stray receipt is not a producer.
    assert production_floor(9_000_000, "2020-01-01", "2026-06-18", has_revaluable_study=False) is False


def test_immaterial_receipts_no_date_is_not_production():
    assert production_floor(500_000, None, "2026-06-18", has_revaluable_study=True) is False


def test_no_signal_is_not_production():
    assert production_floor(None, None, "2026-06-18", has_revaluable_study=True) is False

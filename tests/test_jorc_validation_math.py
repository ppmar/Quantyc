"""JORC post-extraction validation math.

Bug 1: the Total≈Σ(categories) check summed subtotal/combo rows
(Measured+Indicated, Sub-total, In-situ Total, Stockpiles) alongside the pure
categories — double-counting that produced spurious deviation warnings and
masked real ones. Multiple Total rows (one per table) silently kept the last.

Bug 2: the tonnes×grade≈contained cross-check was a `pass` stub — the
strongest consistency check in resource extraction did nothing.
"""
from decimal import Decimal

from parsers.jorc_resource_estimate import _validate_estimate
from parsers.jorc_resource_estimate_schemas import JORCRow


def _row(category, tonnes, grade, contained=None, grade_unit="g/t",
         contained_unit=None):
    return JORCRow(
        category=category,
        tonnes_mt=Decimal(str(tonnes)) if tonnes is not None else None,
        grade=Decimal(str(grade)) if grade is not None else None,
        grade_unit=grade_unit,
        contained_metal=Decimal(str(contained)) if contained is not None else None,
        contained_metal_unit=contained_unit,
    )


# ── Bug 1: subtotal rows excluded from the Total≈Σ check ──────────────

def test_combo_rows_not_double_counted():
    rows = [
        _row("Measured", 10, 2.0),
        _row("Indicated", 20, 1.8),
        _row("Measured+Indicated", 30, 1.87),   # roll-up, not a category
        _row("Inferred", 5, 1.5),
        _row("Total", 35, 1.8),
    ]
    warnings = _validate_estimate(rows, "Au")
    assert not any(w.startswith("total_tonnes_deviation") for w in warnings)


def test_real_deviation_still_caught():
    rows = [
        _row("Measured", 10, 2.0),
        _row("Indicated", 20, 1.8),
        _row("Total", 100, 1.8),  # way off 30
    ]
    warnings = _validate_estimate(rows, "Au")
    assert any(w.startswith("total_tonnes_deviation") for w in warnings)


def test_multiple_total_rows_skips_check():
    rows = [
        _row("Measured", 10, 2.0),
        _row("Total", 10, 2.0),
        _row("Inferred", 5, 1.5),
        _row("Total", 5, 1.5),  # second table's total
    ]
    warnings = _validate_estimate(rows, "Au")
    assert "multiple_total_rows_tonnes_check_skipped" in warnings
    assert not any(w.startswith("total_tonnes_deviation") for w in warnings)


# ── Bug 2: tonnes × grade ≈ contained implemented ─────────────────────

def test_gold_contained_consistent_moz():
    # 10 Mt @ 2.0 g/t = 20 t Au = 643,015 oz ≈ 0.643 Moz
    rows = [_row("Measured", 10, 2.0, contained=0.643, contained_unit="Moz")]
    warnings = _validate_estimate(rows, "Au")
    assert not any(w.startswith("contained_metal_mismatch") for w in warnings)


def test_gold_contained_mismatch_flagged():
    # Same tonnes/grade but contained claims 6.43 Moz (10x)
    rows = [_row("Measured", 10, 2.0, contained=6.43, contained_unit="Moz")]
    warnings = _validate_estimate(rows, "Au")
    assert any(w.startswith("contained_metal_mismatch") for w in warnings)


def test_copper_percent_contained_kt():
    # 50 Mt @ 1.0 % Cu = 500 kt contained
    rows = [_row("Measured", 50, 1.0, contained=500, contained_unit="kt",
                 grade_unit="%")]
    warnings = _validate_estimate(rows, "Cu")
    assert not any(w.startswith("contained_metal_mismatch") for w in warnings)


def test_unknown_contained_unit_skipped():
    rows = [_row("Measured", 10, 2.0, contained=12, contained_unit="widgets")]
    warnings = _validate_estimate(rows, "Au")
    assert not any(w.startswith("contained_metal_mismatch") for w in warnings)

"""Production magnitude heuristics, as one auditable pure function.

Bugs fixed:
- Ag `< 1000 -> x1e6` assumed Moz; a 500 koz/yr silver producer reported as
  "500" became 500 Moz/yr (1000x error). The 100–999 band is ambiguous
  (koz vs Moz) and must refuse, not guess.
- The rescalings only went to logger.warning, never into the persisted
  warnings JSON — the most dangerous transform left no audit trail.
"""
from decimal import Decimal

import pytest

from revaluation.math import (
    RevaluationError,
    apply_production_magnitude_heuristic,
)


def test_au_koz_mislabel_scaled():
    val, warning = apply_production_magnitude_heuristic("Au", Decimal("150"))
    assert val == Decimal("150000")
    assert warning == "production_magnitude_scaled_Au_150_x1000"


def test_au_normal_oz_untouched():
    val, warning = apply_production_magnitude_heuristic("Au", Decimal("150000"))
    assert val == Decimal("150000")
    assert warning is None


def test_ag_moz_scaled_below_100():
    val, warning = apply_production_magnitude_heuristic("Ag", Decimal("2.5"))
    assert val == Decimal("2500000")
    assert warning == "production_magnitude_scaled_Ag_2.5_x1000000"


def test_ag_ambiguous_band_refuses():
    # 100–999 could be koz (x1e3) or Moz (x1e6): a guess can be 1000x wrong.
    with pytest.raises(RevaluationError, match="ag_production_unit_ambiguous"):
        apply_production_magnitude_heuristic("Ag", Decimal("500"))


def test_cu_kt_mislabel_scaled():
    val, warning = apply_production_magnitude_heuristic("Cu", Decimal("25"))
    assert val == Decimal("25000")
    assert warning == "production_magnitude_scaled_Cu_25_x1000"


def test_cu_normal_tonnes_untouched():
    val, warning = apply_production_magnitude_heuristic("Cu", Decimal("30000"))
    assert val == Decimal("30000")
    assert warning is None

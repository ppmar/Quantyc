"""DFS targeted_first_production normalization (production-floor fallback signal)."""
from decimal import Decimal

from parsers.dfs_study_schemas import StudyExtraction


def _study(**kw):
    base = dict(
        project_name="Murchison", study_type="DFS", primary_commodity="Au",
        reporting_currency="AUD", discount_rate_pct=Decimal("5"),
        post_tax_npv_millions=Decimal("244"), initial_capex_millions=Decimal("100"),
    )
    base.update(kw)
    return StudyExtraction(**base)


def test_year_only_normalized_to_first_day():
    assert _study(targeted_first_production="2026").targeted_first_production == "2026-01-01"


def test_year_month_normalized():
    assert _study(targeted_first_production="2025-10").targeted_first_production == "2025-10-01"


def test_full_iso_date_kept():
    assert _study(targeted_first_production="2025-10-01").targeted_first_production == "2025-10-01"


def test_unparseable_nulled_and_warned():
    s = _study(targeted_first_production="Q4 2025")
    assert s.targeted_first_production is None
    assert any("first_production_unparseable" in w for w in s.extraction_warnings)


def test_none_stays_none():
    assert _study().targeted_first_production is None

from decimal import Decimal
from parsers.dfs_study_schemas import StudyExtraction


def _base(**kw):
    d = dict(project_name="Karlawinda", study_type="DFS", primary_commodity="Au",
             reporting_currency="AUD", discount_rate_pct=Decimal("8"),
             initial_capex_millions=Decimal("146"))
    d.update(kw)
    return StudyExtraction(**d)


def test_npv_post_equals_pre_flagged():
    s = _base(post_tax_npv_millions=Decimal("144"), pre_tax_npv_millions=Decimal("144"))
    assert "npv_post_equals_pre_suspected_duplicate" in s.extraction_warnings


def test_npv_post_gt_pre_flagged():
    s = _base(post_tax_npv_millions=Decimal("200"), pre_tax_npv_millions=Decimal("150"))
    assert any(w.startswith("npv_post_gt_pre") for w in s.extraction_warnings)


def test_npv_normal_no_flag():
    s = _base(post_tax_npv_millions=Decimal("451"), pre_tax_npv_millions=Decimal("750"))
    assert not any("npv_post" in w for w in s.extraction_warnings)


def test_aisc_unit_malformed_flagged():
    s = _base(post_tax_npv_millions=Decimal("451"), pre_tax_npv_millions=Decimal("750"),
              aisc_per_unit=Decimal("1243"), aisc_unit="US$1243AUD/oz")
    assert any(w.startswith("aisc_unit_malformed") for w in s.extraction_warnings)


def test_aisc_unit_clean_no_flag():
    s = _base(post_tax_npv_millions=Decimal("451"), pre_tax_npv_millions=Decimal("750"),
              aisc_per_unit=Decimal("1243"), aisc_unit="USD/oz")
    assert not any("aisc_unit_malformed" in w for w in s.extraction_warnings)


def test_only_one_npv_present_no_npv_flag():
    # Partial-tolerant (I4): a single NPV is legitimate, not flagged.
    s = _base(post_tax_npv_millions=Decimal("259"))
    assert not any("npv_post" in w for w in s.extraction_warnings)


# ── Future effective_date guard (PR2) ─────────────────────────────

from datetime import date, timedelta


def test_future_effective_date_discarded():
    future = date.today() + timedelta(days=30)
    s = _base(post_tax_npv_millions=Decimal("1178"), effective_date=future)
    assert s.effective_date is None
    assert any(w.startswith("effective_date_in_future_discarded") for w in s.extraction_warnings)


def test_past_effective_date_kept():
    past = date(2024, 6, 15)
    s = _base(post_tax_npv_millions=Decimal("451"), effective_date=past)
    assert s.effective_date == past
    assert not any("effective_date_in_future" in w for w in s.extraction_warnings)


# ── Multi-commodity / commodity_production (first_order_v4) ────────


def test_commodity_production_basket_no_mismatch():
    from parsers.dfs_study_schemas import CommodityProduction
    s = _base(
        post_tax_npv_millions=Decimal("451"), annual_production=Decimal("180"),
        annual_production_unit="koz",
        commodity_production=[
            CommodityProduction(commodity="Au", annual_production=Decimal("180"),
                                annual_production_unit="koz"),
            CommodityProduction(commodity="Ag", annual_production=Decimal("2.7"),
                                annual_production_unit="Moz"),
        ],
    )
    assert not any("primary_production_mismatch" in w for w in s.extraction_warnings)
    assert not any("primary_commodity_missing" in w for w in s.extraction_warnings)


def test_primary_production_mismatch_flagged():
    from parsers.dfs_study_schemas import CommodityProduction
    s = _base(
        post_tax_npv_millions=Decimal("451"), annual_production=Decimal("180"),
        commodity_production=[
            CommodityProduction(commodity="Au", annual_production=Decimal("999"))],
    )
    assert any(w.startswith("primary_production_mismatch") for w in s.extraction_warnings)


def test_primary_missing_from_basket_flagged():
    from parsers.dfs_study_schemas import CommodityProduction
    s = _base(
        post_tax_npv_millions=Decimal("451"), primary_commodity="Au",
        commodity_production=[
            CommodityProduction(commodity="Ag", annual_production=Decimal("2.7"),
                                annual_production_unit="Moz")],
    )
    assert any(w.startswith("primary_commodity_missing_from_basket") for w in s.extraction_warnings)


def test_byproduct_null_volume_allowed():
    """A by-product with no payable volume (credit-only) is null, not an error (I8)."""
    from parsers.dfs_study_schemas import CommodityProduction
    s = _base(
        post_tax_npv_millions=Decimal("451"), annual_production=Decimal("180"),
        commodity_production=[
            CommodityProduction(commodity="Au", annual_production=Decimal("180")),
            CommodityProduction(commodity="Ag", annual_production=None),  # credit-only
        ],
    )
    ag = next(c for c in s.commodity_production if c.commodity == "Ag")
    assert ag.annual_production is None

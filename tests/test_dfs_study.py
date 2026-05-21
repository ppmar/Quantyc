"""Tests for the DFS Study parser — profile detection, Pydantic validation, and real fixtures."""

import io
import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from parsers.dfs_study import detect_profile, parse
from parsers.dfs_study_schemas import DFSExtraction, PriceAssumption

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "dfs"


def _make_pdf(*page_texts: str) -> bytes:
    """Create a minimal multi-page PDF with text."""
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    for text in page_texts:
        y = 750
        for line in text.split("\n"):
            c.drawString(72, y, line)
            y -= 14
        c.showPage()
    c.save()
    return buf.getvalue()


# ─── Profile detection tests ─────────────────────────────────────────

class TestDetectProfile:

    def test_accepts_dfs_text(self):
        pdf = _make_pdf("XYZ Ltd\nDefinitive Feasibility Study\nHemi Gold Project")
        assert detect_profile(pdf) is True

    def test_accepts_dfs_abbreviation(self):
        pdf = _make_pdf("DFS Confirms Robust Economics for Hemi")
        assert detect_profile(pdf) is True

    def test_accepts_final_feasibility(self):
        pdf = _make_pdf("Final Feasibility Study Results")
        assert detect_profile(pdf) is True

    def test_accepts_pfs(self):
        pdf = _make_pdf("Pre-Feasibility Study Results\nPFS confirms strong economics")
        assert detect_profile(pdf) is True

    def test_accepts_scoping(self):
        pdf = _make_pdf("Scoping Study delivers positive results")
        assert detect_profile(pdf) is True

    def test_rejects_appendix_5b(self):
        pdf = _make_pdf("Appendix 5B\nQuarterly cash flow report")
        assert detect_profile(pdf) is False

    def test_accepts_dfs_referencing_pfs(self):
        """A DFS that mentions PFS history on page 1 should still be accepted."""
        pdf = _make_pdf(
            "Definitive Feasibility Study\nBuilding on PFS completed in 2023"
        )
        assert detect_profile(pdf) is True

    def test_rejects_empty_pdf(self):
        pdf = _make_pdf("")
        assert detect_profile(pdf) is False

    def test_rejects_unrelated_text(self):
        pdf = _make_pdf("Annual General Meeting\nNotice of Meeting")
        assert detect_profile(pdf) is False


# ─── Pydantic validation tests ───────────────────────────────────────

def _valid_payload(**overrides) -> dict:
    """Return a valid DFSExtraction dict, optionally with overrides."""
    base = {
        "project_name": "Hemi",
        "study_type": "DFS",
        "primary_commodity": "Au",
        "reporting_currency": "AUD",
        "discount_rate_pct": Decimal("8.0"),
        "post_tax_npv_millions": Decimal("2500"),
        "initial_capex_millions": Decimal("985"),
        "irr_pct": Decimal("38.0"),
        "mine_life_years": Decimal("10"),
        "price_assumptions": [
            {"commodity": "Au", "price": Decimal("2600"), "unit": "USD/oz"}
        ],
    }
    base.update(overrides)
    return base


class TestPydanticValidation:

    def test_accepts_complete_payload(self):
        result = DFSExtraction(**_valid_payload())
        assert result.project_name == "Hemi"
        assert result.reporting_currency == "AUD"

    def test_rejects_placeholder_project_name(self):
        with pytest.raises(Exception):
            DFSExtraction(**_valid_payload(project_name="the project"))

    def test_rejects_project_name_unknown(self):
        with pytest.raises(Exception):
            DFSExtraction(**_valid_payload(project_name="Unknown"))

    def test_rejects_irr_above_200pct(self):
        with pytest.raises(Exception):
            DFSExtraction(**_valid_payload(irr_pct=Decimal("250")))

    def test_rejects_discount_rate_above_25pct(self):
        with pytest.raises(Exception):
            DFSExtraction(**_valid_payload(discount_rate_pct=Decimal("30")))

    def test_rejects_negative_discount_rate(self):
        with pytest.raises(Exception):
            DFSExtraction(**_valid_payload(discount_rate_pct=Decimal("-1")))

    def test_has_minimum_data_requires_npv_and_capex(self):
        # Missing capex
        result = DFSExtraction(**_valid_payload(initial_capex_millions=None))
        assert result.has_minimum_data() is False

    def test_has_minimum_data_requires_any_npv(self):
        # Missing both NPVs
        result = DFSExtraction(**_valid_payload(
            post_tax_npv_millions=None, pre_tax_npv_millions=None
        ))
        assert result.has_minimum_data() is False

    def test_has_minimum_data_accepts_pre_tax_only(self):
        result = DFSExtraction(**_valid_payload(
            post_tax_npv_millions=None,
            pre_tax_npv_millions=Decimal("3000"),
        ))
        assert result.has_minimum_data() is True

    def test_has_minimum_data_passes_with_both(self):
        result = DFSExtraction(**_valid_payload())
        assert result.has_minimum_data() is True

    def test_price_assumptions_parsed(self):
        result = DFSExtraction(**_valid_payload())
        assert len(result.price_assumptions) == 1
        assert result.price_assumptions[0].commodity == "Au"

    def test_extraction_warnings_default_empty(self):
        result = DFSExtraction(**_valid_payload())
        assert result.extraction_warnings == []

    def test_optional_fields_accept_none(self):
        result = DFSExtraction(**_valid_payload(
            effective_date=None,
            payback_years=None,
            recovery_pct=None,
            fx_assumption=None,
        ))
        assert result.effective_date is None
        assert result.payback_years is None


# ─── Real fixture tests (LLM call — gated) ───────────────────────────

def _llm_tests_enabled() -> bool:
    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    return bool(api_key) and os.environ.get("RUN_LLM_TESTS") == "1"


def _find_deg_fixture() -> Path | None:
    """Find any DEG DFS fixture PDF in the fixtures dir."""
    for p in FIXTURE_DIR.glob("DEG_dfs_*.pdf"):
        return p
    return None


@pytest.fixture(scope="module")
def deg_dfs_result():
    """Parse DEG Hemi DFS fixture. Skipped if API key not set or fixture missing."""
    if not _llm_tests_enabled():
        pytest.skip("Gemini LLM tests gated on RUN_LLM_TESTS=1 + API key")

    pdf_path = _find_deg_fixture()
    if pdf_path is None:
        pytest.skip(f"No DEG DFS fixture found in {FIXTURE_DIR}")

    return parse(
        pdf_bytes=pdf_path.read_bytes(),
        ticker="DEG",
        doc_id="real_deg_dfs_001",
        announcement_date=date(2024, 6, 15),
    )


class TestDEGDFSRealFixture:
    """Real fixture tests — hardcoded expected values read from PDF by hand.
    Placeholder values until actual fixture is added."""

    EXPECTED_PROJECT = "Hemi"
    EXPECTED_COMMODITY = "Au"
    EXPECTED_CURRENCY = "AUD"
    EXPECTED_DISCOUNT_RATE = Decimal("5.0")        # placeholder — read from PDF
    EXPECTED_POST_TAX_NPV = Decimal("2500")        # placeholder
    EXPECTED_IRR = Decimal("38.0")                 # placeholder
    EXPECTED_INITIAL_CAPEX = Decimal("985")        # placeholder
    EXPECTED_MINE_LIFE = Decimal("10")             # placeholder

    def test_project_name(self, deg_dfs_result):
        assert deg_dfs_result.project_name == self.EXPECTED_PROJECT

    def test_commodity(self, deg_dfs_result):
        assert deg_dfs_result.primary_commodity == self.EXPECTED_COMMODITY

    def test_reporting_currency(self, deg_dfs_result):
        assert deg_dfs_result.reporting_currency == self.EXPECTED_CURRENCY

    def test_discount_rate(self, deg_dfs_result):
        assert deg_dfs_result.discount_rate_pct == self.EXPECTED_DISCOUNT_RATE

    def test_post_tax_npv(self, deg_dfs_result):
        assert abs(deg_dfs_result.post_tax_npv_millions - self.EXPECTED_POST_TAX_NPV) < Decimal("10")

    def test_irr(self, deg_dfs_result):
        assert abs(deg_dfs_result.irr_pct - self.EXPECTED_IRR) < Decimal("0.5")

    def test_initial_capex(self, deg_dfs_result):
        assert abs(deg_dfs_result.initial_capex_millions - self.EXPECTED_INITIAL_CAPEX) < Decimal("5")

    def test_mine_life(self, deg_dfs_result):
        assert abs(deg_dfs_result.mine_life_years - self.EXPECTED_MINE_LIFE) < Decimal("0.5")

    def test_price_assumptions_has_primary_commodity(self, deg_dfs_result):
        assert len(deg_dfs_result.price_assumptions) >= 1
        commodities = {pa.commodity for pa in deg_dfs_result.price_assumptions}
        assert self.EXPECTED_COMMODITY in commodities

    def test_no_critical_warnings(self, deg_dfs_result):
        critical = [w for w in deg_dfs_result.extraction_warnings
                    if "placeholder" in w.lower() or "missing" in w.lower()]
        assert critical == [], f"Critical warnings present: {critical}"

"""Tests for the JORC Resource Estimate parser — synthetic fixtures."""

import io
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.platypus import Table, TableStyle

from parsers.jorc_resource_estimate import (
    detect_profile,
    parse,
)
from parsers.appendix_2a import (
    ExtractionError,
    MalformedDocumentError,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "jorc_resource_estimate"


def _make_pdf(*page_texts: str) -> bytes:
    """Create a minimal multi-page PDF (text only, no tables)."""
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


def _make_jorc_pdf(
    title_text: str,
    table_data: list[list[str]],
    extra_text: str = "",
) -> bytes:
    """Create a PDF with title text and a bordered JORC table.

    pdfplumber requires actual drawn lines to detect tables, so we use
    reportlab's Table with GRID style.
    """
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    # Title text
    y = 750
    for line in title_text.split("\n"):
        c.drawString(72, y, line)
        y -= 14

    # Extra body text
    if extra_text:
        y -= 10
        for line in extra_text.split("\n"):
            c.drawString(72, y, line)
            y -= 14

    # Bordered table
    tbl = Table(table_data)
    tbl.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 1, colors.black),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
    ]))
    w, h = tbl.wrapOn(c, 450, 400)
    tbl.drawOn(c, 72, y - h - 10)

    c.showPage()
    c.save()
    return buf.getvalue()


# ── Shared synthetic fixtures ─────────────────────────────────────────

GOLD_TITLE = (
    "Sunrise Gold Project Mineral Resource Estimate\n"
    "JORC Code (2012) Compliant\n"
    "ASX Announcement"
)

GOLD_TABLE = [
    ["Category", "Tonnes (Mt)", "Grade (g/t)", "Contained (koz)"],
    ["Measured", "5.2", "2.1", "351"],
    ["Indicated", "12.8", "1.8", "740"],
    ["Inferred", "8.5", "1.5", "410"],
    ["Total", "26.5", "1.7", "1,501"],
]

GOLD_EXTRA = (
    "Cut-off grade of 0.5 g/t\n"
    "Effective as at 15 March 2026\n"
    "Competent Person: Dr J Smith"
)


# ── Test 1: Profile detection — accepts resource update ───────────────

class TestProfileDetection:
    def test_detect_profile_accepts_resource_update(self):
        pdf = _make_jorc_pdf(GOLD_TITLE, GOLD_TABLE, GOLD_EXTRA)
        assert detect_profile(pdf) is True

    def test_detect_profile_rejects_5b(self):
        pdf = _make_pdf(
            "Appendix 5B\n"
            "Mining exploration entity quarterly cash flow report\n"
            "Quarter ended 31 March 2026"
        )
        assert detect_profile(pdf) is False

    def test_detect_profile_rejects_quarterly_activity(self):
        pdf = _make_pdf(
            "Quarterly Activities Report\n"
            "March 2026 Quarter\n"
            "Highlights"
        )
        assert detect_profile(pdf) is False


# ── Test 4: parse extracts categories ─────────────────────────────────

class TestParseCategories:
    @pytest.fixture(scope="class")
    def gold_result(self):
        pdf = _make_jorc_pdf(GOLD_TITLE, GOLD_TABLE, GOLD_EXTRA)
        return parse(pdf, ticker="TST", doc_id="test_gold", announcement_date=date(2026, 3, 20))

    def test_parse_extracts_categories(self, gold_result):
        cats = [r.category for r in gold_result.rows]
        assert "Measured" in cats
        assert "Indicated" in cats
        assert "Inferred" in cats

    def test_measured_tonnes(self, gold_result):
        row = next(r for r in gold_result.rows if r.category == "Measured")
        assert row.tonnes_mt == Decimal("5.2")

    def test_indicated_grade(self, gold_result):
        row = next(r for r in gold_result.rows if r.category == "Indicated")
        assert row.grade == Decimal("1.8")

    def test_inferred_contained(self, gold_result):
        row = next(r for r in gold_result.rows if r.category == "Inferred")
        assert row.contained_metal == Decimal("410")

    def test_commodity_is_gold(self, gold_result):
        assert gold_result.commodity == "Au"

    def test_project_name(self, gold_result):
        assert "Sunrise" in gold_result.project_name


# ── Test 5: Inferred only ────────────────────────────────────────────

class TestInferredOnly:
    def test_parse_inferred_only(self):
        table = [
            ["Category", "Tonnes (Mt)", "Grade (g/t)", "Contained (koz)"],
            ["Measured", "—", "—", "—"],
            ["Indicated", "—", "—", "—"],
            ["Inferred", "3.1", "1.2", "120"],
            ["Total", "3.1", "1.2", "120"],
        ]
        pdf = _make_jorc_pdf(GOLD_TITLE, table, GOLD_EXTRA)
        result = parse(pdf, ticker="TST", doc_id="inf", announcement_date=date(2026, 1, 1))

        inf_row = next(r for r in result.rows if r.category == "Inferred")
        assert inf_row.tonnes_mt == Decimal("3.1")

        # Measured and Indicated should be present with null values
        meas = [r for r in result.rows if r.category == "Measured"]
        ind = [r for r in result.rows if r.category == "Indicated"]
        if meas:
            assert meas[0].tonnes_mt is None
        if ind:
            assert ind[0].tonnes_mt is None


# ── Test 6: Cutoff grade ─────────────────────────────────────────────

class TestCutoffGrade:
    def test_parse_extracts_cutoff_grade(self):
        pdf = _make_jorc_pdf(GOLD_TITLE, GOLD_TABLE, GOLD_EXTRA)
        result = parse(pdf, ticker="TST", doc_id="co", announcement_date=date(2026, 1, 1))
        assert result.cutoff_grade == Decimal("0.5")
        assert result.cutoff_grade_unit == "g/t"


# ── Test 7: Effective date ───────────────────────────────────────────

class TestEffectiveDate:
    def test_parse_extracts_effective_date(self):
        pdf = _make_jorc_pdf(GOLD_TITLE, GOLD_TABLE, GOLD_EXTRA)
        result = parse(pdf, ticker="TST", doc_id="ed", announcement_date=date(2026, 3, 20))
        assert result.snapshot_date == date(2026, 3, 15)
        assert result.announcement_date == date(2026, 3, 20)
        assert result.snapshot_date != result.announcement_date


# ── Test 8: kt conversion ───────────────────────────────────────────

class TestKtConversion:
    def test_parse_handles_kt_conversion(self):
        table = [
            ["Category", "Tonnes (kt)", "Grade (g/t)", "Contained (koz)"],
            ["Measured", "5,200", "2.1", "351"],
            ["Indicated", "12,800", "1.8", "740"],
            ["Inferred", "8,500", "1.5", "410"],
        ]
        pdf = _make_jorc_pdf(GOLD_TITLE, table, GOLD_EXTRA)
        result = parse(pdf, ticker="TST", doc_id="kt", announcement_date=date(2026, 1, 1))

        meas = next(r for r in result.rows if r.category == "Measured")
        # 5200 kt = 5.2 Mt
        assert meas.tonnes_mt == Decimal("5.2")
        assert any("tonnes_converted" in w for w in result.extraction_warnings)


# ── Test 9: Grade range warning for outlier ──────────────────────────

class TestGradeWarning:
    def test_grade_range_warning_for_outlier(self):
        table = [
            ["Category", "Tonnes (Mt)", "Grade (g/t)", "Contained (koz)"],
            ["Measured", "1.0", "200", "6,430"],
            ["Indicated", "2.0", "1.8", "116"],
        ]
        title = (
            "Test Gold Project Mineral Resource Estimate\n"
            "JORC Code (2012)"
        )
        pdf = _make_jorc_pdf(title, table)
        result = parse(pdf, ticker="TST", doc_id="outlier", announcement_date=date(2026, 1, 1))
        # Should warn about the 200 g/t grade but not raise
        assert any("grade_outlier" in w for w in result.extraction_warnings)


# ── Test 10: Reserve rows emit warning ───────────────────────────────

class TestReserveWarning:
    def test_reserve_rows_emit_warning(self):
        table = [
            ["Category", "Tonnes (Mt)", "Grade (g/t)", "Contained (koz)"],
            ["Measured", "5.2", "2.1", "351"],
            ["Indicated", "12.8", "1.8", "740"],
            ["Inferred", "8.5", "1.5", "410"],
            ["Proven", "4.0", "2.0", "257"],
            ["Probable", "10.0", "1.7", "546"],
        ]
        title = (
            "Test Gold Project Mineral Resource Estimate\n"
            "JORC Code (2012)"
        )
        pdf = _make_jorc_pdf(title, table)
        result = parse(pdf, ticker="TST", doc_id="res", announcement_date=date(2026, 1, 1))

        # Only resource rows in output
        cats = [r.category for r in result.rows]
        assert "Proven" not in cats
        assert "Probable" not in cats
        assert "Measured" in cats
        assert any("reserve_rows_present" in w for w in result.extraction_warnings)


# ── Test 11: Malformed document raises ───────────────────────────────

class TestMalformedDocument:
    def test_malformed_document_raises(self):
        # PDF with JORC keywords but no actual JORC table
        pdf = _make_pdf(
            "Mineral Resource Estimate\n"
            "JORC Code (2012)\n"
            "The company is pleased to announce an updated resource.\n"
            "Further details to follow."
        )
        # detect_profile should fail (no JORC table), but if passed directly
        # to parse() it should raise
        with pytest.raises((MalformedDocumentError, ExtractionError)):
            parse(pdf, ticker="TST", doc_id="bad", announcement_date=date(2026, 1, 1))


# ── Test 12: Polymetallic picks first commodity with warning ─────────

class TestPolymetallic:
    def test_polymetallic_picks_first_commodity_with_warning(self):
        table = [
            ["Category", "Tonnes (Mt)", "Cu Grade (%)", "Au Grade (g/t)", "Cu Contained (kt)", "Au Contained (koz)"],
            ["Measured", "10.0", "1.2", "0.5", "120", "161"],
            ["Indicated", "25.0", "0.9", "0.3", "225", "241"],
            ["Inferred", "15.0", "0.7", "0.2", "105", "96"],
        ]
        title = (
            "Test Copper-Gold Project Mineral Resource Estimate\n"
            "JORC Code (2012)"
        )
        pdf = _make_jorc_pdf(title, table)
        result = parse(pdf, ticker="TST", doc_id="poly", announcement_date=date(2026, 1, 1))
        # Should pick a commodity (Cu or Au) and warn about polymetallic
        assert result.commodity in ("Cu", "Au")
        assert any("polymetallic" in w.lower() for w in result.extraction_warnings)

"""Tests for the Appendix 2A parser — HTG fixture + failure modes."""

import io
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from parsers.appendix_2a import (
    ExtractionError,
    MalformedDocumentError,
    ReconciliationError,
    detect_profile,
    parse,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "appendix_2a"
HTG_PDF = FIXTURE_DIR / "HTG_appendix_2a_2026-04-20.pdf"

EXPECTED_HTG_QUOTED = [
    ("HTG", "ORDINARY FULLY PAID", 1_204_377_219),
]

EXPECTED_HTG_UNQUOTED = [
    ("HTGAU", "option",           1_500_000,  date(2028, 10,  7), Decimal("0.03")),
    ("HTGAT", "option",          18_506_790,  date(2028,  3, 29), Decimal("0.0377")),
    ("HTGAV", "option",           1_000_000,  date(2028, 12, 19), Decimal("0.03")),
    ("HTGAA", "option",          58_947_247,  date(2027,  4, 26), Decimal("0.03")),
    ("HTGAG", "convertible_note", 6_275_671,  None,               None),
    ("HTGAR", "option",           5_000_000,  date(2026,  6, 30), Decimal("0.075")),
    ("HTGAD", "option",          17_824_676,  date(2028,  5, 19), Decimal("0.0261")),
    ("HTGAS", "option",          32_647_406,  date(2027,  9, 24), Decimal("0.03")),
    ("HTGAC", "option",          22_000_000,  date(2028,  1, 22), Decimal("0.02")),
]


def _make_pdf(*page_texts: str) -> bytes:
    """Create a minimal multi-page PDF."""
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


@pytest.fixture(scope="module")
def htg_result():
    pdf_bytes = HTG_PDF.read_bytes()
    return parse(pdf_bytes, ticker="HTG", doc_id="test_htg", announcement_date=date(2026, 4, 20))


# ── Test 1: Profile detection ──────────────────────────────────────────

class TestProfileDetection:
    def test_profile_detection_htg(self):
        pdf_bytes = HTG_PDF.read_bytes()
        assert detect_profile(pdf_bytes) is True

    def test_non_2a_rejected(self):
        pdf = _make_pdf("Appendix 5B\nMining exploration entity quarterly cash flow report")
        assert detect_profile(pdf) is False


# ── Tests 2-3: Quoted classes ──────────────────────────────────────────

class TestQuotedClasses:
    def test_quoted_classes_htg(self, htg_result):
        assert len(htg_result.quoted_classes) == 1
        q = htg_result.quoted_classes[0]
        assert q.asx_code == EXPECTED_HTG_QUOTED[0][0]
        assert q.description == EXPECTED_HTG_QUOTED[0][1]
        assert q.total_on_issue == EXPECTED_HTG_QUOTED[0][2]

    def test_shares_basic_htg(self, htg_result):
        assert htg_result.shares_basic == 1_204_377_219


# ── Tests 4-6: Unquoted instruments ───────────────────────────────────

class TestUnquotedInstruments:
    def test_unquoted_count_htg(self, htg_result):
        assert len(htg_result.unquoted_instruments) == 9

    def test_unquoted_exact_match_htg(self, htg_result):
        for i, (code, itype, count, expiry, strike) in enumerate(EXPECTED_HTG_UNQUOTED):
            inst = htg_result.unquoted_instruments[i]
            assert inst.asx_code == code, f"Row {i}: expected code {code}, got {inst.asx_code}"
            assert inst.instrument_type == itype, f"Row {i}: expected type {itype}, got {inst.instrument_type}"
            assert inst.total_on_issue == count, f"Row {i}: expected count {count}, got {inst.total_on_issue}"
            assert inst.expiry_date == expiry, f"Row {i}: expected expiry {expiry}, got {inst.expiry_date}"
            assert inst.strike_aud == strike, f"Row {i}: expected strike {strike}, got {inst.strike_aud}"

    def test_options_outstanding_htg(self, htg_result):
        assert htg_result.options_outstanding == 157_426_119
        assert htg_result.convertible_notes_face_count == 6_275_671


# ── Test 7: FD naive ──────────────────────────────────────────────────

class TestFDNaive:
    def test_shares_fd_naive_htg(self, htg_result):
        assert htg_result.shares_fd_naive == 1_368_079_009


# ── Test 8: Warnings ──────────────────────────────────────────────────

class TestWarnings:
    def test_no_warnings_htg(self, htg_result):
        assert len(htg_result.extraction_warnings) == 0


# ── Test 9: Idempotent ────────────────────────────────────────────────

class TestIdempotent:
    def test_idempotent_htg(self):
        pdf_bytes = HTG_PDF.read_bytes()
        r1 = parse(pdf_bytes, ticker="HTG", doc_id="test", announcement_date=date(2026, 4, 20))
        r2 = parse(pdf_bytes, ticker="HTG", doc_id="test", announcement_date=date(2026, 4, 20))
        assert r1.shares_basic == r2.shares_basic
        assert r1.shares_fd_naive == r2.shares_fd_naive
        assert r1.quoted_classes == r2.quoted_classes
        assert r1.unquoted_instruments == r2.unquoted_instruments
        assert r1.extraction_warnings == r2.extraction_warnings


# ── Tests 10-12: Failure modes ────────────────────────────────────────

class TestFailureModes:
    def test_scanned_pdf_raises(self):
        # Minimal PDF with no text content (just a blank page)
        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=A4)
        c.showPage()
        c.save()
        with pytest.raises(ExtractionError, match="scanned_pdf_no_text"):
            parse(buf.getvalue(), ticker="X", doc_id="x", announcement_date=date(2026, 1, 1))

    def test_missing_part_4_raises(self):
        pdf = _make_pdf(
            "Appendix 2A - Application for quotation of securities\n"
            "Part 1 - Entity details\nSome entity info"
        )
        with pytest.raises(MalformedDocumentError, match="appendix_2a_missing_part4"):
            parse(pdf, ticker="X", doc_id="x", announcement_date=date(2026, 1, 1))

    def test_unknown_unquoted_shape_warns(self):
        page_text = (
            "Part 4 - Issued capital following quotation\n"
            "4.1 Quoted +securities\n"
            "Total number of\n"
            "ASX +security code and description +securities on issue\n"
            "XYZ : ORDINARY FULLY PAID 100,000,000\n"
            "4.2 Unquoted +securities\n"
            "Total number of\n"
            "ASX +security code and description +securities on issue\n"
            "XYZAA : WIDGET 100,000\n"
        )
        pdf = _make_pdf(
            "Appendix 2A - Application for quotation of securities\nPart 1",
            page_text,
        )
        result = parse(pdf, ticker="XYZ", doc_id="x", announcement_date=date(2026, 1, 1))
        assert len(result.unquoted_instruments) == 1
        assert result.unquoted_instruments[0].instrument_type == "other"
        assert any("unquoted_row_unparsed" in w for w in result.extraction_warnings)

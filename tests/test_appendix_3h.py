"""
Tests for the Appendix 3H/3G parser.

Fixtures:
    AGY_appendix_3h_2026-03-27.pdf — positive (3H with Part 3)
    SMM_appendix_3b_2026-04-23.pdf — negative (3B, no Part 3)
"""

import io
import pytest
from datetime import date
from decimal import Decimal
from pathlib import Path

from parsers.appendix_3h import detect_profile, parse, MalformedDocumentError
from parsers.appendix_2a import ExtractionError

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "appendix_3h"

EXPECTED_AGY_SHARES_BASIC = 1_543_420_934
EXPECTED_AGY_OPTIONS_OUTSTANDING = 39_999_996 + 600_000  # AGYO quoted + AGYAD unquoted
EXPECTED_AGY_PERF_RIGHTS = 10_000_000
EXPECTED_AGY_CN = 0
EXPECTED_AGY_FD_NAIVE = (
    EXPECTED_AGY_SHARES_BASIC
    + EXPECTED_AGY_OPTIONS_OUTSTANDING
    + EXPECTED_AGY_PERF_RIGHTS
)


@pytest.fixture(scope="module")
def agy_pdf() -> bytes:
    return (FIXTURE_DIR / "AGY_appendix_3h_2026-03-27.pdf").read_bytes()


@pytest.fixture(scope="module")
def smm_pdf() -> bytes:
    return (FIXTURE_DIR / "SMM_appendix_3b_2026-04-23.pdf").read_bytes()


@pytest.fixture(scope="module")
def agy_result(agy_pdf):
    return parse(agy_pdf, ticker="AGY", doc_id="test-agy", announcement_date=date(2026, 3, 27))


def _make_pdf(text: str) -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    c.drawString(72, 700, text)
    c.save()
    return buf.getvalue()


class TestProfileDetection:
    def test_detects_agy_as_3h(self, agy_pdf):
        matched, subtype = detect_profile(agy_pdf)
        assert matched is True
        assert subtype == "appendix_3h"

    def test_rejects_smm_3b(self, smm_pdf):
        matched, subtype = detect_profile(smm_pdf)
        assert matched is False
        assert subtype is None

    def test_rejects_non_asx_pdf(self):
        matched, subtype = detect_profile(_make_pdf("hello world"))
        assert matched is False


class TestQuotedShares:
    def test_agy_shares_basic(self, agy_result):
        assert agy_result.shares_basic == EXPECTED_AGY_SHARES_BASIC

    def test_agy_has_exactly_one_quoted_share_class(self, agy_result):
        assert len(agy_result.quoted_classes) == 1

    def test_agy_quoted_option_not_in_quoted_classes(self, agy_result):
        codes = [q.asx_code for q in agy_result.quoted_classes]
        assert "AGYO" not in codes
        # AGYO should be in unquoted_instruments as an option
        agyo = [u for u in agy_result.unquoted_instruments if u.asx_code == "AGYO"]
        assert len(agyo) == 1
        assert agyo[0].instrument_type == "option"
        assert agyo[0].total_on_issue == 39_999_996


class TestUnquotedInstruments:
    def test_agy_has_three_unquoted_after_promotion(self, agy_result):
        # AGYO (promoted from quoted) + AGYAB + AGYAD = 3
        assert len(agy_result.unquoted_instruments) == 3

    def test_agy_perf_rights_count(self, agy_result):
        assert agy_result.performance_rights_count == EXPECTED_AGY_PERF_RIGHTS

    def test_agy_options_outstanding(self, agy_result):
        assert agy_result.options_outstanding == EXPECTED_AGY_OPTIONS_OUTSTANDING

    def test_agyad_strike_parsed(self, agy_result):
        agyad = [u for u in agy_result.unquoted_instruments if u.asx_code == "AGYAD"]
        assert len(agyad) == 1
        assert agyad[0].strike_aud == Decimal("0.7293")

    def test_agyad_expiry_parsed(self, agy_result):
        agyad = [u for u in agy_result.unquoted_instruments if u.asx_code == "AGYAD"]
        assert agyad[0].expiry_date == date(2026, 6, 30)


class TestFullyDiluted:
    def test_agy_fd_naive(self, agy_result):
        assert agy_result.shares_fd_naive == EXPECTED_AGY_FD_NAIVE


class TestFailureModes:
    def test_smm_3b_parse_raises_malformed(self, smm_pdf):
        with pytest.raises(MalformedDocumentError):
            parse(smm_pdf, ticker="SMM", doc_id="test", announcement_date=date(2026, 4, 23))

    def test_scanned_pdf_raises(self):
        with pytest.raises(ExtractionError):
            parse(_make_pdf(""), ticker="X", doc_id="t", announcement_date=date(2026, 1, 1))


class TestIdempotent:
    def test_parse_twice_equal(self, agy_pdf):
        r1 = parse(agy_pdf, ticker="AGY", doc_id="t1", announcement_date=date(2026, 3, 27))
        r2 = parse(agy_pdf, ticker="AGY", doc_id="t2", announcement_date=date(2026, 3, 27))
        assert r1.shares_basic == r2.shares_basic
        assert r1.shares_fd_naive == r2.shares_fd_naive
        assert r1.options_outstanding == r2.options_outstanding
        assert r1.performance_rights_count == r2.performance_rights_count
        assert len(r1.quoted_classes) == len(r2.quoted_classes)
        assert len(r1.unquoted_instruments) == len(r2.unquoted_instruments)

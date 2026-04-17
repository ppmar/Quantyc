import io

import pdfplumber
import pytest
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from pipeline.extractors.appendix_5b import _gate1_first_page_check


def _make_pdf(first_page_text: str) -> bytes:
    """Create a minimal single-page PDF with given text."""
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    y = 750
    for line in first_page_text.split("\n"):
        c.drawString(72, y, line)
        y -= 14
    c.save()
    return buf.getvalue()


class TestGate1Passes:
    def test_appendix_5b_marker(self):
        pdf = _make_pdf("Appendix 5B\nMining exploration entity quarterly cash flow report")
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is True
        assert reason == "ok"

    def test_rule_5_5_marker(self):
        pdf = _make_pdf("Rule 5.5\nQuarterly cash flow report")
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is True
        assert reason == "ok"

    def test_mining_exploration_entity_marker(self):
        pdf = _make_pdf(
            "Mining exploration entity or oil and gas exploration entity\n"
            "quarterly cash flow report"
        )
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is True
        assert reason == "ok"


class TestGate1Rejects:
    def test_no_marker(self):
        pdf = _make_pdf("Quarterly Activities Report\nSome company did some exploration")
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is False
        assert "no_5b_marker" in reason

    def test_appendix_4c_disqualifier(self):
        pdf = _make_pdf("Appendix 5B\nAppendix 4C\nQuarterly report")
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is False
        assert "disqualifier" in reason

    def test_appendix_5a_disqualifier(self):
        pdf = _make_pdf("Appendix 5B\nAppendix 5A\nMining production entity")
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is False
        assert "disqualifier" in reason

    def test_empty_pdf(self):
        # Minimal valid PDF with no text
        pdf = _make_pdf("")
        ok, reason = _gate1_first_page_check(pdf)
        assert ok is False

    def test_invalid_bytes(self):
        ok, reason = _gate1_first_page_check(b"not a pdf")
        assert ok is False
        assert "pdf_read_error" in reason

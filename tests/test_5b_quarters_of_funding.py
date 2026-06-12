"""Item 8.7 (estimated quarters of funding) extraction.

The form's own label reads "8.7 Estimated quarters of funding available
(Item 8.6 divided by Item 8.5)" — the lazy regex captured "8.6" from the
label for every document instead of the actual value.
"""
from pipeline.extractors.appendix_5b import _extract_from_text


def test_value_after_label_paren():
    text = "8.7 Estimated quarters of funding available (Item 8.6 divided by Item 8.5) 12.4"
    assert _extract_from_text(text)["quarters_of_funding"] == 12.4


def test_value_without_label_paren():
    text = "8.7 Estimated quarters of funding available 6.5"
    assert _extract_from_text(text)["quarters_of_funding"] == 6.5


def test_na_value():
    text = "8.7 Estimated quarters of funding available (Item 8.6 divided by Item 8.5) N/A"
    assert _extract_from_text(text)["quarters_of_funding"] is None


def test_greater_than_value():
    text = "8.7 Estimated quarters of funding available (Item 8.6 divided by Item 8.5) > 50"
    assert _extract_from_text(text)["quarters_of_funding"] == 50.0

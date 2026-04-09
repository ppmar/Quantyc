"""Extract release date from PDF header."""

from __future__ import annotations

import logging
import re
from datetime import date

logger = logging.getLogger("parsers.exploration_results")

DATE_PATTERN = re.compile(
    r"\b(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|"
    r"OCTOBER|NOVEMBER|DECEMBER)\s+(\d{1,2}),\s+(\d{4})\b",
    re.IGNORECASE,
)

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def extract_release_date(page1_text: str) -> date | None:
    """Extract the release date from the first 500 characters of page 1."""
    chunk = page1_text[:500]
    m = DATE_PATTERN.search(chunk)
    if not m:
        logger.debug("No date match in first 500 chars of page 1")
        return None
    month = _MONTHS[m.group(1).lower()]
    day = int(m.group(2))
    year = int(m.group(3))
    try:
        return date(year, month, day)
    except ValueError:
        logger.warning("Invalid date components: %s %s %s", year, month, day)
        return None

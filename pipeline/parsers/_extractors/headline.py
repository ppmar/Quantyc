"""Extract headline intercept(s) from page 1."""

from __future__ import annotations

import logging
import re

from pipeline.parsers.schemas import HeadlineIntercept

logger = logging.getLogger("parsers.exploration_results")

HEADLINE_INTERCEPT_PATTERN = re.compile(
    r"""
    (?P<interval_m>\d+(?:\.\d+)?)    \s* m \s* @ \s*
    (?P<aueq>\d+(?:\.\d+)?)          \s* g/t \s* AuEq \s*
    \(
      (?P<au>\d+(?:\.\d+)?)          \s* g/t \s* Au \s*,\s*
      (?P<sb>\d+(?:\.\d+)?)          \s* % \s* Sb
    \)
    \s* from \s*
    (?P<from_m>\d+(?:\.\d+)?)        \s* m
    """,
    re.VERBOSE | re.IGNORECASE,
)

# SXG-specific drill-hole naming, then generic fallback
HOLE_ID_PATTERNS = [
    re.compile(r"\bSDDSC\d+[A-Z]?\d?\b"),
    re.compile(r"\b[A-Z]{2,5}\d{2,4}[A-Z]?\d?\b"),
]


def _find_hole_id(text: str, match_start: int, match_end: int) -> str | None:
    """Look back up to 200 characters and forward up to 100 characters for a hole ID."""
    # Look back first (most common: "SDDSC200 returned 17.3 m @ ...")
    lookback = text[max(0, match_start - 200) : match_start]
    for pattern in HOLE_ID_PATTERNS:
        matches = list(pattern.finditer(lookback))
        if matches:
            return matches[-1].group(0)
    # Look forward (e.g. "17.3 m @ ... from 251.1 m in drill hole SDDSC200")
    lookahead = text[match_end : match_end + 100]
    for pattern in HOLE_ID_PATTERNS:
        m = pattern.search(lookahead)
        if m:
            return m.group(0)
    return None


def extract_headline_intercepts(page1_text: str) -> list[HeadlineIntercept]:
    """Extract all headline intercepts from page 1 text."""
    results = []
    for m in HEADLINE_INTERCEPT_PATTERN.finditer(page1_text):
        hole_id = _find_hole_id(page1_text, m.start(), m.end())
        raw = m.group(0).strip()
        intercept = HeadlineIntercept(
            interval_m=float(m.group("interval_m")),
            aueq_gpt=float(m.group("aueq")),
            au_gpt=float(m.group("au")),
            sb_pct=float(m.group("sb")),
            from_m=float(m.group("from_m")),
            hole_id=hole_id,
            source_page=1,
            raw_text=raw,
        )
        logger.debug("Headline intercept: %s", raw)
        results.append(intercept)
    return results

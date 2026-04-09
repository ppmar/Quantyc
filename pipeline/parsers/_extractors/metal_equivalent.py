"""Extract metal equivalent (AuEq) formula and price assumptions."""

from __future__ import annotations

import logging
import re

from pipeline.parsers.schemas import MetalEquivalentAssumptions

logger = logging.getLogger("parsers.exploration_results")

AUEQ_FORMULA_PATTERN = re.compile(
    r"AuEq\s*=\s*Au\s*\(\s*g/t\s*\)\s*\+\s*"
    r"(?P<multiplier>\d+(?:\.\d+)?)\s*[×x*]\s*"
    r"Sb\s*\(\s*%\s*\)",
    re.IGNORECASE,
)

# Unicode multiplication sign may appear as various renderings in PDF text
AUEQ_FORMULA_PATTERN_ALT = re.compile(
    r"AuEq\s*=\s*Au\s*\(?g/t\)?\s*\+\s*"
    r"(?P<multiplier>\d+(?:\.\d+)?)\s*[\u00d7\u2715\u2a09x*×]\s*"
    r"Sb\s*\(?%\)?",
    re.IGNORECASE,
)

# PDF math-italic Unicode: 𝐴𝑢𝐸𝑞 = 𝐴𝑢 (𝑔/𝑡) + 2.39 × 𝑆𝑏 (%)
# Characters like U+1D434 (𝐴), U+1D462 (𝑢), etc.
AUEQ_FORMULA_PATTERN_MATH = re.compile(
    r"[\U0001D400-\U0001D7FF]*[AaEeQq𝐴𝑢𝐸𝑞]+[\U0001D400-\U0001D7FF]*\s*=\s*"
    r"[\U0001D400-\U0001D7FF]*[Aa𝐴𝑢]+[\U0001D400-\U0001D7FF]*\s*"
    r"\(\s*[g𝑔]/[t𝑡]\s*\)\s*\+\s*"
    r"(?P<multiplier>\d+(?:\.\d+)?)\s*[×x*\u00d7]\s*"
    r"[\U0001D400-\U0001D7FF]*[Ss𝑆𝑏]+[\U0001D400-\U0001D7FF]*\s*"
    r"\(\s*%\s*\)",
)

GOLD_PRICE_PATTERN = re.compile(
    r"gold\s+price\s+of\s+US\$\s*([\d,]+(?:\.\d+)?)\s*(?:per\s+ounce|/\s*oz)",
    re.IGNORECASE,
)

ANTIMONY_PRICE_PATTERN = re.compile(
    r"antimony\s+price\s+of\s+US\$\s*([\d,]+(?:\.\d+)?)\s*(?:per\s+tonne|/\s*t)",
    re.IGNORECASE,
)

RECOVERY_PATTERN = re.compile(
    r"(\d+)%\s+for\s+gold\s+and\s+(\d+)%\s+for\s+antimony",
    re.IGNORECASE,
)


def extract_metal_equivalent(
    page_texts: list[str],
) -> MetalEquivalentAssumptions | None:
    """
    Extract the AuEq formula and commodity price assumptions.
    Searches all pages.
    """
    full_text = "\n".join(page_texts)

    # Find formula — try ASCII patterns first, then Unicode math italic
    m = AUEQ_FORMULA_PATTERN.search(full_text)
    if not m:
        m = AUEQ_FORMULA_PATTERN_ALT.search(full_text)
    if not m:
        m = AUEQ_FORMULA_PATTERN_MATH.search(full_text)
    if not m:
        logger.warning("AuEq formula not found in document")
        return None

    multiplier = float(m.group("multiplier"))
    formula_pos = m.start()
    formula_text = m.group(0).strip()
    logger.debug("Found AuEq formula: multiplier=%.2f", multiplier)

    # Search ±500 chars around formula for prices and recoveries
    context_start = max(0, formula_pos - 500)
    context_end = min(len(full_text), formula_pos + len(m.group(0)) + 500)
    context = full_text[context_start:context_end]

    au_price = None
    sb_price = None
    au_recovery = None
    sb_recovery = None

    gm = GOLD_PRICE_PATTERN.search(context)
    if gm:
        au_price = float(gm.group(1).replace(",", ""))
        logger.debug("Gold price: US$%.0f/oz", au_price)

    am = ANTIMONY_PRICE_PATTERN.search(context)
    if am:
        sb_price = float(am.group(1).replace(",", ""))
        logger.debug("Antimony price: US$%.0f/t", sb_price)

    rm = RECOVERY_PATTERN.search(context)
    if rm:
        au_recovery = int(rm.group(1))
        sb_recovery = int(rm.group(2))
        logger.debug("Recoveries: Au=%d%%, Sb=%d%%", au_recovery, sb_recovery)

    # Determine source page
    source_page = 1
    char_count = 0
    for page_num, text in enumerate(page_texts, 1):
        char_count += len(text) + 1  # +1 for the \n join
        if formula_pos < char_count:
            source_page = page_num
            break

    return MetalEquivalentAssumptions(
        formula_text=formula_text,
        multiplier=multiplier,
        au_price_usd_per_oz=au_price,
        sb_price_usd_per_tonne=sb_price,
        au_recovery_pct=au_recovery,
        sb_recovery_pct=sb_recovery,
        source_page=source_page,
    )

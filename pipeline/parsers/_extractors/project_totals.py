"""Extract project totals snapshot from narrative text."""

from __future__ import annotations

import logging
import re

from pipeline.parsers.schemas import ExtractionWarning, ProjectTotalsSnapshot

logger = logging.getLogger("parsers.exploration_results")

_WORD_TO_INT = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15,
    "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20,
}


def word_to_int(s: str) -> int | None:
    """Convert a word-form number to int. Returns None if not recognized."""
    s = s.strip().lower().replace("-", " ").replace("  ", " ")
    # Try compound like "eighty one"
    if " " in s:
        parts = s.split()
        if len(parts) == 2:
            tens = {
                "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
                "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
            }
            t = tens.get(parts[0])
            o = _WORD_TO_INT.get(parts[1])
            if t is not None and o is not None:
                return t + o
            if t is not None and parts[1] == "":
                return t
    # Direct lookup
    result = _WORD_TO_INT.get(s)
    if result is not None:
        return result
    # Tens
    tens_only = {
        "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
        "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
        "hundred": 100,
    }
    return tens_only.get(s)


def _strip_commas(s: str) -> str:
    return s.replace(",", "")


def extract_project_totals(
    page_texts: list[str],
) -> tuple[ProjectTotalsSnapshot | None, list[ExtractionWarning]]:
    """
    Extract project totals from all page texts.
    Returns (snapshot, warnings).
    """
    warnings: list[ExtractionWarning] = []
    fields: dict[str, object] = {}
    page_hits: dict[int, int] = {}  # page_num -> count of fields found

    full_text = "\n".join(page_texts)

    # --- total_drill_holes and total_metres ---
    # "247 drill holes for 114,806.33 m" or "247 drill holes for 114,806 m"
    # Prefer the most precise match (with decimals) across all pages.
    pat_holes = re.compile(
        r"(\d+)\s+drill\s+holes?\s+for\s+([\d,]+(?:\.\d+)?)\s*m",
        re.IGNORECASE,
    )
    best_holes_match: tuple[int, str, str, int] | None = None  # (holes, metres_str, raw, page)
    for page_num, text in enumerate(page_texts, 1):
        m = pat_holes.search(text)
        if m:
            metres_str = m.group(2)
            # Prefer matches with decimal precision
            if best_holes_match is None or ("." in metres_str and "." not in best_holes_match[1]):
                best_holes_match = (int(m.group(1)), metres_str, m.group(0), page_num)
    if best_holes_match is not None:
        fields["total_drill_holes"] = best_holes_match[0]
        fields["total_metres"] = float(_strip_commas(best_holes_match[1]))
        page_hits[best_holes_match[3]] = page_hits.get(best_holes_match[3], 0) + 2
        logger.debug(
            "drill holes=%s, metres=%s on page %d",
            fields["total_drill_holes"], fields["total_metres"], best_holes_match[3],
        )

    # --- composites_gt_100_au ---
    # "eighty-one (81) composite intersections exceeding 100 g/t Au"
    # or "81 composite intersections exceeding 100 g/t Au"
    pat_gt100 = re.compile(
        r"(?:[\w-]+\s+)?\(?(\d+)\)?\s+composite\s+intersections?\s+exceeding\s+100\s*g/t\s*Au",
        re.IGNORECASE,
    )
    for page_num, text in enumerate(page_texts, 1):
        m = pat_gt100.search(text)
        if m and "composites_gt_100_au" not in fields:
            fields["composites_gt_100_au"] = int(m.group(1))
            page_hits[page_num] = page_hits.get(page_num, 0) + 1
            logger.debug("composites_gt_100_au=%s on page %d", fields["composites_gt_100_au"], page_num)

    # --- composites_50_to_100_au ---
    pat_50_100 = re.compile(
        r"(?:[\w-]+\s+)?\(?(\d+)\)?\s+composite\s+intersections?\s+between\s+50\s*g?/?t?\s*"
        r"(?:and\s+)?(?:g/t\s+and\s+)?100\s*g/t\s*Au",
        re.IGNORECASE,
    )
    for page_num, text in enumerate(page_texts, 1):
        m = pat_50_100.search(text)
        if m and "composites_50_to_100_au" not in fields:
            fields["composites_50_to_100_au"] = int(m.group(1))
            page_hits[page_num] = page_hits.get(page_num, 0) + 1
            logger.debug("composites_50_to_100_au=%s on page %d", fields["composites_50_to_100_au"], page_num)

    # --- composites_gt_10_sb ---
    pat_gt10sb = re.compile(
        r"(?:[\w-]+\s+)?\(?(\d+)\)?\s+composite\s+intersections?\s+exceeding\s+10\s*%?\s*Sb",
        re.IGNORECASE,
    )
    for page_num, text in enumerate(page_texts, 1):
        m = pat_gt10sb.search(text)
        if m and "composites_gt_10_sb" not in fields:
            fields["composites_gt_10_sb"] = int(m.group(1))
            page_hits[page_num] = page_hits.get(page_num, 0) + 1
            logger.debug("composites_gt_10_sb=%s on page %d", fields["composites_gt_10_sb"], page_num)

    # --- holes_pending ---
    pat_pending1 = re.compile(r"(\d+)\s+holes?\s+pending\s+results?", re.IGNORECASE)
    pat_pending2 = re.compile(r"pending\s+(?:results?\s+)?from\s+(\d+)\s+holes?", re.IGNORECASE)
    for page_num, text in enumerate(page_texts, 1):
        m = pat_pending1.search(text) or pat_pending2.search(text)
        if m and "holes_pending" not in fields:
            fields["holes_pending"] = int(m.group(1))
            page_hits[page_num] = page_hits.get(page_num, 0) + 1
            logger.debug("holes_pending=%s on page %d", fields["holes_pending"], page_num)

    # --- active_rigs ---
    pat_rigs = re.compile(
        r"(?:with\s+)?(\w+)\s+(?:drill\s+)?rigs?\s+(?:are\s+)?(?:currently\s+)?operational",
        re.IGNORECASE,
    )
    for page_num, text in enumerate(page_texts, 1):
        m = pat_rigs.search(text)
        if m and "active_rigs" not in fields:
            raw = m.group(1)
            val = word_to_int(raw)
            if val is None:
                try:
                    val = int(raw)
                except ValueError:
                    logger.debug("Cannot parse rig count word: %s", raw)
                    continue
            fields["active_rigs"] = val
            page_hits[page_num] = page_hits.get(page_num, 0) + 1
            logger.debug("active_rigs=%s on page %d", val, page_num)

    # --- regional_rigs ---
    pat_regional = re.compile(
        r"(?:one|\d+)\s+(?:additional\s+)?(?:drill\s+)?rig\s+dedicated\s+to\s+regional",
        re.IGNORECASE,
    )
    for page_num, text in enumerate(page_texts, 1):
        if pat_regional.search(text) and "regional_rigs" not in fields:
            fields["regional_rigs"] = 1
            page_hits[page_num] = page_hits.get(page_num, 0) + 1
            logger.debug("regional_rigs=1 on page %d", page_num)

    # --- program_target_metres ---
    pat_target = re.compile(
        r"(\d{1,3}(?:,\d{3})*|\d+)\s*m\s+drill\s+program",
        re.IGNORECASE,
    )
    for page_num, text in enumerate(page_texts, 1):
        m = pat_target.search(text)
        if m and "program_target_metres" not in fields:
            fields["program_target_metres"] = int(_strip_commas(m.group(1)))
            page_hits[page_num] = page_hits.get(page_num, 0) + 1

    # --- program_end_target ---
    pat_end = re.compile(r"through\s+to\s+(Q[1-4]\s+\d{4})", re.IGNORECASE)
    for page_num, text in enumerate(page_texts, 1):
        m = pat_end.search(text)
        if m and "program_end_target" not in fields:
            fields["program_end_target"] = m.group(1)
            page_hits[page_num] = page_hits.get(page_num, 0) + 1

    if not fields:
        logger.warning("No project totals fields extracted")
        return None, warnings

    # Determine source page (page with most hits)
    best_page = max(page_hits, key=page_hits.get) if page_hits else 1

    # Sanity check: metres per hole
    holes = fields.get("total_drill_holes")
    metres = fields.get("total_metres")
    if holes and metres and holes > 0:
        avg = metres / holes
        if avg < 50 or avg > 5000:
            w = ExtractionWarning(
                code="IMPLAUSIBLE_METRES_PER_HOLE",
                message=f"Average metres/hole = {avg:.1f}, outside [50, 5000]",
                severity="medium",
                source_page=best_page,
            )
            warnings.append(w)
            logger.warning(w.message)

    snapshot = ProjectTotalsSnapshot(
        total_drill_holes=fields.get("total_drill_holes"),
        total_metres=fields.get("total_metres"),
        composites_gt_100_au=fields.get("composites_gt_100_au"),
        composites_50_to_100_au=fields.get("composites_50_to_100_au"),
        composites_gt_10_sb=fields.get("composites_gt_10_sb"),
        holes_pending=fields.get("holes_pending"),
        active_rigs=fields.get("active_rigs"),
        regional_rigs=fields.get("regional_rigs"),
        program_target_metres=fields.get("program_target_metres"),
        program_end_target=fields.get("program_end_target"),
        source_page=best_page,
    )
    return snapshot, warnings

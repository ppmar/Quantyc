"""
Table extraction for drill collars, composite intersections, and individual assays.

Uses pdfplumber table extraction with line-based strategy, falling back to text-based.
"""

from __future__ import annotations

import logging
import re
from typing import Any

import pdfplumber

from pipeline.parsers.schemas import (
    CompositeIntersection,
    DrillCollar,
    ExtractionWarning,
    IndividualAssay,
)

logger = logging.getLogger("parsers.exploration_results")

TABLE_SETTINGS_LINES = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 4,
    "join_tolerance": 4,
    "edge_min_length": 10,
    "min_words_vertical": 1,
    "min_words_horizontal": 1,
}

TABLE_SETTINGS_TEXT = {
    "vertical_strategy": "text",
    "horizontal_strategy": "lines",
    "snap_tolerance": 4,
    "join_tolerance": 4,
    "edge_min_length": 10,
    "min_words_vertical": 1,
    "min_words_horizontal": 1,
}

# Column header patterns for classification
_COLLAR_HEADERS = {"hole id", "depth", "prospect", "east", "north", "elevation", "dip", "azimuth"}
_INTERCEPT_HEADERS = {"hole", "from", "to", "interval", "au", "sb", "aueq"}
_JORC_HEADERS = {"criteria", "jorc", "commentary"}


def _parse_number(s: str) -> float | None:
    """Parse a numeric value from a table cell string."""
    if not s:
        return None
    s = s.strip().replace(",", "").replace(" ", "")
    # Handle multi-line cells
    s = s.split("\n")[0].strip()
    if s.lower() in ("", "-", "–", "n/a", "nil", "nsi", "pending", "na", "in progress", "plan"):
        return None
    m = re.search(r"-?\d+\.?\d*", s)
    if m:
        try:
            return float(m.group())
        except ValueError:
            logger.debug("Failed to parse number from: %r", s)
            return None
    return None


def _header_text(table: list[list[Any]], max_rows: int = 2) -> str:
    """Concatenate first few rows into lowercase text for classification."""
    parts = []
    for row in table[:max_rows]:
        if row:
            parts.extend(str(c).lower() for c in row if c)
    return " ".join(parts)


def _classify_table(table: list[list[Any]]) -> str:
    """Classify a table as 'collar', 'composite', 'individual', 'jorc', or 'unknown'."""
    if not table or len(table) < 2:
        return "unknown"

    ht = _header_text(table)

    # JORC table
    if "criteria" in ht and ("jorc" in ht or "commentary" in ht):
        return "jorc"

    # Collar table — has coordinates
    has_hole = "hole" in ht
    has_coords = any(w in ht for w in ["east", "north", "azimuth", "gda"])
    has_depth = any(w in ht for w in ["depth", "eoh"])
    if has_hole and has_coords and has_depth:
        return "collar"

    # Intercept table — has from/to/interval + grade
    has_from = "from" in ht
    has_interval = "interval" in ht or "width" in ht
    has_grade = any(w in ht for w in ["au g/t", "au\ng/t", "aueq", "sb %", "sb\n%"])
    if has_hole and (has_from or has_interval) and has_grade:
        # Distinguish composite vs individual by row count
        # We'll refine this after extraction
        return "intercept"

    return "unknown"


def _detect_collar_status(page_text: str) -> str:
    """Detect status section from surrounding page text."""
    text_lower = page_text.lower()
    if "this release" in text_lower:
        return "this_release"
    if "being processed" in text_lower or "analyzed" in text_lower:
        return "processing"
    if "in progress" in text_lower:
        return "in_progress"
    if "regional" in text_lower:
        return "regional"
    if "abandoned" in text_lower:
        return "abandoned"
    return "unknown"


def _find_hole_id_column(header: list[str]) -> int | None:
    """Find the column index containing the hole ID."""
    for i, cell in enumerate(header):
        if cell and re.search(r"hole\s*(?:id|number|no)", str(cell), re.I):
            return i
    return None


def _find_header_row(table: list[list[Any]]) -> tuple[list[str], int]:
    """Find the actual header row and return (header, first_data_row_index)."""
    for i, row in enumerate(table[:3]):
        if not row:
            continue
        row_text = " ".join(str(c) for c in row if c).lower()
        # Collar header
        if "hole" in row_text and ("depth" in row_text or "east" in row_text or "from" in row_text):
            return [str(c) if c else "" for c in row], i + 1
    # Default: first row is header
    return [str(c) if c else "" for c in (table[0] if table else [])], 1


def extract_collars(
    pdf: pdfplumber.PDF,
) -> tuple[list[DrillCollar], list[ExtractionWarning]]:
    """Extract drill collar data from all collar tables in the PDF."""
    collars: list[DrillCollar] = []
    warnings: list[ExtractionWarning] = []
    seen_ids: set[str] = set()

    # Collar tables appear in the first ~20 pages, never in the JORC appendix
    max_collar_page = min(len(pdf.pages), 20)
    for page_idx in range(max_collar_page):
        page = pdf.pages[page_idx]
        page_num = page_idx + 1

        # Fast skip: pages with few rects unlikely to have tables
        if len(page.rects) < 10:
            continue

        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if _classify_table(table) != "collar":
                continue

            logger.debug("Collar table found on page %d", page_num)
            header, data_start = _find_header_row(table)

            # Map columns
            col_map: dict[str, int] = {}
            for i, cell in enumerate(header):
                cl = cell.lower().replace("\n", " ")
                if re.search(r"hole\s*(?:id|number)", cl):
                    col_map["hole_id"] = i
                elif re.search(r"depth", cl):
                    col_map["depth_m"] = i
                elif re.search(r"prospect|target|zone", cl):
                    col_map["prospect"] = i
                elif re.search(r"east", cl):
                    col_map["easting"] = i
                elif re.search(r"north", cl):
                    col_map["northing"] = i
                elif re.search(r"elev", cl):
                    col_map["elevation_m"] = i
                elif re.search(r"dip", cl):
                    col_map["dip_deg"] = i
                elif re.search(r"azimuth|azi", cl):
                    col_map["azimuth_deg"] = i

            if "hole_id" not in col_map:
                continue

            # Detect status from section header (first row if it's a label)
            status = "unknown"
            if data_start > 0:
                first_row_text = " ".join(str(c) for c in table[0] if c).lower()
                if "this release" in first_row_text:
                    status = "this_release"
                elif "being processed" in first_row_text or "analyzed" in first_row_text:
                    status = "processing"
                elif "in progress" in first_row_text:
                    status = "in_progress"
                elif "regional" in first_row_text:
                    status = "regional"
                elif "abandoned" in first_row_text:
                    status = "abandoned"

            for row in table[data_start:]:
                if not row:
                    continue
                hid_idx = col_map["hole_id"]
                if hid_idx >= len(row) or not row[hid_idx]:
                    continue
                hole_id = str(row[hid_idx]).strip()
                if not hole_id or hole_id.lower() in ("hole id", "hole number", ""):
                    continue
                if hole_id in seen_ids:
                    continue
                seen_ids.add(hole_id)

                # Parse depth — may contain "In Progress\nplan 610 m"
                depth_val = None
                depth_idx = col_map.get("depth_m")
                if depth_idx is not None and depth_idx < len(row) and row[depth_idx]:
                    depth_str = str(row[depth_idx])
                    if "in progress" in depth_str.lower() or "plan" in depth_str.lower():
                        # Extract planned depth
                        m = re.search(r"(\d+(?:\.\d+)?)\s*m", depth_str)
                        depth_val = float(m.group(1)) if m else None
                        row_status = "in_progress"
                    else:
                        depth_val = _parse_number(depth_str)
                        row_status = status
                else:
                    row_status = status

                collar = DrillCollar(
                    hole_id=hole_id,
                    depth_m=depth_val,
                    prospect=str(row[col_map["prospect"]]).strip() if "prospect" in col_map and col_map["prospect"] < len(row) and row[col_map["prospect"]] else None,
                    easting=_parse_number(str(row[col_map["easting"]])) if "easting" in col_map and col_map["easting"] < len(row) and row[col_map["easting"]] else None,
                    northing=_parse_number(str(row[col_map["northing"]])) if "northing" in col_map and col_map["northing"] < len(row) and row[col_map["northing"]] else None,
                    elevation_m=_parse_number(str(row[col_map["elevation_m"]])) if "elevation_m" in col_map and col_map["elevation_m"] < len(row) and row[col_map["elevation_m"]] else None,
                    dip_deg=_parse_number(str(row[col_map["dip_deg"]])) if "dip_deg" in col_map and col_map["dip_deg"] < len(row) and row[col_map["dip_deg"]] else None,
                    azimuth_deg=_parse_number(str(row[col_map["azimuth_deg"]])) if "azimuth_deg" in col_map and col_map["azimuth_deg"] < len(row) and row[col_map["azimuth_deg"]] else None,
                    status=row_status,
                    source_page=page_num,
                )
                collars.append(collar)

    logger.info("Extracted %d drill collars", len(collars))
    return collars, warnings


def _extract_intercept_rows(
    table: list[list[Any]],
    page_num: int,
) -> list[dict[str, Any]]:
    """Extract rows from an intercept table (composite or individual)."""
    rows_out = []
    header, data_start = _find_header_row(table)

    # Map columns
    col_map: dict[str, int] = {}
    for i, cell in enumerate(header):
        cl = cell.lower().replace("\n", " ")
        if re.search(r"hole\s*(?:id|number|no)", cl):
            col_map["hole_id"] = i
        elif re.search(r"from\s*\(?m\)?", cl):
            col_map["from_m"] = i
        elif re.search(r"to\s*\(?m\)?", cl):
            col_map["to_m"] = i
        elif re.search(r"interval|width", cl):
            col_map["interval_m"] = i
        elif re.search(r"au\s*g/?t|gold\s*g/?t", cl) and not re.search(r"aueq|au\s*eq", cl):
            col_map["au_gpt"] = i
        elif re.search(r"sb\s*%|antimony", cl):
            col_map["sb_pct"] = i
        elif re.search(r"aueq|au\s*eq", cl):
            col_map["aueq_gpt"] = i

    if "from_m" not in col_map and "interval_m" not in col_map:
        return []

    current_hole = None
    for row in table[data_start:]:
        if not row:
            continue

        # Determine hole ID and whether this is an "Including" sub-row
        is_sub = False
        hole_id = None

        # Check all cells for hole ID or "Including"
        for cell in row:
            if not cell:
                continue
            cell_str = str(cell).strip()
            if cell_str.lower().startswith("includ"):
                is_sub = True
                hole_id = current_hole
                break
            if re.match(r"[A-Z]{2,6}\d{2,5}", cell_str):
                hole_id = cell_str
                current_hole = cell_str
                break

        if hole_id is None:
            hole_id = current_hole
        if hole_id is None:
            continue

        # The table may have merged cells with None padding.
        # Find actual numeric columns by position in col_map.
        # For the SXG multi-column layout, numeric data may be offset.
        # We need to handle both compact (7-col) and expanded (21-col) layouts.
        num_cols = len(row)

        def _get_val(key: str) -> float | None:
            idx = col_map.get(key)
            if idx is None:
                return None
            # For expanded tables (e.g., 21 cols where each logical col spans 3),
            # try the mapped index first, then scan nearby cells.
            # Check both the header position and adjacent offsets, because
            # some rows place data at the header column and others offset by 1.
            for offset in range(0, 3):
                ci = idx + offset
                if ci < num_cols and row[ci]:
                    val = _parse_number(str(row[ci]))
                    if val is not None:
                        return val
            return None

        row_data = {
            "hole_id": hole_id,
            "from_m": _get_val("from_m"),
            "to_m": _get_val("to_m"),
            "interval_m": _get_val("interval_m"),
            "au_gpt": _get_val("au_gpt"),
            "sb_pct": _get_val("sb_pct"),
            "aueq_gpt": _get_val("aueq_gpt"),
            "is_sub": is_sub,
            "page_num": page_num,
        }

        # Must have at least interval or from/to, and at least one grade
        has_interval = row_data["from_m"] is not None or row_data["interval_m"] is not None
        has_grade = any(row_data.get(g) is not None for g in ("au_gpt", "sb_pct", "aueq_gpt"))
        if has_interval and has_grade:
            rows_out.append(row_data)

    return rows_out


def extract_intercepts(
    pdf: pdfplumber.PDF,
) -> tuple[list[CompositeIntersection], list[IndividualAssay], list[ExtractionWarning]]:
    """
    Extract composite intersections and individual assays from all pages.
    Returns (composites, individuals, warnings).
    """
    all_rows: list[dict[str, Any]] = []
    warnings: list[ExtractionWarning] = []
    table_page_map: list[tuple[int, int]] = []  # (start_idx_in_all_rows, page_num)

    for page_idx, page in enumerate(pdf.pages):
        page_num = page_idx + 1

        # Fast skip: pages with few rects unlikely to have tables
        if len(page.rects) < 10:
            continue

        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            cls = _classify_table(table)
            if cls == "jorc":
                # Hit JORC appendix — stop scanning
                logger.debug("Hit JORC table on page %d, stopping intercept scan", page_num)
                break
            if cls != "intercept":
                continue

            logger.debug("Intercept table on page %d (%d rows)", page_num, len(table))
            start_idx = len(all_rows)
            rows = _extract_intercept_rows(table, page_num)
            all_rows.extend(rows)
            if rows:
                table_page_map.append((start_idx, page_num))
        else:
            continue
        break  # break outer loop when JORC table found

    if not all_rows:
        return [], [], warnings

    # Classify into composite vs individual.
    # Composite tables have "Including" sub-rows and fewer rows.
    # Individual assay tables have many rows with no sub-rows.
    # Strategy: find the boundary — composite tables come first, individuals after.
    has_subs = any(r["is_sub"] for r in all_rows)

    # If the first table has subs, split at the first table without subs and with many rows
    composite_rows: list[dict[str, Any]] = []
    individual_rows: list[dict[str, Any]] = []

    if has_subs:
        # Find where individual assays begin
        # Individual assay tables typically start on a new page with a fresh header
        # and have no "Including" rows
        in_individual = False
        composite_page_set: set[int] = set()

        # First pass: identify composite pages (pages with Including rows)
        for r in all_rows:
            if r["is_sub"]:
                composite_page_set.add(r["page_num"])

        # A page is composite if it has subs OR is the same page as sub-rows
        for r in all_rows:
            if r["page_num"] in composite_page_set:
                composite_rows.append(r)
            else:
                # Check: if this page comes after all composite pages and has many rows, it's individual
                if not composite_page_set or r["page_num"] > max(composite_page_set):
                    individual_rows.append(r)
                else:
                    # Pages before composites without subs — still composite (main rows)
                    composite_rows.append(r)
    else:
        # No sub-rows at all — likely all individual assays, or the composite table
        # had no "Including" markers. Heuristic: if < 60 rows total, treat as composite.
        if len(all_rows) < 60:
            composite_rows = all_rows
        else:
            individual_rows = all_rows

    # Deduplicate composite rows
    seen_keys: set[tuple[str, float | None, float | None, float | None, float | None]] = set()
    deduped_composites: list[dict[str, Any]] = []
    dup_count = 0
    for r in composite_rows:
        key = (r["hole_id"], r["from_m"], r["to_m"], r["au_gpt"], r["sb_pct"])
        if key in seen_keys:
            dup_count += 1
            continue
        seen_keys.add(key)
        deduped_composites.append(r)

    if dup_count > 0:
        warnings.append(ExtractionWarning(
            code="DUPLICATE_TABLE_ROWS_DROPPED",
            message=f"Dropped {dup_count} duplicate composite intersection rows",
            severity="low",
            count=dup_count,
        ))
        logger.warning("Dropped %d duplicate composite rows", dup_count)

    # Build CompositeIntersection objects
    composites: list[CompositeIntersection] = []
    last_main_idx = -1
    for i, r in enumerate(deduped_composites):
        if r["is_sub"]:
            parent_idx = last_main_idx if last_main_idx >= 0 else None
        else:
            parent_idx = None
            last_main_idx = i

        composites.append(CompositeIntersection(
            hole_id=r["hole_id"],
            from_m=r["from_m"],
            to_m=r["to_m"],
            interval_m=r["interval_m"],
            au_gpt=r["au_gpt"],
            sb_pct=r["sb_pct"],
            aueq_gpt=r["aueq_gpt"],
            is_subinterval=r["is_sub"],
            parent_row_index=parent_idx,
            source_page=r["page_num"],
        ))

    # Build IndividualAssay objects — also deduplicate
    seen_ind: set[tuple[str, float | None, float | None, float | None, float | None]] = set()
    individuals: list[IndividualAssay] = []
    ind_dup = 0
    for r in individual_rows:
        key = (r["hole_id"], r["from_m"], r["to_m"], r["au_gpt"], r["sb_pct"])
        if key in seen_ind:
            ind_dup += 1
            continue
        seen_ind.add(key)
        individuals.append(IndividualAssay(
            hole_id=r["hole_id"],
            from_m=r["from_m"],
            to_m=r["to_m"],
            interval_m=r["interval_m"],
            au_gpt=r["au_gpt"],
            sb_pct=r["sb_pct"],
            aueq_gpt=r["aueq_gpt"],
            source_page=r["page_num"],
        ))

    if ind_dup > 0:
        warnings.append(ExtractionWarning(
            code="DUPLICATE_INDIVIDUAL_ROWS_DROPPED",
            message=f"Dropped {ind_dup} duplicate individual assay rows",
            severity="low",
        ))

    logger.info(
        "Extracted %d composite intersections, %d individual assays",
        len(composites), len(individuals),
    )
    return composites, individuals, warnings


def extract_all_tables(
    pdf: pdfplumber.PDF,
) -> tuple[list[DrillCollar], list[CompositeIntersection], list[IndividualAssay], list[ExtractionWarning]]:
    """
    Single-pass extraction of all table data: collars, composites, and individual assays.
    Avoids re-scanning pages by processing all table types in one loop.
    """
    collars: list[DrillCollar] = []
    collar_warnings: list[ExtractionWarning] = []
    seen_collar_ids: set[str] = set()

    all_intercept_rows: list[dict[str, Any]] = []
    intercept_warnings: list[ExtractionWarning] = []

    for page_idx, page in enumerate(pdf.pages):
        page_num = page_idx + 1

        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            cls = _classify_table(table)

            if cls == "jorc":
                logger.debug("Hit JORC table on page %d, stopping scan", page_num)
                break

            if cls == "collar":
                _process_collar_table(table, page_num, collars, seen_collar_ids)

            elif cls == "intercept":
                rows = _extract_intercept_rows(table, page_num)
                all_intercept_rows.extend(rows)
        else:
            continue
        break  # break outer loop when JORC table found

    logger.info("Extracted %d drill collars", len(collars))

    # Process intercept rows into composites and individuals
    composites, individuals, int_warnings = _build_intercept_objects(all_intercept_rows)
    intercept_warnings.extend(int_warnings)

    all_warnings = collar_warnings + intercept_warnings
    return collars, composites, individuals, all_warnings


def _process_collar_table(
    table: list[list[Any]],
    page_num: int,
    collars: list[DrillCollar],
    seen_ids: set[str],
) -> None:
    """Process a single collar table and append results."""
    header, data_start = _find_header_row(table)

    col_map: dict[str, int] = {}
    for i, cell in enumerate(header):
        cl = cell.lower().replace("\n", " ")
        if re.search(r"hole\s*(?:id|number)", cl):
            col_map["hole_id"] = i
        elif re.search(r"depth", cl):
            col_map["depth_m"] = i
        elif re.search(r"prospect|target|zone", cl):
            col_map["prospect"] = i
        elif re.search(r"east", cl):
            col_map["easting"] = i
        elif re.search(r"north", cl):
            col_map["northing"] = i
        elif re.search(r"elev", cl):
            col_map["elevation_m"] = i
        elif re.search(r"dip", cl):
            col_map["dip_deg"] = i
        elif re.search(r"azimuth|azi", cl):
            col_map["azimuth_deg"] = i

    if "hole_id" not in col_map:
        return

    # Detect status from section header
    status = "unknown"
    if data_start > 0:
        first_row_text = " ".join(str(c) for c in table[0] if c).lower()
        if "this release" in first_row_text:
            status = "this_release"
        elif "being processed" in first_row_text or "analyzed" in first_row_text:
            status = "processing"
        elif "in progress" in first_row_text:
            status = "in_progress"
        elif "regional" in first_row_text:
            status = "regional"
        elif "abandoned" in first_row_text:
            status = "abandoned"

    for row in table[data_start:]:
        if not row:
            continue
        hid_idx = col_map["hole_id"]
        if hid_idx >= len(row) or not row[hid_idx]:
            continue
        hole_id = str(row[hid_idx]).strip()
        if not hole_id or hole_id.lower() in ("hole id", "hole number", ""):
            continue
        if hole_id in seen_ids:
            continue
        seen_ids.add(hole_id)

        depth_val = None
        depth_idx = col_map.get("depth_m")
        row_status = status
        if depth_idx is not None and depth_idx < len(row) and row[depth_idx]:
            depth_str = str(row[depth_idx])
            if "in progress" in depth_str.lower() or "plan" in depth_str.lower():
                m = re.search(r"(\d+(?:\.\d+)?)\s*m", depth_str)
                depth_val = float(m.group(1)) if m else None
                row_status = "in_progress"
            else:
                depth_val = _parse_number(depth_str)

        def _get_collar_val(field: str) -> float | None:
            idx = col_map.get(field)
            if idx is not None and idx < len(row) and row[idx]:
                return _parse_number(str(row[idx]))
            return None

        collars.append(DrillCollar(
            hole_id=hole_id,
            depth_m=depth_val,
            prospect=str(row[col_map["prospect"]]).strip() if "prospect" in col_map and col_map["prospect"] < len(row) and row[col_map["prospect"]] else None,
            easting=_get_collar_val("easting"),
            northing=_get_collar_val("northing"),
            elevation_m=_get_collar_val("elevation_m"),
            dip_deg=_get_collar_val("dip_deg"),
            azimuth_deg=_get_collar_val("azimuth_deg"),
            status=row_status,
            source_page=page_num,
        ))


def _build_intercept_objects(
    all_rows: list[dict[str, Any]],
) -> tuple[list[CompositeIntersection], list[IndividualAssay], list[ExtractionWarning]]:
    """Classify and build intercept objects from raw rows."""
    warnings: list[ExtractionWarning] = []

    if not all_rows:
        return [], [], warnings

    has_subs = any(r["is_sub"] for r in all_rows)

    composite_rows: list[dict[str, Any]] = []
    individual_rows: list[dict[str, Any]] = []

    if has_subs:
        composite_page_set: set[int] = set()
        for r in all_rows:
            if r["is_sub"]:
                composite_page_set.add(r["page_num"])

        for r in all_rows:
            if r["page_num"] in composite_page_set:
                composite_rows.append(r)
            elif not composite_page_set or r["page_num"] > max(composite_page_set):
                individual_rows.append(r)
            else:
                composite_rows.append(r)
    else:
        if len(all_rows) < 60:
            composite_rows = all_rows
        else:
            individual_rows = all_rows

    # Deduplicate composite rows
    seen_keys: set[tuple[str, float | None, float | None, float | None, float | None]] = set()
    deduped: list[dict[str, Any]] = []
    dup_count = 0
    for r in composite_rows:
        key = (r["hole_id"], r["from_m"], r["to_m"], r["au_gpt"], r["sb_pct"])
        if key in seen_keys:
            dup_count += 1
            continue
        seen_keys.add(key)
        deduped.append(r)

    if dup_count > 0:
        warnings.append(ExtractionWarning(
            code="DUPLICATE_TABLE_ROWS_DROPPED",
            message=f"Dropped {dup_count} duplicate composite intersection rows",
            severity="low",
            count=dup_count,
        ))

    composites: list[CompositeIntersection] = []
    last_main_idx = -1
    for i, r in enumerate(deduped):
        if r["is_sub"]:
            parent_idx = last_main_idx if last_main_idx >= 0 else None
        else:
            parent_idx = None
            last_main_idx = i
        composites.append(CompositeIntersection(
            hole_id=r["hole_id"],
            from_m=r["from_m"],
            to_m=r["to_m"],
            interval_m=r["interval_m"],
            au_gpt=r["au_gpt"],
            sb_pct=r["sb_pct"],
            aueq_gpt=r["aueq_gpt"],
            is_subinterval=r["is_sub"],
            parent_row_index=parent_idx,
            source_page=r["page_num"],
        ))

    # Deduplicate individual rows
    seen_ind: set[tuple[str, float | None, float | None, float | None, float | None]] = set()
    individuals: list[IndividualAssay] = []
    for r in individual_rows:
        key = (r["hole_id"], r["from_m"], r["to_m"], r["au_gpt"], r["sb_pct"])
        if key in seen_ind:
            continue
        seen_ind.add(key)
        individuals.append(IndividualAssay(
            hole_id=r["hole_id"],
            from_m=r["from_m"],
            to_m=r["to_m"],
            interval_m=r["interval_m"],
            au_gpt=r["au_gpt"],
            sb_pct=r["sb_pct"],
            aueq_gpt=r["aueq_gpt"],
            source_page=r["page_num"],
        ))

    logger.info(
        "Built %d composite intersections, %d individual assays",
        len(composites), len(individuals),
    )
    return composites, individuals, warnings

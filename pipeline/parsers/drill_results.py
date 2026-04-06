"""
Drill Results Parser

Extracts assay intercepts from drill results announcements.
These are the most common announcement type for concept/discovery-stage juniors.

Drill results are reported as intercept tables with columns like:
    Hole ID | From (m) | To (m) | Interval (m) | Au g/t | Sb % | AuEq g/t

Strategy:
1. Find pages containing intercept tables using keyword scoring
2. Extract tables with pdfplumber
3. Parse columns by matching header patterns
4. Fall back to regex on page text for inline intercepts (e.g. "4.5m @ 12.3 g/t")

Usage:
    python -m pipeline.parsers.drill_results --doc-id <id>
    python -m pipeline.parsers.drill_results --all
"""

import argparse
import logging
import re

import pdfplumber

from db import get_connection, init_db

logger = logging.getLogger(__name__)

# --- Header pattern matching ---
# Maps normalized column names to regex patterns that match table headers
COLUMN_PATTERNS = {
    "hole_id":    re.compile(r"hole\s*(?:id|number|no\.?)|drill\s*hole", re.I),
    "from_m":     re.compile(r"from\s*\(?m\)?|start\s*\(?m\)?", re.I),
    "to_m":       re.compile(r"to\s*\(?m\)?|end\s*\(?m\)?", re.I),
    "interval_m": re.compile(r"interval|width|length|int\.?\s*\(?m\)?", re.I),
    "au_gt":      re.compile(r"au\s*(?:g/?t|ppm)|gold\s*(?:g/?t|ppm)", re.I),
    "au_eq_gt":   re.compile(r"au\s*eq|aueq|gold\s*equiv", re.I),
    "sb_pct":     re.compile(r"sb\s*%|antimony\s*%", re.I),
    "cu_pct":     re.compile(r"cu\s*%|copper\s*%", re.I),
    "ag_gt":      re.compile(r"ag\s*(?:g/?t|ppm)|silver\s*(?:g/?t|ppm)", re.I),
    "prospect":   re.compile(r"prospect|target|zone|area", re.I),
}

# For collar tables
COLLAR_PATTERNS = {
    "hole_id":   re.compile(r"hole\s*(?:id|number|no\.?)", re.I),
    "depth_m":   re.compile(r"depth\s*\(?m\)?|total\s*depth|eoh", re.I),
    "prospect":  re.compile(r"prospect|target|zone|area", re.I),
    "easting":   re.compile(r"east|easting", re.I),
    "northing":  re.compile(r"north|northing", re.I),
    "elevation": re.compile(r"elev|rl|height", re.I),
    "dip":       re.compile(r"dip|inclination", re.I),
    "azimuth":   re.compile(r"azimuth|azi|bearing", re.I),
}

# Regex for inline intercepts in narrative text
# Matches patterns like: "4.5 m @ 12.3 g/t Au" or "17.3m @ 22.9 g/t AuEq (15.3 g/t Au, 3.2% Sb)"
INLINE_INTERCEPT = re.compile(
    r"(\d+\.?\d*)\s*m\s*@\s*(\d+\.?\d*)\s*g/t\s*(Au(?:Eq)?)",
    re.I,
)

# Keywords to find intercept table pages
DRILL_PAGE_KEYWORDS = [
    "hole id", "hole number", "from (m)", "to (m)", "interval",
    "au g/t", "aueq", "au eq", "g/t au", "assay", "intercept",
    "drill hole", "significant intercept", "sb %",
]

# Keywords for collar table pages
COLLAR_PAGE_KEYWORDS = [
    "hole id", "depth", "easting", "northing", "azimuth", "dip",
    "collar", "coordinate", "gda94",
]


def _parse_number(text: str) -> float | None:
    """Parse a number from cell text."""
    if not text:
        return None
    text = text.strip().replace(",", "").replace(" ", "")
    if text in ("-", "–", "", "n/a", "nsi", "pending"):
        return None
    match = re.search(r"-?\d+\.?\d*", text)
    return float(match.group()) if match else None


def _score_page(text: str, keywords: list[str]) -> int:
    """Score a page by keyword matches."""
    text_lower = text.lower()
    return sum(1 for kw in keywords if kw in text_lower)


def _match_columns(header_row: list[str], patterns: dict) -> dict[str, int]:
    """
    Match table header cells to known column names.
    Returns {column_name: column_index}.
    """
    mapping = {}
    for col_idx, cell in enumerate(header_row):
        if not cell:
            continue
        cell_clean = str(cell).strip()
        for col_name, pattern in patterns.items():
            if col_name not in mapping and pattern.search(cell_clean):
                mapping[col_name] = col_idx
                break
    return mapping


def _is_intercept_table(table: list[list]) -> bool:
    """Check if a table looks like a drill intercept table."""
    if not table or len(table) < 2:
        return False
    header_text = " ".join(str(c) for row in table[:2] for c in row if c).lower()
    score = 0
    if any(w in header_text for w in ["hole", "drill"]):
        score += 1
    if any(w in header_text for w in ["from", "to", "interval", "width"]):
        score += 1
    if any(w in header_text for w in ["g/t", "au", "grade", "aueq"]):
        score += 1
    return score >= 2


def _is_collar_table(table: list[list]) -> bool:
    """Check if a table looks like a drill collar table."""
    if not table or len(table) < 2:
        return False
    header_text = " ".join(str(c) for row in table[:2] for c in row if c).lower()
    has_hole = any(w in header_text for w in ["hole"])
    has_coords = any(w in header_text for w in ["east", "north", "azimuth", "dip", "gda"])
    return has_hole and has_coords


def extract_collars(pdf_path: str) -> dict[str, dict]:
    """
    Extract drill collar data (coordinates, dip, azimuth) from collar tables.
    Returns {hole_id: {prospect, easting, northing, elevation, dip, azimuth}}.
    """
    collars = {}
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        logger.error("Failed to open PDF %s: %s", pdf_path, e)
        return collars

    for page in pdf.pages:
        tables = page.extract_tables()
        if not tables:
            continue
        for table in tables:
            if not _is_collar_table(table):
                continue

            # Find header row (could be row 0 or row 1)
            header_idx = 0
            for i in range(min(2, len(table))):
                row_text = " ".join(str(c) for c in table[i] if c).lower()
                if "hole" in row_text and ("east" in row_text or "dip" in row_text):
                    header_idx = i
                    break

            col_map = _match_columns(table[header_idx], COLLAR_PATTERNS)
            if "hole_id" not in col_map:
                continue

            for row in table[header_idx + 1:]:
                if not row:
                    continue
                hole_id_idx = col_map["hole_id"]
                if hole_id_idx >= len(row) or not row[hole_id_idx]:
                    continue

                hole_id = str(row[hole_id_idx]).strip()
                if not hole_id or hole_id.lower() in ("hole id", "hole number", ""):
                    continue

                collar = {}
                for field in ("prospect", "easting", "northing", "elevation", "dip", "azimuth"):
                    idx = col_map.get(field)
                    if idx is not None and idx < len(row) and row[idx]:
                        if field == "prospect":
                            collar[field] = str(row[idx]).strip()
                        else:
                            collar[field] = _parse_number(str(row[idx]))

                collars[hole_id] = collar

    pdf.close()
    return collars


def extract_intercepts_from_tables(pdf_path: str) -> list[dict]:
    """
    Extract drill intercepts from tables in the PDF.
    Returns list of intercept dicts.
    """
    results = []
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        logger.error("Failed to open PDF %s: %s", pdf_path, e)
        return results

    for page_num, page in enumerate(pdf.pages):
        # Score page — skip pages unlikely to have intercept data
        text = page.extract_text() or ""
        if _score_page(text, DRILL_PAGE_KEYWORDS) < 2:
            continue

        tables = page.extract_tables()
        if not tables:
            continue

        for table in tables:
            if not _is_intercept_table(table):
                continue

            logger.info("Found intercept table on page %d of %s", page_num + 1, pdf_path)

            # Find header row
            header_idx = 0
            for i in range(min(3, len(table))):
                row_text = " ".join(str(c) for c in table[i] if c).lower()
                if any(w in row_text for w in ["from", "to", "interval", "g/t", "au"]):
                    header_idx = i
                    break

            col_map = _match_columns(table[header_idx], COLUMN_PATTERNS)

            if "from_m" not in col_map and "interval_m" not in col_map:
                continue

            current_hole = None
            for row in table[header_idx + 1:]:
                if not row:
                    continue

                # Get hole ID — it may be in a column or carried forward
                hole_id = None
                hole_idx = col_map.get("hole_id")
                if hole_idx is not None and hole_idx < len(row) and row[hole_idx]:
                    cell = str(row[hole_idx]).strip()
                    if cell.lower().startswith("includ"):
                        is_including = True
                        hole_id = current_hole
                    elif cell and cell.lower() not in ("", "hole id", "hole number"):
                        hole_id = cell
                        current_hole = cell
                        is_including = False
                    else:
                        hole_id = current_hole
                        is_including = False
                else:
                    hole_id = current_hole
                    # Check if any cell says "including"
                    is_including = any(
                        "includ" in str(c).lower() for c in row if c
                    )

                if not hole_id:
                    continue

                intercept = {
                    "hole_id": hole_id,
                    "is_including": is_including,
                }

                # Extract numeric fields
                for field in ("from_m", "to_m", "interval_m", "au_gt", "au_eq_gt", "sb_pct", "cu_pct", "ag_gt"):
                    idx = col_map.get(field)
                    if idx is not None and idx < len(row) and row[idx]:
                        intercept[field] = _parse_number(str(row[idx]))
                    else:
                        intercept[field] = None

                # Extract prospect if in table
                prospect_idx = col_map.get("prospect")
                if prospect_idx is not None and prospect_idx < len(row) and row[prospect_idx]:
                    intercept["prospect"] = str(row[prospect_idx]).strip()

                # Only include rows with at least from/interval and a grade
                has_interval = intercept.get("from_m") is not None or intercept.get("interval_m") is not None
                has_grade = any(
                    intercept.get(g) is not None
                    for g in ("au_gt", "au_eq_gt", "sb_pct", "cu_pct", "ag_gt")
                )

                if has_interval and has_grade:
                    results.append(intercept)

    pdf.close()
    return results


def extract_intercepts_from_text(pdf_path: str) -> list[dict]:
    """
    Fallback: extract inline intercepts from narrative text using regex.
    e.g. "17.3 m @ 22.9 g/t AuEq (15.3 g/t Au, 3.2% Sb) from 251.1 m"
    """
    results = []
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        logger.error("Failed to open PDF %s: %s", pdf_path, e)
        return results

    # More complete pattern for narrative intercepts
    # "X.X m @ Y.Y g/t Au(Eq) from Z.Z m"
    pattern = re.compile(
        r"(\d+\.?\d*)\s*m\s*@\s*(\d+\.?\d*)\s*g/t\s*(Au(?:Eq)?)"
        r"(?:\s*\((\d+\.?\d*)\s*g/t\s*Au,?\s*(\d+\.?\d*)\s*%?\s*Sb\))?"
        r"(?:\s*from\s*(\d+\.?\d*)\s*m)?",
        re.I,
    )

    # Also match "including X.X m @ Y.Y g/t"
    including_pattern = re.compile(
        r"includ(?:ing|es?)\s+(\d+\.?\d*)\s*m\s*@\s*(\d+\.?\d*)\s*g/t\s*(Au(?:Eq)?)"
        r"(?:\s*from\s*(\d+\.?\d*)\s*m)?",
        re.I,
    )

    # Hole ID pattern — e.g. "SDDSC200" typically appears before intercepts
    hole_pattern = re.compile(r"\b([A-Z]{2,6}[-]?\d{2,5}[A-Z]?\d?)\b")

    for page in pdf.pages[:10]:  # Only check first 10 pages for narrative
        text = page.extract_text() or ""
        if _score_page(text, ["m @", "g/t"]) < 1:
            continue

        lines = text.split("\n")
        current_hole = None

        for line in lines:
            # Try to find hole ID in this line
            hole_match = hole_pattern.search(line)
            if hole_match:
                candidate = hole_match.group(1)
                # Verify it looks like a drill hole (has letters + numbers)
                if re.match(r"[A-Z]+\d+", candidate) and len(candidate) >= 5:
                    current_hole = candidate

            # Match main intercepts
            for m in pattern.finditer(line):
                interval = float(m.group(1))
                grade = float(m.group(2))
                grade_type = m.group(3).lower()

                intercept = {
                    "hole_id": current_hole,
                    "interval_m": interval,
                    "is_including": False,
                }

                if "eq" in grade_type:
                    intercept["au_eq_gt"] = grade
                else:
                    intercept["au_gt"] = grade

                # Optional Au and Sb breakdown
                if m.group(4):
                    intercept["au_gt"] = float(m.group(4))
                if m.group(5):
                    intercept["sb_pct"] = float(m.group(5))

                # From depth
                if m.group(6):
                    intercept["from_m"] = float(m.group(6))
                    intercept["to_m"] = intercept["from_m"] + interval

                results.append(intercept)

            # Match "including" sub-intervals
            for m in including_pattern.finditer(line):
                intercept = {
                    "hole_id": current_hole,
                    "interval_m": float(m.group(1)),
                    "is_including": True,
                }
                grade = float(m.group(2))
                grade_type = m.group(3).lower()
                if "eq" in grade_type:
                    intercept["au_eq_gt"] = grade
                else:
                    intercept["au_gt"] = grade
                if m.group(4):
                    intercept["from_m"] = float(m.group(4))

                results.append(intercept)

    pdf.close()
    return results


def parse_drill_results(doc_id: str) -> int:
    """
    Parse a drill results document.
    Extracts intercepts and collar data, writes to drill_results table.
    Returns number of intercepts loaded.
    """
    conn = get_connection()
    doc = conn.execute(
        "SELECT local_path, company_ticker, announcement_date FROM documents WHERE id = ?",
        (doc_id,),
    ).fetchone()

    if not doc:
        logger.error("Document %s not found", doc_id)
        conn.close()
        return 0

    local_path = doc["local_path"]
    ticker = doc["company_ticker"]
    ann_date = doc["announcement_date"]

    if not local_path:
        logger.error("No local_path for document %s", doc_id)
        conn.close()
        return 0

    logger.info("Parsing drill results: %s", local_path)

    # Extract collar data first
    collars = extract_collars(local_path)
    logger.info("Found %d drill collars", len(collars))

    # Try table extraction for intercepts
    intercepts = extract_intercepts_from_tables(local_path)
    method = "rule_based"
    confidence = "high"

    if not intercepts:
        logger.info("No intercept tables found, trying text extraction for %s", doc_id)
        intercepts = extract_intercepts_from_text(local_path)
        method = "regex"
        confidence = "medium"

    if not intercepts:
        logger.warning("No drill intercepts extracted from %s", doc_id)
        conn.execute(
            "UPDATE documents SET parse_status = 'failed' WHERE id = ?",
            (doc_id,),
        )
        conn.commit()
        conn.close()
        return 0

    # Ensure project exists
    project_id = f"{ticker.lower()}_main"
    conn.execute(
        """INSERT OR IGNORE INTO projects
           (id, ticker, project_name, stage, ownership_pct, is_primary, updated_at)
           VALUES (?, ?, 'Main Project', 'concept', 100.0, 1, CURRENT_TIMESTAMP)""",
        (project_id, ticker),
    )

    # Ensure company exists
    conn.execute(
        "INSERT OR IGNORE INTO companies (ticker, updated_at) VALUES (?, CURRENT_TIMESTAMP)",
        (ticker,),
    )

    # Load intercepts
    loaded = 0
    for intercept in intercepts:
        hole_id = intercept.get("hole_id")
        if not hole_id:
            continue

        # Merge collar data
        collar = collars.get(hole_id, {})

        conn.execute(
            """INSERT INTO drill_results
               (project_id, hole_id, prospect, from_m, to_m, interval_m,
                au_gt, au_eq_gt, sb_pct, cu_pct, ag_gt,
                is_including, azimuth, dip, easting, northing, elevation,
                announcement_date, source_doc_id, extraction_method, confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                project_id,
                hole_id,
                intercept.get("prospect") or collar.get("prospect"),
                intercept.get("from_m"),
                intercept.get("to_m"),
                intercept.get("interval_m"),
                intercept.get("au_gt"),
                intercept.get("au_eq_gt"),
                intercept.get("sb_pct"),
                intercept.get("cu_pct"),
                intercept.get("ag_gt"),
                1 if intercept.get("is_including") else 0,
                collar.get("azimuth"),
                collar.get("dip"),
                collar.get("easting"),
                collar.get("northing"),
                collar.get("elevation"),
                ann_date,
                doc_id,
                method,
                confidence,
            ),
        )
        loaded += 1

    conn.execute(
        "UPDATE documents SET parse_status = 'done' WHERE id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()

    logger.info("Loaded %d drill intercepts from %s (method=%s)", loaded, doc_id, method)
    return loaded


def parse_all_pending() -> int:
    """Parse all pending drill results documents."""
    conn = get_connection()
    rows = conn.execute(
        """SELECT id FROM documents
           WHERE doc_type = 'drill_results' AND parse_status = 'pending'"""
    ).fetchall()
    conn.close()

    parsed = 0
    for row in rows:
        n = parse_drill_results(row["id"])
        if n > 0:
            parsed += 1

    logger.info("Parsed %d / %d drill results documents", parsed, len(rows))
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Parse drill results documents")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--doc-id", type=str, help="Parse a specific document by ID")
    group.add_argument("--ticker", type=str, help="Parse all pending for a ticker")
    group.add_argument("--all", action="store_true", help="Parse all pending")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.doc_id:
        n = parse_drill_results(args.doc_id)
        print(f"Loaded {n} intercepts")
    elif args.ticker:
        conn = get_connection()
        rows = conn.execute(
            """SELECT id FROM documents
               WHERE company_ticker = ? AND doc_type = 'drill_results' AND parse_status = 'pending'""",
            (args.ticker.upper(),),
        ).fetchall()
        conn.close()
        for row in rows:
            n = parse_drill_results(row["id"])
            print(f"  {row['id']}: {n} intercepts")
    else:
        n = parse_all_pending()
        print(f"Parsed {n} drill results documents")


if __name__ == "__main__":
    main()

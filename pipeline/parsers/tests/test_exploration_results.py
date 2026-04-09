"""
Tests for the exploration_results parser.

Validated against Southern Cross Gold (ASX:SX2) drill-results releases.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path

import pytest

from pipeline.parsers.exploration_results import (
    WrongDocumentTypeError,
    parse,
)
from pipeline.parsers.schemas import ExplorationResultsPayload

FIXTURES = Path(__file__).parent / "fixtures"
SXG_FEB = FIXTURES / "sxg_2026_02_18.pdf"
SXG_MAR = FIXTURES / "sxg_2026_03_16.pdf"
SXG_SHELF = FIXTURES / "sxg_2026_04_08_shelf.pdf"
SXG_GDXJ = FIXTURES / "sxg_2026_03_20_gdxj.pdf"

has_feb = SXG_FEB.exists()
has_mar = SXG_MAR.exists()
has_shelf = SXG_SHELF.exists()
has_gdxj = SXG_GDXJ.exists()


# --- Cached fixtures to avoid re-parsing the PDF for every test ---

@pytest.fixture(scope="module")
def payload_mar():
    if not has_mar:
        pytest.skip("March fixture not available")
    return parse(SXG_MAR, ticker="SX2", doc_id="test_mar")


@pytest.fixture(scope="module")
def payload_feb():
    if not has_feb:
        pytest.skip("Feb fixture not available")
    return parse(SXG_FEB, ticker="SX2", doc_id="test_feb")


# --- Profile detection tests ---


def test_detection_accepts_sxg_drill_release_march(payload_mar):
    assert payload_mar is not None
    assert payload_mar.ticker == "SX2"


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_detection_accepts_sxg_drill_release_feb(payload_feb):
    assert payload_feb is not None


@pytest.mark.skipif(not has_shelf, reason="Shelf prospectus fixture not available")
def test_detection_rejects_shelf_prospectus():
    with pytest.raises(WrongDocumentTypeError):
        parse(SXG_SHELF, ticker="SX2", doc_id="test_shelf")


@pytest.mark.skipif(not has_gdxj, reason="GDXJ fixture not available")
def test_detection_rejects_index_inclusion():
    with pytest.raises(WrongDocumentTypeError):
        parse(SXG_GDXJ, ticker="SX2", doc_id="test_gdxj")


# --- Release date ---


def test_release_date_extraction_march(payload_mar):
    assert payload_mar.release_date == date(2026, 3, 16)


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_release_date_extraction_feb(payload_feb):
    assert payload_feb.release_date == date(2026, 2, 18)


# --- Headline intercept ---


def test_headline_intercept_2026_03_16(payload_mar):
    h = payload_mar.headline_intercept
    assert h is not None
    assert h.interval_m == 17.3
    assert h.aueq_gpt == 22.9
    assert h.au_gpt == 15.3
    assert h.sb_pct == 3.2
    assert h.from_m == 251.1
    assert h.hole_id == "SDDSC200"


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_headline_intercept_2026_02_18(payload_feb):
    h = payload_feb.headline_intercept
    assert h is not None
    assert h.interval_m == 1.8
    assert h.aueq_gpt == 80.5
    assert h.au_gpt == 79.9
    assert h.sb_pct == 0.2
    assert h.from_m == 649.4
    assert h.hole_id == "SDDSC208"


# --- Project totals ---


def test_project_totals_2026_03_16(payload_mar):
    pt = payload_mar.project_totals
    assert pt is not None
    assert pt.total_drill_holes == 247
    assert pt.total_metres == 114806.33
    assert pt.composites_gt_100_au == 81
    assert pt.composites_50_to_100_au == 72
    assert pt.composites_gt_10_sb == 101
    assert pt.holes_pending == 46


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_project_totals_2026_02_18(payload_feb):
    pt = payload_feb.project_totals
    assert pt is not None
    assert pt.total_drill_holes == 243
    assert pt.total_metres == 113556.61
    assert pt.composites_gt_100_au == 79
    assert pt.composites_gt_10_sb == 97
    assert pt.holes_pending == 41


# --- Metal equivalent ---


def test_metal_equivalent_extraction_march(payload_mar):
    me = payload_mar.metal_equivalent
    assert me is not None
    assert me.multiplier == 2.39
    assert me.au_price_usd_per_oz == 2500
    assert me.sb_price_usd_per_tonne == 19000
    assert me.au_recovery_pct == 91
    assert me.sb_recovery_pct == 92


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_metal_equivalent_extraction_feb(payload_feb):
    me = payload_feb.metal_equivalent
    assert me is not None
    assert me.multiplier == 2.39
    assert me.au_price_usd_per_oz == 2500
    assert me.sb_price_usd_per_tonne == 19000
    assert me.au_recovery_pct == 91
    assert me.sb_recovery_pct == 92


# --- Composite intersections ---


def test_composite_intersections_count_2026_03_16(payload_mar):
    assert len(payload_mar.composite_intersections) == 41


def test_no_duplicate_table_2026_03_16(payload_mar):
    """March release must NOT have DUPLICATE_TABLE_ROWS_DROPPED warning."""
    dup_warnings = [w for w in payload_mar.extraction_warnings if w.code == "DUPLICATE_TABLE_ROWS_DROPPED"]
    assert len(dup_warnings) == 0


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_composite_intersections_count_2026_02_18(payload_feb):
    """After deduplication, must contain unique rows from Table 2."""
    assert len(payload_feb.composite_intersections) > 0


@pytest.mark.skipif(not has_feb, reason="Feb fixture not available")
def test_duplicate_table_dropped_2026_02_18(payload_feb):
    """Must add a DUPLICATE_TABLE_ROWS_DROPPED warning with count > 0."""
    dup_warnings = [w for w in payload_feb.extraction_warnings if w.code == "DUPLICATE_TABLE_ROWS_DROPPED"]
    assert len(dup_warnings) == 1
    assert dup_warnings[0].count is not None
    assert dup_warnings[0].count > 0


# --- Drill collars ---


def test_drill_collars_2026_03_16(payload_mar):
    """Must contain the 4 holes from this release with depths and coordinates."""
    this_release = {c.hole_id: c for c in payload_mar.drill_collars if c.status == "this_release"}
    expected_holes = {"SDDSC195", "SDDSC198", "SDDSC199", "SDDSC200"}
    assert expected_holes.issubset(this_release.keys())

    c = this_release["SDDSC195"]
    assert c.depth_m == 152.15
    assert c.prospect == "Apollo"
    assert c.easting == 330989.7
    assert c.dip_deg == -53.3


# --- AuEq formula consistency ---


def test_aueq_formula_consistency_2026_03_16(payload_mar):
    """Every composite row must satisfy the AuEq formula within tolerance."""
    multiplier = 2.39
    failures = []
    for i, row in enumerate(payload_mar.composite_intersections):
        if row.au_gpt is None or row.sb_pct is None or row.aueq_gpt is None:
            continue
        expected = row.au_gpt + multiplier * row.sb_pct
        tolerance = max(0.5, 0.05 * row.aueq_gpt)
        if abs(row.aueq_gpt - expected) >= tolerance:
            failures.append(
                f"Row {i}: aueq={row.aueq_gpt} != {expected:.2f} "
                f"(diff={abs(row.aueq_gpt - expected):.2f}, tol={tolerance:.2f})"
            )
    assert not failures, f"AuEq formula failures:\n" + "\n".join(failures)


# --- JSON serialization ---


def test_payload_serializes_to_json(payload_mar):
    """Payload must JSON round-trip successfully."""
    json_str = payload_mar.model_dump_json()
    restored = ExplorationResultsPayload.model_validate_json(json_str)
    assert restored.doc_id == payload_mar.doc_id
    assert restored.ticker == payload_mar.ticker
    assert restored.release_date == payload_mar.release_date
    assert len(restored.composite_intersections) == len(payload_mar.composite_intersections)
    assert len(restored.individual_assays) == len(payload_mar.individual_assays)
    assert len(restored.drill_collars) == len(payload_mar.drill_collars)


# --- Performance ---


@pytest.mark.skipif(not has_mar, reason="March fixture not available")
def test_parse_performance():
    """Single PDF parse: target <5s, hard ceiling 15s on fast hardware.
    pdfplumber table extraction is ~1.5s/page; 13 table pages dominate runtime.
    On slower systems, allow up to 30s.
    """
    start = time.time()
    parse(SXG_MAR, ticker="SX2", doc_id="perf_test")
    elapsed = time.time() - start
    assert elapsed < 30.0, f"Parse took {elapsed:.1f}s, expected < 30s"


# --- No extraction errors ---


def test_no_extraction_errors_march(payload_mar):
    """March fixture must parse without any extraction_error."""
    assert len(payload_mar.extraction_errors) == 0, (
        f"Unexpected errors: {[e.code for e in payload_mar.extraction_errors]}"
    )


def test_warnings_count_march(payload_mar):
    """At most 3 extraction_warnings, all explainable."""
    assert len(payload_mar.extraction_warnings) <= 3, (
        f"Too many warnings ({len(payload_mar.extraction_warnings)}): "
        f"{[w.code for w in payload_mar.extraction_warnings]}"
    )

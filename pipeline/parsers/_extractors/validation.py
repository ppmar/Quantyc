"""Per-row and document-level validation for exploration results."""

from __future__ import annotations

import logging

from pipeline.parsers.schemas import CompositeIntersection, ExtractionWarning

logger = logging.getLogger("parsers.exploration_results")

DEFAULT_MULTIPLIER = 2.39


def validate_composite_intersections(
    composites: list[CompositeIntersection],
    multiplier: float | None = None,
) -> list[ExtractionWarning]:
    """
    Run per-row and document-level validation checks on composite intersections.
    Attaches warnings to individual rows and returns document-level warnings.
    """
    mult = multiplier if multiplier is not None else DEFAULT_MULTIPLIER
    doc_warnings: list[ExtractionWarning] = []
    aueq_mismatch_count = 0

    for i, row in enumerate(composites):
        # interval_m > 0
        if row.interval_m is not None and row.interval_m <= 0:
            row.warnings.append(ExtractionWarning(
                code="INVALID_INTERVAL",
                message=f"Row {i}: interval_m={row.interval_m} <= 0",
                severity="high",
                source_page=row.source_page,
                row_index=i,
            ))

        # to_m > from_m
        if row.from_m is not None and row.to_m is not None and row.to_m <= row.from_m:
            row.warnings.append(ExtractionWarning(
                code="INVERTED_INTERVAL",
                message=f"Row {i}: to_m={row.to_m} <= from_m={row.from_m}",
                severity="high",
                source_page=row.source_page,
                row_index=i,
            ))

        # interval_m == to_m - from_m (±0.05)
        if row.from_m is not None and row.to_m is not None and row.interval_m is not None:
            expected = round(row.to_m - row.from_m, 2)
            if abs(row.interval_m - expected) > 0.05:
                row.warnings.append(ExtractionWarning(
                    code="INTERVAL_MISMATCH",
                    message=f"Row {i}: interval_m={row.interval_m} != to-from={expected}",
                    severity="medium",
                    source_page=row.source_page,
                    row_index=i,
                ))

        # au_gpt >= 0
        if row.au_gpt is not None and row.au_gpt < 0:
            row.warnings.append(ExtractionWarning(
                code="NEGATIVE_AU",
                message=f"Row {i}: au_gpt={row.au_gpt} < 0",
                severity="high",
                source_page=row.source_page,
                row_index=i,
            ))

        # sb_pct >= 0
        if row.sb_pct is not None and row.sb_pct < 0:
            row.warnings.append(ExtractionWarning(
                code="NEGATIVE_SB",
                message=f"Row {i}: sb_pct={row.sb_pct} < 0",
                severity="high",
                source_page=row.source_page,
                row_index=i,
            ))

        # au_gpt <= 5000
        if row.au_gpt is not None and row.au_gpt > 5000:
            row.warnings.append(ExtractionWarning(
                code="EXTREME_AU_GRADE",
                message=f"Row {i}: au_gpt={row.au_gpt} > 5000",
                severity="medium",
                source_page=row.source_page,
                row_index=i,
            ))

        # sb_pct <= 50
        if row.sb_pct is not None and row.sb_pct > 50:
            row.warnings.append(ExtractionWarning(
                code="EXTREME_SB_GRADE",
                message=f"Row {i}: sb_pct={row.sb_pct} > 50",
                severity="medium",
                source_page=row.source_page,
                row_index=i,
            ))

        # aueq_gpt >= au_gpt
        if row.aueq_gpt is not None and row.au_gpt is not None and row.aueq_gpt < row.au_gpt:
            row.warnings.append(ExtractionWarning(
                code="AUEQ_LESS_THAN_AU",
                message=f"Row {i}: aueq_gpt={row.aueq_gpt} < au_gpt={row.au_gpt}",
                severity="high",
                source_page=row.source_page,
                row_index=i,
            ))

        # AuEq formula check
        if row.aueq_gpt is not None and row.au_gpt is not None and row.sb_pct is not None:
            expected_aueq = row.au_gpt + mult * row.sb_pct
            tolerance = max(0.5, 0.05 * row.aueq_gpt)
            if abs(row.aueq_gpt - expected_aueq) >= tolerance:
                row.warnings.append(ExtractionWarning(
                    code="AUEQ_FORMULA_MISMATCH",
                    message=(
                        f"Row {i}: aueq_gpt={row.aueq_gpt} != "
                        f"au_gpt + {mult}*sb_pct = {expected_aueq:.2f} "
                        f"(diff={abs(row.aueq_gpt - expected_aueq):.2f}, tol={tolerance:.2f})"
                    ),
                    severity="high",
                    source_page=row.source_page,
                    row_index=i,
                ))
                aueq_mismatch_count += 1

    # Document-level: > 10% AuEq formula mismatch
    total_checkable = sum(
        1 for r in composites
        if r.aueq_gpt is not None and r.au_gpt is not None and r.sb_pct is not None
    )
    if total_checkable > 0 and aueq_mismatch_count / total_checkable > 0.10:
        doc_warnings.append(ExtractionWarning(
            code="AUEQ_SYSTEMIC_MISMATCH",
            message=(
                f"{aueq_mismatch_count}/{total_checkable} rows "
                f"({100 * aueq_mismatch_count / total_checkable:.0f}%) "
                f"fail AuEq formula check. Recommend human review."
            ),
            severity="high",
        ))
        logger.warning(
            "AUEQ_SYSTEMIC_MISMATCH: %d/%d rows fail formula check",
            aueq_mismatch_count, total_checkable,
        )

    return doc_warnings

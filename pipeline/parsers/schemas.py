"""
Pydantic models for the exploration_results parser.

All models carry a source_page field for provenance tracking.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class ExtractionWarning(BaseModel):
    code: str
    message: str
    severity: str = "medium"  # low | medium | high
    source_page: Optional[int] = None
    row_index: Optional[int] = None
    count: Optional[int] = None


class ExtractionError(BaseModel):
    code: str
    message: str
    source_page: Optional[int] = None


class HeadlineIntercept(BaseModel):
    interval_m: float
    aueq_gpt: float
    au_gpt: float
    sb_pct: float
    from_m: float
    hole_id: Optional[str] = None
    source_page: int
    raw_text: str


class ProjectTotalsSnapshot(BaseModel):
    total_drill_holes: Optional[int] = None
    total_metres: Optional[float] = None
    composites_gt_100_au: Optional[int] = None
    composites_50_to_100_au: Optional[int] = None
    composites_gt_10_sb: Optional[int] = None
    holes_pending: Optional[int] = None
    active_rigs: Optional[int] = None
    regional_rigs: Optional[int] = None
    program_target_metres: Optional[int] = None
    program_end_target: Optional[str] = None
    source_page: int


class MetalEquivalentAssumptions(BaseModel):
    formula_text: str
    multiplier: float
    au_price_usd_per_oz: Optional[float] = None
    sb_price_usd_per_tonne: Optional[float] = None
    au_recovery_pct: Optional[int] = None
    sb_recovery_pct: Optional[int] = None
    source_page: int


class DrillCollar(BaseModel):
    hole_id: str
    depth_m: Optional[float] = None
    prospect: Optional[str] = None
    easting: Optional[float] = None
    northing: Optional[float] = None
    elevation_m: Optional[float] = None
    dip_deg: Optional[float] = None
    azimuth_deg: Optional[float] = None
    status: Optional[str] = None  # "this_release" | "processing" | "in_progress" | "regional" | "abandoned"
    source_page: int


class CompositeIntersection(BaseModel):
    hole_id: str
    from_m: Optional[float] = None
    to_m: Optional[float] = None
    interval_m: Optional[float] = None
    au_gpt: Optional[float] = None
    sb_pct: Optional[float] = None
    aueq_gpt: Optional[float] = None
    is_subinterval: bool = False
    parent_row_index: Optional[int] = None
    source_page: int
    warnings: list[ExtractionWarning] = Field(default_factory=list)


class IndividualAssay(BaseModel):
    hole_id: str
    from_m: Optional[float] = None
    to_m: Optional[float] = None
    interval_m: Optional[float] = None
    au_gpt: Optional[float] = None
    sb_pct: Optional[float] = None
    aueq_gpt: Optional[float] = None
    source_page: int


class ExplorationResultsPayload(BaseModel):
    doc_id: str
    ticker: str
    parser_version: str
    parsed_at: datetime
    release_date: Optional[date] = None
    headline_intercept: Optional[HeadlineIntercept] = None
    all_headline_intercepts: list[HeadlineIntercept] = Field(default_factory=list)
    project_totals: Optional[ProjectTotalsSnapshot] = None
    metal_equivalent: Optional[MetalEquivalentAssumptions] = None
    drill_collars: list[DrillCollar] = Field(default_factory=list)
    composite_intersections: list[CompositeIntersection] = Field(default_factory=list)
    individual_assays: list[IndividualAssay] = Field(default_factory=list)
    extraction_warnings: list[ExtractionWarning] = Field(default_factory=list)
    extraction_errors: list[ExtractionError] = Field(default_factory=list)

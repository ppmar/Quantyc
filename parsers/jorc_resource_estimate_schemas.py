from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Literal

JORCCategory = Literal[
    "Measured", "Indicated", "Inferred",      # resources
    "Proven", "Probable",                      # reserves
    "Total",                                   # roll-up row
]

ResourceOrReserve = Literal["resource", "reserve"]


@dataclass(frozen=True)
class JORCRow:
    """One row of the headline JORC summary table."""
    category: JORCCategory
    tonnes_mt: Optional[Decimal]          # always millions of tonnes; None for empty rows
    grade: Optional[Decimal]              # in source units; see grade_unit
    grade_unit: str                       # 'g/t', '%', 'ppm', 'lb/t', etc.
    contained_metal: Optional[Decimal]
    contained_metal_unit: Optional[str]   # 'Moz', 'koz', 'kt', 'Mlb', etc.
    raw_line: str


@dataclass(frozen=True)
class JORCEstimate:
    # provenance
    ticker: str
    doc_id: str
    snapshot_date: date             # estimate's effective_date
    announcement_date: date
    parsed_at: datetime
    parser_version: str

    # what & where
    project_name: str
    commodity: str                  # 'Au', 'Cu', 'Li2O', 'U3O8', etc.
    resource_or_reserve: ResourceOrReserve
    cutoff_grade: Optional[Decimal]
    cutoff_grade_unit: Optional[str]

    # the table
    rows: list[JORCRow]

    # diagnostics
    extraction_warnings: list[str] = field(default_factory=list)

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, Literal


@dataclass(frozen=True)
class QuotedClass:
    """One row of Part 4.1 — a quoted security class (almost always ordinary shares)."""
    asx_code: str
    description: str
    total_on_issue: int


@dataclass(frozen=True)
class UnquotedInstrument:
    """One row of Part 4.2 — an unquoted option, convertible note, or performance right."""
    asx_code: str
    description: str
    instrument_type: Literal["option", "convertible_note", "performance_right", "other"]
    total_on_issue: int
    expiry_date: Optional[date]
    strike_aud: Optional[Decimal]
    raw_line: str


@dataclass(frozen=True)
class Appendix2ACapitalStructure:
    # provenance
    ticker: str
    doc_id: str
    snapshot_date: date
    parsed_at: datetime
    parser_version: str

    # Part 4.1
    quoted_classes: list[QuotedClass]

    # Part 4.2
    unquoted_instruments: list[UnquotedInstrument]

    # derived totals
    shares_basic: int
    shares_fd_naive: int
    options_outstanding: int
    convertible_notes_face_count: int
    performance_rights_count: int

    # diagnostics
    extraction_warnings: list[str] = field(default_factory=list)

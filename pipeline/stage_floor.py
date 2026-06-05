"""Deterministic study-to-stage floor. Pure functions, hand-checked in tests."""
from typing import Optional

# Most-advanced first. MUST stay in sync with api/portfolio.py STAGE_ORDER.
STAGE_ORDER = [
    "production", "care_and_maintenance", "development", "feasibility",
    "advanced_exploration", "exploration", "unknown",
]
_RANK = {s: i for i, s in enumerate(STAGE_ORDER)}


def study_floor_stage(confidence_tier: Optional[str]) -> Optional[str]:
    """Minimum stage implied by the most-advanced study confidence tier."""
    if confidence_tier in ("definitive", "indicative"):
        return "feasibility"
    if confidence_tier == "conceptual":
        return "advanced_exploration"
    return None  # no study / unknown tier → no floor


def most_advanced(*stages: Optional[str]) -> Optional[str]:
    """Return the most-advanced (lowest-rank) known stage among inputs."""
    known = [s for s in stages if s in _RANK]
    return min(known, key=lambda s: _RANK[s]) if known else None


def apply_floor(llm_stage: Optional[str], confidence_tier: Optional[str]) -> tuple[Optional[str], bool]:
    """
    Returns (resolved_stage, floor_won).
    floor_won is True iff the floor produced a more-advanced stage than the LLM.
    """
    floor = study_floor_stage(confidence_tier)
    if floor is None:
        return llm_stage, False
    resolved = most_advanced(llm_stage, floor)
    floor_won = resolved == floor and _RANK.get(floor, 99) < _RANK.get(llm_stage or "unknown", 99)
    return resolved, floor_won

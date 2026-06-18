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


# Below this quarterly customer-receipt figure (A$), an inflow is treated as
# incidental (interest, small tolling) rather than mine production revenue.
PRODUCTION_RECEIPTS_FLOOR = 1_000_000.0
# Above this (A$5B/quarter) the figure is implausible for an ASX junior and is
# treated as an extraction misparse, not production (sanity bound — e.g. VIT's
# A$26B 1.1 misread). The largest real producer on coverage books ~A$0.7B/qtr.
PRODUCTION_RECEIPTS_MAX = 5_000_000_000.0


def production_floor(
    receipts_from_customers: Optional[float],
    production_start_date: Optional[str],
    today: str,
    has_revaluable_study: bool,
    receipts_threshold: float = PRODUCTION_RECEIPTS_FLOOR,
    receipts_sanity_max: float = PRODUCTION_RECEIPTS_MAX,
) -> bool:
    """Deterministic 'is this producing?' test (no LLM). True if either:

      - material customer receipts (Appendix 5B line 1.1) in
        [threshold, sanity_max] — these SELF-PROVE a producing mine, so they do
        NOT require a study on file (our study coverage is incomplete; e.g. GMD,
        BGL, LYC produce with no DFS in the DB). The bounds reject stray small
        inflows (interest) and implausible misparses.
      - a stated first-production date that has passed, paired with a revaluable
        study (the date comes from that DFS, so the study must exist).
    """
    if (receipts_from_customers is not None
            and receipts_threshold <= receipts_from_customers <= receipts_sanity_max):
        return True
    if production_start_date and production_start_date <= today and has_revaluable_study:
        return True
    return False


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

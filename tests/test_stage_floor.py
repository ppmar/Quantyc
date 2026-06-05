from pipeline.stage_floor import study_floor_stage, most_advanced, apply_floor
from pipeline.stage_floor import STAGE_ORDER as FLOOR_ORDER
from api.portfolio import STAGE_ORDER as PORTFOLIO_ORDER


def test_stage_order_matches_portfolio():
    # Checklist invariant: the duplicated list must not diverge.
    assert FLOOR_ORDER == PORTFOLIO_ORDER


def test_floor_tiers():
    assert study_floor_stage("definitive") == "feasibility"
    assert study_floor_stage("indicative") == "feasibility"
    assert study_floor_stage("conceptual") == "advanced_exploration"
    assert study_floor_stage(None) is None
    assert study_floor_stage("garbage") is None


def test_most_advanced_picks_lowest_rank():
    assert most_advanced("unknown", "feasibility") == "feasibility"
    assert most_advanced("production", "feasibility") == "production"   # floor never downgrades
    assert most_advanced("development", "advanced_exploration") == "development"
    assert most_advanced(None, None) is None


def test_apply_floor_resolution_and_provenance():
    # the RMX/Batangas case: DFS present, LLM said unknown
    assert apply_floor("unknown", "definitive") == ("feasibility", True)
    # LLM already more advanced → floor does not win, no source change
    assert apply_floor("production", "definitive") == ("production", False)
    # conceptual study, LLM unknown
    assert apply_floor("unknown", "conceptual") == ("advanced_exploration", True)
    # no study
    assert apply_floor("exploration", None) == ("exploration", False)

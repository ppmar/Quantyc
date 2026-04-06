"""
Valuation Engine

Produces fair-value estimates for each company on an attributable,
fully-diluted, risk-adjusted basis. Method varies by Lassonde Curve stage.

Usage:
    python -m valuation.engine --ticker DEG
    python -m valuation.engine --all
"""

import argparse
import logging
from dataclasses import dataclass, field

from db import get_connection, init_db

logger = logging.getLogger(__name__)

# Risk discounts by stage
STAGE_RISK = {
    "concept": 0.05,
    "discovery": 0.15,
    "feasibility_scoping": 0.25,
    "feasibility_pfs": 0.45,
    "feasibility_dfs": 0.65,
    "development": 0.80,
    "production": 0.95,
}

# Inferred resource weighting (relative to Measured/Indicated)
CATEGORY_WEIGHTS = {
    "Measured": 1.0,
    "Indicated": 1.0,
    "Measured+Indicated": 1.0,
    "Inferred": 0.15,
    "Proven": 1.0,
    "Probable": 1.0,
    "Total": 0.7,  # Blended — conservative since Total often includes Inferred
}


@dataclass
class ValuationResult:
    ticker: str
    stage: str
    method: str
    ev_aud: float | None = None
    nav_aud: float | None = None
    nav_per_share: float | None = None
    ev_per_resource_unit: float | None = None
    resource_unit: str | None = None
    total_attributable_resource: float | None = None
    shares_fd: float | None = None
    cash_aud: float | None = None
    debt_aud: float | None = None
    red_flags: list[str] = field(default_factory=list)


def _get_latest_financials(conn, ticker: str) -> dict | None:
    """Get the most recent company_financials row."""
    row = conn.execute(
        """SELECT * FROM company_financials
           WHERE ticker = ?
           ORDER BY effective_date DESC LIMIT 1""",
        (ticker,),
    ).fetchone()
    return dict(row) if row else None


def _get_latest_macro(conn) -> dict | None:
    """Get latest macro assumptions."""
    row = conn.execute(
        "SELECT * FROM macro_assumptions ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def _get_project_stage(conn, ticker: str) -> tuple[str, str | None]:
    """
    Determine the company's highest project stage and the primary project ID.
    Returns (stage, project_id).
    """
    stage_order = ["production", "development", "feasibility", "discovery", "concept"]

    projects = conn.execute(
        "SELECT id, stage FROM projects WHERE ticker = ? ORDER BY is_primary DESC",
        (ticker,),
    ).fetchall()

    if not projects:
        return "concept", None

    # Return highest stage found
    for target_stage in stage_order:
        for p in projects:
            if p["stage"] == target_stage:
                return target_stage, p["id"]

    return "concept", projects[0]["id"] if projects else None


def _get_attributable_resources(conn, ticker: str) -> list[dict]:
    """Get all resource rows with attributable values for a ticker."""
    rows = conn.execute(
        """SELECT r.*, p.ownership_pct
           FROM resources r
           JOIN projects p ON r.project_id = p.id
           WHERE p.ticker = ?
           ORDER BY r.effective_date DESC""",
        (ticker,),
    ).fetchall()
    return [dict(r) for r in rows]


def _get_latest_study(conn, project_id: str) -> dict | None:
    """Get the most advanced study for a project."""
    stage_order = ["production", "dfs", "pfs", "scoping"]
    for stage in stage_order:
        row = conn.execute(
            """SELECT * FROM studies
               WHERE project_id = ? AND study_stage = ?
               ORDER BY study_date DESC LIMIT 1""",
            (project_id, stage),
        ).fetchone()
        if row:
            return dict(row)
    # Fallback: just get latest study
    row = conn.execute(
        "SELECT * FROM studies WHERE project_id = ? ORDER BY study_date DESC LIMIT 1",
        (project_id,),
    ).fetchone()
    return dict(row) if row else None


def _study_risk_factor(study_stage: str) -> float:
    """Map study stage to risk factor."""
    mapping = {
        "scoping": STAGE_RISK["feasibility_scoping"],
        "pfs": STAGE_RISK["feasibility_pfs"],
        "dfs": STAGE_RISK["feasibility_dfs"],
        "production": STAGE_RISK["production"],
    }
    return mapping.get(study_stage, STAGE_RISK["feasibility_scoping"])


def _compute_weighted_resource(resources: list[dict], commodity: str) -> float:
    """
    Compute weighted attributable resource for a commodity.
    Weights Inferred at a discount per CATEGORY_WEIGHTS.
    """
    total = 0.0
    for r in resources:
        if r.get("commodity") != commodity:
            continue
        contained = r.get("attributable_contained") or r.get("contained_metal")
        if contained is None:
            continue

        category = r.get("category", "")
        weight = CATEGORY_WEIGHTS.get(category, 0.5)
        total += float(contained) * weight

    return total


def valuate_concept_discovery(conn, ticker: str, financials: dict | None) -> ValuationResult:
    """
    Valuation for concept/discovery stage:
    EV/attributable resource comparison.
    """
    stage, project_id = _get_project_stage(conn, ticker)
    result = ValuationResult(ticker=ticker, stage=stage, method="ev_resource_comp")

    # Get financials
    cash = financials.get("cash_aud", 0) if financials else 0
    debt = financials.get("debt_aud", 0) if financials else 0
    shares_fd = financials.get("shares_fd") if financials else None
    result.cash_aud = cash
    result.debt_aud = debt
    result.shares_fd = shares_fd

    # Get resources
    resources = _get_attributable_resources(conn, ticker)
    if not resources:
        result.red_flags.append("No resource data available")
        return result

    # Determine primary commodity
    company = conn.execute(
        "SELECT primary_commodity FROM companies WHERE ticker = ?",
        (ticker,),
    ).fetchone()
    primary_commodity = company["primary_commodity"] if company else None

    if not primary_commodity:
        # Infer from resources
        commodity_counts = {}
        for r in resources:
            c = r.get("commodity")
            if c:
                commodity_counts[c] = commodity_counts.get(c, 0) + 1
        if commodity_counts:
            primary_commodity = max(commodity_counts, key=commodity_counts.get)

    if not primary_commodity:
        result.red_flags.append("Cannot determine primary commodity")
        return result

    # Compute weighted attributable resource
    weighted_resource = _compute_weighted_resource(resources, primary_commodity)
    result.total_attributable_resource = weighted_resource

    # Determine resource unit
    for r in resources:
        if r.get("commodity") == primary_commodity and r.get("contained_unit"):
            result.resource_unit = r["contained_unit"]
            break

    if weighted_resource <= 0:
        result.red_flags.append("No attributable resource computed")
        return result

    # EV/resource is just reported — fair value requires peer comps
    # which we don't have yet. Report the metric for manual comparison.
    if shares_fd and shares_fd > 0:
        # Placeholder market cap — would need share price from market data
        # For now, just report the resource denominator
        result.ev_per_resource_unit = None  # Needs market cap

    # Check red flags
    inferred_total = sum(
        float(r.get("attributable_contained") or r.get("contained_metal") or 0)
        for r in resources
        if r.get("commodity") == primary_commodity and r.get("category") == "Inferred"
    )
    total_contained = sum(
        float(r.get("attributable_contained") or r.get("contained_metal") or 0)
        for r in resources
        if r.get("commodity") == primary_commodity and r.get("category") != "Total"
    )
    if total_contained > 0 and inferred_total / total_contained > 0.7:
        result.red_flags.append("Resource is >70% Inferred category")

    if financials and financials.get("cash_runway_months") and financials["cash_runway_months"] < 6:
        result.red_flags.append("Cash runway < 6 months")

    if financials and shares_fd and financials.get("shares_basic"):
        if shares_fd > financials["shares_basic"] * 1.5:
            result.red_flags.append("Heavy dilution overhang (FD > 1.5x basic)")

    return result


def valuate_feasibility(conn, ticker: str, financials: dict | None) -> ValuationResult:
    """
    Valuation for feasibility stage:
    Risked NAV from study NPV, adjusted to current commodity prices.
    """
    stage, project_id = _get_project_stage(conn, ticker)
    result = ValuationResult(ticker=ticker, stage=stage, method="risked_nav")

    if not project_id:
        result.red_flags.append("No project found")
        return result

    study = _get_latest_study(conn, project_id)
    if not study:
        result.red_flags.append("No study data available — cannot compute NAV")
        return result

    # Get project ownership
    project = conn.execute(
        "SELECT ownership_pct FROM projects WHERE id = ?",
        (project_id,),
    ).fetchone()
    ownership_pct = (project["ownership_pct"] or 100.0) / 100.0

    # Get macro data for price adjustment
    macro = _get_latest_macro(conn)

    # Base NPV from study
    base_npv = study.get("post_tax_npv_musd")
    if base_npv is None:
        result.red_flags.append("Study has no post-tax NPV")
        return result

    base_npv = float(base_npv)

    # Price adjustment
    assumed_price = study.get("assumed_commodity_price")
    price_adjustment = 1.0

    if assumed_price and macro:
        assumed_price = float(assumed_price)
        price_unit = (study.get("assumed_price_unit") or "").lower()

        current_price = None
        if "gold" in price_unit or "/oz" in price_unit:
            current_price = macro.get("gold_spot_usd")
        elif "copper" in price_unit or "/lb" in price_unit:
            current_price = macro.get("copper_spot_usd")
        elif "lithium" in price_unit:
            current_price = macro.get("lithium_spot_usd")
        elif "silver" in price_unit:
            current_price = macro.get("silver_spot_usd")

        if current_price and assumed_price > 0:
            price_adjustment = float(current_price) / assumed_price

            # Red flag if study price is >20% below current spot
            if price_adjustment > 1.2:
                result.red_flags.append(
                    f"Study commodity price ({assumed_price}) is >20% below current spot ({current_price})"
                )

    adjusted_npv = base_npv * price_adjustment

    # Risk factor
    study_stage = study.get("study_stage", "scoping")
    risk_factor = _study_risk_factor(study_stage)

    # Risked NAV = adjusted NPV × risk factor × ownership
    risked_nav = adjusted_npv * risk_factor * ownership_pct

    # FX adjustment if NPV is in USD
    fx_rate = None
    if macro:
        fx_rate = macro.get("aud_usd")

    # Convert to AUD if needed (assume study NPV is in USD unless stated otherwise)
    nav_aud = risked_nav
    if fx_rate and fx_rate > 0:
        nav_aud = risked_nav / fx_rate  # USD millions → AUD millions

    # Add net cash
    cash = financials.get("cash_aud", 0) if financials else 0
    debt = financials.get("debt_aud", 0) if financials else 0
    convertibles = financials.get("convertibles_aud", 0) if financials else 0
    net_cash_aud = (cash or 0) - (debt or 0) - (convertibles or 0)

    # nav_aud is in millions, net_cash is in AUD
    company_nav_aud = (nav_aud * 1_000_000) + net_cash_aud

    result.nav_aud = company_nav_aud
    result.cash_aud = cash
    result.debt_aud = debt

    # Per share
    shares_fd = financials.get("shares_fd") if financials else None
    result.shares_fd = shares_fd
    if shares_fd and shares_fd > 0:
        result.nav_per_share = company_nav_aud / shares_fd

    # Red flags
    if financials and financials.get("cash_runway_months") and financials["cash_runway_months"] < 6:
        result.red_flags.append("Cash runway < 6 months")

    study_date = study.get("study_date")
    if study_date:
        # Simple staleness check — study older than 3 years
        from datetime import datetime, timedelta
        try:
            sd = datetime.strptime(str(study_date), "%Y-%m-%d")
            if (datetime.now() - sd).days > 3 * 365:
                result.red_flags.append("Study is older than 3 years")
        except (ValueError, TypeError):
            pass

    if financials and shares_fd and financials.get("shares_basic"):
        if shares_fd > financials["shares_basic"] * 1.5:
            result.red_flags.append("Heavy dilution overhang (FD > 1.5x basic)")

    return result


def valuate_development_production(conn, ticker: str, financials: dict | None) -> ValuationResult:
    """
    Valuation for development/production stage:
    NAV with smaller risk haircut.
    """
    # Same as feasibility but with different risk factor
    result = valuate_feasibility(conn, ticker, financials)
    stage, _ = _get_project_stage(conn, ticker)
    result.stage = stage

    if stage == "development":
        result.method = "nav_development"
    else:
        result.method = "nav_production"

    return result


def valuate_ticker(ticker: str) -> ValuationResult:
    """
    Run the full valuation for a single ticker.
    Selects the appropriate method based on the company's stage.
    """
    ticker = ticker.upper()
    conn = get_connection()

    financials = _get_latest_financials(conn, ticker)
    stage, project_id = _get_project_stage(conn, ticker)

    if stage in ("concept", "discovery"):
        result = valuate_concept_discovery(conn, ticker, financials)
    elif stage == "feasibility":
        result = valuate_feasibility(conn, ticker, financials)
    elif stage in ("development", "production"):
        result = valuate_development_production(conn, ticker, financials)
    else:
        result = valuate_concept_discovery(conn, ticker, financials)

    conn.close()

    logger.info(
        "Valuation for %s: stage=%s, method=%s, NAV=%s, NAV/share=%s, flags=%s",
        ticker, result.stage, result.method, result.nav_aud,
        result.nav_per_share, result.red_flags,
    )
    return result


def valuate_all() -> list[ValuationResult]:
    """Run valuation for all companies in the database."""
    conn = get_connection()
    tickers = conn.execute("SELECT ticker FROM companies").fetchall()
    conn.close()

    results = []
    for row in tickers:
        result = valuate_ticker(row["ticker"])
        results.append(result)

    return results


def print_valuation(result: ValuationResult):
    """Pretty-print a valuation result."""
    print(f"\n{'='*60}")
    print(f"  {result.ticker}  |  Stage: {result.stage}  |  Method: {result.method}")
    print(f"{'='*60}")

    if result.cash_aud is not None:
        print(f"  Cash:           A${result.cash_aud:,.0f}")
    if result.debt_aud is not None:
        print(f"  Debt:           A${result.debt_aud:,.0f}")
    if result.shares_fd is not None:
        print(f"  Shares (FD):    {result.shares_fd:,.0f}")
    if result.total_attributable_resource is not None:
        unit = result.resource_unit or "units"
        print(f"  Attrib. Resource: {result.total_attributable_resource:,.1f} {unit}")
    if result.nav_aud is not None:
        print(f"  NAV:            A${result.nav_aud:,.0f}")
    if result.nav_per_share is not None:
        print(f"  NAV/share:      A${result.nav_per_share:.4f}")

    if result.red_flags:
        print(f"\n  RED FLAGS:")
        for flag in result.red_flags:
            print(f"    - {flag}")

    print()


def main():
    parser = argparse.ArgumentParser(description="Run valuations")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--ticker", type=str, help="Valuate a specific ticker")
    group.add_argument("--all", action="store_true", help="Valuate all companies")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    init_db()

    if args.ticker:
        result = valuate_ticker(args.ticker)
        print_valuation(result)
    else:
        results = valuate_all()
        for result in results:
            print_valuation(result)


if __name__ == "__main__":
    main()

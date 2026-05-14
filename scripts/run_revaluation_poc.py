"""
Run the full revaluation pipeline on one ticker as proof-of-concept.

Steps:
1. Find the most recent DFS for the ticker (in studies, study_stage='DFS')
2. Fetch current spot price (cached if recent)
3. Compute revaluation
4. Print human-readable summary

Usage:
    python -m scripts.run_revaluation_poc DEG
    python -m scripts.run_revaluation_poc DEG --study-id 42
"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone

sys.path.insert(0, ".")

from db import get_connection, init_db
from revaluation.pipeline import revalue_study
from revaluation.math import RevaluationError
from revaluation.prices import PriceFetchError

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def find_latest_dfs(conn, ticker: str) -> int:
    """Find the most recent DFS study_id for a ticker."""
    row = conn.execute("""
        SELECT s.study_id
        FROM studies s
        JOIN projects p ON p.project_id = s.project_id
        JOIN companies c ON c.company_id = p.company_id
        WHERE c.ticker = ?
          AND s.study_stage IN ('DFS', 'Updated DFS', 'Revised DFS', 'FFS')
        ORDER BY s.study_date DESC, s.study_id DESC
        LIMIT 1
    """, (ticker,)).fetchone()
    if not row:
        print(f"No DFS found for ticker {ticker}")
        sys.exit(1)
    return row["study_id"]


def print_summary(conn, revaluation_id: int):
    """Print human-readable revaluation summary."""
    row = conn.execute("""
        SELECT r.*, c.ticker, p.project_name, s.study_date, s.study_stage,
               cp_spot.source AS spot_source, cp_spot.fetched_at AS spot_fetched_at,
               cp_fx.source AS fx_source
        FROM revaluations r
        JOIN companies c ON c.company_id = r.company_id
        JOIN projects p ON p.project_id = r.project_id
        JOIN studies s ON s.study_id = r.study_id
        JOIN commodity_prices cp_spot ON cp_spot.price_id = r.price_spot_id
        LEFT JOIN commodity_prices cp_fx ON cp_fx.price_id = r.fx_rate_price_id
        WHERE r.revaluation_id = ?
    """, (revaluation_id,)).fetchone()

    if not row:
        print(f"Revaluation row #{revaluation_id} not found")
        return

    warnings = json.loads(row["warnings"] or "[]")
    price_unit = "USD/oz" if row["commodity"] == "Au" else "USD/lb"

    price_change_pct = ((row["price_spot"] - row["price_dfs"]) / row["price_dfs"]) * 100

    print(f"""
Ticker:           {row['ticker']}
Project:          {row['project_name']}
Study:            {row['study_stage']} at {row['study_date'] or 'unknown date'}

DFS assumption:   {row['price_dfs']:.2f} {price_unit} {row['commodity']}
Spot price:       {row['price_spot']:.2f} {price_unit} {row['commodity']}  (fetched {row['spot_fetched_at']}, {row['spot_source']})
Price uplift:     {price_change_pct:+.1f}%""")

    if row["fx_rate"]:
        print(f"\nFX rate:          {row['fx_rate']:.4f} AUD/USD  ({row['fx_source']})")

    prod_display = f"{row['annual_production']:,.0f}" if row["annual_production"] >= 1 else f"{row['annual_production']}"

    print(f"""
Annual production:   {prod_display} {row['annual_production_unit']} {row['commodity']}
Mine life:           {row['mine_life_years']:.1f} years
Discount rate:       {row['discount_rate_pct']:.1f}%
Tax rate:            {row['tax_rate_pct']:.1f}%{' (defaulted)' if any('defaulted' in w for w in warnings) else ''}
Annuity factor:      {row['annuity_factor']:.4f}

NPV (DFS, M):        {row['npv_dfs']:.2f}
NPV uplift (M):      {row['npv_uplift']:+.2f}
NPV (spot, M):       {row['npv_spot']:.2f}

Signal:           {row['npv_uplift_pct']*100:+.1f}% uplift to NPV at current spot""")

    if abs(row["npv_uplift_pct"]) > 0.3:
        print("                  Market may not have repriced this stock against current commodity prices.")

    if warnings:
        print(f"\nWarnings:         {', '.join(warnings)}")

    print(f"\nRevaluation row:  #{revaluation_id} (study_id={row['study_id']}, computed_at={row['computed_at']})")


def main():
    parser = argparse.ArgumentParser(description="DFS revaluation POC")
    parser.add_argument("ticker", help="ASX ticker (e.g., DEG)")
    parser.add_argument("--study-id", type=int, help="Specific study_id (default: latest DFS)")
    args = parser.parse_args()

    init_db()
    conn = get_connection()

    study_id = args.study_id or find_latest_dfs(conn, args.ticker)
    logger.info("Revaluing study_id=%d for %s", study_id, args.ticker)

    try:
        revaluation_id = revalue_study(conn, study_id)
    except (RevaluationError, PriceFetchError) as e:
        print(f"Revaluation failed: {e}")
        sys.exit(1)

    if revaluation_id is None:
        print("Study skipped (unsupported commodity for POC)")
        sys.exit(0)

    print_summary(conn, revaluation_id)
    conn.close()


if __name__ == "__main__":
    main()

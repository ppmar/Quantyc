"""
Diagnose the state of resource_update documents and projects/resources tables.

Usage:
    python -m scripts.diagnose_resource_updates
    python -m scripts.diagnose_resource_updates --ticker DEG
"""

import argparse
import sys

from db import get_connection, init_db


def main():
    parser = argparse.ArgumentParser(description="Diagnose resource_update pipeline state")
    parser.add_argument("--ticker", type=str, default=None, help="Filter by ticker")
    args = parser.parse_args()

    init_db()
    conn = get_connection()

    # 1. Document counts by parse_status
    print("=== resource_update documents by parse_status ===")
    if args.ticker:
        rows = conn.execute(
            """SELECT parse_status, COUNT(*) as n FROM documents
               WHERE doc_type = 'resource_update' AND ticker = ?
               GROUP BY parse_status""",
            (args.ticker,),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT parse_status, COUNT(*) as n FROM documents
               WHERE doc_type = 'resource_update'
               GROUP BY parse_status""",
        ).fetchall()

    if not rows:
        print("  (none)")
    for r in rows:
        print(f"  {r['parse_status']}: {r['n']}")

    # 2. Top parse_error values
    print("\n=== Top 10 parse_error values ===")
    errors = conn.execute(
        """SELECT parse_error, COUNT(*) as n FROM documents
           WHERE doc_type = 'resource_update' AND parse_error IS NOT NULL
           GROUP BY parse_error ORDER BY n DESC LIMIT 10""",
    ).fetchall()
    if not errors:
        print("  (none)")
    for r in errors:
        print(f"  [{r['n']}] {r['parse_error']}")

    # 3. Recent resource_update documents
    print("\n=== Recent resource_update documents (20) ===")
    ticker_filter = "AND ticker = ?" if args.ticker else ""
    params = (args.ticker,) if args.ticker else ()
    docs = conn.execute(
        f"""SELECT ticker, header, parse_status, parse_error, announcement_date
           FROM documents WHERE doc_type = 'resource_update' {ticker_filter}
           ORDER BY announcement_date DESC LIMIT 20""",
        params,
    ).fetchall()
    if not docs:
        print("  (none)")
    for d in docs:
        print(f"  {d['ticker']} | {d['announcement_date']} | {d['parse_status']} | "
              f"{d['parse_error'] or '-'} | {(d['header'] or '')[:60]}")

    # 4. Table counts
    print("\n=== Table counts ===")
    for table in ("projects", "project_commodities", "resources"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count}")

    conn.close()


if __name__ == "__main__":
    main()

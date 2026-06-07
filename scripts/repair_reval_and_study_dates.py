"""One-time repair: null future study_dates, delete conceptual revaluations.
Dry-run by default. Pass --apply to mutate. Safe to re-run (idempotent)."""
import argparse
from db import get_connection


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually mutate (default: dry-run)")
    args = ap.parse_args()
    conn = get_connection()

    future = conn.execute(
        "SELECT study_id, study_stage, study_date FROM studies WHERE study_date > date('now')"
    ).fetchall()
    bad_reval = conn.execute(
        """SELECT r.revaluation_id, r.study_id, r.study_confidence_tier
           FROM revaluations r
           WHERE r.study_confidence_tier IS NULL
              OR r.study_confidence_tier = 'conceptual'
              OR r.study_id IN (SELECT study_id FROM studies WHERE study_confidence_tier = 'conceptual')"""
    ).fetchall()

    print(f"[future study_date] {len(future)} row(s):")
    for r in future:
        print(f"  study_id={r['study_id']} stage={r['study_stage']} date={r['study_date']}")
    print(f"[conceptual/invalid reval] {len(bad_reval)} row(s):")
    for r in bad_reval:
        print(f"  revaluation_id={r['revaluation_id']} study_id={r['study_id']} tier={r['study_confidence_tier']}")

    if not args.apply:
        print("\nDRY-RUN — no changes. Re-run with --apply to mutate.")
        return

    conn.execute("UPDATE studies SET study_date = NULL WHERE study_date > date('now')")
    conn.execute(
        """DELETE FROM revaluations
           WHERE study_confidence_tier IS NULL
              OR study_confidence_tier = 'conceptual'
              OR study_id IN (SELECT study_id FROM studies WHERE study_confidence_tier = 'conceptual')"""
    )
    conn.commit()
    print(f"\nAPPLIED — nulled {len(future)} study_date(s), deleted {len(bad_reval)} reval row(s).")


if __name__ == "__main__":
    main()

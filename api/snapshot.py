"""
Snapshot endpoint — GET /api/company/<ticker>/snapshot

Returns a fully composed, display-ready snapshot for the company detail page.
The frontend renders; it does not compute, format, or label.
"""

import csv
import math
from datetime import date, datetime, timezone
from pathlib import Path

from flask import Blueprint, jsonify

from db import get_connection

bp = Blueprint("snapshot", __name__)

# ── Company meta (commodity, project, jurisdiction) ──────────────────────

_META_PATH = Path(__file__).resolve().parent.parent / "data" / "company_meta.csv"
_META_CACHE: dict[str, dict] = {}


def _load_meta() -> dict[str, dict]:
    if _META_CACHE:
        return _META_CACHE
    if not _META_PATH.exists():
        return {}
    with open(_META_PATH) as f:
        for row in csv.DictReader(f):
            _META_CACHE[row["ticker"].upper().strip()] = row
    return _META_CACHE


# ── Formatting helpers ───────────────────────────────────────────────────

def _fmt_aud(val: float | None) -> str | None:
    if val is None:
        return None
    sign = "-" if val < 0 else ""
    v = abs(val)
    if v >= 1e9:
        return f"{sign}A${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"{sign}A${v / 1e6:.1f}M"
    if v >= 1e3:
        return f"{sign}A${v / 1e3:.0f}K"
    return f"{sign}A${v:.0f}"


def _fmt_shares(val: float | None) -> str | None:
    if val is None:
        return None
    if val >= 1e9:
        return f"{val / 1e9:.2f}B"
    if val >= 1e6:
        return f"{val / 1e6:.1f}M"
    if val >= 1e3:
        return f"{val / 1e3:.0f}K"
    return str(int(val))


def _fmt_date_display(iso_date: str | None) -> str:
    """'2025-12-31' → '31 Dec 2025'"""
    if not iso_date:
        return ""
    try:
        d = date.fromisoformat(iso_date)
        return d.strftime("%-d %b %Y")
    except (ValueError, TypeError):
        return iso_date


def _relative_date(iso_date: str | None) -> str:
    """'2025-12-31' → '113 days ago'"""
    if not iso_date:
        return ""
    try:
        d = date.fromisoformat(iso_date)
    except (ValueError, TypeError):
        return ""
    delta = (date.today() - d).days
    if delta < 0:
        return "in the future"
    if delta == 0:
        return "today"
    if delta == 1:
        return "yesterday"
    if delta < 60:
        return f"{delta} days ago"
    months = delta // 30
    if months < 12:
        return f"{months} month{'s' if months != 1 else ''} ago"
    years = delta // 365
    return f"{years} year{'s' if years != 1 else ''} ago"


def _quarter_label(iso_date: str | None) -> str:
    """'2025-12-31' → 'Q4 25'"""
    if not iso_date:
        return ""
    try:
        d = date.fromisoformat(iso_date)
        q = (d.month - 1) // 3 + 1
        return f"Q{q} {d.strftime('%y')}"
    except (ValueError, TypeError):
        return ""


# ── Doc type → product label translation ─────────────────────────────────

_DOC_TYPE_LABELS = {
    "appendix_5b": "Quarterly update",
    "quarterly_activity": "Quarterly update",
    "issue_of_securities": "Securities issuance",
    "placement": "Placement",
    "resource_update": "Resource update",
    "study_scoping": "Scoping study",
    "study_pfs": "Pre-feasibility study",
    "study_dfs": "Feasibility study",
    "annual_report": "Annual report",
    "half_year_report": "Half year report",
    "presentation": "Corporate update",
}


def _translate_doc_type(doc_type: str | None) -> str:
    if not doc_type:
        return "Announcement"
    return _DOC_TYPE_LABELS.get(doc_type, "Announcement")


# ── Main endpoint ────────────────────────────────────────────────────────

@bp.route("/api/company/<ticker>/snapshot")
def api_company_snapshot(ticker: str):
    ticker = ticker.upper().strip()
    conn = get_connection()

    # Company row
    company = conn.execute(
        "SELECT * FROM companies WHERE ticker = ?", (ticker,)
    ).fetchone()

    if not company:
        conn.close()
        return jsonify({
            "ticker": ticker,
            "name": ticker,
            "exchange": "ASX",
            "meta_line": "",
            "has_data": False,
            "cash_history": [],
            "activity": [],
            "tabs": {"summary": True, "financials": False, "capital": False,
                     "operations": False, "documents": False, "holders": False},
        })

    company_name = company["name"] or ticker

    # Meta line from CSV
    meta = _load_meta().get(ticker, {})
    meta_parts = [
        meta.get("commodity", "").capitalize(),
        meta.get("flagship_project", ""),
        meta.get("jurisdiction", ""),
    ]
    meta_line = " · ".join(p for p in meta_parts if p)

    # ── Cash data (from 5B extractions only — these have burn data) ─────
    cash_rows = conn.execute(
        """SELECT cf.effective_date, cf.announcement_date, cf.cash, cf.debt,
                  cf.quarterly_opex_burn, cf.quarterly_invest_burn
           FROM company_financials cf
           JOIN companies c ON cf.company_id = c.company_id
           JOIN documents d ON cf.document_id = d.document_id
           WHERE c.ticker = ? AND cf.cash IS NOT NULL
                 AND d.doc_type IN ('appendix_5b', 'quarterly_activity')
           ORDER BY cf.effective_date ASC""",
        (ticker,),
    ).fetchall()

    cash_section = None
    if cash_rows:
        latest_cash_row = cash_rows[-1]
        cash_val = latest_cash_row["cash"]
        burn = latest_cash_row["quarterly_opex_burn"]
        as_of = latest_cash_row["effective_date"]

        # Runway
        runway_display = None
        if cash_val and burn and burn < 0:
            quarters = cash_val / abs(burn)
            runway_display = f"~{quarters:.0f} quarters of runway"
        elif cash_val and burn and burn > 0:
            # positive burn means net inflow
            runway_display = "Net cash positive"

        # Prose
        prose_parts = []
        if burn is not None:
            prose_parts.append(f"Burn {_fmt_aud(abs(burn))} per quarter")
            # Compare to prior
            if len(cash_rows) >= 2:
                prior_burn = cash_rows[-2]["quarterly_opex_burn"]
                if prior_burn is not None and prior_burn != 0:
                    if abs(burn) > abs(prior_burn):
                        prose_parts[0] += f", up from {_fmt_aud(abs(prior_burn))} prior"
                    elif abs(burn) < abs(prior_burn):
                        prose_parts[0] += f", down from {_fmt_aud(abs(prior_burn))} prior"
        if as_of:
            prose_parts.append(f"Treasury {_fmt_date_display(as_of)}")

        cash_section = {
            "amount_display": _fmt_aud(cash_val),
            "as_of_display": _fmt_date_display(as_of),
            "runway_display": runway_display,
            "prose": ". ".join(prose_parts) + "." if prose_parts else "",
        }

    # ── Capital data ────────────────────────────────────────────────────
    # 1) Prefer capital_structure_snapshots (2A-derived, authoritative)
    latest_cs = None
    try:
        latest_cs = conn.execute(
            """SELECT css.snapshot_date, css.shares_basic, css.shares_fd_naive,
                      css.options_outstanding, css.performance_rights_count,
                      css.doc_id
               FROM capital_structure_snapshots css
               WHERE css.ticker = ?
               ORDER BY css.snapshot_date DESC
               LIMIT 1""",
            (ticker,),
        ).fetchone()
    except Exception:
        pass  # table may not exist on older DBs

    # 2) Fallback: legacy company_financials (only when no 2A snapshot exists)
    legacy_cap = None
    if latest_cs is None:
        legacy_cap = conn.execute(
            """SELECT cf.effective_date, cf.announcement_date, cf.shares_basic
               FROM company_financials cf
               JOIN companies c ON cf.company_id = c.company_id
               WHERE c.ticker = ? AND cf.shares_basic IS NOT NULL
               ORDER BY cf.effective_date DESC
               LIMIT 1""",
            (ticker,),
        ).fetchone()

    capital_section = None
    if latest_cs:
        capital_section = {
            "shares_display": _fmt_shares(latest_cs["shares_basic"]),
            "shares_label": "shares on issue",
            "prose": "",
        }
    elif legacy_cap:
        capital_section = {
            "shares_display": _fmt_shares(legacy_cap["shares_basic"]),
            "shares_label": "shares on issue",
            "prose": "",
        }

    # ── Cash history for chart ───────────────────────────────────────────
    cash_history = []
    for row in cash_rows:
        ql = _quarter_label(row["effective_date"])
        burn_raw = row["quarterly_opex_burn"]
        cash_history.append({
            "quarter": ql,
            "quarter_end_display": _fmt_date_display(row["effective_date"]),
            "cash_balance": row["cash"],
            "burn": abs(burn_raw) if burn_raw is not None else None,
            "burn_display": _fmt_aud(abs(burn_raw)) if burn_raw is not None else None,
        })

    # ── Activity feed ────────────────────────────────────────────────────
    # Pull recent parsed documents as activity events
    activity_docs = conn.execute(
        """SELECT d.document_id, d.doc_type, d.header, d.announcement_date, d.url,
                  cf.cash, cf.quarterly_opex_burn, cf.shares_basic
           FROM documents d
           LEFT JOIN company_financials cf ON cf.document_id = d.document_id
           WHERE d.ticker = ? AND d.parse_status = 'parsed'
           ORDER BY d.announcement_date DESC
           LIMIT 20""",
        (ticker,),
    ).fetchall()

    activity = []
    for doc in activity_docs:
        headline = _translate_doc_type(doc["doc_type"])
        ann_date = doc["announcement_date"]
        rel_date = _relative_date(ann_date)

        # Compose detail prose based on doc type
        detail = ""
        if doc["doc_type"] in ("appendix_5b", "quarterly_activity"):
            parts = []
            if doc["cash"] is not None:
                parts.append(f"Closed at {_fmt_aud(doc['cash'])} cash")
            if doc["quarterly_opex_burn"] is not None:
                parts.append(f"Burn {_fmt_aud(abs(doc['quarterly_opex_burn']))}")
            detail = ". ".join(parts) + "." if parts else ""
        elif doc["doc_type"] in ("issue_of_securities", "placement"):
            if doc["shares_basic"] is not None:
                detail = f"{_fmt_shares(doc['shares_basic'])} shares on issue post-event."
        else:
            # Generic: use headline from ASX
            detail = doc["header"] or ""

        if not rel_date and not detail:
            continue

        source_url = doc["url"] if doc["url"] and not doc["url"].startswith("upload://") else None

        activity.append({
            "id": str(doc["document_id"]),
            "headline": headline,
            "relative_date": rel_date,
            "detail": detail,
            "source_url": source_url,
        })

    # ── Tab visibility ───────────────────────────────────────────────────
    has_financials = len(cash_rows) > 0
    has_capital = capital_section is not None
    doc_count = conn.execute(
        "SELECT COUNT(*) as n FROM documents WHERE ticker = ?", (ticker,)
    ).fetchone()["n"]

    tabs = {
        "summary": True,
        "financials": has_financials,
        "capital": has_capital,
        "operations": False,
        "documents": doc_count > 0,
        "holders": False,
    }

    conn.close()

    has_data = has_financials or has_capital or doc_count > 0

    snapshot = {
        "ticker": ticker,
        "name": company_name,
        "exchange": "ASX",
        "meta_line": meta_line,
        "has_data": has_data,
        "cash_history": cash_history,
        "activity": activity[:10],
        "tabs": tabs,
    }

    if cash_section:
        snapshot["cash"] = cash_section
    if capital_section:
        snapshot["capital"] = capital_section

    return jsonify(snapshot)

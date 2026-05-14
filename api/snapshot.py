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

    # ── Projects & resources ───────────────────────────────────────────
    projects_data = []
    company_id = company["company_id"]
    projects = conn.execute(
        "SELECT * FROM projects WHERE company_id = ? ORDER BY created_at DESC",
        (company_id,),
    ).fetchall()

    for proj in projects:
        pid = proj["project_id"]

        # Commodities
        commodities = conn.execute(
            "SELECT commodity, is_primary FROM project_commodities WHERE project_id = ? ORDER BY is_primary DESC",
            (pid,),
        ).fetchall()
        commodity_list = [c["commodity"] for c in commodities]
        primary_commodity = next((c["commodity"] for c in commodities if c["is_primary"]), None)

        # Latest resource estimate
        resource_rows = conn.execute(
            """SELECT category, tonnes, grade, grade_unit, contained_metal, contained_metal_unit,
                      effective_date, resource_or_reserve, section
               FROM resources WHERE project_id = ?
               ORDER BY effective_date DESC, resource_id ASC""",
            (pid,),
        ).fetchall()

        resources_out = []
        latest_date = None
        for r in resource_rows:
            if latest_date is None:
                latest_date = r["effective_date"]
            # Only show most recent estimate
            if r["effective_date"] != latest_date:
                break
            resources_out.append({
                "category": r["category"],
                "tonnes_mt": r["tonnes"],
                "grade": r["grade"],
                "grade_unit": r["grade_unit"],
                "contained_metal": r["contained_metal"],
                "contained_metal_unit": r["contained_metal_unit"],
                "type": r["resource_or_reserve"],
                "section": r["section"],
            })

        # Latest study
        study_row = conn.execute(
            """SELECT study_stage, study_date, mine_life_years, annual_production,
                      recovery_pct, initial_capex, sustaining_capex, opex,
                      post_tax_npv, pre_tax_npv, irr_pct, payback_years,
                      aisc_per_unit, aisc_unit, assumed_price_deck, assumed_fx,
                      reporting_currency, discount_rate_pct, extraction_model
               FROM studies WHERE project_id = ?
               ORDER BY study_date DESC LIMIT 1""",
            (pid,),
        ).fetchone()

        study_out = None
        if study_row:
            import json as _json
            price_deck = None
            if study_row["assumed_price_deck"]:
                try:
                    price_deck = _json.loads(study_row["assumed_price_deck"])
                except Exception:
                    pass
            study_out = {
                "study_type": study_row["study_stage"],
                "study_date": _fmt_date_display(study_row["study_date"]),
                "reporting_currency": study_row["reporting_currency"],
                "discount_rate_pct": study_row["discount_rate_pct"],
                "post_tax_npv": study_row["post_tax_npv"],
                "pre_tax_npv": study_row["pre_tax_npv"],
                "irr_pct": study_row["irr_pct"],
                "payback_years": study_row["payback_years"],
                "initial_capex": study_row["initial_capex"],
                "sustaining_capex": study_row["sustaining_capex"],
                "opex": study_row["opex"],
                "aisc_per_unit": study_row["aisc_per_unit"],
                "aisc_unit": study_row["aisc_unit"],
                "mine_life_years": study_row["mine_life_years"],
                "annual_production": study_row["annual_production"],
                "recovery_pct": study_row["recovery_pct"],
                "assumed_fx": study_row["assumed_fx"],
                "price_assumptions": price_deck,
            }

        # Latest revaluation for this project
        reval_out = None
        try:
            import json as _rjson
            reval_row = conn.execute(
                """SELECT r.commodity, r.price_dfs, r.price_spot, r.fx_rate,
                          r.annual_production, r.annual_production_unit,
                          r.mine_life_years, r.discount_rate_pct, r.tax_rate_pct,
                          r.annuity_factor, r.npv_dfs, r.npv_spot,
                          r.npv_uplift, r.npv_uplift_pct,
                          r.method_version, r.warnings, r.computed_at,
                          cp.source AS spot_source, cp.fetched_at AS spot_fetched_at,
                          s.reporting_currency
                   FROM revaluations r
                   JOIN commodity_prices cp ON cp.price_id = r.price_spot_id
                   JOIN studies s ON s.study_id = r.study_id
                   WHERE r.project_id = ?
                   ORDER BY r.computed_at DESC LIMIT 1""",
                (pid,),
            ).fetchone()
            if reval_row:
                warnings = []
                if reval_row["warnings"]:
                    try:
                        warnings = _rjson.loads(reval_row["warnings"])
                    except Exception:
                        pass
                price_unit = "USD/oz" if reval_row["commodity"] == "Au" else "USD/lb"
                reval_out = {
                    "commodity": reval_row["commodity"],
                    "price_dfs": reval_row["price_dfs"],
                    "price_spot": reval_row["price_spot"],
                    "price_unit": price_unit,
                    "fx_rate": reval_row["fx_rate"],
                    "annual_production": reval_row["annual_production"],
                    "annual_production_unit": reval_row["annual_production_unit"],
                    "mine_life_years": reval_row["mine_life_years"],
                    "discount_rate_pct": reval_row["discount_rate_pct"],
                    "tax_rate_pct": reval_row["tax_rate_pct"],
                    "annuity_factor": reval_row["annuity_factor"],
                    "npv_dfs": reval_row["npv_dfs"],
                    "npv_spot": reval_row["npv_spot"],
                    "npv_uplift": reval_row["npv_uplift"],
                    "npv_uplift_pct": reval_row["npv_uplift_pct"],
                    "reporting_currency": reval_row["reporting_currency"],
                    "method_version": reval_row["method_version"],
                    "computed_at": reval_row["computed_at"],
                    "spot_source": reval_row["spot_source"],
                    "spot_fetched_at": reval_row["spot_fetched_at"],
                    "warnings": warnings,
                }
        except Exception:
            pass  # revaluations table may not exist on older DBs

        # sqlite3.Row doesn't support .get(); safely read optional columns
        try:
            source = proj["source"]
        except (IndexError, KeyError):
            source = None

        projects_data.append({
            "name": proj["project_name"],
            "stage": proj["stage"],
            "state": proj["state"],
            "country": proj["country"] or "Australia",
            "source": source,
            "commodities": commodity_list,
            "primary_commodity": primary_commodity,
            "resources": resources_out,
            "resource_date": _fmt_date_display(latest_date) if latest_date else None,
            "study": study_out,
            "revaluation": reval_out,
        })

    has_projects = len(projects_data) > 0

    # Dynamic meta_line from projects if static CSV is empty
    if not meta_line and has_projects:
        p = projects_data[0]
        meta_parts = [
            p["primary_commodity"] or "",
            p["name"],
            p["state"] or p["country"],
        ]
        meta_line = " · ".join(part for part in meta_parts if part)

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
        "operations": has_projects,
        "documents": doc_count > 0,
        "holders": False,
    }

    conn.close()

    has_data = has_financials or has_capital or doc_count > 0 or has_projects

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
    if has_projects:
        snapshot["projects"] = projects_data

    return jsonify(snapshot)

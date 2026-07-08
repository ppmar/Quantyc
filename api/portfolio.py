"""
Portfolio endpoints — project portfolio view per company.

GET /api/portfolio/companies          — aggregated list with filters
GET /api/portfolio/companies/<ticker> — per-project breakdown
"""

import json
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from db import get_connection

bp = Blueprint("portfolio", __name__)

STAGE_ORDER = [
    "production", "care_and_maintenance", "development", "feasibility",
    "advanced_exploration", "exploration", "unknown",
]

_STAGE_RANK = {s: i for i, s in enumerate(STAGE_ORDER)}

# Below this study NPV (reporting-currency millions), an uplift % is dominated by
# its small base and must not be ranked on % alone. Tunable; not ticker-specific.
_LOW_BASE_NPV_M = 50.0

# Studies older than this have an outdated price deck AND cost base: their uplift
# means "restudy needed", not signal. Mirrors revaluation.pipeline.STALE_STUDY_YEARS.
_STALE_STUDY_YEARS = 3.0


def _npv_review_reasons(reason: str | None) -> str:
    """Keep only review reasons that cast doubt on the NPV base used by the reval."""
    if not reason:
        return ""
    keep = [r.strip() for r in reason.split(";")
            if r.strip() and ("npv_ge_pre" in r or "implied_tax_gap" in r or "npv_post" in r)]
    return "; ".join(keep)


def _most_advanced_stage(stages: list[str]) -> str | None:
    if not stages:
        return None
    valid = [s for s in stages if s in _STAGE_RANK]
    if not valid:
        return None
    return min(valid, key=lambda s: _STAGE_RANK[s])


def _split_csv(val: str | None) -> list[str]:
    if not val:
        return []
    return [v.strip() for v in val.split(",") if v.strip()]


def _query_db(sql, params=()):
    conn = get_connection()
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _query_one(sql, params=()):
    conn = get_connection()
    row = conn.execute(sql, params).fetchone()
    conn.close()
    return dict(row) if row else None


# ─── GET /api/portfolio/companies ────────────────────────────────────

@bp.route("/api/portfolio/companies")
def portfolio_companies():
    rows = _query_db("""
        WITH active_projects AS (
            SELECT p.*
            FROM projects p
            WHERE EXISTS (SELECT 1 FROM studies WHERE project_id = p.project_id)
               OR EXISTS (SELECT 1 FROM resources WHERE project_id = p.project_id)
        ),
        latest_study AS (
            SELECT s.project_id,
                   s.study_stage,
                   s.study_confidence_tier,
                   COALESCE(s.study_date, d.announcement_date) AS effective_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY s.project_id
                       ORDER BY COALESCE(s.study_date, d.announcement_date) DESC
                   ) AS rn
            FROM studies s
            LEFT JOIN documents d ON d.document_id = s.document_id
        ),
        company_latest_study AS (
            -- The company row's stage and date must come from the SAME study (the
            -- most recent across its projects; ties: definitive > indicative > other).
            -- Two independent MAX() aggregates could pair "PFS" (alphabetical max)
            -- with another study's date (I3).
            SELECT p.company_id, ls.study_stage, ls.effective_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY p.company_id
                       ORDER BY ls.effective_date DESC,
                                CASE ls.study_confidence_tier
                                     WHEN 'definitive' THEN 0
                                     WHEN 'indicative' THEN 1
                                     ELSE 2 END
                   ) AS crn
            FROM latest_study ls
            JOIN projects p ON p.project_id = ls.project_id
            WHERE ls.rn = 1
        )
        SELECT c.ticker,
               c.name AS company_name,
               COUNT(DISTINCT ap.project_id) AS active_project_count,
               (SELECT COUNT(*) FROM projects WHERE company_id = c.company_id) AS total_project_count,
               GROUP_CONCAT(DISTINCT ap.stage) AS stages_csv,
               GROUP_CONCAT(DISTINCT ap.country) AS countries_csv,
               GROUP_CONCAT(DISTINCT ap.state) AS states_csv,
               GROUP_CONCAT(DISTINCT ap.region) AS regions_csv,
               MAX(cls.effective_date) AS latest_study_date,
               MAX(cls.study_stage) AS latest_study_stage,
               EXISTS (
                   SELECT 1 FROM studies st
                   JOIN projects pp ON pp.project_id = st.project_id
                   WHERE pp.company_id = c.company_id
                     AND st.study_confidence_tier IN ('definitive', 'indicative')
               ) AS has_dfs_pfs,
               (
                   SELECT COUNT(DISTINCT s2.project_id)
                   FROM studies s2
                   JOIN projects p2 ON p2.project_id = s2.project_id
                   WHERE p2.company_id = c.company_id
               ) AS study_project_count
        FROM companies c
        LEFT JOIN active_projects ap ON ap.company_id = c.company_id
        LEFT JOIN company_latest_study cls
               ON cls.company_id = c.company_id AND cls.crn = 1
        GROUP BY c.company_id
        HAVING active_project_count > 0
        ORDER BY c.ticker
    """)

    # Primary commodities per company — ACTIVE projects only (same filter as the
    # active_projects CTE above). Inactive projects aren't displayed, so their
    # commodities must not tag the company (a dormant lithium tenement was tagging
    # gold producers with Li).
    commodity_map: dict[str, list[str]] = {}
    commodity_rows = _query_db("""
        SELECT c.ticker, pc.commodity
        FROM project_commodities pc
        JOIN projects p ON p.project_id = pc.project_id
        JOIN companies c ON c.company_id = p.company_id
        WHERE pc.is_primary = 1
          AND (EXISTS (SELECT 1 FROM studies WHERE project_id = p.project_id)
               OR EXISTS (SELECT 1 FROM resources WHERE project_id = p.project_id))
    """)
    for r in commodity_rows:
        comms = commodity_map.setdefault(r["ticker"], [])
        if r["commodity"] not in comms:
            comms.append(r["commodity"])

    # Revaluation of each company's LATEST revaluable study — the most-recent
    # definitive/indicative study, not whichever row was written last. A
    # superseded PFS (small NPV base → huge uplift %) must not win over the
    # current DFS. Matches the per-project rule in api/snapshot.py.
    reval_map: dict[str, dict] = {}
    reval_rows = _query_db("""
        SELECT c.ticker, r.npv_dfs, r.npv_spot, r.npv_uplift, r.npv_uplift_pct,
               r.price_spot, r.price_dfs, r.commodity, r.computed_at, r.warnings,
               s.reporting_currency, s.study_date, s.needs_review, s.review_reason,
               ROW_NUMBER() OVER (
                   PARTITION BY c.company_id
                   ORDER BY
                       CASE WHEN s.study_confidence_tier IN ('definitive', 'indicative')
                            THEN 0 ELSE 1 END,
                       COALESCE(s.study_date, '') DESC,
                       r.computed_at DESC
               ) AS rn
        FROM revaluations r
        JOIN companies c ON c.company_id = r.company_id
        LEFT JOIN studies s ON s.study_id = r.study_id
    """)
    for rv in reval_rows:
        if rv["rn"] == 1:
            npv_dfs = rv["npv_dfs"]
            # Basket coverage (first_order_v4) parsed from the persisted warning; 100 for
            # single-commodity v3 rows. A partial basket isn't a full uplift signal (PR5).
            coverage_pct = 100.0
            if rv["warnings"]:
                try:
                    for w in json.loads(rv["warnings"]):
                        if isinstance(w, str) and w.startswith("coverage_pct:"):
                            coverage_pct = float(w.split(":", 1)[1])
                            break
                except (ValueError, TypeError):
                    pass
            # Staleness computed at read time from study_date so it covers every
            # existing reval row, not just freshly recomputed ones. An old study's
            # deck AND cost base are outdated: a big uplift off a stale study means
            # "restudy needed", not a tradeable signal.
            study_age_years = None
            if rv["study_date"]:
                try:
                    study_age_years = round(
                        (datetime.now(timezone.utc).date()
                         - datetime.strptime(rv["study_date"][:10], "%Y-%m-%d").date()).days
                        / 365.25, 1)
                except ValueError:
                    pass
            deck_far_below_spot = (
                rv["price_dfs"] is not None and rv["price_spot"] is not None
                and rv["price_dfs"] < rv["price_spot"] / 2)
            reval_map[rv["ticker"]] = {
                "npv_dfs": npv_dfs,
                "npv_spot": rv["npv_spot"],
                "npv_uplift_abs": rv["npv_uplift"],
                "npv_uplift_pct": rv["npv_uplift_pct"],
                "commodity": rv["commodity"],
                "price_spot": rv["price_spot"],
                "reporting_currency": rv["reporting_currency"],
                "coverage_pct": coverage_pct,
                "is_partial_basket": coverage_pct < 100.0,
                "study_age_years": study_age_years,
                "is_stale_study": (study_age_years is not None and study_age_years > _STALE_STUDY_YEARS),
                "deck_far_below_spot": deck_far_below_spot,
                # A reval built on a review-flagged study is a weak signal ONLY when the
                # flag impugns the NPV base itself (BTR: post_tax == pre_tax; MM8: implied
                # tax gap). missing_tax_rate / missing_pre_tax_npv are benign here — the
                # reval defaults the tax rate and never uses pre-tax NPV.
                "study_needs_review": bool(_npv_review_reasons(rv["review_reason"]) if rv["needs_review"] else None),
                "study_review_reason": (_npv_review_reasons(rv["review_reason"]) or None) if rv["needs_review"] else None,
                # A huge % on a tiny base is not a strong signal — flag it (I6).
                "low_base": (npv_dfs is not None and npv_dfs < _LOW_BASE_NPV_M),
            }

    companies = []
    for r in rows:
        stages_list = _split_csv(r["stages_csv"])
        most_advanced = _most_advanced_stage(stages_list)
        stage_breakdown = {}
        for s in stages_list:
            stage_breakdown[s] = stage_breakdown.get(s, 0) + 1

        active_count = r["active_project_count"]
        companies.append({
            "ticker": r["ticker"],
            "company_name": r["company_name"],
            "active_project_count": active_count,
            "study_project_count": r["study_project_count"] or 0,
            "total_project_count": r["total_project_count"],
            "is_single_project": active_count == 1,
            "most_advanced_stage": most_advanced,
            "stage_breakdown": stage_breakdown,
            "primary_commodities": commodity_map.get(r["ticker"], []),
            "countries": _split_csv(r["countries_csv"]),
            "states": _split_csv(r["states_csv"]),
            "regions": _split_csv(r["regions_csv"]),
            "has_recent_study": bool(r["latest_study_date"] and r["latest_study_date"] >= "2024-01-01"),
            "has_dfs_pfs": bool(r["has_dfs_pfs"]),
            "latest_study_date": r["latest_study_date"],
            "latest_study_stage": r["latest_study_stage"],
            "latest_revaluation": reval_map.get(r["ticker"]),
        })

    # Apply filters
    single_only = request.args.get("single_project_only", "false").lower() == "true"
    min_stage = request.args.get("min_stage")
    commodity = request.args.get("commodity")
    country = request.args.get("country")
    recent_study = request.args.get("has_recent_study", "false").lower() == "true"
    study_after = request.args.get("study_after")  # ISO date 'YYYY-MM-DD'
    supported_only = request.args.get("supported_only", "false").lower() == "true"
    sort_key = request.args.get("sort", "most_advanced_stage_desc")
    limit = min(int(request.args.get("limit", "200")), 500)

    if single_only:
        companies = [c for c in companies if c["is_single_project"]]
    if min_stage and min_stage in _STAGE_RANK:
        # Exact match: picking "feasibility" returns companies whose most-advanced
        # stage IS feasibility — not more-advanced or less-advanced ones.
        companies = [
            c for c in companies
            if c["most_advanced_stage"] == min_stage
        ]
    if commodity:
        companies = [c for c in companies if commodity in c["primary_commodities"]]
    if country:
        companies = [c for c in companies if country in c["countries"]]
    if recent_study:
        companies = [c for c in companies if c["has_recent_study"]]
    if study_after:
        companies = [
            c for c in companies
            if c["latest_study_date"] and c["latest_study_date"] >= study_after
        ]
    if supported_only:
        _SUPPORTED = {"Au", "Ag", "Cu", "Pd", "Pt", "U3O8"}
        companies = [
            c for c in companies
            if c["has_dfs_pfs"] and any(cm in _SUPPORTED for cm in c["primary_commodities"])
        ]

    # Sort
    def _reval_field(c, field):
        rv = c.get("latest_revaluation")
        return rv.get(field) if rv else None

    if sort_key == "most_advanced_stage_desc":
        companies.sort(key=lambda c: _STAGE_RANK.get(c["most_advanced_stage"] or "unknown", 99))
    elif sort_key == "project_count":
        companies.sort(key=lambda c: -c["active_project_count"])
    elif sort_key == "ticker":
        companies.sort(key=lambda c: c["ticker"])
    elif sort_key == "uplift_abs_desc":
        # Best signal: rank by absolute uplift, not %. `is not None` — 0.0 is a real value.
        def _abs_key(c):
            v = _reval_field(c, "npv_uplift_abs")
            return -v if v is not None else float("inf")
        companies.sort(key=_abs_key)
    elif sort_key == "uplift_pct_desc":
        # Pure % sort. Signal quality is carried visually (age dot + chips), not by rank.
        def _pct_key(c):
            pct = _reval_field(c, "npv_uplift_pct")
            return -pct if pct is not None else float("inf")   # no-reval last; 0.0 is a real value
        companies.sort(key=_pct_key)

    total = len(companies)
    companies = companies[:limit]

    return jsonify({
        "filters_applied": {
            "single_project_only": single_only,
            "min_stage": min_stage,
            "commodity": commodity,
            "country": country,
            "has_recent_study": recent_study,
            "study_after": study_after,
            "supported_only": supported_only,
            "sort": sort_key,
            "limit": limit,
        },
        "as_of": datetime.now(timezone.utc).isoformat(),
        "total_companies": total,
        "companies": companies,
    })


# ─── GET /api/portfolio/companies/<ticker> ───────────────────────────

@bp.route("/api/portfolio/companies/<ticker>")
def portfolio_company_detail(ticker: str):
    ticker = ticker.upper()

    company = _query_one(
        "SELECT company_id, ticker, name FROM companies WHERE ticker = ?", (ticker,)
    )
    if not company:
        return jsonify({"error": "Company not found"}), 404

    cid = company["company_id"]

    projects = _query_db("""
        SELECT p.project_id, p.project_name, p.stage, p.stage_source, p.stage_inferred_at,
               p.country, p.state, p.region, p.ownership_pct
        FROM projects p
        WHERE p.company_id = ?
        ORDER BY p.project_name
    """, (cid,))

    result_projects = []
    for proj in projects:
        pid = proj["project_id"]

        # Active check
        has_study = _query_one("SELECT 1 FROM studies WHERE project_id = ?", (pid,))
        has_resource = _query_one("SELECT 1 FROM resources WHERE project_id = ?", (pid,))
        is_active = bool(has_study or has_resource)

        # Commodities
        commodities = _query_db(
            "SELECT commodity, is_primary FROM project_commodities WHERE project_id = ?", (pid,)
        )
        primary = next((c["commodity"] for c in commodities if c["is_primary"]), None)
        all_comms = [c["commodity"] for c in commodities]

        # Multi-commodity bucket (matches the revaluation guard: distinct commodities
        # across declared commodities OR resources). These are not yet revaluable by the
        # single-commodity model — surfaced separately until first_order_v4 values them.
        n_distinct = _query_one(
            "SELECT COUNT(DISTINCT commodity) AS n FROM ("
            "  SELECT commodity FROM project_commodities WHERE project_id = ?"
            "  UNION SELECT commodity FROM resources WHERE project_id = ?)",
            (pid, pid),
        )
        is_multi_commodity = bool(n_distinct and n_distinct["n"] > 1)

        # Latest study
        latest_study = _query_one("""
            SELECT s.study_stage, s.study_date, s.study_confidence_tier,
                   s.post_tax_npv,
                   COALESCE(s.reporting_currency, 'AUD') AS reporting_currency
            FROM studies s
            WHERE s.project_id = ?
            ORDER BY COALESCE(s.study_date, '') DESC
            LIMIT 1
        """, (pid,))

        # Latest resource
        latest_resource = _query_one("""
            SELECT commodity, category, tonnes, grade, grade_unit,
                   contained_metal, contained_metal_unit, effective_date
            FROM resources
            WHERE project_id = ?
            ORDER BY effective_date DESC
            LIMIT 1
        """, (pid,))

        # Document counts
        doc_counts = _query_one("""
            SELECT
                (SELECT COUNT(*) FROM studies WHERE project_id = ?) AS studies,
                (SELECT COUNT(*) FROM resources WHERE project_id = ?) AS resources,
                (SELECT COUNT(*) FROM documents WHERE ticker = ?) AS all_documents
        """, (pid, pid, ticker))

        # Latest revaluation — of the project's latest REVALUABLE study (most-recent
        # definitive/indicative that has reval rows), NOT whichever row was written
        # last (RMX: the 2014 DFS row landed microseconds after the 2016 PFS row and
        # won on computed_at — the same PDI bug, on the detail route).
        latest_reval = _query_one("""
            SELECT r.commodity, r.price_dfs, r.price_spot, r.npv_dfs, r.npv_spot,
                   r.npv_uplift_pct, r.computed_at, r.method_version,
                   r.study_confidence_tier, s.study_date AS reval_study_date
            FROM revaluations r
            LEFT JOIN studies s ON s.study_id = r.study_id
            WHERE r.project_id = ?
            ORDER BY CASE WHEN s.study_confidence_tier IN ('definitive', 'indicative')
                          THEN 0 ELSE 1 END,
                     COALESCE(s.study_date, '') DESC,
                     r.computed_at DESC
            LIMIT 1
        """, (pid,))
        if latest_reval:
            # Same read-time weak-signal fields as the list (I5: list and detail
            # render weak signals identically, so both need the same inputs).
            age = None
            if latest_reval.get("reval_study_date"):
                try:
                    age = round(
                        (datetime.now(timezone.utc).date()
                         - datetime.strptime(latest_reval["reval_study_date"][:10], "%Y-%m-%d").date()).days
                        / 365.25, 1)
                except ValueError:
                    pass
            latest_reval["study_age_years"] = age
            latest_reval["is_stale_study"] = age is not None and age > _STALE_STUDY_YEARS
            latest_reval["low_base"] = (latest_reval["npv_dfs"] is not None
                                        and latest_reval["npv_dfs"] < _LOW_BASE_NPV_M)

        # Stage confidence from latest inference
        stage_confidence = None
        if proj["stage_source"] == "gemini_inferred":
            inf = _query_one("""
                SELECT stage_confidence FROM project_stage_inferences
                WHERE project_id = ? ORDER BY inferred_at DESC LIMIT 1
            """, (pid,))
            if inf:
                stage_confidence = inf["stage_confidence"]

        result_projects.append({
            "project_name": proj["project_name"],
            "stage": proj["stage"],
            "stage_source": proj["stage_source"],
            "stage_confidence": stage_confidence,
            "stage_inferred_at": proj["stage_inferred_at"],
            "country": proj["country"],
            "state": proj["state"],
            "region": proj["region"],
            "primary_commodity": primary,
            "all_commodities": all_comms,
            "is_multi_commodity": is_multi_commodity,
            "ownership_pct": proj["ownership_pct"],
            "is_active": is_active,
            "latest_study": latest_study,
            "latest_resource": latest_resource,
            "latest_revaluation": latest_reval,
            "document_counts": doc_counts or {"studies": 0, "resources": 0, "all_documents": 0},
        })

    return jsonify({
        "ticker": ticker,
        "company_name": company["name"],
        "as_of": datetime.now(timezone.utc).isoformat(),
        "projects": result_projects,
    })

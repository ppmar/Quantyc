"""
Portfolio endpoints — project portfolio view per company.

GET /api/portfolio/companies          — aggregated list with filters
GET /api/portfolio/companies/<ticker> — per-project breakdown
"""

from datetime import datetime, timezone

from flask import Blueprint, jsonify, request

from db import get_connection

bp = Blueprint("portfolio", __name__)

STAGE_ORDER = [
    "production", "care_and_maintenance", "development", "feasibility",
    "advanced_exploration", "exploration", "unknown",
]

_STAGE_RANK = {s: i for i, s in enumerate(STAGE_ORDER)}


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
                   COALESCE(s.study_date, d.announcement_date) AS effective_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY s.project_id
                       ORDER BY COALESCE(s.study_date, d.announcement_date) DESC
                   ) AS rn
            FROM studies s
            LEFT JOIN documents d ON d.document_id = s.document_id
        )
        SELECT c.ticker,
               c.name AS company_name,
               COUNT(DISTINCT ap.project_id) AS active_project_count,
               (SELECT COUNT(*) FROM projects WHERE company_id = c.company_id) AS total_project_count,
               GROUP_CONCAT(DISTINCT ap.stage) AS stages_csv,
               GROUP_CONCAT(DISTINCT ap.country) AS countries_csv,
               GROUP_CONCAT(DISTINCT ap.state) AS states_csv,
               GROUP_CONCAT(DISTINCT ap.region) AS regions_csv,
               MAX(CASE WHEN ls.rn = 1 THEN ls.effective_date END) AS latest_study_date,
               MAX(CASE WHEN ls.rn = 1 THEN ls.study_stage END) AS latest_study_stage
        FROM companies c
        LEFT JOIN active_projects ap ON ap.company_id = c.company_id
        LEFT JOIN latest_study ls ON ls.project_id = ap.project_id AND ls.rn = 1
        GROUP BY c.company_id
        HAVING active_project_count > 0
        ORDER BY c.ticker
    """)

    # Primary commodities per company
    commodity_map: dict[str, list[str]] = {}
    commodity_rows = _query_db("""
        SELECT c.ticker, pc.commodity
        FROM project_commodities pc
        JOIN projects p ON p.project_id = pc.project_id
        JOIN companies c ON c.company_id = p.company_id
        WHERE pc.is_primary = 1
    """)
    for r in commodity_rows:
        comms = commodity_map.setdefault(r["ticker"], [])
        if r["commodity"] not in comms:
            comms.append(r["commodity"])

    # Latest revaluation per company (best uplift across projects)
    reval_map: dict[str, dict] = {}
    reval_rows = _query_db("""
        SELECT c.ticker, r.npv_dfs, r.npv_spot, r.npv_uplift_pct, r.price_spot, r.commodity,
               r.computed_at,
               ROW_NUMBER() OVER (PARTITION BY c.company_id ORDER BY r.computed_at DESC) AS rn
        FROM revaluations r
        JOIN companies c ON c.company_id = r.company_id
    """)
    for rv in reval_rows:
        if rv["rn"] == 1:
            reval_map[rv["ticker"]] = {
                "npv_dfs": rv["npv_dfs"],
                "npv_spot": rv["npv_spot"],
                "npv_uplift_pct": rv["npv_uplift_pct"],
                "commodity": rv["commodity"],
                "price_spot": rv["price_spot"],
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
            "total_project_count": r["total_project_count"],
            "is_single_project": active_count == 1,
            "most_advanced_stage": most_advanced,
            "stage_breakdown": stage_breakdown,
            "primary_commodities": commodity_map.get(r["ticker"], []),
            "countries": _split_csv(r["countries_csv"]),
            "states": _split_csv(r["states_csv"]),
            "regions": _split_csv(r["regions_csv"]),
            "has_recent_study": bool(r["latest_study_date"] and r["latest_study_date"] >= "2024-01-01"),
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
    sort_key = request.args.get("sort", "most_advanced_stage_desc")
    limit = min(int(request.args.get("limit", "200")), 500)

    if single_only:
        companies = [c for c in companies if c["is_single_project"]]
    if min_stage and min_stage in _STAGE_RANK:
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

    # Sort
    if sort_key == "most_advanced_stage_desc":
        companies.sort(key=lambda c: _STAGE_RANK.get(c["most_advanced_stage"] or "unknown", 99))
    elif sort_key == "project_count":
        companies.sort(key=lambda c: -c["active_project_count"])
    elif sort_key == "ticker":
        companies.sort(key=lambda c: c["ticker"])

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

        # Latest revaluation
        latest_reval = _query_one("""
            SELECT commodity, price_dfs, price_spot, npv_dfs, npv_spot,
                   npv_uplift_pct, computed_at, method_version,
                   study_confidence_tier
            FROM revaluations
            WHERE project_id = ?
            ORDER BY computed_at DESC
            LIMIT 1
        """, (pid,))

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

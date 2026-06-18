#!/usr/bin/env python3
"""
Backfill project stages via Gemini classifier.

Reads projects from DB, builds evidence per project, calls the classifier,
writes results back. Idempotent with caching.

Usage:
    python -m scripts.backfill_project_stages              # all unclassified
    python -m scripts.backfill_project_stages --ticker DEG  # one company
    python -m scripts.backfill_project_stages --dry-run --limit 5
    python -m scripts.backfill_project_stages --all         # re-classify everything
"""
import argparse
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from db import init_db, get_connection
from parsers.project_stage_classifier import (
    ClassificationError,
    InsufficientEvidenceError,
    ProjectEvidence,
    ProjectStageInference,
    StudyEvidence,
    ResourceEvidence,
    AnnEvidence,
    classify_project,
)
from pipeline.stage_floor import (
    study_floor_stage,
    most_advanced,
    apply_floor,
    production_floor,
    STAGE_ORDER,
)

_PROD_RANK = {s: i for i, s in enumerate(STAGE_ORDER)}


def _best_study_tier(conn, project_id: int) -> str | None:
    """Most-advanced study confidence tier present for the project, or None."""
    rows = conn.execute(
        """SELECT study_confidence_tier FROM studies
           WHERE project_id = ? AND study_confidence_tier IS NOT NULL""",
        (project_id,),
    ).fetchall()
    tiers = {r["study_confidence_tier"] for r in rows}
    for t in ("definitive", "indicative", "conceptual"):  # most → least advanced
        if t in tiers:
            return t
    return None


def _apply_migrations():
    conn = get_connection()
    migrations_dir = Path(__file__).resolve().parent.parent / "db" / "migrations"
    if migrations_dir.exists():
        for m in sorted(migrations_dir.glob("*.sql")):
            try:
                conn.executescript(m.read_text())
            except Exception:
                pass
    conn.close()


def _fetch_projects(ticker: str | None, classify_all: bool, limit: int | None) -> list[dict]:
    conn = get_connection()
    if classify_all:
        sql = """
            SELECT p.project_id, p.project_name, p.company_id, p.country, p.state,
                   p.stage, p.stage_source, p.stage_inferred_at,
                   c.ticker
            FROM projects p
            JOIN companies c ON c.company_id = p.company_id
        """
        params: list = []
    else:
        sql = """
            SELECT p.project_id, p.project_name, p.company_id, p.country, p.state,
                   p.stage, p.stage_source, p.stage_inferred_at,
                   c.ticker
            FROM projects p
            JOIN companies c ON c.company_id = p.company_id
            WHERE (p.stage IS NULL
               OR p.stage_source IN ('ozmin', 'study_floor')
               OR p.stage_source IS NULL)
              AND COALESCE(p.stage_source, '') != 'insufficient_evidence'
        """
        params = []

    if ticker:
        sql += " AND c.ticker = ?" if "WHERE" in sql else " WHERE c.ticker = ?"
        params.append(ticker.upper())

    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def _should_skip_cached(project: dict) -> bool:
    """Skip if already classified (gemini_inferred or study_floor) and no new
    evidence since the last attempt — avoids re-spending Gemini on the same
    floored projects every run. A study_floor project with no recorded attempt
    (stage_inferred_at NULL, e.g. floored at study-ingest) is NOT skipped."""
    if project["stage_source"] not in ("gemini_inferred", "study_floor"):
        return False
    if not project["stage_inferred_at"]:
        return False

    conn = get_connection()
    inferred_at = project["stage_inferred_at"]
    pid = project["project_id"]

    # Check for new studies
    new_study = conn.execute(
        "SELECT 1 FROM studies WHERE project_id = ? AND created_at > ? LIMIT 1",
        (pid, inferred_at),
    ).fetchone()

    # Check for new resources
    new_resource = conn.execute(
        "SELECT 1 FROM resources WHERE project_id = ? AND created_at > ? LIMIT 1",
        (pid, inferred_at),
    ).fetchone()

    # Check for new documents (announcements)
    new_doc = conn.execute("""
        SELECT 1 FROM documents d
        JOIN companies c ON c.ticker = d.ticker
        JOIN projects p ON p.company_id = c.company_id AND p.project_id = ?
        WHERE d.announcement_date > ?
        LIMIT 1
    """, (pid, inferred_at)).fetchone()

    conn.close()
    return not (new_study or new_resource or new_doc)


def build_evidence(project: dict) -> ProjectEvidence:
    """Assemble evidence from DB for a single project."""
    conn = get_connection()
    pid = project["project_id"]
    cid = project["company_id"]
    pname = project["project_name"] or ""

    # Studies (latest 3)
    study_rows = conn.execute("""
        SELECT s.study_stage, s.study_date, d.header AS title, d.announcement_date
        FROM studies s
        LEFT JOIN documents d ON d.document_id = s.document_id
        WHERE s.project_id = ?
        ORDER BY COALESCE(s.study_date, d.announcement_date) DESC
        LIMIT 3
    """, (pid,)).fetchall()

    studies = [
        StudyEvidence(
            study_stage=r["study_stage"] or "Unknown",
            study_date=r["study_date"] or r["announcement_date"],
            document_title=r["title"],
        )
        for r in study_rows
    ]

    # Resources (latest 3)
    resource_rows = conn.execute("""
        SELECT commodity, category, tonnes, grade, grade_unit, effective_date
        FROM resources
        WHERE project_id = ?
        ORDER BY effective_date DESC
        LIMIT 3
    """, (pid,)).fetchall()

    resources = [
        ResourceEvidence(
            commodity=r["commodity"],
            category=r["category"],
            tonnes=r["tonnes"],
            effective_date=r["effective_date"],
        )
        for r in resource_rows
    ]

    # Recent announcements — try project name match first
    ann_rows = conn.execute("""
        SELECT header AS title, announcement_date
        FROM documents
        WHERE ticker = (SELECT ticker FROM companies WHERE company_id = ?)
          AND announcement_date >= date('now', '-180 days')
          AND (header LIKE '%' || ? || '%' OR ? = '')
        ORDER BY announcement_date DESC
        LIMIT 6
    """, (cid, pname, pname)).fetchall()

    # Fallback: any announcement by this company in last 90 days
    if not ann_rows and pname:
        ann_rows = conn.execute("""
            SELECT header AS title, announcement_date
            FROM documents
            WHERE ticker = (SELECT ticker FROM companies WHERE company_id = ?)
              AND announcement_date >= date('now', '-90 days')
            ORDER BY announcement_date DESC
            LIMIT 6
        """, (cid,)).fetchall()

    announcements = [
        AnnEvidence(title=r["title"] or "", announcement_date=r["announcement_date"])
        for r in ann_rows
    ]

    conn.close()
    return ProjectEvidence(
        studies=studies,
        resources=resources,
        recent_announcements=announcements,
    )


def _persist_result(project: dict, inference: ProjectStageInference, evidence: ProjectEvidence):
    """Write classification result to DB."""
    conn = get_connection()
    now = datetime.utcnow().isoformat()
    pid = project["project_id"]

    # Deterministic study-to-stage floor: a definitive/indicative study can never
    # leave a project below feasibility, regardless of the LLM result. The floor is
    # a floor only — it never downgrades a more-advanced LLM stage (I1, I2).
    tier = _best_study_tier(conn, pid)
    resolved_stage, floor_won = apply_floor(inference.stage, tier)
    source = "study_floor" if floor_won else "gemini_inferred"

    conn.execute("""
        UPDATE projects
        SET stage = ?,
            region = COALESCE(?, region),
            stage_source = ?,
            stage_inferred_at = ?
        WHERE project_id = ?
    """, (resolved_stage, inference.region, source, now, pid))

    # Audit row keeps the RAW LLM stage (provenance of what the LLM actually said).
    conn.execute("""
        INSERT INTO project_stage_inferences
            (project_id, stage, stage_confidence, region, reasoning, evidence_json, inferred_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        pid,
        inference.stage,
        inference.stage_confidence,
        inference.region,
        inference.reasoning,
        json.dumps(evidence.to_dict()),
        now,
    ))

    conn.commit()
    conn.close()


def _persist_insufficient(project: dict):
    """Mark project as insufficient evidence — but still apply the study floor.

    When Gemini returns nothing, a project that nonetheless has a study must not be
    left at unknown/exploration: the study itself is deterministic evidence (I1).
    """
    conn = get_connection()
    now = datetime.utcnow().isoformat()
    pid = project["project_id"]

    tier = _best_study_tier(conn, pid)
    floor = study_floor_stage(tier)
    if floor is not None:
        resolved = most_advanced(project.get("stage"), floor)
        conn.execute("""
            UPDATE projects
            SET stage = ?,
                stage_source = 'study_floor',
                stage_inferred_at = ?
            WHERE project_id = ?
        """, (resolved, now, pid))
    else:
        conn.execute("""
            UPDATE projects
            SET stage_source = 'insufficient_evidence',
                stage_inferred_at = ?
            WHERE project_id = ?
        """, (now, pid))
    conn.commit()
    conn.close()


def apply_production_floors(conn, tickers: list[str] | None = None) -> dict:
    """Deterministic production sweep (no LLM). Sets stage='production'
    (stage_source='production_floor') for any project with a production signal:
      - Appendix 5B receipts from customers (company-level) >= threshold, OR
      - a passed first-production date (project-level, from the DFS).
    Company-level receipts are attributed to the company's most-advanced built
    project only. Idempotent; never downgrades a non-production stage by accident.
    """
    from datetime import date as _date
    today = _date.today().isoformat()
    now = datetime.utcnow().isoformat()

    sql = ("SELECT p.project_id, p.company_id, p.stage, p.production_start_date "
           "FROM projects p JOIN companies c ON c.company_id = p.company_id")
    params: list = []
    if tickers:
        placeholders = ",".join("?" * len(tickers))
        sql += f" WHERE c.ticker IN ({placeholders})"
        params = [t.upper() for t in tickers]
    projects = conn.execute(sql, params).fetchall()

    def _has_revaluable_study(pid: int) -> bool:
        return conn.execute(
            "SELECT 1 FROM studies WHERE project_id = ? "
            "AND study_confidence_tier IN ('definitive', 'indicative') LIMIT 1",
            (pid,),
        ).fetchone() is not None

    receipts_cache: dict = {}
    def _latest_receipts(cid: int):
        if cid not in receipts_cache:
            row = conn.execute(
                "SELECT receipts_from_customers FROM company_financials "
                "WHERE company_id = ? AND receipts_from_customers IS NOT NULL "
                "ORDER BY effective_date DESC LIMIT 1",
                (cid,),
            ).fetchone()
            receipts_cache[cid] = row["receipts_from_customers"] if row else None
        return receipts_cache[cid]

    to_promote: set = set()

    # 1) First-production date — project-level, precise.
    for p in projects:
        if _has_revaluable_study(p["project_id"]) and production_floor(
            None, p["production_start_date"], today, True
        ):
            to_promote.add(p["project_id"])

    # 2) Customer receipts — company-level; attribute to the most-advanced built project.
    by_company: dict = {}
    for p in projects:
        by_company.setdefault(p["company_id"], []).append(p)
    for cid, plist in by_company.items():
        rec = _latest_receipts(cid)
        if rec is None:
            continue
        built = [p for p in plist if _has_revaluable_study(p["project_id"])]
        if not built:
            continue
        best = min(built, key=lambda p: (_PROD_RANK.get(p["stage"], 99), p["project_id"]))
        if production_floor(rec, None, today, True):
            to_promote.add(best["project_id"])

    promoted = 0
    for p in projects:
        if p["project_id"] in to_promote and p["stage"] != "production":
            conn.execute(
                "UPDATE projects SET stage = 'production', stage_source = 'production_floor', "
                "stage_inferred_at = ? WHERE project_id = ?",
                (now, p["project_id"]),
            )
            promoted += 1
    conn.commit()
    return {"promoted": promoted, "scanned": len(projects)}


def _classify_one(project: dict, dry_run: bool) -> dict:
    """Classify a single project. Returns stats dict."""
    ticker = project["ticker"]
    pname = project["project_name"]
    pid = project["project_id"]

    evidence = build_evidence(project)

    if dry_run:
        logger.info("[%s] %s — evidence: %d studies, %d resources, %d announcements",
                     ticker, pname, len(evidence.studies), len(evidence.resources),
                     len(evidence.recent_announcements))
        return {"status": "dry_run"}

    try:
        inference = classify_project(
            project_id=pid,
            project_name=pname,
            company_ticker=ticker,
            state=project["state"],
            country=project["country"],
            evidence=evidence,
        )
        _persist_result(project, inference, evidence)
        region_str = inference.region or "no region"
        logger.info("[%s] %s → %s (%s) [%s]",
                     ticker, pname, inference.stage, inference.stage_confidence, region_str)
        return {"status": "classified", "stage": inference.stage, "confidence": inference.stage_confidence}

    except InsufficientEvidenceError:
        _persist_insufficient(project)
        logger.info("[%s] %s → insufficient evidence", ticker, pname)
        return {"status": "insufficient"}

    except ClassificationError as e:
        logger.error("[%s] %s → classification error: %s", ticker, pname, e)
        return {"status": "error", "error": str(e)}


def _production_sweep_into(stats: dict, ticker) -> None:
    """Run the deterministic production floor and fold its result into stats.
    Non-fatal — a sweep failure must not break a backfill."""
    try:
        conn = get_connection()
        try:
            ps = apply_production_floors(conn, [ticker] if ticker else None)
        finally:
            conn.close()
        stats["production_promoted"] = ps["promoted"]
        if ps["promoted"]:
            logger.info("Production floor promoted %d project(s) to production", ps["promoted"])
    except Exception:
        logger.error("Production floor sweep failed", exc_info=True)


def run_backfill(ticker=None, classify_all=False, limit=None,
                 workers=4, dry_run=False) -> dict:
    """Fetch unclassified projects, classify each via Gemini, persist results.

    Reusable by the CLI, the /api/backfill-stages endpoint, and the orchestrator.
    Returns a stats dict. Idempotent: default mode only touches projects whose
    stage is unset (or ozmin-sourced), and skips gemini-cached ones with no new
    evidence.
    """
    projects = _fetch_projects(ticker, classify_all, limit)
    logger.info("Found %d projects to process", len(projects))

    if not classify_all:
        before = len(projects)
        projects = [p for p in projects if not _should_skip_cached(p)]
        cached = before - len(projects)
        if cached:
            logger.info("Skipped %d cached (no new evidence)", cached)

    stats = {"classified": 0, "insufficient": 0, "error": 0, "dry_run": 0,
             "cached": 0, "stage_counts": {}, "confidence_counts": {}}
    if not projects:
        logger.info("Nothing to classify")
        if not dry_run:
            _production_sweep_into(stats, ticker)
        return stats

    stage_counts = stats["stage_counts"]
    confidence_counts = stats["confidence_counts"]

    def _tally(result):
        stats[result["status"]] = stats.get(result["status"], 0) + 1
        if result.get("stage"):
            stage_counts[result["stage"]] = stage_counts.get(result["stage"], 0) + 1
        if result.get("confidence"):
            confidence_counts[result["confidence"]] = confidence_counts.get(result["confidence"], 0) + 1

    if dry_run or workers <= 1:
        for p in projects:
            _tally(_classify_one(p, dry_run))
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for p in projects:
                time.sleep(0.05)  # jitter to avoid rate-limit bursts
                futures[executor.submit(_classify_one, p, dry_run)] = p
            for f in as_completed(futures):
                try:
                    _tally(f.result())
                except Exception as e:
                    logger.error("Worker error for %s: %s", futures[f]["project_name"], e)
                    stats["error"] += 1

    if not dry_run:
        _production_sweep_into(stats, ticker)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Backfill project stages via Gemini classifier")
    parser.add_argument("--ticker", help="Restrict to one company ticker")
    parser.add_argument("--limit", type=int, help="Max projects to process")
    parser.add_argument("--all", action="store_true", help="Re-classify everything (including cached)")
    parser.add_argument("--dry-run", action="store_true", help="Build evidence without calling Gemini")
    parser.add_argument("--workers", type=int, default=4, help="Concurrency (default: 4)")
    args = parser.parse_args()

    init_db()
    _apply_migrations()

    stats = run_backfill(
        ticker=args.ticker, classify_all=args.all, limit=args.limit,
        workers=args.workers, dry_run=args.dry_run,
    )

    logger.info("=" * 50)
    logger.info("BACKFILL SUMMARY")
    logger.info("  Classified: %d", stats["classified"])
    logger.info("  Insufficient evidence: %d", stats["insufficient"])
    logger.info("  Errors: %d", stats["error"])
    if stats["dry_run"]:
        logger.info("  Dry-run: %d", stats["dry_run"])
    if stats["stage_counts"]:
        logger.info("  By stage: %s", stats["stage_counts"])
    if stats["confidence_counts"]:
        logger.info("  By confidence: %s", stats["confidence_counts"])


if __name__ == "__main__":
    main()

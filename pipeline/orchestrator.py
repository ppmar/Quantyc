"""
Pipeline Orchestrator

Week 1: pick pending docs → classify → update doc_type → set parse_status='classified'.
Week 2: classified docs → extract → normalize → flag.

Usage:
    from pipeline.orchestrator import run_orchestrator
"""

import logging
import re
import sqlite3

from datetime import datetime, timezone

from db import get_connection
from pipeline.classify import classify, contains_standardized_form

logger = logging.getLogger(__name__)


def classify_pending() -> int:
    """
    Classify all documents with parse_status='pending'.
    Sets doc_type and advances parse_status to 'classified'.
    Returns count of classified documents.
    """
    conn = get_connection()
    docs = conn.execute(
        "SELECT document_id, header FROM documents WHERE parse_status = 'pending'"
    ).fetchall()
    conn.close()

    classified = 0
    for doc in docs:
        doc_id = doc["document_id"]
        headline = doc["header"] or ""

        # Classify from headline only (no PDF download for classification)
        doc_type = classify(headline=headline)

        conn = get_connection()
        conn.execute(
            "UPDATE documents SET doc_type = ?, parse_status = 'classified' WHERE document_id = ?",
            (doc_type, doc_id),
        )
        conn.commit()
        conn.close()

        classified += 1
        logger.info("Classified doc %d as '%s' (headline: %s)", doc_id, doc_type, headline[:60])

    logger.info("Classified %d pending documents", classified)
    return classified


def extract_classified() -> dict:
    """
    Extract data from classified documents.

    For each classified doc:
    1. If already a standardized type (appendix_5b, issue_of_securities), extract directly.
    2. Otherwise, download the PDF and scan for an embedded standardized form
       (e.g. a quarterly report with an Appendix 5B at the end).
       Only scans the 5 most recent non-standardized docs to avoid wasting downloads.
    3. If a standardized form is found, reclassify and extract.

    Returns stats dict.
    """
    from ingest.asx_poller import fetch_pdf_bytes
    from pipeline.extractors.appendix_5b import extract_appendix_5b
    from pipeline.extractors.issue_of_securities import extract_issue_of_securities
    from pipeline.normalize.company_financials import normalize_from_5b, normalize_from_securities

    STANDARDIZED_TYPES = {"appendix_5b", "issue_of_securities", "resource_update", "study_dfs", "study_pfs", "study_scoping"}

    stats = {"extracted": 0, "skipped": 0, "failed": 0}

    conn = get_connection()
    now_iso = datetime.now(timezone.utc).isoformat()
    docs = _select_extractable(conn, now_iso)
    conn.close()

    # Types that might contain an embedded standardized form (worth downloading to check)
    SCAN_TYPES = {"quarterly_activity"}

    for doc in docs:
        doc_id = doc["document_id"]
        doc_type = doc["doc_type"]
        url = doc["url"]

        if doc_type in STANDARDIZED_TYPES:
            # Direct extraction — download and parse
            pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
            if not pdf_bytes:
                _record_failure(doc_id, "download_failed")
                stats["failed"] += 1
                continue
            _extract_doc(doc_id, doc_type, pdf_bytes, stats,
                         extract_appendix_5b, extract_issue_of_securities,
                         normalize_from_5b, normalize_from_securities,
                         ticker=doc["ticker"], announcement_date=doc["announcement_date"])
            del pdf_bytes

        elif doc_type in SCAN_TYPES:
            # Download and scan for embedded standardized forms (e.g. 5B at end of quarterly report)
            pdf_bytes = fetch_pdf_bytes(url) if url.startswith("http") else None
            if not pdf_bytes:
                _mark_skipped(doc_id)
                stats["skipped"] += 1
                continue
            found_type = contains_standardized_form(pdf_bytes)
            if found_type:
                logger.info("Doc %d reclassified: %s → %s (embedded form)", doc_id, doc_type, found_type)
                _update_doc_type(doc_id, found_type)
                _extract_doc(doc_id, found_type, pdf_bytes, stats,
                             extract_appendix_5b, extract_issue_of_securities,
                             normalize_from_5b, normalize_from_securities,
                             ticker=doc["ticker"], announcement_date=doc["announcement_date"])
            else:
                _mark_skipped(doc_id)
                stats["skipped"] += 1
            del pdf_bytes

        else:
            # Not useful — skip without downloading
            _mark_skipped(doc_id)
            stats["skipped"] += 1

    logger.info("Extraction: %s", stats)
    return stats


def _extract_doc(doc_id, doc_type, pdf_bytes, stats,
                 extract_appendix_5b, extract_issue_of_securities,
                 normalize_from_5b, normalize_from_securities,
                 ticker=None, announcement_date=None):
    """Run the appropriate extractor + normalizer for a document."""
    if doc_type == "appendix_5b":
        result = extract_appendix_5b(doc_id, pdf_bytes)
        if result:
            normalize_from_5b(doc_id)
            _mark_parsed(doc_id)
            stats["extracted"] += 1
        else:
            _record_failure(doc_id, "extraction_empty")
            stats["failed"] += 1

    elif doc_type == "issue_of_securities":
        result = extract_issue_of_securities(doc_id, pdf_bytes)
        if result:
            normalize_from_securities(doc_id)
            _mark_parsed(doc_id)
            stats["extracted"] += 1
        else:
            _record_failure(doc_id, "extraction_empty")
            stats["failed"] += 1

    elif doc_type == "resource_update":
        _mark_skipped(doc_id)
        stats["skipped"] += 1

    elif doc_type in ("study_dfs", "study_pfs", "study_scoping"):
        _extract_study(doc_id, doc_type, pdf_bytes, ticker, announcement_date, stats)


def _extract_study(doc_id, doc_type, pdf_bytes, ticker, announcement_date, stats):
    """Run LLM study extractor (DFS/PFS/Scoping) and persist."""
    from datetime import date as date_type
    from parsers.dfs_study import (
        parse as parse_study, detect_profile,
        ExtractionError, MalformedDocumentError, LLM_MODEL,
    )

    if not ticker or not announcement_date:
        _record_failure(doc_id, "missing_ticker_or_date")
        stats["failed"] += 1
        return

    if not detect_profile(pdf_bytes):
        _mark_skipped(doc_id)
        stats["skipped"] += 1
        return

    ann_date = announcement_date
    if isinstance(ann_date, str):
        ann_date = date_type.fromisoformat(ann_date)

    try:
        result = parse_study(pdf_bytes, ticker=ticker, doc_id=str(doc_id),
                             announcement_date=ann_date)
    except (ExtractionError, MalformedDocumentError) as e:
        _record_failure(doc_id, f"study_parse_error:{e}")
        stats["failed"] += 1
        return

    # Tier mismatch logging: classifier said X, LLM extracted Y
    expected_tier = {
        "study_dfs": "definitive",
        "study_pfs": "indicative",
        "study_scoping": "conceptual",
    }[doc_type]
    actual_tier = result.confidence_tier()
    if expected_tier != actual_tier:
        logger.warning(
            "Tier mismatch for doc %d (%s): classifier expected %s, LLM returned %s (%s)",
            doc_id, ticker, expected_tier, actual_tier, result.study_type
        )

    study_id = _persist_study(doc_id, ticker, result, LLM_MODEL)
    _mark_parsed(doc_id)
    stats["extracted"] += 1

    # Auto-trigger revaluation for definitive and indicative tiers only.
    # Conceptual (Scoping/PEA) studies have too much uncertainty.
    if study_id and actual_tier in ("definitive", "indicative"):
        try:
            from revaluation.pipeline import revalue_study
            conn = get_connection()
            reval_id = revalue_study(conn, study_id)
            if reval_id:
                logger.info("Auto-revaluation #%d for study %d (%s, tier=%s)",
                            reval_id, study_id, ticker, actual_tier)
            conn.close()
        except Exception as e:
            logger.warning("Auto-revaluation failed for study %d: %s", study_id, e)


# Plausible band for the implied effective-tax gap (1 - post_tax/pre_tax) on a
# real study. Outside this, one of the NPVs is likely mislabelled (e.g. the LLM
# grabbed two pre-tax figures at different price cases). Gold studies cluster ~27-30%.
_TAX_GAP_MIN = 0.20
_TAX_GAP_MAX = 0.45


def check_study_review_flags(pre_tax_npv, post_tax_npv, tax_rate_pct,
                             discount_rate_pct=None, aisc_per_unit=None):
    """Return (needs_review: bool, review_reason: str|None) for a study's economics.

    Flags (mirrors company_financials._check_review_flags — surfaces, never blocks):
      - missing_pre_tax_npv / missing_post_tax_npv  (revaluation needs post-tax)
      - post_tax_npv_ge_pre_tax_npv                 (inverted/equal — tax can't add value)
      - implied_tax_gap_<pct>_out_of_band           (one NPV likely mislabelled)
      - missing_tax_rate                            (revaluation silently defaults to 30%)
      - discount_rate_nonpositive                   (0/neg discount — invalid NPV basis)
      - aisc_negative                               (negative cost — extraction error)
    """
    reasons = []
    if pre_tax_npv is None:
        reasons.append("missing_pre_tax_npv")
    if post_tax_npv is None:
        reasons.append("missing_post_tax_npv")
    if pre_tax_npv is not None and post_tax_npv is not None and pre_tax_npv != 0:
        if post_tax_npv >= pre_tax_npv:
            reasons.append("post_tax_npv_ge_pre_tax_npv")
        else:
            gap = 1 - (post_tax_npv / pre_tax_npv)
            if gap < _TAX_GAP_MIN or gap > _TAX_GAP_MAX:
                reasons.append(f"implied_tax_gap_{gap * 100:.1f}pct_out_of_band")
    if tax_rate_pct is None:
        reasons.append("missing_tax_rate")
    if discount_rate_pct is not None and discount_rate_pct <= 0:
        reasons.append("discount_rate_nonpositive")
    if aisc_per_unit is not None and aisc_per_unit < 0:
        reasons.append("aisc_negative")
    return (len(reasons) > 0, "; ".join(reasons) if reasons else None)


# Tier implied by an announcement title. Short tokens (pea/dfs/pfs/bfs) MUST be
# matched on word boundaries — bare-substring "pea" matched Peak/Appears/repeat and
# silently routed real DFS announcements to conceptual, killing their revaluation.
# Precedence stays conceptual -> indicative -> definitive (first match wins).
_HDR_CONCEPTUAL = re.compile(r"\b(scoping|pea)\b", re.IGNORECASE)
_HDR_INDICATIVE = re.compile(r"\b(pre[-\s]?feasibility|prefeasibility|pfs)\b", re.IGNORECASE)
_HDR_DEFINITIVE = re.compile(
    r"\b(definitive\s+feasibility|dfs|bfs|bankable\s+feasibility|feasibility\s+study)\b",
    re.IGNORECASE,
)


def header_stage_tier(header: str | None) -> str | None:
    """Confidence tier implied by a document header/title, or None if generic.

    The header is the most reliable stage signal; the LLM's study_type sometimes
    contradicts it (AZY: "Scoping Study Update" extracted as "Updated DFS"). Used
    to override the reval gate TOWARD conceptual (never revalue a Scoping study).
    """
    if not header:
        return None
    if _HDR_CONCEPTUAL.search(header):
        return "conceptual"
    if _HDR_INDICATIVE.search(header):
        return "indicative"
    if _HDR_DEFINITIVE.search(header):
        return "definitive"
    return None


_PROD_UNIT_MULT = {
    "oz": 1, "ozs": 1, "koz": 1_000, "kozs": 1_000, "moz": 1_000_000,
    "t": 1, "tonnes": 1, "tonne": 1, "kt": 1_000, "ktpa": 1_000, "mt": 1_000_000, "mtpa": 1_000_000,
}


def normalize_annual_production(value, unit):
    """Convert annual production to a canonical absolute unit (oz or t) using the
    extracted unit. Returns (canonical_value, warning_or_None). Falls back to the
    raw value when the unit is unknown (the magnitude heuristic stays downstream)."""
    if value is None:
        return None, None
    if not unit:
        return value, None
    # Tokenize on whitespace and '/', take the first recognised unit token. This
    # handles a magnitude prefix followed by a stuck-on word: "kt Cu", "koz pa",
    # "Mt/yr". Falls back to raw value when no token is a known unit.
    mult = None
    for tok in unit.strip().lower().replace("/", " ").split():
        if tok in _PROD_UNIT_MULT:
            mult = _PROD_UNIT_MULT[tok]
            break
    if mult is None:
        return value, None
    if mult != 1:
        return value * mult, f"production_normalized_{unit}_x{mult}"
    return value, None


def _persist_study(doc_id, ticker, result, model_name):
    """Persist study extraction to projects (upsert) + studies (insert). Returns study_id."""
    import json as _json

    conn = get_connection()
    try:
        company = conn.execute(
            "SELECT company_id FROM companies WHERE ticker = ?", (ticker,)
        ).fetchone()
        if not company:
            raise Exception(f"company_not_found:{ticker}")
        company_id = company["company_id"]

        project_id = _get_or_create_project(conn, company_id, result.project_name)

        # Ensure project_commodities has the primary commodity
        existing = conn.execute(
            "SELECT id FROM project_commodities WHERE project_id = ? AND commodity = ?",
            (project_id, result.primary_commodity),
        ).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO project_commodities (project_id, commodity, is_primary) VALUES (?, ?, 1)",
                (project_id, result.primary_commodity),
            )

        # Dedup: skip if same project already has a study with same stage and NPV
        study_date_str = result.effective_date.isoformat() if result.effective_date else None
        npv_val = float(result.post_tax_npv_millions) if result.post_tax_npv_millions else None
        # `IS ?` so a NULL NPV deduplicates too (= ? never matches NULL).
        existing_study = conn.execute(
            """SELECT study_id FROM studies
               WHERE project_id = ? AND study_stage = ? AND post_tax_npv IS ?""",
            (project_id, result.study_type, npv_val),
        ).fetchone()
        if existing_study:
            logger.info("Skipping duplicate study for %s — %s (stage=%s, npv=%s) already exists as study #%d",
                        ticker, result.project_name, result.study_type, npv_val, existing_study["study_id"])
            return existing_study["study_id"]

        pre_tax_val = float(result.pre_tax_npv_millions) if result.pre_tax_npv_millions else None
        post_tax_val = float(result.post_tax_npv_millions) if result.post_tax_npv_millions else None
        tax_rate_val = float(result.tax_rate_pct) if result.tax_rate_pct else None
        discount_val = float(result.discount_rate_pct) if result.discount_rate_pct is not None else None
        aisc_val = float(result.aisc_per_unit) if result.aisc_per_unit is not None else None
        # Header-derived tier (R1): authoritative stage signal from the announcement
        # title; flag when it contradicts the LLM and is more conceptual.
        hdr_row = conn.execute("SELECT header, announcement_date FROM documents WHERE document_id = ?", (doc_id,)).fetchone()
        header_tier = header_stage_tier(hdr_row["header"] if hdr_row else None)

        # R4: a study can't be 'as at' after its announcement; clamp the off-by-one.
        eff = result.effective_date
        ann = hdr_row["announcement_date"] if hdr_row else None
        eff_iso = eff.isoformat() if eff else None
        if eff_iso and ann and eff_iso > ann:
            warns_clamp = f"effective_date_after_announcement_clamped:{eff_iso}->{ann}"
            eff_iso = ann
        elif eff_iso is None and ann:
            # No 'as at' date in the study: fall back to the announcement date
            # (an honest upper bound) so latest-study ordering and vintage work.
            eff_iso = ann
            warns_clamp = "study_date_from_announcement_date"
        else:
            warns_clamp = None
        _RANK = {"definitive": 0, "indicative": 1, "conceptual": 2}
        llm_tier = result.confidence_tier()
        warns = list(result.extraction_warnings or [])
        if warns_clamp:
            warns.append(warns_clamp)
        if header_tier and _RANK.get(header_tier, 9) > _RANK.get(llm_tier, 9):
            review_extra = f"study_type_header_mismatch:hdr={header_tier},llm={llm_tier}"
        else:
            review_extra = None

        # Production normalized to canonical absolute units via the source unit (R2).
        prod_norm, prod_warn = normalize_annual_production(
            float(result.annual_production) if result.annual_production else None,
            result.annual_production_unit)
        if prod_warn:
            warns.append(prod_warn)

        needs_review, review_reason = check_study_review_flags(
            pre_tax_val, post_tax_val, tax_rate_val, discount_val, aisc_val)
        if review_extra:
            needs_review = True
            review_reason = (review_reason + "; " + review_extra) if review_reason else review_extra
        if needs_review:
            logger.warning("Study for %s — %s flagged for review: %s",
                           ticker, result.project_name, review_reason)

        cur = conn.execute("""
            INSERT INTO studies (
                project_id, document_id, study_stage, study_confidence_tier, study_date,
                mine_life_years, annual_production, annual_production_unit, recovery_pct,
                initial_capex, sustaining_capex, opex,
                post_tax_npv, pre_tax_npv, irr_pct, payback_years,
                aisc_per_unit, aisc_unit,
                assumed_price_deck, assumed_fx,
                reporting_currency, discount_rate_pct, tax_rate_pct,
                extraction_method, extraction_model,
                needs_review, review_reason, extraction_warnings, header_tier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            project_id, doc_id, result.study_type, result.confidence_tier(),
            eff_iso,
            float(result.mine_life_years) if result.mine_life_years else None,
            prod_norm,
            result.annual_production_unit,
            float(result.recovery_pct) if result.recovery_pct else None,
            float(result.initial_capex_millions) if result.initial_capex_millions else None,
            float(result.sustaining_capex_millions) if result.sustaining_capex_millions else None,
            float(result.opex_per_unit) if result.opex_per_unit else None,
            post_tax_val,
            pre_tax_val,
            float(result.irr_pct) if result.irr_pct else None,
            float(result.payback_years) if result.payback_years else None,
            float(result.aisc_per_unit) if result.aisc_per_unit else None,
            result.aisc_unit,
            _json.dumps([p.model_dump(mode="json") for p in result.price_assumptions]),
            float(result.fx_assumption) if result.fx_assumption else None,
            result.reporting_currency,
            float(result.discount_rate_pct),
            tax_rate_val,
            "llm",
            model_name,
            1 if needs_review else 0,
            review_reason,
            _json.dumps(warns),
            header_tier,
        ))

        # Study-to-stage floor: a freshly parsed DFS/PFS lifts the project to at
        # least feasibility immediately, without waiting for the end-of-run Gemini
        # backfill. Floor-only — never downgrades a more-advanced stage (I1, I2, I4).
        from pipeline.stage_floor import study_floor_stage, most_advanced
        floor = study_floor_stage(result.confidence_tier())
        if floor is not None:
            cur_stage_row = conn.execute(
                "SELECT stage FROM projects WHERE project_id = ?", (project_id,)
            ).fetchone()
            cur_stage = cur_stage_row["stage"] if cur_stage_row else None
            resolved = most_advanced(cur_stage, floor)
            if resolved != cur_stage:
                conn.execute(
                    "UPDATE projects SET stage = ?, stage_source = 'study_floor' WHERE project_id = ?",
                    (resolved, project_id),
                )

        conn.commit()
        study_id = cur.lastrowid
        logger.info("Persisted %s study #%d for %s — %s", result.study_type, study_id, ticker, result.project_name)
        return study_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _extract_resource_update(doc_id, pdf_bytes, ticker, announcement_date, stats):
    """Parse a JORC resource update and persist to projects + resources."""
    from datetime import date as date_type, datetime, timezone
    from parsers.jorc_resource_estimate import detect_profile, parse
    from parsers.appendix_2a import ExtractionError, MalformedDocumentError

    if not ticker or not announcement_date:
        _record_failure(doc_id, "missing_ticker_or_date")
        stats["failed"] += 1
        return

    if not detect_profile(pdf_bytes):
        _mark_skipped(doc_id)
        stats["skipped"] += 1
        return

    ann_date = announcement_date
    if isinstance(ann_date, str):
        ann_date = date_type.fromisoformat(ann_date)

    try:
        estimate = parse(pdf_bytes, ticker=ticker, doc_id=str(doc_id), announcement_date=ann_date)
    except (ExtractionError, MalformedDocumentError) as e:
        _record_failure(doc_id, str(e))
        stats["failed"] += 1
        return

    conn = get_connection()
    try:
        # Look up company_id
        row = conn.execute("SELECT company_id FROM companies WHERE ticker = ?", (ticker,)).fetchone()
        if not row:
            _record_failure(doc_id, "company_not_found")
            stats["failed"] += 1
            return
        company_id = row["company_id"]

        # Project bootstrap: look up or insert
        project_id = _get_or_create_project(conn, company_id, estimate.project_name)

        # Insert commodity association
        conn.execute(
            """INSERT OR IGNORE INTO project_commodities (project_id, commodity, is_primary)
               VALUES (?, ?, 1)""",
            (project_id, estimate.commodity),
        )

        # Insert resource rows (skip Total rows — they're derived)
        now = datetime.now(timezone.utc).isoformat()
        for row in estimate.rows:
            if row.category == "Total":
                continue
            conn.execute(
                """INSERT INTO resources
                   (project_id, document_id, effective_date, commodity,
                    resource_or_reserve, category, tonnes, grade, grade_unit,
                    contained_metal, contained_metal_unit,
                    cutoff_grade, cutoff_grade_unit,
                    attributable_contained_metal, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)""",
                (
                    project_id, doc_id, estimate.snapshot_date.isoformat(),
                    estimate.commodity, estimate.resource_or_reserve,
                    row.category,
                    float(row.tonnes_mt) if row.tonnes_mt is not None else None,
                    float(row.grade) if row.grade is not None else None,
                    row.grade_unit,
                    float(row.contained_metal) if row.contained_metal is not None else None,
                    row.contained_metal_unit,
                    float(estimate.cutoff_grade) if estimate.cutoff_grade is not None else None,
                    estimate.cutoff_grade_unit,
                    now,
                ),
            )

        conn.commit()
        _mark_parsed(doc_id)
        stats["extracted"] += 1
        logger.info(
            "Extracted JORC resource for %s — %s (%s): %d rows",
            ticker, estimate.project_name, estimate.commodity, len(estimate.rows),
        )
    except Exception as e:
        conn.rollback()
        _record_failure(doc_id, f"resource_persist_error:{e}")
        stats["failed"] += 1
        logger.exception("Failed to persist resource update for doc %d", doc_id)
    finally:
        conn.close()


# Leading tokens that mark an LLM-extracted project name as an announcement
# prose fragment rather than a real deposit name.
_JUNK_FIRST_TOKENS = {
    "the", "a", "an", "and", "or", "for", "its", "our", "their", "this", "that",
    "these", "those", "both", "each", "all", "it", "further", "updated", "revised",
    "resumption", "details", "reporting", "continues", "later", "regarding",
    "information", "following", "additional",
}
# Lowercase function words; their presence mid-name marks a sentence fragment.
_JUNK_CONNECTIVES = {
    "the", "a", "an", "and", "or", "of", "at", "to", "in", "for", "as", "with",
    "by", "on", "from", "that", "this", "its", "our", "their", "it",
}


def _is_junk_project_name(name: str) -> bool:
    """True if the name looks like an announcement fragment, not a deposit name.

    Real deposit names are capitalised proper nouns ("Hemi", "Karlawinda",
    "Vulcan Zero Carbon Lithium Phase One"). Fragments start lowercase ("our",
    "later in the"), are lone/leading stopwords ("Updated", "Further information
    regarding the"), or contain a lowercase connective mid-name ("Table 4 and
    the 2018 Annual") — which an all-proper-noun project name never does.
    """
    n = (name or "").strip()
    if not n:
        return True
    if n[0].islower():
        return True
    words = n.split()
    first = words[0].lower().strip(".,:;")
    if first in _JUNK_FIRST_TOKENS and (len(words) == 1 or len(words) >= 3):
        return True
    for w in words[1:]:
        if w.islower() and w.strip(".,:;") in _JUNK_CONNECTIVES:
            return True
    return False


def normalize_project_name(project_name: str) -> str:
    """Canonical project name for matching/merging.

    Strips trailing qualifiers + commodity tokens so "Syama Gold Project" and
    "Syama" map to one project (R6). Scope words (Underground, Sulphide,
    Expansion, Stage N) are NOT stripped — they are real sub-projects.
    """
    import re
    clean_name = project_name
    for _ in range(3):  # strip repeated trailing tokens, e.g. "X Gold Project"
        new = re.sub(
            r"\s+(?:Project|Deposit|Mine|Operations?|Limited|Ltd|"
            r"Gold|Copper|Lithium|Nickel|Silver|Zinc|Uranium|Cobalt|Graphite|"
            r"Iron\s*Ore|Iron|Rare\s*Earths?)\s*$",
            "", clean_name, flags=re.I).strip()
        if new == clean_name:
            break
        clean_name = new
    return clean_name or project_name.strip()


def _get_or_create_project(conn, company_id: int, project_name: str) -> int:
    """Look up a project by (company_id, normalized name) or create it.

    Both sides are normalized: a legacy stored "Paris Silver" must match an
    incoming "Paris" (and vice versa) or the project forks and the same study
    lands twice with different numbers (IVR Paris / RMS Rebecca-Roe bug).

    Junk fragment names are not allowed to spawn new projects: if the company
    already has a real (non-junk) project, attach to its most recent one instead.
    """
    from datetime import datetime, timezone

    clean_name = normalize_project_name(project_name)
    clean_lower = clean_name.lower()

    row = None
    for cand in conn.execute(
        """SELECT project_id, project_name FROM projects
           WHERE company_id = ? ORDER BY created_at DESC""",
        (company_id,),
    ).fetchall():
        if normalize_project_name(cand["project_name"]).lower() == clean_lower:
            row = cand
            break

    if row:
        return row["project_id"]

    # Don't let an announcement-fragment name create a phantom project. Attach to
    # the company's most-recent real project if one exists.
    if _is_junk_project_name(clean_name):
        for cand in conn.execute(
            "SELECT project_id, project_name FROM projects WHERE company_id = ? ORDER BY created_at DESC",
            (company_id,),
        ).fetchall():
            if not _is_junk_project_name(cand["project_name"]):
                logger.warning("Junk project name %r attached to existing project #%d (%r)",
                               clean_name, cand["project_id"], cand["project_name"])
                return cand["project_id"]
        logger.warning("Junk project name %r and no real project for company %d; creating anyway",
                       clean_name, company_id)

    now = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """INSERT INTO projects (company_id, project_name, created_at)
           VALUES (?, ?, ?)""",
        (company_id, clean_name, now),
    )
    return cursor.lastrowid


def _update_doc_type(doc_id: int, doc_type: str) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET doc_type = ? WHERE document_id = ?",
        (doc_type, doc_id),
    )
    conn.commit()
    conn.close()


def _select_extractable(conn: sqlite3.Connection, now_iso: str) -> list:
    """Docs ready to extract: freshly classified, plus retries now due."""
    return conn.execute(
        """SELECT document_id, doc_type, url, header, ticker, announcement_date
           FROM documents
           WHERE parse_status = 'classified'
              OR (parse_status = 'retry_scheduled' AND next_retry_at <= ?)
           ORDER BY announcement_date DESC""",
        (now_iso,),
    ).fetchall()


def _mark_skipped(doc_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET parse_status = 'skipped' WHERE document_id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()


def _mark_parsed(doc_id: int) -> None:
    conn = get_connection()
    conn.execute(
        "UPDATE documents SET parse_status = 'parsed' WHERE document_id = ?",
        (doc_id,),
    )
    conn.commit()
    conn.close()


def _record_failure(doc_id: int, error: str) -> None:
    """Transient failures get scheduled for backoff retry; everything else
    (and exhausted transients) becomes a terminal 'failed'."""
    from pipeline.failure import classify_failure, compute_next_retry, MAX_RETRIES

    conn = get_connection()
    row = conn.execute(
        "SELECT retry_count FROM documents WHERE document_id = ?", (doc_id,)
    ).fetchone()
    retry_count = row["retry_count"] if row and row["retry_count"] is not None else 0
    cls = classify_failure(error)

    if cls == "transient" and retry_count < MAX_RETRIES:
        conn.execute(
            """UPDATE documents
               SET parse_status='retry_scheduled', failure_class='transient',
                   retry_count = ?, next_retry_at = ?, parse_error = ?
               WHERE document_id = ?""",
            (retry_count + 1, compute_next_retry(retry_count), error, doc_id),
        )
    else:
        final_error = f"{error}:retries_exhausted" if cls == "transient" else error
        conn.execute(
            """UPDATE documents
               SET parse_status='failed', failure_class=?, parse_error=?
               WHERE document_id = ?""",
            (cls, final_error, doc_id),
        )
    conn.commit()
    conn.close()


def run_orchestrator() -> dict:
    """Full pipeline run: classify → extract → classify project stages."""
    classified = classify_pending()
    stats = extract_classified()
    stats["classified"] = classified

    # Self-maintaining stage inference for newly-ingested projects.
    # Best-effort: a classifier/quota failure must not break extraction.
    stats["stage_backfill"] = None
    try:
        from scripts.backfill_project_stages import run_backfill
        stats["stage_backfill"] = run_backfill(classify_all=False)
    except Exception as e:
        logger.warning("Stage backfill during orchestration failed: %s", e)

    return stats

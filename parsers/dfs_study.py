"""
DFS Study Parser — Gemini 2.5 Flash with native PDF input.

DOCUMENTED EXCEPTION to deterministic-first principle. See spec_gemini_flash.md.
"""
import io
import logging
import os
import re
from datetime import date
from typing import Optional

from pydantic import ValidationError

from parsers.dfs_study_schemas import DFSExtraction

logger = logging.getLogger(__name__)

PARSER_VERSION = "1.0.0"
LLM_MODEL = os.environ.get("DFS_LLM_MODEL", "gemini-2.5-flash")


class ExtractionError(Exception):
    """Document is a DFS but extraction failed."""


class MalformedDocumentError(Exception):
    """Document does not appear to be a DFS at all."""


# ─── Profile detection (cheap, deterministic) ────────────────────────

_DFS_PROFILE_PATTERNS = [
    re.compile(r"Definitive\s+Feasibility\s+Study", re.IGNORECASE),
    re.compile(r"\bDFS\b", re.IGNORECASE),
    re.compile(r"Final\s+Feasibility\s+Study", re.IGNORECASE),
]

_DFS_DISQUALIFIER_PATTERNS = [
    re.compile(r"Pre[-\s]?Feasibility\s+Study", re.IGNORECASE),
    re.compile(r"\bPFS\b", re.IGNORECASE),
    re.compile(r"Scoping\s+Study", re.IGNORECASE),
    re.compile(r"Appendix\s*5B", re.IGNORECASE),
]


def detect_profile(pdf_bytes: bytes) -> bool:
    """
    Cheap deterministic check that this PDF is plausibly a DFS announcement.
    Runs BEFORE any LLM call to avoid wasting quota on misclassified documents.
    Reads only the first 3 pages of text via pdfplumber.
    """
    import pdfplumber

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_to_check = pdf.pages[:3]
            text = "\n".join((p.extract_text() or "") for p in pages_to_check)
            first_page_text = pages_to_check[0].extract_text() if pages_to_check else ""
    except Exception as e:
        logger.warning("detect_profile: pdfplumber failed: %s", e)
        return False

    has_profile = any(p.search(text) for p in _DFS_PROFILE_PATTERNS)
    if not has_profile:
        return False

    has_disqualifier = any(p.search(first_page_text or "") for p in _DFS_DISQUALIFIER_PATTERNS)
    if has_disqualifier:
        # Allow if DFS profile pattern also appears on page 1 (DFS that mentions PFS history)
        if any(p.search(first_page_text) for p in _DFS_PROFILE_PATTERNS[:1]):
            return True
        return False

    return True


# ─── LLM extraction ──────────────────────────────────────────────────

EXTRACTION_PROMPT = """You are extracting economic parameters from a Definitive Feasibility Study (DFS) published by an ASX-listed mining company.

Extract structured data per the provided schema.

CRITICAL RULES:
1. Use null for any field where the value is not stated explicitly in the document. Never invent or estimate.
2. Distinguish post-tax NPV from pre-tax NPV. Populate each only with its specific value.
3. The "reporting_currency" is the currency of the headline NPV. If capex is in a different currency, normalize to reporting_currency ONLY if an explicit FX rate is given; otherwise add to extraction_warnings.
4. Monetary values are in MILLIONS of reporting_currency. "$2.4 billion NPV" → 2400.
5. discount_rate_pct is REQUIRED — DFS always state their discount rate (e.g., 8.0 for "NPV8" or "NPV at 8%").
6. project_name is the deposit/project name only (e.g., "Hemi", "Kathleen Valley", "Pilgangoora"). Strip trailing "Project", "Mine", "Deposit". Never use placeholder text.
7. price_assumptions: extract base case prices used in the economic model. One entry per commodity. Include unit explicitly.
8. study_type: "DFS" for first DFS, "Updated DFS" or "Revised DFS" if explicitly stated, "FFS" for Final Feasibility Study.
9. extraction_warnings: include concerns like mixed currencies without FX, multiple scenarios where you picked base case, project_name ambiguity.
10. All numeric fields must be single numbers, not ranges. If a value is a range (e.g., "6-7 Mt/yr"), use the midpoint (6.5) and add a warning to extraction_warnings noting the original range."""


def _fix_string_nulls(obj):
    """Gemini sometimes returns string 'null' instead of JSON null in response_schema mode."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if v == "null":
                obj[k] = None
            elif isinstance(v, (dict, list)):
                _fix_string_nulls(v)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if v == "null":
                obj[i] = None
            elif isinstance(v, (dict, list)):
                _fix_string_nulls(v)


def parse(
    pdf_bytes: bytes,
    ticker: str,
    doc_id: str,
    announcement_date: date,
) -> DFSExtraction:
    """
    Extract DFS data via Gemini 2.5 Flash. Returns validated Pydantic model or raises.
    """
    if not detect_profile(pdf_bytes):
        raise MalformedDocumentError("not_a_dfs_document")

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ExtractionError("google_api_key_not_set")

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)

        response = client.models.generate_content(
            model=LLM_MODEL,
            contents=[
                types.Part.from_bytes(
                    data=pdf_bytes,
                    mime_type="application/pdf",
                ),
                EXTRACTION_PROMPT,
            ],
            config={
                "response_mime_type": "application/json",
                "response_schema": DFSExtraction,
                "temperature": 0.0,
            },
        )
    except Exception as e:
        logger.error("DFS Gemini call failed for doc %s: %s", doc_id, e)
        raise ExtractionError(f"llm_api_error:{type(e).__name__}:{e}")

    try:
        result: DFSExtraction = response.parsed
        if result is None:
            import json
            raw = json.loads(response.text)
            # Gemini sometimes returns string "null" instead of JSON null
            _fix_string_nulls(raw)
            result = DFSExtraction.model_validate(raw)
    except ValidationError as e:
        logger.error("DFS Pydantic validation failed for doc %s: %s", doc_id, e)
        raise ExtractionError(f"validation_error:{e.error_count()}_errors")
    except Exception as e:
        logger.error("DFS response parse failed for doc %s: %s\nRaw: %s",
                     doc_id, e, response.text[:1000] if response.text else "")
        raise ExtractionError(f"response_parse_error:{type(e).__name__}")

    if not result.has_minimum_data():
        raise ExtractionError("minimum_data_missing:requires_npv_and_initial_capex")

    return result

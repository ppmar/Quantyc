"""
DFS Study Parser — Gemini 2.5 Flash with native PDF input.

DOCUMENTED EXCEPTION to deterministic-first principle. See spec_gemini_flash.md.
"""
import io
import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Optional

from pydantic import ValidationError

from parsers.dfs_study_schemas import StudyExtraction, DFSExtraction  # DFSExtraction kept as alias

logger = logging.getLogger(__name__)

PARSER_VERSION = "1.0.0"
LLM_MODEL = os.environ.get("DFS_LLM_MODEL", "gemini-2.5-flash")


class ExtractionError(Exception):
    """Document is a DFS but extraction failed."""


class MalformedDocumentError(Exception):
    """Document does not appear to be a DFS at all."""


# ─── Profile detection (cheap, deterministic) ────────────────────────

# Any of these in pages 1-3 qualifies the document as a study extraction candidate.
_STUDY_PROFILE_PATTERNS = [
    # Definitive
    re.compile(r"Definitive\s+Feasibility\s+Study", re.IGNORECASE),
    re.compile(r"\bDFS\b", re.IGNORECASE),
    re.compile(r"Final\s+Feasibility\s+Study", re.IGNORECASE),
    re.compile(r"Bankable\s+Feasibility\s+Study", re.IGNORECASE),
    re.compile(r"\bBFS\b", re.IGNORECASE),
    re.compile(r"Feasibility\s+(?:Study\s+)?(?:Update|Results|Outcomes)", re.IGNORECASE),
    re.compile(r"Feasibility\s+Study\s+(?:Confirms|Delivers|Completed?)", re.IGNORECASE),
    # Pre-feasibility
    re.compile(r"Pre[-\s]?Feasibility\s+Study", re.IGNORECASE),
    re.compile(r"\bPFS\b", re.IGNORECASE),
    # Scoping / PEA
    re.compile(r"Scoping\s+Study", re.IGNORECASE),
    re.compile(r"Preliminary\s+Economic\s+Assessment", re.IGNORECASE),
    re.compile(r"\bPEA\b", re.IGNORECASE),
]

# Documents we still want to disqualify (these are not study announcements at all).
_STUDY_DISQUALIFIER_PATTERNS = [
    re.compile(r"Appendix\s*5B", re.IGNORECASE),
    re.compile(r"Appendix\s*3[BHG]", re.IGNORECASE),
    re.compile(r"Quarterly\s+Activities\s+Report", re.IGNORECASE),
    re.compile(r"Half[-\s]?Year(?:ly)?\s+Report", re.IGNORECASE),
    re.compile(r"Annual\s+Report", re.IGNORECASE),
]


def detect_profile(pdf_bytes: bytes) -> bool:
    """
    Cheap deterministic check that this PDF is plausibly a study announcement
    (DFS, PFS, or Scoping). Runs BEFORE any LLM call.
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

    has_profile = any(p.search(text) for p in _STUDY_PROFILE_PATTERNS)
    if not has_profile:
        return False

    # Disqualifiers must be on page 1 to win — a body-text mention of "Appendix 5B"
    # in a real study PDF should not disqualify it.
    has_disqualifier = any(p.search(first_page_text or "") for p in _STUDY_DISQUALIFIER_PATTERNS)
    if has_disqualifier:
        if any(p.search(first_page_text or "") for p in _STUDY_PROFILE_PATTERNS):
            return True
        return False

    return True


# ─── LLM extraction ──────────────────────────────────────────────────

_PROMPT_PATH = Path(__file__).parent / "dfs_study_prompt.md"


def _load_extraction_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


EXTRACTION_PROMPT = _load_extraction_prompt()


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
) -> StudyExtraction:
    """
    Extract DFS data via Gemini 2.5 Flash. Returns validated Pydantic model or raises.
    """
    if not detect_profile(pdf_bytes):
        raise MalformedDocumentError("not_a_study_document")

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
                "response_schema": StudyExtraction,
                "temperature": 0.0,
            },
        )
    except Exception as e:
        logger.error("DFS Gemini call failed for doc %s: %s", doc_id, e)
        raise ExtractionError(f"llm_api_error:{type(e).__name__}:{e}")

    try:
        result: StudyExtraction = response.parsed
        if result is None:
            import json
            raw = json.loads(response.text)
            # Gemini sometimes returns string "null" instead of JSON null
            _fix_string_nulls(raw)
            result = StudyExtraction.model_validate(raw)
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

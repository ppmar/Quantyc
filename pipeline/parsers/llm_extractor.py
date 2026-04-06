"""
Shared LLM Extraction Utility

Sends targeted page excerpts to Claude for structured data extraction.
Never sends a full document — maximum 3 pages of context per call.

Usage:
    from pipeline.parsers.llm_extractor import extract_with_llm
    result = extract_with_llm(schema, page_texts)
"""

import json
import logging
import os

import anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 800
MAX_PAGES = 3

EXTRACTION_PROMPT = """You are extracting structured data from an ASX mining company announcement.
Return ONLY a valid JSON object matching this exact schema. Use null for any field not found.
Do not invent or estimate values. Do not add any text outside the JSON.

Schema:
{schema}

Document excerpt:
{chunk}
"""


def _get_client() -> anthropic.Anthropic:
    """Create an Anthropic client, loading the API key from environment."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY not set. Copy .env.example to .env and add your key."
        )
    return anthropic.Anthropic(api_key=api_key)


def extract_with_llm(
    schema: dict,
    page_texts: list[str],
    max_pages: int = MAX_PAGES,
) -> dict | None:
    """
    Send page excerpts to Claude and extract structured data.

    Args:
        schema: JSON schema dict describing expected fields.
                 Each key maps to a description string, e.g.
                 {"tonnes_mt": "Tonnes in millions", "grade": "Grade value"}
        page_texts: List of page text strings (already extracted from PDF).
                    Only the first `max_pages` are sent.
        max_pages: Maximum number of pages to include (default 3).

    Returns:
        Parsed dict matching schema keys, or None if extraction failed.
    """
    if not page_texts:
        logger.warning("No page texts provided for LLM extraction")
        return None

    # Truncate to max pages
    pages = page_texts[:max_pages]
    chunk = "\n\n--- PAGE BREAK ---\n\n".join(pages)

    schema_str = json.dumps(schema, indent=2)
    prompt = EXTRACTION_PROMPT.format(schema=schema_str, chunk=chunk)

    try:
        client = _get_client()
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = response.content[0].text.strip()
    except anthropic.APIError as e:
        logger.error("Anthropic API error: %s", e)
        return None
    except Exception as e:
        logger.error("LLM extraction failed: %s", e)
        return None

    # Parse JSON from response — handle markdown code fences
    return _parse_json_response(raw_text, schema)


def _parse_json_response(raw_text: str, schema: dict) -> dict | None:
    """
    Parse a JSON response from the LLM, handling common formatting issues
    like markdown code fences.
    """
    text = raw_text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json or ```) and last line (```)
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM JSON response: %s\nRaw: %s", e, raw_text[:500])
        return None

    if not isinstance(parsed, dict):
        logger.error("LLM returned non-dict JSON: %s", type(parsed))
        return None

    # Validate that returned keys are a subset of schema keys
    unknown_keys = set(parsed.keys()) - set(schema.keys())
    if unknown_keys:
        logger.warning("LLM returned unexpected keys: %s", unknown_keys)

    # Build result with only expected keys, defaulting to None
    result = {}
    for key in schema:
        result[key] = parsed.get(key)

    return result


def validate_extraction(result: dict, schema: dict) -> tuple[bool, list[str]]:
    """
    Validate an extraction result against expected types.

    Returns:
        (is_valid, list of warning messages)
    """
    warnings = []
    has_any_value = False

    for key, description in schema.items():
        value = result.get(key)
        if value is None:
            continue
        has_any_value = True

        # Basic type checks — numeric fields should be numbers
        if any(
            hint in description.lower()
            for hint in ["million", "percent", "rate", "price", "grade", "tonnes", "years", "koz", "moz", "kt"]
        ):
            if not isinstance(value, (int, float)):
                warnings.append(f"{key}: expected numeric, got {type(value).__name__}")

    if not has_any_value:
        warnings.append("No values extracted — all fields are null")

    return len(warnings) == 0, warnings

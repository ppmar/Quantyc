"""
LLM Fallback Extractor

Wraps the Anthropic API for structured extraction when rule-based fails.
Takes (doc_type, pdf_text, target_fields_spec) and returns JSON.

Always sets extraction_method='llm', confidence='medium' (downgraded to
'low' if any target field is null).
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

MODEL = os.environ.get("LLM_MODEL", "claude-sonnet-4-20250514")
MAX_TOKENS = 800
MAX_PAGES = 3

EXTRACTION_PROMPT = """You are extracting structured data from an ASX mining company filing.
Return ONLY a valid JSON object matching this exact schema. Use null for any field not found.
Do not invent or estimate values. Do not add any text outside the JSON.

Document type: {doc_type}

Schema:
{schema}

Document excerpt:
{chunk}
"""


def extract_with_llm(
    doc_type: str,
    page_texts: list[str],
    target_fields: dict,
    max_pages: int = MAX_PAGES,
) -> tuple[dict | None, str]:
    """
    Send page excerpts to Claude and extract structured data.

    Args:
        doc_type: The classified document type.
        page_texts: List of page text strings (already extracted from PDF).
        target_fields: Dict of {field_name: description} defining expected output.
        max_pages: Maximum pages to include.

    Returns:
        (result_dict, confidence) where confidence is 'medium' or 'low'.
    """
    if not page_texts:
        logger.warning("No page texts provided for LLM extraction")
        return None, "low"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY not set — cannot use LLM fallback")
        return None, "low"

    pages = page_texts[:max_pages]
    chunk = "\n\n--- PAGE BREAK ---\n\n".join(pages)
    schema_str = json.dumps(target_fields, indent=2)
    prompt = EXTRACTION_PROMPT.format(doc_type=doc_type, schema=schema_str, chunk=chunk)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = response.content[0].text.strip()
    except Exception as e:
        logger.error("LLM extraction failed: %s", e)
        return None, "low"

    # Parse JSON
    result = _parse_json_response(raw_text, target_fields)
    if result is None:
        return None, "low"

    # Confidence: medium if all target fields present, low otherwise
    null_count = sum(1 for v in result.values() if v is None)
    confidence = "low" if null_count > 0 else "medium"

    return result, confidence


def _parse_json_response(raw_text: str, target_fields: dict) -> dict | None:
    text = raw_text.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error("Failed to parse LLM JSON: %s\nRaw: %s", e, raw_text[:500])
        return None

    if not isinstance(parsed, dict):
        logger.error("LLM returned non-dict: %s", type(parsed))
        return None

    # Only keep expected keys, default to None
    return {key: parsed.get(key) for key in target_fields}

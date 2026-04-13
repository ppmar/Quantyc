"""
Central configuration — env vars + paths.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent

DB_PATH = ROOT / "db" / "quantyc.db"
SCHEMA_PATH = ROOT / "db" / "schema.sql"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
LLM_MODEL = "claude-sonnet-4-20250514"
LLM_MAX_TOKENS = 800

ASX_API_URL = "https://asx.api.markitdigital.com/asx-research/1.0/companies/{ticker}/announcements"
ASX_CDN_BASE = "https://cdn-api.markitdigital.com/apiman-gateway/ASX/asx-research/1.0/file/"

FETCH_DELAY = 1.5  # seconds between ASX requests

USER_AGENT = "Mozilla/5.0 (compatible; Quantyc/1.0)"

"""Failure classification + retry backoff for the extraction pipeline.

Pure functions — no DB, no network. The orchestrator uses these to decide
whether a parse failure is worth retrying and when.
"""
from datetime import datetime, timedelta, timezone

MAX_RETRIES = 5
_BACKOFF_BASE_HOURS = 1
_BACKOFF_CAP_HOURS = 24

# Substrings (matched case-insensitively) that mark a failure as transient —
# an infrastructure hiccup (quota, rate-limit, timeout, network) rather than a
# problem with the document itself.
# These are matched as plain substrings and rely on the controlled internal pipeline never emitting these tokens in a non-transient position (e.g. "500" only appears as an HTTP status).
_TRANSIENT_MARKERS = (
    "429", "resource_exhausted", "503", "overloaded", "500",
    "timeout", "deadline", "connection", "network",
    "download_failed", "temporarily",
)


def classify_failure(error: str) -> str:
    """Return 'transient' (worth retrying) or 'permanent'.

    Unknown errors default to 'permanent' so we never retry-loop forever on a
    failure mode we haven't characterised.
    """
    e = (error or "").lower()
    if any(marker in e for marker in _TRANSIENT_MARKERS):
        return "transient"
    return "permanent"


def compute_next_retry(retry_count: int, now: datetime | None = None) -> str:
    """ISO-8601 UTC timestamp for the next retry attempt.

    Exponential backoff: BASE * 2**retry_count, capped at CAP hours.
    `retry_count` is the count BEFORE this attempt (0 -> 1h, 1 -> 2h, ...).

    The caller is responsible for checking ``retry_count < MAX_RETRIES`` before
    scheduling a retry; enforcement lives in the orchestrator, not here.
    ``now`` must be timezone-aware (UTC) — a naive datetime yields an
    offset-less timestamp.
    """
    now = now or datetime.now(timezone.utc)
    hours = min(_BACKOFF_BASE_HOURS * (2 ** retry_count), _BACKOFF_CAP_HOURS)
    return (now + timedelta(hours=hours)).isoformat()

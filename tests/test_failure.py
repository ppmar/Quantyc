"""Unit tests for pipeline.failure — error taxonomy + retry backoff."""
from datetime import datetime, timezone

import pytest

from pipeline.failure import classify_failure, compute_next_retry, MAX_RETRIES


@pytest.mark.parametrize("error", [
    "llm_api_error:ClientError:429 RESOURCE_EXHAUSTED. monthly spending cap",
    "study_parse_error:llm_api_error:503 overloaded",
    "request timeout after 60s",
    "deadline exceeded",
    "connection reset by peer",
    "download_failed",
    "Server returned 500",
    "temporarily unavailable",
])
def test_transient_errors(error):
    assert classify_failure(error) == "transient"


@pytest.mark.parametrize("error", [
    "minimum_data_missing:requires_npv_and_initial_capex",
    "validation_error:1_errors",
    "malformed document: no text layer",
    "missing_ticker_or_date",
    "company_not_found:ZZZ",
    "some unrecognised failure",
    "",
])
def test_permanent_errors(error):
    assert classify_failure(error) == "permanent"


def test_backoff_grows_then_caps():
    now = datetime(2026, 6, 2, 12, 0, 0, tzinfo=timezone.utc)
    def hours(rc):
        ts = datetime.fromisoformat(compute_next_retry(rc, now=now))
        return (ts - now).total_seconds() / 3600
    assert hours(0) == 1
    assert hours(1) == 2
    assert hours(2) == 4
    assert hours(3) == 8
    assert hours(10) == 24   # capped


def test_max_retries_constant():
    assert MAX_RETRIES == 5

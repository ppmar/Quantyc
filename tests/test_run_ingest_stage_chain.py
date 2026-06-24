"""PR4 smoke tests: _run_ingest chains stage classification after bootstrap.

Not load-bearing — the backfill itself is covered by test_backfill_project_stages.
These only assert the wiring: the classifier runs once with classify_all=False,
shares the _stage_backfill_running guard, and a classifier failure is non-fatal.
"""
import os
from unittest.mock import patch

# Keep the APScheduler off during import (module-level scheduler.start()).
os.environ.setdefault("INGEST_SCHEDULE", "0")

import app as app_module


def _reset_status():
    app_module.pipeline_status = {
        "running": True, "phase": "polling", "failed_count": 0, "error": None,
    }
    app_module._stage_backfill_running = False


def test_ingest_runs_classification_once_with_auto_mode():
    _reset_status()
    with patch("ingest.asx_poller.poll_tickers"), \
         patch("ingest.ozmin_loader.load_ozmin", return_value={}), \
         patch("scripts.backfill_project_stages.run_backfill",
               return_value={"classified": 0}) as mock_bf:
        app_module._run_ingest(["DEG"], 20)

    mock_bf.assert_called_once_with(classify_all=False)
    assert app_module.pipeline_status["phase"] == "done"
    assert app_module._stage_backfill_running is False  # guard released


def test_classifier_failure_is_non_fatal():
    _reset_status()
    with patch("ingest.asx_poller.poll_tickers"), \
         patch("ingest.ozmin_loader.load_ozmin", return_value={}), \
         patch("scripts.backfill_project_stages.run_backfill",
               side_effect=RuntimeError("gemini throttled")):
        app_module._run_ingest(["DEG"], 20)

    # Ingest still completes; the failure does not blank out the run.
    assert app_module.pipeline_status["phase"] in ("done", "done_with_errors")
    assert app_module._stage_backfill_running is False  # guard released even on error


def test_classification_skipped_when_guard_already_held():
    _reset_status()
    app_module._stage_backfill_running = True  # /api/backfill-stages already running
    with patch("ingest.asx_poller.poll_tickers"), \
         patch("ingest.ozmin_loader.load_ozmin", return_value={}), \
         patch("scripts.backfill_project_stages.run_backfill") as mock_bf:
        app_module._run_ingest(["DEG"], 20)

    mock_bf.assert_not_called()  # never concurrent with the endpoint path
    assert app_module.pipeline_status["phase"] == "done"

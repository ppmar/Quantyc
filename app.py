"""
Quantyc API

Flask app with blueprints. Registers routes from api/ modules.

Usage:
    python app.py                        # dev server on port 8000
    gunicorn app:app -b 0.0.0.0:$PORT    # production (Railway)
"""

import logging
import os
import threading
import time
import traceback
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

from db import init_db

# Pipeline progress tracking
pipeline_status = {
    "running": False,
    "ticker": None,
    "phase": None,
    "current_doc": None,
    "docs_total": 0,
    "docs_done": 0,
    "started_at": None,
    "error": None,
    "failed_count": 0,
}

app = Flask(__name__)
CORS(app)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-key-change-in-prod")

init_db()

# Register blueprints
from api.upload import bp as upload_bp
from api.documents import bp as documents_bp
from api.financials import bp as financials_bp
from api.review import bp as review_bp

app.register_blueprint(upload_bp)
app.register_blueprint(documents_bp)
app.register_blueprint(financials_bp)
app.register_blueprint(review_bp)


# ---------------------------------------------------------------------------
# Pipeline trigger endpoints
# ---------------------------------------------------------------------------

@app.route("/api/pipeline/status")
def api_pipeline_status():
    return jsonify(pipeline_status)


@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    """
    Fetch announcements from ASX API and run orchestrator.
    Body: { "tickers": ["DEG", "WAF"], "count": 50 }
    """
    data = request.get_json(silent=True) or {}
    tickers = data.get("tickers", [])
    count = data.get("count", 50)

    if not tickers:
        from pathlib import Path
        pilot_path = Path(__file__).resolve().parent / "pilot_tickers.txt"
        if pilot_path.exists():
            tickers = [
                line.strip() for line in pilot_path.read_text().splitlines()
                if line.strip() and not line.startswith("#")
            ]

    if not tickers:
        return jsonify({"error": "No tickers provided and no pilot_tickers.txt found"}), 400

    _start_ingest(tickers, count)
    return jsonify({"status": "started", "tickers": tickers, "count": count})


@app.route("/api/orchestrate", methods=["POST"])
def api_orchestrate():
    """Run the orchestrator on all pending/classified docs."""
    _start_orchestrate()
    return jsonify({"status": "started"})


def _start_ingest(tickers, count):
    global pipeline_status
    pipeline_status = {
        "running": True,
        "ticker": ", ".join(t.upper() for t in tickers),
        "phase": "polling",
        "current_doc": None,
        "docs_total": 0,
        "docs_done": 0,
        "started_at": time.time(),
        "error": None,
        "failed_count": 0,
    }
    thread = threading.Thread(target=_run_ingest, args=(tickers, count), daemon=True)
    thread.start()


def _run_ingest(tickers, count):
    global pipeline_status
    try:
        from ingest.asx_poller import poll_tickers
        from pipeline.orchestrator import run_orchestrator

        # Phase 1: Poll ASX
        pipeline_status["phase"] = "polling"
        poll_tickers(tickers, count=count, status=pipeline_status)

        # Phase 2: Classify + Extract + Normalize
        pipeline_status["phase"] = "processing"
        run_orchestrator()

        failed = pipeline_status.get("failed_count", 0)
        pipeline_status["phase"] = "done_with_errors" if failed > 0 else "done"
        pipeline_status["running"] = False
    except Exception:
        pipeline_status["phase"] = "error"
        pipeline_status["error"] = traceback.format_exc().split("\n")[-2]
        pipeline_status["running"] = False
        logger.error("Ingest failed:\n%s", traceback.format_exc())


def _start_orchestrate():
    global pipeline_status
    pipeline_status = {
        "running": True,
        "ticker": "all",
        "phase": "processing",
        "current_doc": None,
        "docs_total": 0,
        "docs_done": 0,
        "started_at": time.time(),
        "error": None,
        "failed_count": 0,
    }
    thread = threading.Thread(target=_run_orchestrate, daemon=True)
    thread.start()


def _run_orchestrate():
    global pipeline_status
    try:
        from pipeline.orchestrator import run_orchestrator
        stats = run_orchestrator()
        pipeline_status["phase"] = "done"
        pipeline_status["running"] = False
        logger.info("Orchestrator done: %s", stats)
    except Exception:
        pipeline_status["phase"] = "error"
        pipeline_status["error"] = traceback.format_exc().split("\n")[-2]
        pipeline_status["running"] = False
        logger.error("Orchestrator failed:\n%s", traceback.format_exc())


# ---------------------------------------------------------------------------
# Scheduled auto-ingest
# ---------------------------------------------------------------------------

SCHEDULE_INTERVAL_HOURS = int(os.environ.get("INGEST_INTERVAL_HOURS", "24"))
SCHEDULE_ENABLED = os.environ.get("INGEST_SCHEDULE", "1") == "1"

scheduler = BackgroundScheduler(daemon=True)


def _load_pilot_tickers() -> list[str]:
    pilot_path = Path(__file__).resolve().parent / "pilot_tickers.txt"
    if not pilot_path.exists():
        return []
    return [
        line.strip() for line in pilot_path.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


def _scheduled_ingest():
    """Called by APScheduler on interval."""
    if pipeline_status.get("running"):
        logger.info("Scheduled ingest skipped — pipeline already running")
        return
    tickers = _load_pilot_tickers()
    if not tickers:
        logger.warning("Scheduled ingest skipped — no tickers in pilot_tickers.txt")
        return
    logger.info("Scheduled ingest starting for %d tickers", len(tickers))
    _start_ingest(tickers, 50)


if SCHEDULE_ENABLED:
    scheduler.add_job(
        _scheduled_ingest,
        "interval",
        hours=SCHEDULE_INTERVAL_HOURS,
        id="auto_ingest",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Auto-ingest scheduled every %dh", SCHEDULE_INTERVAL_HOURS)


@app.route("/api/schedule")
def api_schedule():
    """Get schedule status."""
    job = scheduler.get_job("auto_ingest")
    return jsonify({
        "enabled": job is not None,
        "interval_hours": SCHEDULE_INTERVAL_HOURS,
        "next_run": str(job.next_run_time) if job else None,
        "tickers": _load_pilot_tickers(),
    })


@app.route("/api/schedule/toggle", methods=["POST"])
def api_schedule_toggle():
    """Enable or disable the scheduled ingest."""
    job = scheduler.get_job("auto_ingest")
    if job:
        scheduler.remove_job("auto_ingest")
        return jsonify({"enabled": False})
    else:
        scheduler.add_job(
            _scheduled_ingest,
            "interval",
            hours=SCHEDULE_INTERVAL_HOURS,
            id="auto_ingest",
            replace_existing=True,
        )
        return jsonify({"enabled": True, "interval_hours": SCHEDULE_INTERVAL_HOURS})


@app.route("/api/schedule/run", methods=["POST"])
def api_schedule_run_now():
    """Trigger an immediate ingest run (same as scheduled)."""
    if pipeline_status.get("running"):
        return jsonify({"error": "Pipeline already running"}), 409
    _scheduled_ingest()
    return jsonify({"status": "started"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)

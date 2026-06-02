"""Tests for POST /api/backfill-stages."""
from unittest.mock import patch

import pytest


@pytest.fixture
def client():
    import app as app_module
    app_module.app.config["TESTING"] = True
    app_module._stage_backfill_running = False
    with app_module.app.test_client() as c:
        yield c, app_module


def test_backfill_starts(client):
    c, app_module = client
    with patch("scripts.backfill_project_stages.run_backfill", return_value={"classified": 0}):
        resp = c.post("/api/backfill-stages")
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "started"


def test_backfill_rejects_concurrent(client):
    c, app_module = client
    app_module._stage_backfill_running = True
    resp = c.post("/api/backfill-stages")
    assert resp.get_json()["status"] == "already_running"

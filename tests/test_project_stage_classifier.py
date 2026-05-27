"""Tests for parsers/project_stage_classifier.py"""
import json
from unittest.mock import MagicMock, patch

import pytest

from parsers.project_stage_classifier import (
    ClassificationError,
    InsufficientEvidenceError,
    ProjectEvidence,
    ProjectStageInference,
    StudyEvidence,
    ResourceEvidence,
    AnnEvidence,
    classify_project,
)


# ─── Helpers ─────────────────────────────────────────────────────────

def _make_evidence(**kwargs) -> ProjectEvidence:
    defaults = {
        "studies": [StudyEvidence("DFS", "2024-08-15", "Hemi Gold Project DFS")],
        "resources": [ResourceEvidence("Au", "Indicated", 250_000_000, "2024-04-20")],
        "recent_announcements": [AnnEvidence("DFS Confirms Tier-1 Gold Project", "2024-08-15")],
    }
    defaults.update(kwargs)
    return ProjectEvidence(**defaults)


def _valid_response_json(**overrides) -> str:
    base = {
        "stage": "feasibility",
        "stage_confidence": "high",
        "region": "Pilbara",
        "reasoning": "DFS completed August 2024 for the Hemi Gold Project confirms feasibility stage.",
    }
    base.update(overrides)
    return json.dumps(base)


def _mock_genai_response(text: str, parsed=None):
    resp = MagicMock()
    resp.text = text
    resp.parsed = parsed
    return resp


# ─── Pydantic schema tests ──────────────────────────────────────────

class TestProjectStageInference:
    def test_valid_payload(self):
        inf = ProjectStageInference(
            stage="feasibility",
            stage_confidence="high",
            region="Pilbara",
            reasoning="DFS completed in 2024.",
        )
        assert inf.stage == "feasibility"
        assert inf.region == "Pilbara"

    def test_region_none_valid(self):
        inf = ProjectStageInference(
            stage="exploration",
            stage_confidence="low",
            region=None,
            reasoning="RC drilling commenced.",
        )
        assert inf.region is None

    def test_invalid_stage_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            ProjectStageInference(
                stage="mining",  # not in taxonomy
                stage_confidence="high",
                region=None,
                reasoning="test",
            )

    def test_invalid_confidence_rejected(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            ProjectStageInference(
                stage="production",
                stage_confidence="very_high",
                region=None,
                reasoning="test",
            )


# ─── Evidence tests ─────────────────────────────────────────────────

class TestProjectEvidence:
    def test_empty_evidence(self):
        ev = ProjectEvidence()
        assert ev.is_empty()

    def test_non_empty_with_studies(self):
        ev = ProjectEvidence(studies=[StudyEvidence("DFS", "2024-01-01", "Test")])
        assert not ev.is_empty()

    def test_to_dict(self):
        ev = _make_evidence()
        d = ev.to_dict()
        assert "studies" in d
        assert len(d["studies"]) == 1


# ─── Classifier function tests ──────────────────────────────────────

class TestClassifyProject:
    def test_empty_evidence_raises_before_api(self):
        """InsufficientEvidenceError raised before any HTTP call."""
        with pytest.raises(InsufficientEvidenceError):
            classify_project(
                project_id=1,
                project_name="Test",
                company_ticker="TST",
                state=None,
                country=None,
                evidence=ProjectEvidence(),
            )

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("parsers.project_stage_classifier.genai")
    def test_valid_response_parsed(self, mock_genai):
        """Valid Gemini response is parsed into ProjectStageInference."""
        parsed_obj = ProjectStageInference(
            stage="feasibility",
            stage_confidence="high",
            region="Pilbara",
            reasoning="DFS completed August 2024.",
        )
        mock_genai.Client.return_value.models.generate_content.return_value = (
            _mock_genai_response(_valid_response_json(), parsed=parsed_obj)
        )

        result = classify_project(
            project_id=1,
            project_name="Hemi",
            company_ticker="DEG",
            state="Western Australia",
            country="Australia",
            evidence=_make_evidence(),
        )

        assert result.stage == "feasibility"
        assert result.stage_confidence == "high"
        assert result.region == "Pilbara"

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("parsers.project_stage_classifier.genai")
    def test_api_failure_raises_classification_error(self, mock_genai):
        """API failure raises ClassificationError."""
        mock_genai.Client.return_value.models.generate_content.side_effect = (
            RuntimeError("API down")
        )
        with pytest.raises(ClassificationError, match="llm_api_error"):
            classify_project(
                project_id=1,
                project_name="Hemi",
                company_ticker="DEG",
                state="Western Australia",
                country="Australia",
                evidence=_make_evidence(),
            )

    @patch.dict("os.environ", {"GOOGLE_API_KEY": "fake-key"})
    @patch("parsers.project_stage_classifier.genai")
    def test_fallback_to_text_parse(self, mock_genai):
        """When response.parsed is None, falls back to JSON text parse."""
        mock_genai.Client.return_value.models.generate_content.return_value = (
            _mock_genai_response(_valid_response_json(region=None), parsed=None)
        )

        result = classify_project(
            project_id=1,
            project_name="Hemi",
            company_ticker="DEG",
            state=None,
            country="Australia",
            evidence=_make_evidence(),
        )

        assert result.stage == "feasibility"
        assert result.region is None

    def test_no_api_key_raises(self):
        """Missing API key raises ClassificationError."""
        with patch.dict("os.environ", {}, clear=True):
            with pytest.raises(ClassificationError, match="google_api_key_not_set"):
                classify_project(
                    project_id=1,
                    project_name="Test",
                    company_ticker="TST",
                    state=None,
                    country=None,
                    evidence=_make_evidence(),
                )

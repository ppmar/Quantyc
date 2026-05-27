"""
Project Stage Classifier — Gemini 2.5 Flash.

Infers the development stage and sub-state region of a mining project
from document evidence already in the database.
"""
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Literal, Optional

from google import genai
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

LLM_MODEL = os.environ.get("STAGE_LLM_MODEL", "gemini-2.5-flash")

_PROMPT_PATH = Path(__file__).parent / "project_stage_classifier_prompt.md"


def _load_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


CLASSIFIER_PROMPT = _load_prompt()


# ─── Exceptions ──────────────────────────────────────────────────────

class ClassificationError(Exception):
    """Gemini call or validation failed."""


class InsufficientEvidenceError(Exception):
    """Not enough document evidence to attempt classification."""


# ─── Pydantic response schema ───────────────────────────────────────

class ProjectStageInference(BaseModel):
    stage: Literal[
        "production",
        "care_and_maintenance",
        "development",
        "feasibility",
        "advanced_exploration",
        "exploration",
        "unknown",
    ]
    stage_confidence: Literal["high", "medium", "low"]
    region: Optional[str] = Field(
        None,
        max_length=80,
        description=(
            "Finer-than-state region name (e.g., 'Pilbara', 'Salta', "
            "'Cariboo'). NULL if not clearly attestable from the evidence."
        ),
    )
    reasoning: str = Field(
        ...,
        max_length=800,
        description="One paragraph citing the specific evidence used.",
    )


# ─── Evidence structs ───────────────────────────────────────────────

@dataclass
class StudyEvidence:
    study_stage: str
    study_date: Optional[str]
    document_title: Optional[str]


@dataclass
class ResourceEvidence:
    commodity: str
    category: str
    tonnes: Optional[float]
    effective_date: Optional[str]


@dataclass
class AnnEvidence:
    title: str
    announcement_date: Optional[str]


@dataclass
class ProjectEvidence:
    studies: list[StudyEvidence] = field(default_factory=list)
    resources: list[ResourceEvidence] = field(default_factory=list)
    recent_announcements: list[AnnEvidence] = field(default_factory=list)

    def is_empty(self) -> bool:
        return (
            len(self.studies) == 0
            and len(self.resources) == 0
            and len(self.recent_announcements) == 0
        )

    def to_dict(self) -> dict:
        return asdict(self)


# ─── Classifier ─────────────────────────────────────────────────────

def _build_user_content(
    project_name: str,
    company_ticker: str,
    state: Optional[str],
    country: Optional[str],
    evidence: ProjectEvidence,
) -> str:
    """Build the user-facing content block sent alongside the system prompt."""
    lines = [
        f"Project: {project_name}",
        f"Ticker: {company_ticker}",
        f"Country: {country or 'Unknown'}",
        f"State: {state or 'Unknown'}",
        "",
        "=== Studies ===",
    ]
    if evidence.studies:
        for s in evidence.studies:
            lines.append(f"- {s.study_stage} ({s.study_date or 'no date'}): {s.document_title or 'untitled'}")
    else:
        lines.append("(none)")

    lines.append("")
    lines.append("=== JORC Resources ===")
    if evidence.resources:
        for r in evidence.resources:
            tonnes_str = f"{r.tonnes:,.0f}t" if r.tonnes else "unknown tonnage"
            lines.append(f"- {r.commodity} {r.category} ({r.effective_date or 'no date'}): {tonnes_str}")
    else:
        lines.append("(none)")

    lines.append("")
    lines.append("=== Recent Announcements (last 6 months) ===")
    if evidence.recent_announcements:
        for a in evidence.recent_announcements:
            lines.append(f"- [{a.announcement_date or 'no date'}] {a.title}")
    else:
        lines.append("(none)")

    return "\n".join(lines)


def classify_project(
    project_id: int,
    project_name: str,
    company_ticker: str,
    state: Optional[str],
    country: Optional[str],
    evidence: ProjectEvidence,
) -> ProjectStageInference:
    """Call Gemini Flash with the evidence and return a validated inference."""
    if evidence.is_empty():
        raise InsufficientEvidenceError(
            f"No studies, resources, or announcements for project {project_id} ({project_name})"
        )

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise ClassificationError("google_api_key_not_set")

    user_content = _build_user_content(
        project_name, company_ticker, state, country, evidence
    )

    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model=LLM_MODEL,
            contents=[CLASSIFIER_PROMPT, user_content],
            config={
                "response_mime_type": "application/json",
                "response_schema": ProjectStageInference,
                "temperature": 0.0,
            },
        )
    except Exception as e:
        logger.error("Gemini call failed for project %d (%s): %s", project_id, project_name, e)
        raise ClassificationError(f"llm_api_error:{type(e).__name__}:{e}")

    try:
        result: ProjectStageInference = response.parsed
        if result is None:
            raw = json.loads(response.text)
            result = ProjectStageInference.model_validate(raw)
    except Exception as e:
        logger.error("Response parse failed for project %d: %s", project_id, e)
        raise ClassificationError(f"response_parse_error:{type(e).__name__}")

    return result

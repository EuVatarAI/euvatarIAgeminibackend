"""DTOs for public-experience endpoints."""

from typing import Any

from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class LeadConfigData(BaseModel):
    """Lead form configuration returned to the public player."""

    enabled: bool = Field(default=False)
    require_before_unlock: bool = Field(default=False)
    fields: list[dict[str, object]] = Field(default_factory=list)
    button_label: str | None = Field(default=None)
    avatar_generation: dict[str, object] = Field(default_factory=dict)


class CreateLeadRequest(BaseModel):
    """Request payload used to create a public lead."""

    mode_used: str = Field(default="mobile")
    create_credential: bool = Field(default=False)
    data: dict[str, Any] = Field(default_factory=dict)


class CreateLeadData(BaseModel):
    """Lead creation payload returned by the public lead endpoint."""

    lead_id: str


class CompleteLeadRequest(BaseModel):
    """Request payload used to mark a public lead as completed."""

    archetype_result_id: str = Field(min_length=1)


class MetricsData(BaseModel):
    """Aggregate metrics for a public experience."""

    started: int = 0
    completed: int = 0
    dropped: int = 0


LeadConfigResponse = ApiResponse[LeadConfigData]
CreateLeadResponse = ApiResponse[CreateLeadData]
MetricsResponse = ApiResponse[MetricsData]

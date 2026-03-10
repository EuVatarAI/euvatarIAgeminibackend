from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class LeadConfigData(BaseModel):
    enabled: bool = Field(default=False)
    require_before_unlock: bool = Field(default=False)
    fields: list[dict[str, str]] = Field(default_factory=list)
    button_label: str | None = Field(default=None)


class CreateLeadRequest(BaseModel):
    mode_used: str = Field(default="mobile")
    create_credential: bool = Field(default=False)
    data: dict[str, str] = Field(default_factory=dict)


class CreateLeadData(BaseModel):
    lead_id: str


class CompleteLeadRequest(BaseModel):
    archetype_result_id: str = Field(min_length=1)


class MetricsData(BaseModel):
    started: int = 0
    completed: int = 0
    dropped: int = 0


LeadConfigResponse = ApiResponse[LeadConfigData]
CreateLeadResponse = ApiResponse[CreateLeadData]
MetricsResponse = ApiResponse[MetricsData]

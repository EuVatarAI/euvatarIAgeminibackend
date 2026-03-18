"""DTOs for generation lifecycle endpoints."""

from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class CreateGenerationRequest(BaseModel):
    """Request payload used to create or reuse a generation."""

    experience_id: str = Field(min_length=1)
    credential_id: str = Field(min_length=1)
    phone: str | None = None


class CreateGenerationData(BaseModel):
    """Response payload returned after generation creation."""

    generation_id: str
    reused: bool = False
    token: str | None = None


class GenerationStatusData(BaseModel):
    """Status payload returned when polling a generation."""

    id: str
    status: str
    output_url: str | None = None
    cutout_url: str | None = None
    final_card_url: str | None = None
    error_message: str | None = None


class CreateGenerationFinalCardSignedUrlRequest(BaseModel):
    """Request payload for generating a signed upload URL for a final card."""

    file_size_bytes: int = Field(ge=1)


class CreateGenerationFinalCardSignedUrlData(BaseModel):
    """Signed upload URL payload for final card uploads."""

    upload_url: str
    storage_path: str
    bucket: str


class ConfirmGenerationFinalCardRequest(BaseModel):
    """Request payload used to confirm a final card upload."""

    storage_path: str = Field(min_length=1)
    bucket: str | None = None
    public_url: str | None = None


class ConfirmGenerationFinalCardData(BaseModel):
    """Confirmation payload returned after persisting final card metadata."""

    final_card_path: str
    final_card_url: str | None = None


class GenerationLogItem(BaseModel):
    """Single persisted worker log entry returned by the generation logs endpoint."""

    id: str
    level: str
    event: str
    message: str
    payload_json: dict[str, object] = Field(default_factory=dict)
    created_at: str


CreateGenerationResponse = ApiResponse[CreateGenerationData]
GenerationStatusResponse = ApiResponse[GenerationStatusData]
CreateGenerationFinalCardSignedUrlResponse = ApiResponse[
    CreateGenerationFinalCardSignedUrlData
]
ConfirmGenerationFinalCardResponse = ApiResponse[ConfirmGenerationFinalCardData]

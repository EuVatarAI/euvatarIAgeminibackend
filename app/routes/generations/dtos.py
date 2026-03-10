from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class CreateGenerationRequest(BaseModel):
    experience_id: str = Field(min_length=1)
    credential_id: str = Field(min_length=1)
    phone: str | None = None


class CreateGenerationData(BaseModel):
    generation_id: str
    reused: bool = False
    token: str | None = None


class GenerationStatusData(BaseModel):
    id: str
    status: str
    output_url: str | None = None
    final_card_url: str | None = None
    error_message: str | None = None


class CreateGenerationFinalCardSignedUrlRequest(BaseModel):
    file_size_bytes: int = Field(ge=1)


class CreateGenerationFinalCardSignedUrlData(BaseModel):
    upload_url: str
    storage_path: str
    bucket: str


class ConfirmGenerationFinalCardRequest(BaseModel):
    storage_path: str = Field(min_length=1)
    bucket: str | None = None
    public_url: str | None = None


class ConfirmGenerationFinalCardData(BaseModel):
    final_card_path: str
    final_card_url: str | None = None


class GenerationLogItem(BaseModel):
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

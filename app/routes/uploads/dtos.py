"""DTOs for upload endpoints."""

from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class CreateSignedUploadRequest(BaseModel):
    """Request payload used to create a signed upload URL."""

    experience_id: str = Field(min_length=1)
    type: str = Field(min_length=1)
    file_size_bytes: int = Field(ge=1)
    field_key: str | None = None


class CreateSignedUploadData(BaseModel):
    """Signed upload URL payload returned to the client."""

    upload_url: str
    storage_path: str


class ConfirmUploadRequest(BaseModel):
    """Request payload used to confirm a completed upload."""

    experience_id: str = Field(min_length=1)
    credential_id: str = Field(min_length=1)
    storage_path: str = Field(min_length=1)
    type: str = Field(min_length=1)
    phone: str | None = None
    field_key: str | None = None
    field_label: str | None = None


class ConfirmUploadData(BaseModel):
    """Upload confirmation payload with an optional eager generation id."""

    generation_id: str | None = None


SignedUploadResponse = ApiResponse[CreateSignedUploadData]
ConfirmUploadResponse = ApiResponse[ConfirmUploadData]

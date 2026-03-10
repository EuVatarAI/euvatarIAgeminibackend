from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class CreateSignedUploadRequest(BaseModel):
    experience_id: str = Field(min_length=1)
    type: str = Field(min_length=1)
    file_size_bytes: int = Field(ge=1)


class CreateSignedUploadData(BaseModel):
    upload_url: str
    storage_path: str


class ConfirmUploadRequest(BaseModel):
    experience_id: str = Field(min_length=1)
    credential_id: str = Field(min_length=1)
    storage_path: str = Field(min_length=1)
    type: str = Field(min_length=1)
    phone: str | None = None


class ConfirmUploadData(BaseModel):
    generation_id: str | None = None


SignedUploadResponse = ApiResponse[CreateSignedUploadData]
ConfirmUploadResponse = ApiResponse[ConfirmUploadData]

from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class ValidateGeminiKeyRequest(BaseModel):
    api_key: str = Field(min_length=1)
    model: str | None = None


class ValidateGeminiKeyData(BaseModel):
    valid: bool
    model: str


ValidateGeminiKeyResponse = ApiResponse[ValidateGeminiKeyData]

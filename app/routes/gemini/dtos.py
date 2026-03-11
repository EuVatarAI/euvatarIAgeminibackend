"""DTOs for Gemini utility endpoints."""

from pydantic import BaseModel, Field

from app.core.dtos import ApiResponse


class ValidateGeminiKeyRequest(BaseModel):
    """Request payload for Gemini key validation."""

    api_key: str = Field(min_length=1)
    model: str | None = None


class ValidateGeminiKeyData(BaseModel):
    """Validation result returned by the Gemini utility endpoint."""

    valid: bool
    model: str


ValidateGeminiKeyResponse = ApiResponse[ValidateGeminiKeyData]

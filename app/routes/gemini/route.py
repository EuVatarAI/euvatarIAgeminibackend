"""FastAPI routes for Gemini-related utility operations."""

from fastapi import APIRouter

from app.routes.gemini.controller import GeminiController
from app.routes.gemini.dtos import ValidateGeminiKeyRequest, ValidateGeminiKeyResponse

router = APIRouter()
controller = GeminiController()


@router.post("/gemini/validate-key", response_model=ValidateGeminiKeyResponse)
async def validate_key(request: ValidateGeminiKeyRequest):
    """Validate a Gemini API key payload.

    Args:
        request (ValidateGeminiKeyRequest): Request body containing the key to validate.

    Returns:
        ValidateGeminiKeyResponse: Validation result returned by the controller.
    """
    return await controller.validate_key(request)

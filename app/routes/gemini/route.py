from fastapi import APIRouter

from app.routes.gemini.controller import GeminiController
from app.routes.gemini.dtos import ValidateGeminiKeyRequest, ValidateGeminiKeyResponse

router = APIRouter()
controller = GeminiController()


@router.post("/gemini/validate-key", response_model=ValidateGeminiKeyResponse)
async def validate_key(request: ValidateGeminiKeyRequest):
    return await controller.validate_key(request)

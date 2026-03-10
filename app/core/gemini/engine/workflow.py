from app.core.config import get_settings
from app.routes.gemini.dtos import ValidateGeminiKeyData, ValidateGeminiKeyRequest


class GeminiWorkflow:
    async def validate_key(
        self, request: ValidateGeminiKeyRequest
    ) -> ValidateGeminiKeyData:
        settings = get_settings()
        model = request.model or settings.gemini_model
        return ValidateGeminiKeyData(valid=True, model=model)

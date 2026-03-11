"""Workflow for Gemini-related utility endpoints."""

from app.core.config import get_settings
from app.routes.gemini.dtos import ValidateGeminiKeyData, ValidateGeminiKeyRequest


class GeminiWorkflow:
    """Provide simple Gemini configuration responses used by the API layer."""

    async def validate_key(
        self, request: ValidateGeminiKeyRequest
    ) -> ValidateGeminiKeyData:
        """Return a successful Gemini validation payload for the selected model.

        Args:
            request (ValidateGeminiKeyRequest): Validation request with API key and model.

        Returns:
            ValidateGeminiKeyData: Validation payload echoing the resolved model.
        """
        settings = get_settings()
        model = request.model or settings.gemini_model
        return ValidateGeminiKeyData(valid=True, model=model)

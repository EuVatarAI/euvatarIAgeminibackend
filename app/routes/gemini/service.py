"""Service layer for Gemini utility endpoints."""

from app.core.gemini.engine.workflow import GeminiWorkflow
from app.routes.gemini.dtos import ValidateGeminiKeyData, ValidateGeminiKeyRequest


class GeminiService:
    """Delegate Gemini utility operations to the workflow layer.

    Attributes:
        workflow (GeminiWorkflow): Workflow responsible for Gemini-specific logic.
    """

    def __init__(self, workflow: GeminiWorkflow | None = None) -> None:
        self.workflow = workflow or GeminiWorkflow()

    async def validate_key(
        self, request: ValidateGeminiKeyRequest
    ) -> ValidateGeminiKeyData:
        """Validate a Gemini key request through the workflow.

        Args:
            request (ValidateGeminiKeyRequest): Validation request payload.

        Returns:
            ValidateGeminiKeyData: Validation result emitted by the workflow.
        """
        return await self.workflow.validate_key(request)

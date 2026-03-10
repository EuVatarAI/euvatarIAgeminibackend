from app.core.gemini.engine.workflow import GeminiWorkflow
from app.routes.gemini.dtos import ValidateGeminiKeyData, ValidateGeminiKeyRequest


class GeminiService:
    def __init__(self, workflow: GeminiWorkflow | None = None) -> None:
        self.workflow = workflow or GeminiWorkflow()

    async def validate_key(
        self, request: ValidateGeminiKeyRequest
    ) -> ValidateGeminiKeyData:
        return await self.workflow.validate_key(request)

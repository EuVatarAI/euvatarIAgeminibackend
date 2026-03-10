from app.core.generations.engine.workflow import GenerationsWorkflow
from app.routes.generations.dtos import (
    ConfirmGenerationFinalCardRequest,
    CreateGenerationFinalCardSignedUrlRequest,
    CreateGenerationRequest,
)


class GenerationsService:
    def __init__(self, workflow: GenerationsWorkflow | None = None) -> None:
        self.workflow = workflow or GenerationsWorkflow()

    async def create_generation(self, request: CreateGenerationRequest) -> dict:
        return await self.workflow.create_generation(request)

    async def get_generation_status(self, generation_id: str) -> dict:
        return await self.workflow.get_generation_status(generation_id)

    async def get_generation_logs(self, generation_id: str, limit: int = 200) -> dict:
        return await self.workflow.get_generation_logs(generation_id, limit)

    async def create_final_card_signed_url(
        self,
        generation_id: str,
        request: CreateGenerationFinalCardSignedUrlRequest,
    ) -> dict:
        return await self.workflow.create_final_card_signed_url(generation_id, request)

    async def confirm_final_card(
        self,
        generation_id: str,
        request: ConfirmGenerationFinalCardRequest,
    ) -> dict:
        return await self.workflow.confirm_final_card(generation_id, request)

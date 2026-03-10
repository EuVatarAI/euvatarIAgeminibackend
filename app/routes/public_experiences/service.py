from app.core.public_experiences.engine.workflow import PublicExperiencesWorkflow
from app.routes.public_experiences.dtos import (
    CompleteLeadRequest,
    CreateLeadRequest,
)


class PublicExperiencesService:
    def __init__(self, workflow: PublicExperiencesWorkflow | None = None) -> None:
        self.workflow = workflow or PublicExperiencesWorkflow()

    async def get_lead_config(self, slug: str) -> dict:
        return await self.workflow.get_lead_config(slug)

    async def create_lead(self, slug: str, request: CreateLeadRequest) -> dict:
        return await self.workflow.create_lead(slug, request)

    async def complete_lead(
        self,
        slug: str,
        lead_id: str,
        request: CompleteLeadRequest,
    ) -> dict:
        return await self.workflow.complete_lead(slug, lead_id, request)

    async def get_metrics(self, slug: str) -> dict:
        return await self.workflow.get_metrics(slug)

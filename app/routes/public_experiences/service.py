"""Service layer for public-experience endpoints."""

from app.core.public_experiences.engine.workflow import PublicExperiencesWorkflow
from app.routes.public_experiences.dtos import (
    CompleteLeadRequest,
    CreateLeadRequest,
)


class PublicExperiencesService:
    """Delegate public-experience operations to the workflow layer.

    Attributes:
        workflow (PublicExperiencesWorkflow): Workflow responsible for lead and metrics logic.
    """

    def __init__(self, workflow: PublicExperiencesWorkflow | None = None) -> None:
        self.workflow = workflow or PublicExperiencesWorkflow()

    async def get_lead_config(self, slug: str) -> dict:
        """Fetch the public lead configuration for a slug.

        Args:
            slug (str): Public experience slug.

        Returns:
            dict: Lead configuration payload from the workflow.
        """
        return await self.workflow.get_lead_config(slug)

    async def create_lead(self, slug: str, request: CreateLeadRequest) -> dict:
        """Create a lead through the workflow layer.

        Args:
            slug (str): Public experience slug.
            request (CreateLeadRequest): Lead creation payload.

        Returns:
            dict: Lead creation result emitted by the workflow.
        """
        return await self.workflow.create_lead(slug, request)

    async def complete_lead(
        self,
        slug: str,
        lead_id: str,
        request: CompleteLeadRequest,
    ) -> dict:
        """Complete a lead through the workflow layer.

        Args:
            slug (str): Public experience slug.
            lead_id (str): Lead identifier to complete.
            request (CompleteLeadRequest): Completion payload.

        Returns:
            dict: Completion result emitted by the workflow.
        """
        return await self.workflow.complete_lead(slug, lead_id, request)

    async def get_metrics(self, slug: str) -> dict:
        """Fetch aggregate metrics for a public experience slug.

        Args:
            slug (str): Public experience slug.

        Returns:
            dict: Metrics payload emitted by the workflow.
        """
        return await self.workflow.get_metrics(slug)

"""Service layer for generation lifecycle endpoints."""

from app.core.generations.engine.workflow import GenerationsWorkflow
from app.routes.generations.dtos import (
    ConfirmGenerationFinalCardRequest,
    CreateGenerationFinalCardSignedUrlRequest,
    CreateGenerationRequest,
)


class GenerationsService:
    """Delegate generation operations to the workflow layer.

    Attributes:
        workflow (GenerationsWorkflow): Workflow responsible for generation persistence
            and status resolution.
    """

    def __init__(self, workflow: GenerationsWorkflow | None = None) -> None:
        self.workflow = workflow or GenerationsWorkflow()

    async def create_generation(self, request: CreateGenerationRequest) -> dict:
        """Create or reuse a generation through the workflow layer.

        Args:
            request (CreateGenerationRequest): Generation creation request payload.

        Returns:
            dict: Generation payload emitted by the workflow.
        """
        return await self.workflow.create_generation(request)

    async def get_generation_status(self, generation_id: str) -> dict:
        """Fetch generation status through the workflow layer.

        Args:
            generation_id (str): Generation identifier to query.

        Returns:
            dict: Current generation status payload.
        """
        return await self.workflow.get_generation_status(generation_id)

    async def get_generation_logs(self, generation_id: str, limit: int = 200) -> dict:
        """Fetch persisted generation logs through the workflow layer.

        Args:
            generation_id (str): Generation identifier to inspect.
            limit (int): Maximum number of log rows to return.

        Returns:
            dict: Generation log payload.
        """
        return await self.workflow.get_generation_logs(generation_id, limit)

    async def create_final_card_signed_url(
        self,
        generation_id: str,
        request: CreateGenerationFinalCardSignedUrlRequest,
    ) -> dict:
        """Create a signed upload URL for a final card asset.

        Args:
            generation_id (str): Generation that owns the final card.
            request (CreateGenerationFinalCardSignedUrlRequest): Upload request payload.

        Returns:
            dict: Signed upload response emitted by the workflow.
        """
        return await self.workflow.create_final_card_signed_url(generation_id, request)

    async def confirm_final_card(
        self,
        generation_id: str,
        request: ConfirmGenerationFinalCardRequest,
    ) -> dict:
        """Confirm a final card upload through the workflow layer.

        Args:
            generation_id (str): Generation that owns the uploaded card.
            request (ConfirmGenerationFinalCardRequest): Final card confirmation payload.

        Returns:
            dict: Confirmation payload emitted by the workflow.
        """
        return await self.workflow.confirm_final_card(generation_id, request)

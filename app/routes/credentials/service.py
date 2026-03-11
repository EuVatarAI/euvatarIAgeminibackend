"""Service layer for credential endpoints."""

from app.core.credentials.engine.workflow import CredentialsWorkflow
from app.routes.credentials.dtos import CreateCredentialRequest


class CredentialsService:
    """Delegate credential operations to the workflow layer.

    Attributes:
        workflow (CredentialsWorkflow): Workflow responsible for credential persistence.
    """

    def __init__(self, workflow: CredentialsWorkflow | None = None) -> None:
        self.workflow = workflow or CredentialsWorkflow()

    async def create_credential(self, request: CreateCredentialRequest) -> dict:
        """Create a credential through the workflow layer.

        Args:
            request (CreateCredentialRequest): Validated credential request payload.

        Returns:
            dict: Workflow result containing the created credential identifier.
        """
        return await self.workflow.create_credential(request)

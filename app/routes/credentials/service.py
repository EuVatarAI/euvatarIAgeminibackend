from app.core.credentials.engine.workflow import CredentialsWorkflow
from app.routes.credentials.dtos import CreateCredentialRequest


class CredentialsService:
    def __init__(self, workflow: CredentialsWorkflow | None = None) -> None:
        self.workflow = workflow or CredentialsWorkflow()

    async def create_credential(self, request: CreateCredentialRequest) -> dict:
        return await self.workflow.create_credential(request)

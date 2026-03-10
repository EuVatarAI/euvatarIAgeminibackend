from app.core.uploads.engine.workflow import UploadsWorkflow
from app.routes.uploads.dtos import ConfirmUploadRequest, CreateSignedUploadRequest


class UploadsService:
    def __init__(self, workflow: UploadsWorkflow | None = None) -> None:
        self.workflow = workflow or UploadsWorkflow()

    async def create_signed_url(self, request: CreateSignedUploadRequest) -> dict:
        return await self.workflow.create_signed_url(request)

    async def confirm_upload(self, request: ConfirmUploadRequest) -> dict:
        return await self.workflow.confirm_upload(request)

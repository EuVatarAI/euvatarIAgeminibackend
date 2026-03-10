from fastapi import APIRouter

from app.routes.credentials.controller import CredentialsController
from app.routes.credentials.dtos import CreateCredentialRequest

router = APIRouter()
controller = CredentialsController()


@router.post("/credentials")
async def create_credential(request: CreateCredentialRequest):
    return await controller.create_credential(request)

"""FastAPI routes for credential operations."""

from fastapi import APIRouter

from app.routes.credentials.controller import CredentialsController
from app.routes.credentials.dtos import CreateCredentialRequest

router = APIRouter()
controller = CredentialsController()


@router.post("/credentials")
async def create_credential(request: CreateCredentialRequest):
    """Create a credential for an experience session.

    Args:
        request (CreateCredentialRequest): Request body with experience and collected data.

    Returns:
        dict | JSONResponse: Response produced by the controller.
    """
    return await controller.create_credential(request)

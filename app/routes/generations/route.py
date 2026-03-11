"""FastAPI routes for generation lifecycle operations."""

from fastapi import APIRouter

from app.routes.generations.controller import GenerationsController
from app.routes.generations.dtos import (
    ConfirmGenerationFinalCardRequest,
    CreateGenerationFinalCardSignedUrlRequest,
    CreateGenerationRequest,
)

router = APIRouter()
controller = GenerationsController()


@router.post("/generations")
async def create_generation(request: CreateGenerationRequest):
    """Create or reuse a generation for a credential.

    Args:
        request (CreateGenerationRequest): Request body for generation creation.

    Returns:
        dict | JSONResponse: Response produced by the generations controller.
    """
    return await controller.create_generation(request)


@router.get("/generations/{generation_id}")
async def get_generation_status(generation_id: str):
    """Return the current status for a generation.

    Args:
        generation_id (str): Generation identifier to query.

    Returns:
        dict | JSONResponse: Response produced by the generations controller.
    """
    return await controller.get_generation_status(generation_id)


@router.get("/generations/{generation_id}/logs")
async def get_generation_logs(generation_id: str, limit: int = 200):
    """Return persisted generation logs for a generation.

    Args:
        generation_id (str): Generation identifier to inspect.
        limit (int): Maximum number of log entries to return.

    Returns:
        dict | JSONResponse: Response produced by the generations controller.
    """
    return await controller.get_generation_logs(generation_id, limit)


@router.post("/generations/{generation_id}/final-card/signed-url")
async def create_generation_final_card_signed_url(
    generation_id: str,
    request: CreateGenerationFinalCardSignedUrlRequest,
):
    """Create a signed upload URL for a rendered final card.

    Args:
        generation_id (str): Generation that will own the final card asset.
        request (CreateGenerationFinalCardSignedUrlRequest): Request body with upload size.

    Returns:
        dict | JSONResponse: Response produced by the generations controller.
    """
    return await controller.create_final_card_signed_url(generation_id, request)


@router.post("/generations/{generation_id}/final-card")
async def confirm_generation_final_card(
    generation_id: str,
    request: ConfirmGenerationFinalCardRequest,
):
    """Confirm a previously uploaded final card asset.

    Args:
        generation_id (str): Generation that owns the card.
        request (ConfirmGenerationFinalCardRequest): Final card confirmation payload.

    Returns:
        dict | JSONResponse: Response produced by the generations controller.
    """
    return await controller.confirm_final_card(generation_id, request)

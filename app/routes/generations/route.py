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
    return await controller.create_generation(request)


@router.get("/generations/{generation_id}")
async def get_generation_status(generation_id: str):
    return await controller.get_generation_status(generation_id)


@router.get("/generations/{generation_id}/logs")
async def get_generation_logs(generation_id: str, limit: int = 200):
    return await controller.get_generation_logs(generation_id, limit)


@router.post("/generations/{generation_id}/final-card/signed-url")
async def create_generation_final_card_signed_url(
    generation_id: str,
    request: CreateGenerationFinalCardSignedUrlRequest,
):
    return await controller.create_final_card_signed_url(generation_id, request)


@router.post("/generations/{generation_id}/final-card")
async def confirm_generation_final_card(
    generation_id: str,
    request: ConfirmGenerationFinalCardRequest,
):
    return await controller.confirm_final_card(generation_id, request)

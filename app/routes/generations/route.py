from fastapi import APIRouter

from app.routes.generations.controller import GenerationsController
from app.routes.generations.dtos import CreateGenerationRequest

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

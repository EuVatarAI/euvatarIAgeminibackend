from fastapi import APIRouter

from app.routes.public_experiences.controller import PublicExperiencesController
from app.routes.public_experiences.dtos import (
    CompleteLeadRequest,
    CreateLeadRequest,
)

router = APIRouter()
controller = PublicExperiencesController()


@router.get("/public/experience/{slug}/lead-config")
async def get_lead_config(slug: str):
    return await controller.get_lead_config(slug)


@router.post("/public/experience/{slug}/leads")
async def create_lead(slug: str, request: CreateLeadRequest):
    return await controller.create_lead(slug, request)


@router.post("/public/experience/{slug}/leads/{lead_id}/complete")
async def complete_lead(slug: str, lead_id: str, request: CompleteLeadRequest):
    return await controller.complete_lead(slug, lead_id, request)


@router.get("/public/experience/{slug}/metrics")
async def get_metrics(slug: str):
    return await controller.get_metrics(slug)

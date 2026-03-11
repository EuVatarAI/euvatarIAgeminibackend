"""FastAPI routes for public experience operations."""

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
    """Return the lead form configuration for a public experience.

    Args:
        slug (str): Public experience slug.

    Returns:
        dict | JSONResponse: Response produced by the public experiences controller.
    """
    return await controller.get_lead_config(slug)


@router.post("/public/experience/{slug}/leads")
async def create_lead(slug: str, request: CreateLeadRequest):
    """Create a lead for a public experience.

    Args:
        slug (str): Public experience slug.
        request (CreateLeadRequest): Lead creation payload.

    Returns:
        dict | JSONResponse: Response produced by the public experiences controller.
    """
    return await controller.create_lead(slug, request)


@router.post("/public/experience/{slug}/leads/{lead_id}/complete")
async def complete_lead(slug: str, lead_id: str, request: CompleteLeadRequest):
    """Complete a lead for a public experience.

    Args:
        slug (str): Public experience slug.
        lead_id (str): Lead identifier.
        request (CompleteLeadRequest): Completion payload.

    Returns:
        dict | JSONResponse: Response produced by the public experiences controller.
    """
    return await controller.complete_lead(slug, lead_id, request)


@router.get("/public/experience/{slug}/metrics")
async def get_metrics(slug: str):
    """Return aggregate metrics for a public experience.

    Args:
        slug (str): Public experience slug.

    Returns:
        dict | JSONResponse: Response produced by the public experiences controller.
    """
    return await controller.get_metrics(slug)

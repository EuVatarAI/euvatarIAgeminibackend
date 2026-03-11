"""FastAPI routes for health-check endpoints."""

from fastapi import APIRouter

from app.core.dtos import ApiResponse
from app.routes.health.controller import HealthController

router = APIRouter()
controller = HealthController()


@router.get("/health", response_model=ApiResponse[dict[str, str]])
async def healthcheck() -> ApiResponse[dict[str, str]]:
    """Return a lightweight health-check response for uptime monitoring.

    Returns:
        ApiResponse[dict[str, str]]: Wrapped health payload from the controller layer.
    """
    return await controller.get_health()

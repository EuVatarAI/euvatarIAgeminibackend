"""Top-level API router composition for the FastAPI application."""

from fastapi import APIRouter

from app.routes.credentials.route import router as credentials_router
from app.routes.generations.route import router as generations_router
from app.routes.gemini.route import router as gemini_router
from app.routes.health.route import router as health_router
from app.routes.public_experiences.route import router as public_experiences_router
from app.routes.uploads.route import router as uploads_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(
    public_experiences_router, prefix="/v2", tags=["public-experiences"]
)
api_router.include_router(credentials_router, prefix="/v2", tags=["credentials"])
api_router.include_router(uploads_router, prefix="/v2", tags=["uploads"])
api_router.include_router(generations_router, prefix="/v2", tags=["generations"])
api_router.include_router(gemini_router, prefix="/v2", tags=["gemini"])

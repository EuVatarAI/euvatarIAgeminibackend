"""Controller layer for public experience endpoints."""

from fastapi.responses import JSONResponse

from app.core.exceptions import AppError
from app.core.exceptions import FeatureNotImplementedError
from app.routes.public_experiences.dtos import (
    CompleteLeadRequest,
    CreateLeadRequest,
)
from app.routes.public_experiences.service import PublicExperiencesService


class PublicExperiencesController:
    """Handle public-experience requests and map workflow failures into HTTP responses.

    Attributes:
        service (PublicExperiencesService): Service responsible for public-experience logic.
    """

    def __init__(self, service: PublicExperiencesService | None = None) -> None:
        self.service = service or PublicExperiencesService()

    async def get_lead_config(self, slug: str) -> dict | JSONResponse:
        """Return the lead form configuration for a published experience slug.

        Args:
            slug (str): Public experience slug.

        Returns:
            dict | JSONResponse: Lead-config payload or a mapped error response.
        """
        try:
            return await self.service.get_lead_config(slug)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except FeatureNotImplementedError as exc:
            return JSONResponse(
                status_code=501,
                content={"ok": False, "error": str(exc)},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"lead_config_exception:{exc}"},
            )

    async def create_lead(
        self,
        slug: str,
        request: CreateLeadRequest,
    ) -> dict | JSONResponse:
        """Create a lead row for a published experience.

        Args:
            slug (str): Public experience slug.
            request (CreateLeadRequest): Lead creation payload.

        Returns:
            dict | JSONResponse: Created lead payload or a mapped error response.
        """
        try:
            payload = await self.service.create_lead(slug, request)
            return JSONResponse(status_code=201, content=payload)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except FeatureNotImplementedError as exc:
            return JSONResponse(
                status_code=501,
                content={"ok": False, "error": str(exc)},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"create_public_lead_exception:{exc}"},
            )

    async def complete_lead(
        self,
        slug: str,
        lead_id: str,
        request: CompleteLeadRequest,
    ) -> dict | JSONResponse:
        """Mark a lead as completed for a published experience.

        Args:
            slug (str): Public experience slug.
            lead_id (str): Lead identifier to complete.
            request (CompleteLeadRequest): Completion payload.

        Returns:
            dict | JSONResponse: Completion payload or a mapped error response.
        """
        try:
            return await self.service.complete_lead(slug, lead_id, request)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except FeatureNotImplementedError as exc:
            return JSONResponse(
                status_code=501,
                content={"ok": False, "error": str(exc)},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"complete_public_lead_exception:{exc}"},
            )

    async def get_metrics(self, slug: str) -> dict | JSONResponse:
        """Return aggregate lead and generation metrics for a public experience.

        Args:
            slug (str): Public experience slug.

        Returns:
            dict | JSONResponse: Metrics payload or a mapped error response.
        """
        try:
            return await self.service.get_metrics(slug)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except FeatureNotImplementedError as exc:
            return JSONResponse(
                status_code=501,
                content={"ok": False, "error": str(exc)},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"public_experience_metrics_exception:{exc}"},
            )

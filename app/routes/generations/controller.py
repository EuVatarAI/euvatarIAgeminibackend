"""Controller layer for generation lifecycle endpoints."""

from fastapi.responses import JSONResponse

from app.core.exceptions import AppError
from app.routes.generations.dtos import (
    ConfirmGenerationFinalCardRequest,
    CreateGenerationFinalCardSignedUrlRequest,
    CreateGenerationRequest,
)
from app.routes.generations.service import GenerationsService


class GenerationsController:
    """Handle generation API requests and normalize workflow failures into HTTP responses.

    The controller delegates generation lifecycle operations to the service layer and maps
    domain errors into JSON responses that the frontend can consume consistently.

    Attributes:
        service (GenerationsService): Service responsible for generation operations.
    """

    def __init__(self, service: GenerationsService | None = None) -> None:
        self.service = service or GenerationsService()

    async def create_generation(
        self,
        request: CreateGenerationRequest,
    ) -> dict | JSONResponse:
        """Create or reuse a generation for the given credential.

        Args:
            request (CreateGenerationRequest): Validated generation creation payload.

        Returns:
            dict | JSONResponse: `201` or `200` success response, or a mapped error response.
        """
        try:
            payload = await self.service.create_generation(request)
            status_code = 200 if payload.get("reused") else 201
            return JSONResponse(status_code=status_code, content=payload)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"create_generation_exception:{exc}"},
            )

    async def get_generation_status(
        self,
        generation_id: str,
    ) -> dict | JSONResponse:
        """Return the current status for a generation id.

        Args:
            generation_id (str): Generation identifier to query.

        Returns:
            dict | JSONResponse: Status payload or a mapped error response.
        """
        try:
            return await self.service.get_generation_status(generation_id)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"generation_status_exception:{exc}"},
            )

    async def get_generation_logs(
        self,
        generation_id: str,
        limit: int = 200,
    ) -> dict | JSONResponse:
        """Return persisted worker logs for a generation.

        Args:
            generation_id (str): Generation identifier to inspect.
            limit (int): Maximum number of log entries to return.

        Returns:
            dict | JSONResponse: Log payload or a mapped error response.
        """
        try:
            return await self.service.get_generation_logs(generation_id, limit)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"generation_logs_exception:{exc}"},
            )

    async def create_final_card_signed_url(
        self,
        generation_id: str,
        request: CreateGenerationFinalCardSignedUrlRequest,
    ) -> dict | JSONResponse:
        """Create a signed upload URL for the rendered final card image.

        Args:
            generation_id (str): Generation that will own the final card asset.
            request (CreateGenerationFinalCardSignedUrlRequest): Upload request payload.

        Returns:
            dict | JSONResponse: Signed upload response or a mapped error response.
        """
        try:
            return await self.service.create_final_card_signed_url(
                generation_id,
                request,
            )
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "error": f"generation_final_card_signed_url_exception:{exc}",
                },
            )

    async def confirm_final_card(
        self,
        generation_id: str,
        request: ConfirmGenerationFinalCardRequest,
    ) -> dict | JSONResponse:
        """Confirm a previously uploaded final card and persist its metadata.

        Args:
            generation_id (str): Generation that owns the final card.
            request (ConfirmGenerationFinalCardRequest): Final card confirmation payload.

        Returns:
            dict | JSONResponse: Confirmation payload or a mapped error response.
        """
        try:
            return await self.service.confirm_final_card(generation_id, request)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={
                    "ok": False,
                    "error": f"generation_final_card_confirm_exception:{exc}",
                },
            )

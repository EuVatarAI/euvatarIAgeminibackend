from fastapi.responses import JSONResponse

from app.core.exceptions import AppError
from app.routes.generations.dtos import (
    ConfirmGenerationFinalCardRequest,
    CreateGenerationFinalCardSignedUrlRequest,
    CreateGenerationRequest,
)
from app.routes.generations.service import GenerationsService


class GenerationsController:
    def __init__(self, service: GenerationsService | None = None) -> None:
        self.service = service or GenerationsService()

    async def create_generation(
        self,
        request: CreateGenerationRequest,
    ) -> dict | JSONResponse:
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

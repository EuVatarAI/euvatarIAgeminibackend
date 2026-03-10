from fastapi.responses import JSONResponse

from app.core.exceptions import AppError
from app.routes.uploads.dtos import ConfirmUploadRequest, CreateSignedUploadRequest
from app.routes.uploads.service import UploadsService


class UploadsController:
    def __init__(self, service: UploadsService | None = None) -> None:
        self.service = service or UploadsService()

    async def create_signed_url(
        self,
        request: CreateSignedUploadRequest,
    ) -> dict | JSONResponse:
        try:
            return await self.service.create_signed_url(request)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"signed_url_exception:{exc}"},
            )

    async def confirm_upload(
        self,
        request: ConfirmUploadRequest,
    ) -> dict | JSONResponse:
        try:
            return await self.service.confirm_upload(request)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"confirm_upload_exception:{exc}"},
            )

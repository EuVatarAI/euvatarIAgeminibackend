from fastapi.responses import JSONResponse

from app.core.exceptions import AppError
from app.routes.credentials.dtos import CreateCredentialRequest
from app.routes.credentials.service import CredentialsService


class CredentialsController:
    def __init__(self, service: CredentialsService | None = None) -> None:
        self.service = service or CredentialsService()

    async def create_credential(
        self,
        request: CreateCredentialRequest,
    ) -> dict | JSONResponse:
        try:
            return await self.service.create_credential(request)
        except AppError as exc:
            return JSONResponse(
                status_code=exc.status_code,
                content={"ok": False, "error": exc.message},
            )
        except Exception as exc:
            return JSONResponse(
                status_code=500,
                content={"ok": False, "error": f"create_credential_exception:{exc}"},
            )

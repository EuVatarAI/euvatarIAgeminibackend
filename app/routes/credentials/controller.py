"""Controller layer for credential creation requests."""

from fastapi.responses import JSONResponse

from app.core.exceptions import AppError
from app.routes.credentials.dtos import CreateCredentialRequest
from app.routes.credentials.service import CredentialsService


class CredentialsController:
    """Handle credential creation and translate workflow errors into HTTP responses.

    Attributes:
        service (CredentialsService): Service responsible for credential creation logic.
    """

    def __init__(self, service: CredentialsService | None = None) -> None:
        self.service = service or CredentialsService()

    async def create_credential(
        self,
        request: CreateCredentialRequest,
    ) -> dict | JSONResponse:
        """Create a credential row for a public experience session.

        Args:
            request (CreateCredentialRequest): Validated credential payload.

        Returns:
            dict | JSONResponse: Success payload from the service or an error response.
        """
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

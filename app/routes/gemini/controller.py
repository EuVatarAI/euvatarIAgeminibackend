"""Controller layer for Gemini-related utility endpoints."""

from fastapi.responses import JSONResponse

from app.core.dtos import ApiResponse
from app.core.exceptions import FeatureNotImplementedError
from app.routes.gemini.dtos import ValidateGeminiKeyData, ValidateGeminiKeyRequest
from app.routes.gemini.service import GeminiService


class GeminiController:
    """Validate Gemini settings requests and map failures into HTTP responses.

    Attributes:
        service (GeminiService): Service responsible for Gemini-related operations.
    """

    def __init__(self, service: GeminiService | None = None) -> None:
        self.service = service or GeminiService()

    async def validate_key(
        self,
        request: ValidateGeminiKeyRequest,
    ) -> ApiResponse[ValidateGeminiKeyData] | JSONResponse:
        """Validate a Gemini API key request through the service layer.

        Args:
            request (ValidateGeminiKeyRequest): Request payload with API key and model.

        Returns:
            ApiResponse[ValidateGeminiKeyData] | JSONResponse: Success payload when validation
            completes, or a JSON error response when the route is not implemented.
        """
        try:
            payload = await self.service.validate_key(request)
            return ApiResponse(message="gemini_key_validated", data=payload)
        except FeatureNotImplementedError as exc:
            return JSONResponse(
                status_code=501,
                content={"success": False, "message": str(exc), "data": None},
            )

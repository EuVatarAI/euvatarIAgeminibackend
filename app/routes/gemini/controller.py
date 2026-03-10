from fastapi.responses import JSONResponse

from app.core.dtos import ApiResponse
from app.core.exceptions import FeatureNotImplementedError
from app.routes.gemini.dtos import ValidateGeminiKeyData, ValidateGeminiKeyRequest
from app.routes.gemini.service import GeminiService


class GeminiController:
    def __init__(self, service: GeminiService | None = None) -> None:
        self.service = service or GeminiService()

    async def validate_key(
        self,
        request: ValidateGeminiKeyRequest,
    ) -> ApiResponse[ValidateGeminiKeyData] | JSONResponse:
        try:
            payload = await self.service.validate_key(request)
            return ApiResponse(message="gemini_key_validated", data=payload)
        except FeatureNotImplementedError as exc:
            return JSONResponse(
                status_code=501,
                content={"success": False, "message": str(exc), "data": None},
            )

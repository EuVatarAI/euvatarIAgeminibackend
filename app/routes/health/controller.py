"""Controller layer for the health-check endpoint."""

from app.core.dtos import ApiResponse
from app.routes.health.service import HealthService


class HealthController:
    """Handle health-check requests and format the shared response envelope.

    The controller delegates the health lookup to the service layer and wraps the result
    in the generic API response model used by the backend.

    Attributes:
        service (HealthService): Service responsible for reading health information.
    """

    def __init__(self, service: HealthService | None = None) -> None:
        self.service = service or HealthService()

    async def get_health(self) -> ApiResponse[dict[str, str]]:
        """Return the backend health payload in the API response envelope.

        Returns:
            ApiResponse[dict[str, str]]: Service metadata describing status and environment.
        """
        payload = await self.service.get_health()
        return ApiResponse(message="service_available", data=payload)

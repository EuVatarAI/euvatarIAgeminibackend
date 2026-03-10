from app.core.dtos import ApiResponse
from app.routes.health.service import HealthService


class HealthController:
    def __init__(self, service: HealthService | None = None) -> None:
        self.service = service or HealthService()

    async def get_health(self) -> ApiResponse[dict[str, str]]:
        payload = await self.service.get_health()
        return ApiResponse(message="service_available", data=payload)

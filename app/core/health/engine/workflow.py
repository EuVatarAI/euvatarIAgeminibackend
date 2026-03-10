from app.core.config import get_settings


class HealthWorkflow:
    async def execute(self) -> dict[str, str]:
        settings = get_settings()
        return {
            "status": "ok",
            "service": settings.app_name,
            "environment": settings.app_env,
        }

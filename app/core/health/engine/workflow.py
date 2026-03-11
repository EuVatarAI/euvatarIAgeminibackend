"""Health workflow used by the API layer to expose service metadata."""

from app.core.config import get_settings


class HealthWorkflow:
    """Assemble a lightweight health payload from runtime settings."""

    async def execute(self) -> dict[str, str]:
        """Return the current service identity and environment.

        Returns:
            dict[str, str]: Health payload containing status, service name, and environment.
        """
        settings = get_settings()
        return {
            "status": "ok",
            "service": settings.app_name,
            "environment": settings.app_env,
        }

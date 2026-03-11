"""Service layer for health-check requests."""

from app.core.health.engine.workflow import HealthWorkflow


class HealthService:
    """Delegate health reads to the underlying workflow.

    Attributes:
        workflow (HealthWorkflow): Workflow that assembles the health payload.
    """

    def __init__(self, workflow: HealthWorkflow | None = None) -> None:
        self.workflow = workflow or HealthWorkflow()

    async def get_health(self) -> dict[str, str]:
        """Fetch the current health payload from the workflow.

        Returns:
            dict[str, str]: Health fields describing the running service.
        """
        return await self.workflow.execute()

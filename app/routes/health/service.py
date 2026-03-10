from app.core.health.engine.workflow import HealthWorkflow


class HealthService:
    def __init__(self, workflow: HealthWorkflow | None = None) -> None:
        self.workflow = workflow or HealthWorkflow()

    async def get_health(self) -> dict[str, str]:
        return await self.workflow.execute()

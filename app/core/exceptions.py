"""Domain exceptions shared across routes, workflows, and infrastructure adapters."""

class FeatureNotImplementedError(NotImplementedError):
    """Signal an API capability that is intentionally not implemented yet."""

    pass


class AppError(Exception):
    """Represent a controlled application error with an HTTP-friendly status code."""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code

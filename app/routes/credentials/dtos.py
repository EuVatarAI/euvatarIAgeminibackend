"""DTOs for credential creation endpoints."""

from typing import Any

from pydantic import BaseModel
from pydantic import Field


class CreateCredentialRequest(BaseModel):
    """Request payload used to create a credential row for an experience."""

    experience_id: str = Field(min_length=1)
    data: dict[str, Any] = Field(default_factory=dict)
    mode_used: str = Field(default="mobile")

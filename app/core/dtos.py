"""Shared response DTOs used across API routes."""

from typing import Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class ApiResponse(BaseModel, Generic[T]):
    """Generic API response envelope for successful route responses."""

    success: bool = Field(default=True)
    message: str = Field(default="ok")
    data: T | None = Field(default=None)

"""Pydantic settings definitions backed by environment variables."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables and the local `.env`.

    The settings model centralizes runtime values used by the API, Supabase integration,
    and Gemini clients. Derived helpers expose normalized values consumed by the rest of
    the backend.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = Field(default="gemini-v2-backend", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    app_debug: bool = Field(default=True, alias="APP_DEBUG")
    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8010, alias="APP_PORT")
    app_cors_origins: str = Field(
        default="http://localhost:8080", alias="APP_CORS_ORIGINS"
    )
    supabase_url: str = Field(default="", alias="SUPABASE_URL")
    supabase_publishable_key: str = Field(default="", alias="SUPABASE_PUBLISHABLE_KEY")
    supabase_service_role_key: str = Field(
        default="", alias="SUPABASE_SERVICE_ROLE_KEY"
    )
    supabase_bucket: str = Field(default="layout-assets", alias="SUPABASE_BUCKET")
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    gemini_model: str = Field(default="gemini-2.5-flash", alias="GEMINI_MODEL")
    gemini_image_model: str = Field(
        default="gemini-2.5-flash-image", alias="GEMINI_IMAGE_MODEL"
    )
    gemini_image_aspect_ratio: str = Field(
        default="9:16", alias="GEMINI_IMAGE_ASPECT_RATIO"
    )

    @property
    def cors_origins(self) -> list[str]:
        """Return the configured CORS origins as a normalized list of URLs.

        Returns:
            list[str]: Comma-separated origins split, trimmed, and filtered of blanks.
        """
        return [
            item.strip() for item in self.app_cors_origins.split(",") if item.strip()
        ]

    @property
    def supabase_service_role(self) -> str:
        """Expose the service-role key under the legacy property name.

        Returns:
            str: Supabase service-role token configured for privileged API calls.
        """
        return self.supabase_service_role_key


@lru_cache
def get_settings() -> Settings:
    """Load and cache the application settings singleton.

    Returns:
        Settings: Cached settings instance for the current process.
    """
    return Settings()

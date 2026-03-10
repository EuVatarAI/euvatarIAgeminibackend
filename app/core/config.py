from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
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

    @property
    def cors_origins(self) -> list[str]:
        return [
            item.strip() for item in self.app_cors_origins.split(",") if item.strip()
        ]

    @property
    def supabase_service_role(self) -> str:
        return self.supabase_service_role_key


@lru_cache
def get_settings() -> Settings:
    return Settings()

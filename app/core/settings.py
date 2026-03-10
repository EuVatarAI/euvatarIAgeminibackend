from dataclasses import dataclass

from app.core.config import get_settings


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    supabase_service_role: str
    supabase_bucket: str
    gemini_api_key: str | None
    gemini_image_model: str

    @staticmethod
    def load() -> "Settings":
        config = get_settings()
        return Settings(
            supabase_url=config.supabase_url,
            supabase_service_role=config.supabase_service_role_key,
            supabase_bucket=config.supabase_bucket,
            gemini_api_key=config.gemini_api_key or None,
            gemini_image_model=config.gemini_image_model,
        )

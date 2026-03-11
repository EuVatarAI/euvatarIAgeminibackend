"""Workflow responsible for creating credential rows for public experiences."""

import requests

from app.core.config import Settings
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.core.logging import get_logger
from app.infrastructure.supabase_rest import get_json
from app.infrastructure.supabase_rest import rest_headers
from app.routes.credentials.dtos import CreateCredentialRequest


_ALLOWED_MODES = {"mobile", "totem", "auto"}

logger = get_logger(__name__)


class CredentialsWorkflow:
    """Persist participant credential data after validating experience and mode inputs.

    Attributes:
        settings (Settings): Runtime settings used for Supabase REST requests.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def create_credential(self, request: CreateCredentialRequest) -> dict:
        """Create a credential row for a published experience.

        Args:
            request (CreateCredentialRequest): Credential payload collected from the player.

        Returns:
            dict: Success payload containing the created credential identifier.

        Raises:
            AppError: Raised when validation fails or Supabase operations fail.
        """
        experience_id = (request.experience_id or "").strip()
        mode_used = (request.mode_used or "").strip().lower()
        data = request.data or {}

        if not experience_id:
            raise AppError("missing_experience_id", status_code=400)
        if not isinstance(data, dict):
            raise AppError("invalid_data_payload", status_code=400)
        if mode_used not in _ALLOWED_MODES:
            raise AppError("invalid_mode_used", status_code=400)

        self._load_active_experience_by_id(experience_id)
        credential_id = self._insert_credential_row(
            experience_id=experience_id,
            data=data,
            mode_used=mode_used,
        )

        logger.info(
            "[credentials] credential_created experience_id=%s credential_id=%s mode_used=%s fields=%s",
            experience_id,
            credential_id,
            mode_used,
            len(data.keys()),
        )
        return {"ok": True, "credential_id": credential_id}

    def _load_active_experience_by_id(self, experience_id: str) -> dict:
        """Load an experience and ensure it is active or published.

        Args:
            experience_id (str): Experience identifier to load.

        Returns:
            dict: Supabase row for the matching experience.

        Raises:
            AppError: Raised when the experience is missing, inactive, or Supabase fails.
        """
        try:
            rows = get_json(
                self.settings,
                "experiences",
                "id,status",
                {"id": f"eq.{experience_id}", "status": "in.(active,published)"},
                limit=1,
            )
        except requests.RequestException as exc:
            logger.error(
                "[credentials] supabase_unreachable operation=load_experience experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[credentials] experience_query_failed experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("experience_query_failed", status_code=502) from exc

        if not rows:
            raise AppError("experience_not_found_or_inactive", status_code=404)
        return rows[0]

    def _insert_credential_row(
        self,
        experience_id: str,
        data: dict,
        mode_used: str,
    ) -> str:
        """Insert a credential row into Supabase.

        Args:
            experience_id (str): Experience identifier associated with the credential.
            data (dict): Participant data to persist in `data_json`.
            mode_used (str): Capture mode used for the credential.

        Returns:
            str: Identifier of the inserted credential row.

        Raises:
            AppError: Raised when the insert fails or returns an empty payload.
        """
        url = f"{self.settings.supabase_url}/rest/v1/credentials"
        try:
            response = requests.post(
                url,
                headers={
                    **rest_headers(self.settings),
                    "Content-Type": "application/json",
                    "Prefer": "return=representation",
                },
                json=[
                    {
                        "experience_id": experience_id,
                        "data_json": data,
                        "mode_used": mode_used,
                    }
                ],
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[credentials] supabase_unreachable operation=insert_credential experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc

        if not response.ok:
            logger.error(
                "[credentials] credential_insert_failed experience_id=%s status=%s body=%s",
                experience_id,
                response.status_code,
                response.text[:200],
            )
            raise AppError("credential_insert_failed", status_code=502)

        rows = response.json() or []
        credential_id = str((rows[0] or {}).get("id") or "").strip() if rows else ""
        if not credential_id:
            raise AppError("credential_insert_empty", status_code=502)
        return credential_id

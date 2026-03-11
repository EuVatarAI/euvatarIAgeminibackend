"""Workflow responsible for generation lifecycle persistence and status queries."""

import datetime as dt

import requests

from app.core.config import Settings
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.core.logging import get_logger
from app.infrastructure.supabase_rest import get_json
from app.infrastructure.supabase_rest import rest_headers
from app.routes.generations.dtos import (
    ConfirmGenerationFinalCardRequest,
    CreateGenerationFinalCardSignedUrlRequest,
    CreateGenerationRequest,
)

_ALLOWED_GENERATION_KINDS = {"credential_card", "quiz_result", "photo_with"}
_MAX_FINAL_CARD_UPLOAD_BYTES = 20 * 1024 * 1024

logger = get_logger(__name__)


class GenerationsWorkflow:
    """Create, reuse, and inspect generation rows and final card metadata.

    Attributes:
        settings (Settings): Runtime settings used for Supabase REST operations.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def create_generation(self, request: CreateGenerationRequest) -> dict:
        """Create or reuse a generation row for a credential.

        Args:
            request (CreateGenerationRequest): Generation creation request payload.

        Returns:
            dict: Success payload with generation id, reuse flag, and optional token.

        Raises:
            AppError: Raised when validation fails or Supabase operations fail.
        """
        experience_id = (request.experience_id or "").strip()
        credential_id = (request.credential_id or "").strip()
        token = (request.phone or "").strip()

        if not experience_id:
            raise AppError("missing_experience_id", status_code=400)
        if not credential_id:
            raise AppError("missing_credential_id", status_code=400)

        experience = self._load_experience_by_id(experience_id)
        status = str(experience.get("status") or "").strip().lower()
        if status not in {"active", "published"}:
            raise AppError("experience_not_found_or_inactive", status_code=404)

        if not self._load_credential_for_experience(credential_id, experience_id):
            raise AppError("credential_not_found_for_experience", status_code=404)

        max_generations = int(experience.get("max_generations") or 0)
        if (
            max_generations > 0
            and self._count_done_generations(experience_id) >= max_generations
        ):
            raise AppError("generation_limit_exceeded", status_code=429)

        kind = self._kind_from_experience_type(str(experience.get("type") or ""))
        if kind not in _ALLOWED_GENERATION_KINDS:
            raise AppError("invalid_generation_kind", status_code=400)

        generation_id, reused = self._create_or_reuse_generation(
            experience_id=experience_id,
            credential_id=credential_id,
            kind=kind,
            token=token or None,
        )

        if token:
            self._update_generation_token(generation_id, token)

        logger.info(
            "[generations] generation_created experience_id=%s credential_id=%s generation_id=%s kind=%s reused=%s token_present=%s",
            experience_id,
            credential_id,
            generation_id,
            kind,
            reused,
            bool(token),
        )
        return {
            "ok": True,
            "generation_id": generation_id,
            "reused": reused,
            "token": token or None,
        }

    async def get_generation_status(self, generation_id: str) -> dict:
        """Return the current status payload for a generation.

        Args:
            generation_id (str): Generation identifier to query.

        Returns:
            dict: Status payload including output URLs when available.

        Raises:
            AppError: Raised when the id is missing, not found, or Supabase fails.
        """
        clean_generation_id = (generation_id or "").strip()
        if not clean_generation_id:
            raise AppError("missing_generation_id", status_code=400)

        try:
            try:
                rows = get_json(
                    self.settings,
                    "generations",
                    "id,status,output_path,output_url,final_card_path,final_card_url,error_message,duration_ms",
                    {"id": f"eq.{clean_generation_id}"},
                    limit=1,
                )
            except RuntimeError:
                rows = get_json(
                    self.settings,
                    "generations",
                    "id,status,output_path,output_url,error_message,duration_ms",
                    {"id": f"eq.{clean_generation_id}"},
                    limit=1,
                )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=get_status generation_id=%s error=%s",
                clean_generation_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[generations] status_query_failed generation_id=%s error=%s",
                clean_generation_id,
                str(exc),
            )
            raise AppError("generation_status_query_failed", status_code=502) from exc

        if not rows:
            raise AppError("generation_not_found", status_code=404)

        row = rows[0]
        output_url = row.get("output_url")
        if row.get("status") == "done" and not output_url and row.get("output_path"):
            output_url = self._build_signed_download_url(str(row.get("output_path")))
        final_card_url = row.get("final_card_url")
        if (
            row.get("status") == "done"
            and not final_card_url
            and row.get("final_card_path")
        ):
            final_card_url = self._build_signed_download_url(
                str(row.get("final_card_path"))
            )

        return {
            "ok": True,
            "id": clean_generation_id,
            "status": row.get("status"),
            "duration_ms": row.get("duration_ms"),
            "output_url": output_url,
            "final_card_url": final_card_url,
            "error_message": row.get("error_message"),
        }

    async def get_generation_logs(self, generation_id: str, limit: int = 200) -> dict:
        """Return persisted generation logs for a generation id.

        Args:
            generation_id (str): Generation identifier to inspect.
            limit (int): Maximum number of log entries to return.

        Returns:
            dict: Payload with generation id and ordered log entries.

        Raises:
            AppError: Raised when the id is missing or Supabase fails.
        """
        clean_generation_id = (generation_id or "").strip()
        if not clean_generation_id:
            raise AppError("missing_generation_id", status_code=400)

        safe_limit = max(1, min(int(limit), 500))

        try:
            rows = get_json(
                self.settings,
                "generation_logs",
                "id,level,event,message,payload_json,created_at",
                {
                    "generation_id": f"eq.{clean_generation_id}",
                    "order": "created_at.asc",
                },
                limit=safe_limit,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=get_logs generation_id=%s error=%s",
                clean_generation_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[generations] generation_logs_query_failed generation_id=%s error=%s",
                clean_generation_id,
                str(exc),
            )
            raise AppError("generation_logs_query_failed", status_code=502) from exc

        logs = [
            {
                "id": str(row.get("id") or ""),
                "level": str(row.get("level") or "info"),
                "event": str(row.get("event") or ""),
                "message": str(row.get("message") or ""),
                "payload_json": (
                    row.get("payload_json")
                    if isinstance(row.get("payload_json"), dict)
                    else {}
                ),
                "created_at": str(row.get("created_at") or ""),
            }
            for row in rows
        ]

        logger.info(
            "[generations] generation_logs_loaded generation_id=%s count=%s",
            clean_generation_id,
            len(logs),
        )
        return {
            "ok": True,
            "generation_id": clean_generation_id,
            "logs": logs,
        }

    async def create_final_card_signed_url(
        self,
        generation_id: str,
        request: CreateGenerationFinalCardSignedUrlRequest,
    ) -> dict:
        """Create a signed upload URL for a rendered final card image.

        Args:
            generation_id (str): Generation that owns the final card asset.
            request (CreateGenerationFinalCardSignedUrlRequest): Upload request payload.

        Returns:
            dict: Signed upload payload containing URL, storage path, and bucket.

        Raises:
            AppError: Raised when validation fails or storage signing fails.
        """
        clean_generation_id = (generation_id or "").strip()
        if not clean_generation_id:
            raise AppError("missing_generation_id", status_code=400)

        file_size_bytes = int(request.file_size_bytes)
        if file_size_bytes <= 0:
            raise AppError("invalid_file_size", status_code=400)
        if file_size_bytes > _MAX_FINAL_CARD_UPLOAD_BYTES:
            raise AppError("file_too_large", status_code=413)

        generation = self._load_generation_by_id(clean_generation_id)
        experience_id = str(generation.get("experience_id") or "").strip()
        if not experience_id:
            raise AppError("generation_missing_experience", status_code=500)

        bucket = self.settings.supabase_bucket
        storage_path = f"quiz/{experience_id}/final-cards/{clean_generation_id}.png"
        sign_url = (
            f"{self.settings.supabase_url}/storage/v1/object/upload/sign/"
            f"{bucket}/{storage_path}"
        )
        try:
            response = requests.post(
                sign_url,
                headers={
                    **rest_headers(self.settings),
                    "Content-Type": "application/json",
                },
                json={"expiresIn": 600},
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=create_final_card_signed_url generation_id=%s error=%s",
                clean_generation_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[generations] final_card_signed_url_failed generation_id=%s status=%s body=%s",
                clean_generation_id,
                response.status_code,
                response.text[:200],
            )
            raise AppError("final_card_signed_url_failed", status_code=502)

        data = response.json() or {}
        signed_url = (
            data.get("signedURL")
            or data.get("signedUrl")
            or data.get("uploadURL")
            or data.get("upload_url")
        )
        if not signed_url and data.get("url") and data.get("token"):
            base_url = str(data.get("url"))
            token = str(data.get("token"))
            if "token=" in base_url:
                signed_url = base_url
            else:
                sep = "&" if "?" in base_url else "?"
                signed_url = f"{base_url}{sep}token={token}"

        if not signed_url:
            raise AppError("signed_url_missing_in_response", status_code=502)

        upload_url = (
            signed_url
            if str(signed_url).startswith("http")
            else f"{self.settings.supabase_url}/storage/v1{signed_url}"
        )
        logger.info(
            "[generations] final_card_signed_url_created generation_id=%s storage_path=%s",
            clean_generation_id,
            storage_path,
        )
        return {
            "ok": True,
            "upload_url": upload_url,
            "storage_path": storage_path,
            "bucket": bucket,
        }

    async def confirm_final_card(
        self,
        generation_id: str,
        request: ConfirmGenerationFinalCardRequest,
    ) -> dict:
        """Confirm a final card upload and persist its metadata on the generation row.

        Args:
            generation_id (str): Generation that owns the final card asset.
            request (ConfirmGenerationFinalCardRequest): Confirmation payload with storage metadata.

        Returns:
            dict: Success payload containing the final card path and resolved URL.

        Raises:
            AppError: Raised when validation fails or persistence operations fail.
        """
        clean_generation_id = (generation_id or "").strip()
        if not clean_generation_id:
            raise AppError("missing_generation_id", status_code=400)

        storage_path = (request.storage_path or "").strip()
        bucket = (request.bucket or self.settings.supabase_bucket or "").strip()
        public_url = (request.public_url or "").strip()
        if not storage_path:
            raise AppError("missing_storage_path", status_code=400)
        if not bucket:
            raise AppError("missing_bucket", status_code=400)

        generation = self._load_generation_by_id(clean_generation_id)
        experience_id = str(generation.get("experience_id") or "").strip()
        if not experience_id:
            raise AppError("generation_missing_experience", status_code=500)

        expected_prefix = f"quiz/{experience_id}/final-cards/{clean_generation_id}"
        if not storage_path.startswith(expected_prefix):
            raise AppError("invalid_storage_path_scope", status_code=400)

        resolved_public_url = public_url or None
        self._update_generation_final_card(
            generation_id=clean_generation_id,
            storage_path=storage_path,
            bucket=bucket,
            public_url=resolved_public_url,
        )
        logger.info(
            "[generations] final_card_confirmed generation_id=%s storage_path=%s bucket=%s",
            clean_generation_id,
            storage_path,
            bucket,
        )
        return {
            "ok": True,
            "final_card_path": storage_path,
            "final_card_url": resolved_public_url
            or self._build_signed_download_url(storage_path),
        }

    def _load_experience_by_id(self, experience_id: str) -> dict:
        """Load an experience row by id.

        Args:
            experience_id (str): Experience identifier to query.

        Returns:
            dict: Matching experience row from Supabase.

        Raises:
            AppError: Raised when the row is missing or Supabase fails.
        """
        try:
            rows = get_json(
                self.settings,
                "experiences",
                "id,type,status,max_generations",
                {"id": f"eq.{experience_id}"},
                limit=1,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=load_experience experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[generations] experience_query_failed experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("experience_query_failed", status_code=502) from exc

        if not rows:
            raise AppError("experience_not_found_or_inactive", status_code=404)
        return rows[0]

    def _load_generation_by_id(self, generation_id: str) -> dict:
        """Load a generation row by id.

        Args:
            generation_id (str): Generation identifier to query.

        Returns:
            dict: Matching generation row from Supabase.

        Raises:
            AppError: Raised when the row is missing or Supabase fails.
        """
        try:
            rows = get_json(
                self.settings,
                "generations",
                "id,experience_id,credential_id,status,output_path,final_card_path",
                {"id": f"eq.{generation_id}"},
                limit=1,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=load_generation generation_id=%s error=%s",
                generation_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[generations] generation_query_failed generation_id=%s error=%s",
                generation_id,
                str(exc),
            )
            raise AppError("generation_query_failed", status_code=502) from exc
        if not rows:
            raise AppError("generation_not_found", status_code=404)
        return rows[0]

    def _load_credential_for_experience(
        self,
        credential_id: str,
        experience_id: str,
    ) -> dict | None:
        """Load a credential row scoped to an experience.

        Args:
            credential_id (str): Credential identifier to query.
            experience_id (str): Experience identifier used to scope the lookup.

        Returns:
            dict | None: Matching credential row, or `None` when absent.

        Raises:
            AppError: Raised when Supabase operations fail.
        """
        try:
            rows = get_json(
                self.settings,
                "credentials",
                "id,experience_id",
                {
                    "id": f"eq.{credential_id}",
                    "experience_id": f"eq.{experience_id}",
                },
                limit=1,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=load_credential experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[generations] credential_query_failed experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("credential_query_failed", status_code=502) from exc
        return rows[0] if rows else None

    def _count_done_generations(self, experience_id: str) -> int:
        """Count finished generations for an experience.

        Args:
            experience_id (str): Experience identifier to count rows for.

        Returns:
            int: Number of completed generations for the experience.
        """
        return self._count_rows(
            "generations",
            {"experience_id": f"eq.{experience_id}", "status": "eq.done"},
        )

    def _count_rows(self, table: str, filters: dict[str, str]) -> int:
        """Count rows in a Supabase table using exact-count headers.

        Args:
            table (str): Table name to count rows from.
            filters (dict[str, str]): Supabase filter parameters applied to the count query.

        Returns:
            int: Number of rows matching the requested filters.

        Raises:
            AppError: Raised when the count query fails.
        """
        url = f"{self.settings.supabase_url}/rest/v1/{table}"
        try:
            response = requests.get(
                url,
                headers={
                    **rest_headers(self.settings),
                    "Prefer": "count=exact",
                },
                params={"select": "id", **filters, "limit": "1"},
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=count_rows table=%s error=%s",
                table,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[generations] count_rows_failed table=%s status=%s body=%s",
                table,
                response.status_code,
                response.text[:200],
            )
            raise AppError(f"metrics_count_failed:{table}", status_code=502)

        content_range = str(response.headers.get("Content-Range") or "").strip()
        if "/" in content_range:
            total = content_range.rsplit("/", 1)[-1].strip()
            if total.isdigit():
                return int(total)
        rows = response.json() or []
        return len(rows)

    def _kind_from_experience_type(self, experience_type: str) -> str:
        """Map an experience type to the stored generation kind.

        Args:
            experience_type (str): Raw experience type string.

        Returns:
            str: Generation kind persisted in the database.
        """
        clean_type = (experience_type or "").strip().lower()
        if clean_type == "credentialing":
            return "credential_card"
        if clean_type == "photo_with":
            return "photo_with"
        return "quiz_result"

    def _find_reusable_generation(self, credential_id: str, kind: str) -> dict | None:
        """Return the latest reusable generation for a credential and kind.

        Args:
            credential_id (str): Credential identifier to search by.
            kind (str): Generation kind to reuse when possible.

        Returns:
            dict | None: Existing reusable generation row, or `None` when absent.

        Raises:
            AppError: Raised when Supabase operations fail.
        """
        try:
            rows = get_json(
                self.settings,
                "generations",
                "id,status,kind,credential_id,experience_id",
                {
                    "credential_id": f"eq.{credential_id}",
                    "kind": f"eq.{kind}",
                    "status": "in.(pending,processing,done)",
                    "order": "created_at.desc",
                },
                limit=1,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=find_reusable credential_id=%s kind=%s error=%s",
                credential_id,
                kind,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[generations] reusable_generation_query_failed credential_id=%s kind=%s error=%s",
                credential_id,
                kind,
                str(exc),
            )
            raise AppError("generation_query_failed", status_code=502) from exc
        return rows[0] if rows else None

    def _insert_generation(
        self,
        experience_id: str,
        credential_id: str,
        kind: str,
        token: str | None = None,
    ) -> str:
        """Insert a new pending generation row.

        Args:
            experience_id (str): Experience identifier associated with the generation.
            credential_id (str): Credential identifier associated with the generation.
            kind (str): Generation kind to persist.
            token (str | None): Optional token stored with the generation row.

        Returns:
            str: Identifier of the inserted generation row.

        Raises:
            AppError: Raised when the insert fails or returns an empty payload.
        """
        url = f"{self.settings.supabase_url}/rest/v1/generations"
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
                        "credential_id": credential_id,
                        "kind": kind,
                        "token": token,
                        "status": "pending",
                    }
                ],
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=insert_generation experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[generations] generation_insert_failed experience_id=%s credential_id=%s status=%s body=%s",
                experience_id,
                credential_id,
                response.status_code,
                response.text[:200],
            )
            raise AppError("generation_insert_failed", status_code=502)
        rows = response.json() or []
        generation_id = str((rows[0] or {}).get("id") or "").strip() if rows else ""
        if not generation_id:
            raise AppError("generation_insert_empty", status_code=502)
        return generation_id

    def _create_or_reuse_generation(
        self,
        experience_id: str,
        credential_id: str,
        kind: str,
        token: str | None = None,
    ) -> tuple[str, bool]:
        """Reuse a pending/processing generation or insert a new one.

        Args:
            experience_id (str): Experience identifier associated with the generation.
            credential_id (str): Credential identifier associated with the generation.
            kind (str): Generation kind to reuse or create.
            token (str | None): Optional token stored with new generations.

        Returns:
            tuple[str, bool]: Generation identifier and whether it was reused.
        """
        reusable = self._find_reusable_generation(credential_id, kind)
        if reusable and reusable.get("id"):
            generation_id = str(reusable["id"])
            logger.info(
                "[generations] generation_reused generation_id=%s credential_id=%s kind=%s status=%s",
                generation_id,
                credential_id,
                kind,
                str(reusable.get("status") or ""),
            )
            return generation_id, True

        generation_id = self._insert_generation(
            experience_id=experience_id,
            credential_id=credential_id,
            kind=kind,
            token=token,
        )
        return generation_id, False

    def _update_generation_token(self, generation_id: str, token: str) -> None:
        """Persist the provided token on an existing generation row.

        Args:
            generation_id (str): Generation identifier to update.
            token (str): Token value to persist.

        Raises:
            AppError: Raised when the update fails.
        """
        url = f"{self.settings.supabase_url}/rest/v1/generations?id=eq.{generation_id}"
        try:
            response = requests.patch(
                url,
                headers={
                    **rest_headers(self.settings),
                    "Content-Type": "application/json",
                    "Prefer": "return=representation",
                },
                json={"token": token},
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=update_token generation_id=%s error=%s",
                generation_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[generations] generation_token_update_failed generation_id=%s status=%s body=%s",
                generation_id,
                response.status_code,
                response.text[:200],
            )
            raise AppError("generation_token_update_failed", status_code=502)

    def _update_generation_final_card(
        self,
        generation_id: str,
        storage_path: str,
        bucket: str,
        public_url: str | None,
    ) -> None:
        """Persist final-card metadata on an existing generation row.

        Args:
            generation_id (str): Generation identifier to update.
            storage_path (str): Storage path of the final card.
            bucket (str): Storage bucket used for the final card.
            public_url (str | None): Optional pre-resolved URL for the final card.

        Raises:
            AppError: Raised when the update fails.
        """
        url = f"{self.settings.supabase_url}/rest/v1/generations?id=eq.{generation_id}"
        try:
            response = requests.patch(
                url,
                headers={
                    **rest_headers(self.settings),
                    "Content-Type": "application/json",
                },
                json={
                    "final_card_path": storage_path,
                    "final_card_bucket": bucket,
                    "final_card_url": public_url or None,
                    "final_card_uploaded_at": self._now_iso(),
                },
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[generations] supabase_unreachable operation=update_final_card generation_id=%s error=%s",
                generation_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if response.ok:
            return
        logger.error(
            "[generations] final_card_update_failed generation_id=%s status=%s body=%s",
            generation_id,
            response.status_code,
            response.text[:200],
        )
        raise AppError("generation_final_card_update_failed", status_code=502)

    def _build_signed_download_url(
        self,
        storage_path: str,
        expires_in: int = 900,
    ) -> str | None:
        """Build a temporary signed download URL for a storage object.

        Args:
            storage_path (str): Storage path to sign.
            expires_in (int): Expiration time in seconds for the signed URL.

        Returns:
            str | None: Signed download URL, or `None` when signing fails.
        """
        clean_storage_path = (storage_path or "").strip()
        if not clean_storage_path:
            return None

        bucket = self.settings.supabase_bucket
        sign_url = f"{self.settings.supabase_url}/storage/v1/object/sign/{bucket}/{clean_storage_path}"
        try:
            response = requests.post(
                sign_url,
                headers={
                    **rest_headers(self.settings),
                    "Content-Type": "application/json",
                },
                json={"expiresIn": max(60, int(expires_in))},
                timeout=20,
            )
        except requests.RequestException:
            return None
        if not response.ok:
            return None

        data = response.json() or {}
        signed = data.get("signedURL") or data.get("signedUrl") or data.get("url")
        if not signed and data.get("path") and data.get("token"):
            path = str(data.get("path"))
            token = str(data.get("token"))
            signed = f"/object/sign/{bucket}/{path}?token={token}"
        if not signed:
            return None
        return (
            signed
            if str(signed).startswith("http")
            else f"{self.settings.supabase_url}/storage/v1{signed}"
        )

    def _now_iso(self) -> str:
        """Return the current UTC timestamp in ISO-8601 format.

        Returns:
            str: Current UTC timestamp suffixed with `Z`.
        """
        return dt.datetime.utcnow().isoformat() + "Z"

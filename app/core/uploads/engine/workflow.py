import os
import uuid

import requests

from app.core.config import Settings
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.core.logging import get_logger
from app.infrastructure.supabase_rest import get_json
from app.infrastructure.supabase_rest import rest_headers
from app.routes.uploads.dtos import ConfirmUploadRequest, CreateSignedUploadRequest


_ALLOWED_UPLOAD_TYPES = {"user_photo", "video", "asset"}
_ALLOWED_GENERATION_KINDS = {"credential_card", "quiz_result", "photo_with"}
_MAX_UPLOAD_SIZE_BYTES_BY_TYPE = {
    "user_photo": int(os.getenv("QUIZ_MAX_USER_PHOTO_MB", "20")) * 1024 * 1024,
    "video": 100 * 1024 * 1024,
    "asset": 20 * 1024 * 1024,
}

logger = get_logger(__name__)


class UploadsWorkflow:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def create_signed_url(self, request: CreateSignedUploadRequest) -> dict:
        experience_id = (request.experience_id or "").strip()
        upload_type = (request.type or "").strip().lower()
        file_size_bytes = int(request.file_size_bytes)

        if not experience_id:
            raise AppError("missing_experience_id", status_code=400)
        if upload_type not in _ALLOWED_UPLOAD_TYPES:
            raise AppError("invalid_upload_type", status_code=400)
        if file_size_bytes <= 0:
            raise AppError("invalid_file_size", status_code=400)
        if file_size_bytes > _MAX_UPLOAD_SIZE_BYTES_BY_TYPE[upload_type]:
            raise AppError("file_too_large", status_code=413)

        self._load_active_experience_by_id(experience_id)

        ext_by_type = {"user_photo": "jpg", "video": "mp4", "asset": "bin"}
        storage_path = (
            f"quiz/{experience_id}/{upload_type}/{uuid.uuid4().hex}.{ext_by_type[upload_type]}"
        )
        bucket = self.settings.supabase_bucket
        sign_url = (
            f"{self.settings.supabase_url}/storage/v1/object/upload/sign/{bucket}/{storage_path}"
        )

        try:
            response = requests.post(
                sign_url,
                headers={**rest_headers(self.settings), "Content-Type": "application/json"},
                json={"expiresIn": 600},
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[uploads] supabase_unreachable operation=create_signed_url experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc

        if not response.ok:
            logger.error(
                "[uploads] signed_url_failed experience_id=%s upload_type=%s status=%s body=%s",
                experience_id,
                upload_type,
                response.status_code,
                response.text[:200],
            )
            raise AppError("signed_url_failed", status_code=502)

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
            "[uploads] signed_url_created experience_id=%s upload_type=%s storage_path=%s",
            experience_id,
            upload_type,
            storage_path,
        )
        return {
            "ok": True,
            "upload_url": upload_url,
            "storage_path": storage_path,
        }

    async def confirm_upload(self, request: ConfirmUploadRequest) -> dict:
        experience_id = (request.experience_id or "").strip()
        credential_id = (request.credential_id or "").strip()
        storage_path = (request.storage_path or "").strip()
        upload_type = (request.type or "user_photo").strip().lower()
        token = (request.phone or "").strip()

        if not experience_id:
            raise AppError("missing_experience_id", status_code=400)
        if not credential_id:
            raise AppError("missing_credential_id", status_code=400)
        if not storage_path:
            raise AppError("missing_storage_path", status_code=400)
        if upload_type not in _ALLOWED_UPLOAD_TYPES:
            raise AppError("invalid_upload_type", status_code=400)

        self._load_active_experience_by_id(experience_id)
        if not self._load_credential_for_experience(credential_id, experience_id):
            raise AppError("credential_not_found_for_experience", status_code=404)
        if not storage_path.startswith(f"quiz/{experience_id}/"):
            raise AppError("invalid_storage_path_scope", status_code=400)

        self._insert_upload_row(
            experience_id=experience_id,
            credential_id=credential_id,
            upload_type=upload_type,
            storage_path=storage_path,
        )

        generation_id: str | None = None
        if upload_type == "user_photo":
            self._update_credential_photo_path(
                credential_id=credential_id,
                storage_path=storage_path,
            )
            if self._is_eager_generation_enabled():
                experience = self._load_experience_by_id(experience_id)
                kind = self._kind_from_experience_type(str(experience.get("type") or ""))
                generation_id, reused = self._create_or_reuse_generation(
                    experience_id=experience_id,
                    credential_id=credential_id,
                    kind=kind,
                    token=token or None,
                )
                logger.info(
                    "[uploads] eager_generation_started generation_id=%s credential_id=%s reused=%s",
                    generation_id,
                    credential_id,
                    reused,
                )

        logger.info(
            "[uploads] upload_confirmed experience_id=%s credential_id=%s upload_type=%s storage_path=%s generation_id=%s",
            experience_id,
            credential_id,
            upload_type,
            storage_path,
            generation_id or "-",
        )
        return {"ok": True, "generation_id": generation_id}

    def _load_active_experience_by_id(self, experience_id: str) -> dict:
        experience = self._load_experience_by_id(experience_id)
        status = str(experience.get("status") or "").strip().lower()
        if status not in {"active", "published"}:
            raise AppError("experience_not_found_or_inactive", status_code=404)
        return experience

    def _load_experience_by_id(self, experience_id: str) -> dict:
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
                "[uploads] supabase_unreachable operation=load_experience experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[uploads] experience_query_failed experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("experience_query_failed", status_code=502) from exc
        if not rows:
            raise AppError("experience_not_found_or_inactive", status_code=404)
        return rows[0]

    def _load_credential_for_experience(
        self,
        credential_id: str,
        experience_id: str,
    ) -> dict | None:
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
                "[uploads] supabase_unreachable operation=load_credential experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[uploads] credential_query_failed experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("credential_query_failed", status_code=502) from exc
        return rows[0] if rows else None

    def _insert_upload_row(
        self,
        experience_id: str,
        credential_id: str,
        upload_type: str,
        storage_path: str,
    ) -> None:
        url = f"{self.settings.supabase_url}/rest/v1/uploads"
        try:
            response = requests.post(
                url,
                headers={**rest_headers(self.settings), "Content-Type": "application/json"},
                json=[
                    {
                        "experience_id": experience_id,
                        "credential_id": credential_id,
                        "type": upload_type,
                        "storage_path": storage_path,
                    }
                ],
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[uploads] supabase_unreachable operation=insert_upload experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[uploads] upload_audit_insert_failed experience_id=%s credential_id=%s status=%s body=%s",
                experience_id,
                credential_id,
                response.status_code,
                response.text[:200],
            )
            raise AppError("upload_audit_insert_failed", status_code=502)

    def _update_credential_photo_path(self, credential_id: str, storage_path: str) -> None:
        url = f"{self.settings.supabase_url}/rest/v1/credentials?id=eq.{credential_id}"
        try:
            response = requests.patch(
                url,
                headers={**rest_headers(self.settings), "Content-Type": "application/json"},
                json={"photo_path": storage_path},
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[uploads] supabase_unreachable operation=update_credential_photo credential_id=%s error=%s",
                credential_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[uploads] credential_photo_update_failed credential_id=%s status=%s body=%s",
                credential_id,
                response.status_code,
                response.text[:200],
            )
            raise AppError("credential_photo_update_failed", status_code=502)

    def _is_eager_generation_enabled(self) -> bool:
        return os.getenv("QUIZ_EAGER_GENERATION_ON_UPLOAD", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def _kind_from_experience_type(self, experience_type: str) -> str:
        clean_type = (experience_type or "").strip().lower()
        if clean_type == "credentialing":
            return "credential_card"
        if clean_type == "photo_with":
            return "photo_with"
        return "quiz_result"

    def _find_reusable_generation(self, credential_id: str, kind: str) -> dict | None:
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
                "[uploads] supabase_unreachable operation=find_reusable_generation credential_id=%s kind=%s error=%s",
                credential_id,
                kind,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[uploads] generation_query_failed credential_id=%s kind=%s error=%s",
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
                "[uploads] supabase_unreachable operation=insert_generation experience_id=%s credential_id=%s error=%s",
                experience_id,
                credential_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[uploads] generation_insert_failed experience_id=%s credential_id=%s status=%s body=%s",
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
        reusable = self._find_reusable_generation(credential_id, kind)
        if reusable and reusable.get("id"):
            generation_id = str(reusable["id"])
            return generation_id, True

        generation_id = self._insert_generation(
            experience_id=experience_id,
            credential_id=credential_id,
            kind=kind,
            token=token,
        )
        return generation_id, False

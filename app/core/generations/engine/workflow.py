import requests

from app.core.config import Settings
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.core.logging import get_logger
from app.infrastructure.supabase_rest import get_json
from app.infrastructure.supabase_rest import rest_headers
from app.routes.generations.dtos import CreateGenerationRequest

_ALLOWED_GENERATION_KINDS = {"credential_card", "quiz_result", "photo_with"}

logger = get_logger(__name__)


class GenerationsWorkflow:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def create_generation(self, request: CreateGenerationRequest) -> dict:
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
        clean_generation_id = (generation_id or "").strip()
        if not clean_generation_id:
            raise AppError("missing_generation_id", status_code=400)

        try:
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

        return {
            "ok": True,
            "status": row.get("status"),
            "duration_ms": row.get("duration_ms"),
            "output_url": output_url,
            "error_message": row.get("error_message"),
        }

    async def get_generation_logs(self, generation_id: str, limit: int = 200) -> dict:
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
        return self._count_rows(
            "generations",
            {"experience_id": f"eq.{experience_id}", "status": "eq.done"},
        )

    def _count_rows(self, table: str, filters: dict[str, str]) -> int:
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

    def _build_signed_download_url(
        self,
        storage_path: str,
        expires_in: int = 900,
    ) -> str | None:
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

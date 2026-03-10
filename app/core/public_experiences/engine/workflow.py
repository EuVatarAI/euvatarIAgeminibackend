import re
from datetime import datetime
from datetime import timezone

import requests

from app.core.config import Settings
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.core.logging import get_logger
from app.infrastructure.supabase_rest import get_json
from app.infrastructure.supabase_rest import rest_headers
from app.routes.public_experiences.dtos import CompleteLeadRequest
from app.routes.public_experiences.dtos import CreateLeadRequest


_ALLOWED_MODES = {"mobile", "totem", "auto"}
_ALLOWED_VARIABLE_FIELD_TYPES = {"text", "email", "phone", "number", "select"}
_MAX_LEAD_VALUE_LENGTH = 300
_MAX_LEAD_FIELD_COUNT = 30

logger = get_logger(__name__)


class PublicExperiencesWorkflow:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def get_lead_config(self, slug: str) -> dict:
        clean_slug = (slug or "").strip()
        if not clean_slug:
            raise AppError("missing_slug", status_code=400)

        experience = self._load_active_experience_by_slug(clean_slug)
        experience_id = self._require_experience_id(experience)
        variables = self._load_experience_variables(experience_id)
        lead_fields = [self._map_variable_to_field(row) for row in variables]
        config = (
            experience.get("config_json")
            if isinstance(experience.get("config_json"), dict)
            else {}
        )
        lead_capture = config.get("lead_capture") if isinstance(config, dict) else None
        enabled_from_config = (
            bool(lead_capture.get("enabled"))
            if isinstance(lead_capture, dict)
            else False
        )
        lead_enabled = enabled_from_config or len(lead_fields) > 0
        gate_before_unlock = (
            bool(lead_capture.get("gate_before_unlock"))
            if isinstance(lead_capture, dict)
            else lead_enabled
        )

        logger.info(
            "[public_experience] lead_config_loaded slug=%s experience_id=%s enabled=%s fields=%s",
            clean_slug,
            experience_id,
            lead_enabled,
            len(lead_fields),
        )
        return {
            "ok": True,
            "experience_id": experience_id,
            "lead_capture": {
                "enabled": lead_enabled,
                "gate_before_unlock": gate_before_unlock,
                "fields": lead_fields,
            },
        }

    async def create_lead(self, slug: str, request: CreateLeadRequest) -> dict:
        clean_slug = (slug or "").strip()
        if not clean_slug:
            raise AppError("missing_slug", status_code=400)

        mode_used = (request.mode_used or "mobile").strip().lower()
        if mode_used not in _ALLOWED_MODES:
            raise AppError("invalid_mode_used", status_code=400)

        experience = self._load_active_experience_by_slug(clean_slug)
        experience_id = self._require_experience_id(experience)
        variables = self._load_experience_variables(experience_id)
        clean_data = self._clean_lead_data(request.data, variables)

        credential_id: str | None = None
        if request.create_credential:
            credential_id = self._insert_credential_row(
                experience_id=experience_id,
                data=clean_data,
                mode_used=mode_used,
            )

        lead_inserted, lead_id = self._insert_lead_row(
            experience_id=experience_id,
            data=clean_data,
        )

        logger.info(
            "[public_experience] lead_created slug=%s experience_id=%s lead_id=%s credential_id=%s lead_inserted=%s fields=%s create_credential=%s",
            clean_slug,
            experience_id,
            lead_id or "-",
            credential_id or "-",
            lead_inserted,
            len(clean_data.keys()),
            request.create_credential,
        )
        return {
            "ok": True,
            "experience_id": experience_id,
            "credential_id": credential_id,
            "lead_id": lead_id,
            "lead_inserted": lead_inserted,
            "unlock": True,
        }

    async def complete_lead(
        self,
        slug: str,
        lead_id: str,
        request: CompleteLeadRequest,
    ) -> dict:
        clean_slug = (slug or "").strip()
        clean_lead_id = (lead_id or "").strip()
        archetype_result_id = (request.archetype_result_id or "").strip()
        if not clean_slug:
            raise AppError("missing_slug", status_code=400)
        if not clean_lead_id:
            raise AppError("missing_lead_id", status_code=400)
        if not archetype_result_id:
            raise AppError("missing_archetype_result_id", status_code=400)

        experience = self._load_active_experience_by_slug(clean_slug)
        experience_id = self._require_experience_id(experience)
        self._complete_lead_row(
            experience_id=experience_id,
            lead_id=clean_lead_id,
            archetype_result_id=archetype_result_id,
        )
        logger.info(
            "[public_experience] lead_completed slug=%s experience_id=%s lead_id=%s archetype_result_id=%s",
            clean_slug,
            experience_id,
            clean_lead_id,
            archetype_result_id,
        )
        return {
            "ok": True,
            "lead_id": clean_lead_id,
            "completed": True,
        }

    async def get_metrics(self, slug: str) -> dict:
        clean_slug = (slug or "").strip()
        if not clean_slug:
            raise AppError("missing_slug", status_code=400)

        experience = self._load_active_experience_by_slug(clean_slug)
        experience_id = self._require_experience_id(experience)
        started = self._count_rows("leads", {"experience_id": f"eq.{experience_id}"})
        done_generations = self._count_rows(
            "generations",
            {
                "experience_id": f"eq.{experience_id}",
                "status": "eq.done",
            },
        )
        completed = min(started, done_generations)
        dropped = max(0, started - completed)
        logger.info(
            "[public_experience] metrics_loaded slug=%s experience_id=%s started=%s completed=%s dropped=%s done_generations=%s",
            clean_slug,
            experience_id,
            started,
            completed,
            dropped,
            done_generations,
        )
        return {
            "ok": True,
            "experience_id": experience_id,
            "started": started,
            "completed": completed,
            "dropped": dropped,
            "done_generations": done_generations,
        }

    def _load_active_experience_by_slug(self, slug: str) -> dict:
        try:
            rows = get_json(
                self.settings,
                "experiences",
                "id,type,status,config_json",
                {"slug": f"eq.{slug}", "status": "in.(active,published)"},
                limit=1,
            )
        except requests.RequestException as exc:
            logger.error(
                "[public_experience] supabase_unreachable operation=load_experience slug=%s error=%s",
                slug,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[public_experience] supabase_query_failed operation=load_experience slug=%s error=%s",
                slug,
                str(exc),
            )
            raise AppError("experience_query_failed", status_code=502) from exc
        if not rows:
            raise AppError("experience_not_found_or_inactive", status_code=404)
        return rows[0]

    def _load_experience_variables(self, experience_id: str) -> list[dict]:
        try:
            return get_json(
                self.settings,
                "experience_variables",
                "variable_key,label,field_type,required,sort_order,options",
                {"experience_id": f"eq.{experience_id}", "order": "sort_order.asc"},
            )
        except requests.RequestException as exc:
            logger.error(
                "[public_experience] supabase_unreachable operation=load_variables experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        except RuntimeError as exc:
            logger.error(
                "[public_experience] variables_query_failed experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("variables_query_failed", status_code=502) from exc

    def _require_experience_id(self, experience: dict) -> str:
        experience_id = str(experience.get("id") or "").strip()
        if not experience_id:
            raise AppError("experience_missing_id", status_code=500)
        return experience_id

    def _map_variable_to_field(self, row: dict) -> dict:
        field_type = str(row.get("field_type") or "text").strip().lower()
        if field_type not in _ALLOWED_VARIABLE_FIELD_TYPES:
            field_type = "text"
        return {
            "key": self._normalize_variable_key(str(row.get("variable_key") or "")),
            "label": str(row.get("label") or "").strip(),
            "field_type": field_type,
            "required": bool(row.get("required")),
            "options": row.get("options") or [],
        }

    def _normalize_variable_key(self, value: str) -> str:
        return re.sub(r"[^a-z0-9_]", "_", (value or "").strip().lower())

    def _clean_lead_data(self, raw: dict[str, str], variables: list[dict]) -> dict[str, str]:
        if not isinstance(raw, dict):
            raise AppError("invalid_data_payload", status_code=400)
        if len(raw.keys()) > _MAX_LEAD_FIELD_COUNT:
            raise AppError("too_many_fields", status_code=400)

        variables_by_key: dict[str, dict] = {}
        for item in variables:
            key = self._normalize_variable_key(str(item.get("variable_key") or ""))
            if key:
                variables_by_key[key] = item

        cleaned: dict[str, str] = {}
        for raw_key, raw_value in raw.items():
            key = self._normalize_variable_key(str(raw_key or ""))
            if not key:
                continue
            value = str(raw_value or "").strip()
            if len(value) > _MAX_LEAD_VALUE_LENGTH:
                raise AppError(f"value_too_large:{key}", status_code=400)

            rule = variables_by_key.get(key)
            if rule:
                self._validate_field_value(key, value, rule)
            cleaned[key] = value

        for key, rule in variables_by_key.items():
            if bool(rule.get("required")) and not (cleaned.get(key) or "").strip():
                raise AppError(f"missing_required_field:{key}", status_code=400)
        return cleaned

    def _validate_field_value(self, key: str, value: str, rule: dict) -> None:
        field_type = str(rule.get("field_type") or "text").strip().lower()
        if field_type not in _ALLOWED_VARIABLE_FIELD_TYPES or not value:
            return
        if field_type == "email" and not re.match(r"^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$", value):
            raise AppError(f"invalid_email:{key}", status_code=400)
        if field_type == "phone" and not re.match(r"^\\+?[0-9()\\-\\s]{8,20}$", value):
            raise AppError(f"invalid_phone:{key}", status_code=400)
        if field_type == "number" and not re.match(r"^-?\\d+([.,]\\d+)?$", value):
            raise AppError(f"invalid_number:{key}", status_code=400)
        if field_type == "select":
            valid_options = [
                str(opt).strip() for opt in (rule.get("options") or []) if str(opt).strip()
            ]
            if valid_options and value not in valid_options:
                raise AppError(f"invalid_option:{key}", status_code=400)

    def _insert_credential_row(
        self,
        experience_id: str,
        data: dict[str, str],
        mode_used: str,
    ) -> str:
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
                "[public_experience] supabase_unreachable operation=insert_credential experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if isinstance(response, requests.Response) and not response.ok:
            logger.error(
                "[public_experience] credential_insert_failed experience_id=%s status=%s body=%s",
                experience_id,
                response.status_code,
                response.text[:200],
            )
        if not response.ok:
            raise AppError("credential_insert_failed", status_code=502)
        rows = response.json() or []
        credential_id = str((rows[0] or {}).get("id") or "").strip() if rows else ""
        if not credential_id:
            raise AppError("credential_insert_empty", status_code=502)
        return credential_id

    def _insert_lead_row(
        self,
        experience_id: str,
        data: dict[str, str],
    ) -> tuple[bool, str | None]:
        name = data.get("name") or data.get("nome")
        email = data.get("email")
        phone = data.get("phone") or data.get("telefone")
        url = f"{self.settings.supabase_url}/rest/v1/leads"
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
                        "name": name,
                        "email": email,
                        "phone": phone,
                        "quiz_answers": data,
                        "lead_data": data,
                    }
                ],
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[public_experience] supabase_unreachable operation=insert_lead experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if isinstance(response, requests.Response) and not response.ok:
            logger.error(
                "[public_experience] lead_insert_failed experience_id=%s status=%s body=%s",
                experience_id,
                response.status_code,
                response.text[:200],
            )
        if not response.ok:
            raise AppError(f"lead_insert_failed:{response.status_code}", status_code=502)
        rows = response.json() or []
        lead_id = str((rows[0] or {}).get("id") or "").strip() if rows else ""
        return True, (lead_id or None)

    def _complete_lead_row(
        self,
        experience_id: str,
        lead_id: str,
        archetype_result_id: str,
    ) -> None:
        url = (
            f"{self.settings.supabase_url}/rest/v1/leads"
            f"?id=eq.{lead_id}&experience_id=eq.{experience_id}"
        )
        try:
            response = requests.patch(
                url,
                headers={
                    **rest_headers(self.settings),
                    "Content-Type": "application/json",
                    "Prefer": "return=representation",
                },
                json={
                    "archetype_result_id": archetype_result_id,
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                },
                timeout=20,
            )
        except requests.RequestException as exc:
            logger.error(
                "[public_experience] supabase_unreachable operation=complete_lead experience_id=%s lead_id=%s error=%s",
                experience_id,
                lead_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if isinstance(response, requests.Response) and not response.ok:
            logger.error(
                "[public_experience] lead_complete_failed experience_id=%s lead_id=%s status=%s body=%s",
                experience_id,
                lead_id,
                response.status_code,
                response.text[:200],
            )
        if not response.ok:
            raise AppError(
                f"lead_complete_failed:{response.status_code}",
                status_code=502,
            )
        rows = response.json() or []
        if not rows:
            raise AppError("lead_not_found_for_experience", status_code=404)

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
                "[public_experience] supabase_unreachable operation=count_rows table=%s error=%s",
                table,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc
        if not response.ok:
            logger.error(
                "[public_experience] metrics_count_failed table=%s status=%s body=%s",
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

"""Workflow responsible for public experience lead capture and metrics."""

import re
from typing import Any
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
_ALLOWED_VARIABLE_FIELD_TYPES = {
    "text",
    "email",
    "phone",
    "number",
    "select",
    "prompt_image",
    "prompt_asset_select",
    "prompt_asset_multi_select",
}
_MAX_LEAD_VALUE_LENGTH = 300
_MAX_PROMPT_TEXT_LENGTH = 5000
_MAX_LEAD_FIELD_COUNT = 30
_MAX_MULTI_SELECT_ASSETS = 12

logger = get_logger(__name__)


class PublicExperiencesWorkflow:
    """Load public experience metadata, validate lead inputs, and persist lead rows.

    Attributes:
        settings (Settings): Runtime settings used for Supabase REST operations.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()

    async def get_lead_config(self, slug: str) -> dict:
        """Return the lead-capture configuration for a public experience slug.

        Args:
            slug (str): Public experience slug to resolve.

        Returns:
            dict: Lead configuration payload consumed by the public player.

        Raises:
            AppError: Raised when the slug is invalid or Supabase operations fail.
        """
        clean_slug = (slug or "").strip()
        if not clean_slug:
            raise AppError("missing_slug", status_code=400)

        experience = self._load_active_experience_by_slug(clean_slug)
        experience_id = self._require_experience_id(experience)
        variables = self._load_experience_variables(experience_id)
        prompt_assets = self._load_experience_prompt_assets(experience_id)
        selectable_assets_by_variable = self._group_selectable_assets_by_variable(
            prompt_assets
        )
        lead_fields = []
        for row in variables:
            field_payload = self._map_variable_to_field(
                row, selectable_assets_by_variable
            )
            if field_payload:
                lead_fields.append(field_payload)
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
        avatar_generation = (
            config.get("avatar_generation")
            if isinstance(config.get("avatar_generation"), dict)
            else {}
        )
        return {
            "ok": True,
            "experience_id": experience_id,
            "lead_capture": {
                "enabled": lead_enabled,
                "gate_before_unlock": gate_before_unlock,
                "fields": lead_fields,
                "avatar_generation": {
                    "enabled": bool(avatar_generation.get("enabled")),
                    "background_mode": str(
                        avatar_generation.get("background_mode")
                        or "generated_scene"
                    ).strip(),
                },
            },
        }

    async def create_lead(self, slug: str, request: CreateLeadRequest) -> dict:
        """Create a lead row and optional credential for a public experience.

        Args:
            slug (str): Public experience slug to resolve.
            request (CreateLeadRequest): Lead payload collected by the player.

        Returns:
            dict: Success payload with lead and optional credential identifiers.

        Raises:
            AppError: Raised when validation fails or persistence operations fail.
        """
        clean_slug = (slug or "").strip()
        if not clean_slug:
            raise AppError("missing_slug", status_code=400)

        mode_used = (request.mode_used or "mobile").strip().lower()
        if mode_used not in _ALLOWED_MODES:
            raise AppError("invalid_mode_used", status_code=400)

        experience = self._load_active_experience_by_slug(clean_slug)
        experience_id = self._require_experience_id(experience)
        variables = self._load_experience_variables(experience_id)
        prompt_assets = self._load_experience_prompt_assets(experience_id)
        clean_data = self._clean_lead_data(request.data, variables, prompt_assets)

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
        """Mark a lead as completed once the archetype result is known.

        Args:
            slug (str): Public experience slug to resolve.
            lead_id (str): Lead identifier to update.
            request (CompleteLeadRequest): Completion payload with the result id.

        Returns:
            dict: Success payload confirming the lead completion.

        Raises:
            AppError: Raised when identifiers are missing or persistence fails.
        """
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
        """Return aggregate lead and generation metrics for a public experience.

        Args:
            slug (str): Public experience slug to resolve.

        Returns:
            dict: Metrics payload containing started, completed, dropped, and done counts.

        Raises:
            AppError: Raised when the slug is invalid or metrics queries fail.
        """
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
        """Load a published or active experience row by slug.

        Args:
            slug (str): Public slug to resolve.

        Returns:
            dict: Matching experience row from Supabase.

        Raises:
            AppError: Raised when the experience is missing or Supabase fails.
        """
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
        """Load configured lead variables for an experience.

        Args:
            experience_id (str): Experience identifier whose variables should be fetched.

        Returns:
            list[dict]: Ordered variable rows used to build lead fields.

        Raises:
            AppError: Raised when Supabase operations fail.
        """
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

    def _load_experience_prompt_assets(self, experience_id: str) -> list[dict]:
        """Load prompt-asset catalog rows for an experience.

        Args:
            experience_id (str): Experience identifier whose prompt assets should be loaded.

        Returns:
            list[dict]: Ordered prompt-asset rows, or an empty list when the table is absent.

        Raises:
            AppError: Raised when Supabase operations fail for reasons other than a missing table.
        """
        try:
            return get_json(
                self.settings,
                "experience_prompt_assets",
                "variable_key,asset_key,label,bucket,storage_path,public_url,required,sort_order",
                {"experience_id": f"eq.{experience_id}", "order": "sort_order.asc"},
            )
        except RuntimeError as exc:
            message = str(exc)
            if "supabase_experience_prompt_assets_404" in message:
                return []
            logger.error(
                "[public_experience] prompt_assets_query_failed experience_id=%s error=%s",
                experience_id,
                message,
            )
            raise AppError("prompt_assets_query_failed", status_code=502) from exc
        except requests.RequestException as exc:
            logger.error(
                "[public_experience] supabase_unreachable operation=load_prompt_assets experience_id=%s error=%s",
                experience_id,
                str(exc),
            )
            raise AppError("supabase_unreachable", status_code=502) from exc

    def _require_experience_id(self, experience: dict) -> str:
        """Return the experience id or raise when it is missing.

        Args:
            experience (dict): Experience row returned from Supabase.

        Returns:
            str: Non-empty experience identifier.

        Raises:
            AppError: Raised when the row lacks an id.
        """
        experience_id = str(experience.get("id") or "").strip()
        if not experience_id:
            raise AppError("experience_missing_id", status_code=500)
        return experience_id

    def _group_selectable_assets_by_variable(
        self, rows: list[dict]
    ) -> dict[str, list[dict[str, str | None]]]:
        """Group non-required prompt assets by their owning variable key.

        Args:
            rows (list[dict]): Prompt-asset rows loaded from Supabase.

        Returns:
            dict[str, list[dict[str, str | None]]]: Selectable assets indexed by variable key.
        """
        grouped: dict[str, list[dict[str, str | None]]] = {}
        for row in rows:
            if bool(row.get("required")):
                continue
            variable_key = self._normalize_variable_key(
                str(row.get("variable_key") or "")
            )
            asset_key = self._normalize_asset_key(str(row.get("asset_key") or ""))
            label = str(row.get("label") or "").strip()
            if not variable_key or not asset_key or not label:
                continue
            grouped.setdefault(variable_key, []).append(
                {
                    "asset_key": asset_key,
                    "label": label,
                    "public_url": str(row.get("public_url") or "").strip() or None,
                }
            )
        return grouped

    def _map_variable_to_field(
        self,
        row: dict,
        selectable_assets_by_variable: dict[str, list[dict[str, str | None]]],
    ) -> dict | None:
        """Map a variable row into the public lead-field payload shape.

        Args:
            row (dict): Variable row loaded from Supabase.
            selectable_assets_by_variable (dict[str, list[dict[str, str | None]]]):
                Selectable prompt assets grouped by variable key.

        Returns:
            dict | None: Lead-field payload consumed by the public player, or
                `None` when the variable should stay internal to the builder.
        """
        field_type = str(row.get("field_type") or "text").strip().lower()
        if field_type == "prompt_text":
            return None
        if field_type not in _ALLOWED_VARIABLE_FIELD_TYPES:
            field_type = "text"
        variable_key = self._normalize_variable_key(str(row.get("variable_key") or ""))
        payload = {
            "key": self._normalize_variable_key(str(row.get("variable_key") or "")),
            "label": str(row.get("label") or "").strip(),
            "field_type": field_type,
            "required": bool(row.get("required")),
            "options": row.get("options") or [],
        }
        if field_type in {"prompt_asset_select", "prompt_asset_multi_select"}:
            payload["asset_options"] = selectable_assets_by_variable.get(
                variable_key, []
            )
        return payload

    def _normalize_variable_key(self, value: str) -> str:
        """Normalize a variable key to the backend's storage format.

        Args:
            value (str): Raw variable key.

        Returns:
            str: Lowercased key containing only letters, numbers, and underscores.
        """
        return re.sub(r"[^a-z0-9_]", "_", (value or "").strip().lower())

    def _normalize_asset_key(self, value: str) -> str:
        """Normalize an asset key using the same rules as variable keys.

        Args:
            value (str): Raw asset key.

        Returns:
            str: Normalized asset key.
        """
        return self._normalize_variable_key(value)

    def _clean_lead_data(
        self,
        raw: dict[str, Any],
        variables: list[dict],
        prompt_assets: list[dict],
    ) -> dict[str, Any]:
        """Normalize, validate, and filter a raw public lead payload.

        Args:
            raw (dict[str, Any]): Raw data sent by the player.
            variables (list[dict]): Configured variable rows for the experience.
            prompt_assets (list[dict]): Prompt-asset catalog rows for the experience.

        Returns:
            dict[str, Any]: Cleaned payload ready for persistence.

        Raises:
            AppError: Raised when validation rules are violated.
        """
        if not isinstance(raw, dict):
            raise AppError("invalid_data_payload", status_code=400)
        if len(raw.keys()) > _MAX_LEAD_FIELD_COUNT:
            raise AppError("too_many_fields", status_code=400)

        variables_by_key: dict[str, dict] = {}
        for item in variables:
            key = self._normalize_variable_key(str(item.get("variable_key") or ""))
            if key:
                variables_by_key[key] = item

        selectable_assets_by_variable = self._group_selectable_assets_by_variable(
            prompt_assets
        )

        cleaned: dict[str, Any] = {}
        for raw_key, raw_value in raw.items():
            key = self._normalize_variable_key(str(raw_key or ""))
            if not key:
                continue
            rule = variables_by_key.get(key)
            if rule:
                cleaned_value = self._normalize_field_value(
                    key,
                    raw_value,
                    rule,
                    selectable_assets_by_variable.get(key, []),
                )
            else:
                cleaned_value = self._normalize_untyped_field_value(raw_value)
            if cleaned_value is None:
                continue
            cleaned[key] = cleaned_value

        for key, rule in variables_by_key.items():
            field_type = str(rule.get("field_type") or "").strip().lower()
            if field_type == "prompt_image":
                continue
            if bool(rule.get("required")) and self._is_missing_required_value(
                cleaned.get(key), field_type
            ):
                raise AppError(f"missing_required_field:{key}", status_code=400)
        return cleaned

    def _normalize_untyped_field_value(self, raw_value: Any) -> Any | None:
        """Normalize a value when no explicit variable definition exists.

        Args:
            raw_value (Any): Incoming untyped field value.

        Returns:
            Any | None: Sanitized scalar or list value, or `None` when empty.
        """
        if raw_value is None:
            return None
        if isinstance(raw_value, list):
            normalized = [
                self._sanitize_string_value(item)
                for item in raw_value
                if self._sanitize_string_value(item)
            ]
            return normalized or None
        value = self._sanitize_string_value(raw_value)
        return value or None

    def _sanitize_string_value(
        self,
        raw_value: Any,
        *,
        max_length: int = _MAX_LEAD_VALUE_LENGTH,
    ) -> str:
        """Trim a scalar value and enforce the per-field size limit.

        Args:
            raw_value (Any): Incoming value to sanitize.
            max_length (int): Maximum allowed string length for the field.

        Returns:
            str: Trimmed string representation of the value.

        Raises:
            AppError: Raised when the resulting string exceeds the maximum length.
        """
        value = str(raw_value or "").strip()
        if len(value) > max_length:
            raise AppError("value_too_large", status_code=400)
        return value

    def _is_missing_required_value(self, value: Any, field_type: str) -> bool:
        """Determine whether a normalized field still violates the required rule.

        Args:
            value (Any): Normalized field value.
            field_type (str): Variable field type associated with the value.

        Returns:
            bool: `True` when the required value is effectively missing.
        """
        if field_type == "prompt_asset_multi_select":
            return not isinstance(value, list) or len(value) == 0
        return not str(value or "").strip()

    def _normalize_field_value(
        self,
        key: str,
        raw_value: Any,
        rule: dict,
        selectable_assets: list[dict[str, str | None]],
    ) -> Any | None:
        """Normalize and validate a typed variable value.

        Args:
            key (str): Normalized variable key being processed.
            raw_value (Any): Raw value received from the player.
            rule (dict): Variable definition row.
            selectable_assets (list[dict[str, str | None]]): Catalog assets available for the variable.

        Returns:
            Any | None: Normalized value ready for persistence, or `None` when omitted.
        """
        field_type = str(rule.get("field_type") or "text").strip().lower()
        if field_type == "prompt_text":
            return None
        if field_type not in _ALLOWED_VARIABLE_FIELD_TYPES:
            return self._normalize_untyped_field_value(raw_value)
        if field_type == "prompt_image":
            return None
        if field_type == "prompt_asset_multi_select":
            values = self._normalize_multi_select_assets(
                key, raw_value, selectable_assets
            )
            if values:
                self._validate_field_value(key, values, rule, selectable_assets)
            return values
        value = self._sanitize_string_value(raw_value, max_length=_MAX_LEAD_VALUE_LENGTH)
        if not value:
            return ""
        self._validate_field_value(key, value, rule, selectable_assets)
        return value

    def _normalize_multi_select_assets(
        self,
        key: str,
        raw_value: Any,
        selectable_assets: list[dict[str, str | None]],
    ) -> list[str]:
        """Normalize a multi-select prompt-asset payload into distinct asset keys.

        Args:
            key (str): Variable key being processed.
            raw_value (Any): Raw selection payload.
            selectable_assets (list[dict[str, str | None]]): Catalog assets allowed for the variable.

        Returns:
            list[str]: Distinct normalized asset keys selected by the user.

        Raises:
            AppError: Raised when too many or invalid asset keys are supplied.
        """
        if raw_value is None:
            return []
        if isinstance(raw_value, str):
            raw_items = [item.strip() for item in raw_value.split(",")]
        elif isinstance(raw_value, list):
            raw_items = [self._sanitize_string_value(item) for item in raw_value]
        else:
            raise AppError(f"invalid_option:{key}", status_code=400)
        normalized: list[str] = []
        for item in raw_items:
            asset_key = self._normalize_asset_key(item)
            if asset_key and asset_key not in normalized:
                normalized.append(asset_key)
        if len(normalized) > _MAX_MULTI_SELECT_ASSETS:
            raise AppError(f"too_many_options:{key}", status_code=400)
        valid_keys = {
            self._normalize_asset_key(str(asset.get("asset_key") or ""))
            for asset in selectable_assets
        }
        if valid_keys and any(item not in valid_keys for item in normalized):
            raise AppError(f"invalid_option:{key}", status_code=400)
        return normalized

    def _validate_field_value(
        self,
        key: str,
        value: Any,
        rule: dict,
        selectable_assets: list[dict[str, str | None]],
    ) -> None:
        """Validate a normalized field value against the configured variable rule.

        Args:
            key (str): Variable key being validated.
            value (Any): Normalized value to validate.
            rule (dict): Variable definition row.
            selectable_assets (list[dict[str, str | None]]): Catalog assets allowed for the variable.

        Raises:
            AppError: Raised when the value violates the rule for its field type.
        """
        field_type = str(rule.get("field_type") or "text").strip().lower()
        if field_type not in _ALLOWED_VARIABLE_FIELD_TYPES or value in ("", None, []):
            return
        if field_type == "email" and not re.match(
            r"^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$", value
        ):
            raise AppError(f"invalid_email:{key}", status_code=400)
        if field_type == "phone" and not re.match(r"^\\+?[0-9()\\-\\s]{8,20}$", value):
            raise AppError(f"invalid_phone:{key}", status_code=400)
        if field_type == "number" and not re.match(r"^-?\\d+([.,]\\d+)?$", value):
            raise AppError(f"invalid_number:{key}", status_code=400)
        if field_type == "select":
            valid_options = [
                str(opt).strip()
                for opt in (rule.get("options") or [])
                if str(opt).strip()
            ]
            if valid_options and value not in valid_options:
                raise AppError(f"invalid_option:{key}", status_code=400)
        if field_type == "prompt_asset_select":
            valid_keys = {
                self._normalize_asset_key(str(asset.get("asset_key") or ""))
                for asset in selectable_assets
            }
            if valid_keys and self._normalize_asset_key(str(value)) not in valid_keys:
                raise AppError(f"invalid_option:{key}", status_code=400)

    def _insert_credential_row(
        self,
        experience_id: str,
        data: dict[str, Any],
        mode_used: str,
    ) -> str:
        """Insert a credential row when lead capture should also create a credential.

        Args:
            experience_id (str): Experience identifier that owns the credential.
            data (dict[str, Any]): Normalized lead payload to persist.
            mode_used (str): Capture mode recorded for the credential.

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
        data: dict[str, Any],
    ) -> tuple[bool, str | None]:
        """Insert a lead row for a public experience.

        Args:
            experience_id (str): Experience identifier that owns the lead.
            data (dict[str, Any]): Normalized lead payload to persist.

        Returns:
            tuple[bool, str | None]: Flag indicating success and the inserted lead id when available.

        Raises:
            AppError: Raised when the insert fails.
        """
        name = self._sanitize_scalar_field(data.get("name") or data.get("nome"))
        email = self._sanitize_scalar_field(data.get("email"))
        phone = self._sanitize_scalar_field(data.get("phone") or data.get("telefone"))
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
            raise AppError(
                f"lead_insert_failed:{response.status_code}", status_code=502
            )
        rows = response.json() or []
        lead_id = str((rows[0] or {}).get("id") or "").strip() if rows else ""
        return True, (lead_id or None)

    def _sanitize_scalar_field(self, value: Any) -> str | None:
        """Convert an optional scalar field into a trimmed string or `None`.

        Args:
            value (Any): Incoming scalar value from the cleaned lead payload.

        Returns:
            str | None: Trimmed scalar string, or `None` for empty and list values.
        """
        if value is None or isinstance(value, list):
            return None
        clean_value = str(value).strip()
        return clean_value or None

    def _complete_lead_row(
        self,
        experience_id: str,
        lead_id: str,
        archetype_result_id: str,
    ) -> None:
        """Persist completion metadata for a lead row.

        Args:
            experience_id (str): Experience identifier that owns the lead.
            lead_id (str): Lead identifier to update.
            archetype_result_id (str): Result identifier associated with the completed lead.

        Raises:
            AppError: Raised when the update fails or the lead is not found.
        """
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

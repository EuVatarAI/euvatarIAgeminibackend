#!/usr/bin/env python3
"""Simple queue worker for quiz generations (phase 3).

Consumes pending rows from public.generations, marks processing with atomic claim,
builds a basic SVG output card, uploads to Supabase Storage, then marks done/error.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import html
import json
import os
import re
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path

import requests
from dotenv import load_dotenv

# Allow running as "python3 scripts/quiz_generation_worker.py"
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.settings import Settings
from app.application.services.image_prompt_builder import build_editorial_prompt
from app.infrastructure.gemini_image_client import GeminiImageClient
from app.infrastructure.supabase_rest import get_json, rest_headers


@dataclass
class Job:
    """Serializable representation of a generation row claimed by the worker.

    Attributes:
        id (str): Generation identifier being processed.
        experience_id (str): Experience associated with the generation.
        credential_id (str): Credential whose data drives the generation.
        kind (str): Generation kind stored in the queue row.
        token (str): Optional token persisted with the generation row.
    """

    id: str
    experience_id: str
    credential_id: str
    kind: str
    token: str = ""


_ALLOWED_GENDERS = {"mulher", "homem"}
_ALLOWED_HAIR_COLORS = {"loiro", "castanho", "preto", "ruivo", "grisalho"}
_PROMPT_IMAGE_DATA_KEY = "_prompt_images"


def _estimated_cost_usd(job: Job) -> float:
    """Estimate the USD cost for a generation job based on its kind.

    Args:
        job (Job): Claimed generation job.

    Returns:
        float: Estimated cost in USD for the processed generation.
    """
    # Allows tuning by kind via env while keeping a safe default.
    default = float(os.getenv("QUIZ_GENERATION_ESTIMATED_COST_USD", "0.04"))
    by_kind = {
        "credential_card": float(
            os.getenv("QUIZ_COST_CREDENTIAL_CARD_USD", str(default))
        ),
        "quiz_result": float(os.getenv("QUIZ_COST_QUIZ_RESULT_USD", str(default))),
        "photo_with": float(os.getenv("QUIZ_COST_PHOTO_WITH_USD", str(default))),
    }
    return float(by_kind.get(job.kind, default))


def _write_generation_log(
    settings: Settings,
    generation_id: str,
    *,
    level: str,
    event: str,
    message: str,
    payload: dict | None = None,
):
    """Persist a structured generation log entry and mirror it to stdout.

    This sink is best-effort by design. Failures writing to `generation_logs` do not
    interrupt the worker so generation processing can continue.

    Args:
        settings (Settings): Runtime settings used for Supabase writes.
        generation_id (str): Generation identifier associated with the log.
        level (str): Log severity level.
        event (str): Short event identifier.
        message (str): Human-readable log message.
        payload (dict | None): Optional structured payload stored with the log.
    """
    try:
        print(
            "[GENERATION_LOG] "
            f"generation_id={generation_id} "
            f"level={level} "
            f"event={event} "
            f"message={message} "
            f"payload={json.dumps(payload or {}, ensure_ascii=True, sort_keys=True)}",
            flush=True,
        )
    except Exception:
        print(
            "[GENERATION_LOG] "
            f"generation_id={generation_id} "
            f"level={level} "
            f"event={event} "
            f"message={message}",
            flush=True,
        )

    url = f"{settings.supabase_url}/rest/v1/generation_logs"
    body = [
        {
            "generation_id": generation_id,
            "level": level,
            "event": event,
            "message": message,
            "payload_json": payload or {},
        }
    ]
    try:
        requests.post(
            url,
            headers={**rest_headers(settings), "Content-Type": "application/json"},
            json=body,
            timeout=10,
        )
    except Exception:
        pass


def _now_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns:
        str: Current UTC timestamp suffixed with `Z`.
    """
    return dt.datetime.utcnow().isoformat() + "Z"


def _claim_job(settings: Settings, job_id: str) -> Job | None:
    """Atomically claim a pending generation row for processing.

    Args:
        settings (Settings): Runtime settings used for Supabase requests.
        job_id (str): Pending generation identifier to claim.

    Returns:
        Job | None: Claimed job payload, or `None` when another worker won the claim.
    """
    url = (
        f"{settings.supabase_url}/rest/v1/generations?id=eq.{job_id}&status=eq.pending"
    )
    body = {"status": "processing", "updated_at": _now_iso(), "error_message": None}
    r = requests.patch(
        url,
        headers={
            **rest_headers(settings),
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        json=body,
        timeout=20,
    )
    if not r.ok:
        return None
    rows = r.json() or []
    if not rows:
        return None
    row = rows[0]
    return Job(
        id=str(row.get("id") or ""),
        experience_id=str(row.get("experience_id") or ""),
        credential_id=str(row.get("credential_id") or ""),
        kind=str(row.get("kind") or "quiz_result"),
        token=str(row.get("token") or "").strip(),
    )


def _load_credential_data(settings: Settings, credential_id: str) -> dict:
    """Load the credential row required to build generation inputs.

    Args:
        settings (Settings): Runtime settings used for Supabase reads.
        credential_id (str): Credential identifier to fetch.

    Returns:
        dict: Credential row containing `data_json` and `photo_path`.

    Raises:
        RuntimeError: Raised when the credential does not exist.
    """
    rows = get_json(
        settings,
        "credentials",
        "id,data_json,photo_path",
        {"id": f"eq.{credential_id}"},
        limit=1,
    )
    if not rows:
        raise RuntimeError("credential_not_found")
    return rows[0]


def _load_experience_prompt_assets(
    settings: Settings, experience_id: str
) -> list[dict]:
    """Load prompt-asset catalog rows for the current experience.

    Args:
        settings (Settings): Runtime settings used for Supabase reads.
        experience_id (str): Experience identifier to fetch assets for.

    Returns:
        list[dict]: Prompt-asset rows, or an empty list when the table is unavailable.
    """
    try:
        return get_json(
            settings,
            "experience_prompt_assets",
            "variable_key,asset_key,label,storage_path,required,sort_order",
            {"experience_id": f"eq.{experience_id}", "order": "sort_order.asc"},
        )
    except RuntimeError as exc:
        if "supabase_experience_prompt_assets_404" in str(exc):
            return []
        raise


def _ext_from_mime(mime: str) -> str:
    """Map an image MIME type to a storage file extension.

    Args:
        mime (str): MIME type returned by Gemini or storage.

    Returns:
        str: File extension suitable for the generated asset path.
    """
    m = (mime or "").lower()
    if "jpeg" in m or "jpg" in m:
        return "jpg"
    if "webp" in m:
        return "webp"
    if "svg" in m:
        return "svg"
    return "png"


def _guess_mime_from_storage_path(storage_path: str) -> str:
    """Infer a MIME type from a storage path extension.

    Args:
        storage_path (str): Storage path whose extension should be inspected.

    Returns:
        str: Best-effort MIME type guess for the storage object.
    """
    p = (storage_path or "").lower()
    if p.endswith(".jpg") or p.endswith(".jpeg"):
        return "image/jpeg"
    if p.endswith(".webp"):
        return "image/webp"
    if p.endswith(".png"):
        return "image/png"
    return "image/jpeg"


def _download_reference_image(
    settings: Settings, storage_path: str
) -> tuple[bytes, str]:
    """Download a reference image from Supabase Storage.

    Args:
        settings (Settings): Runtime settings used for storage requests.
        storage_path (str): Storage path of the reference image.

    Returns:
        tuple[bytes, str]: Downloaded image bytes and resolved MIME type.

    Raises:
        RuntimeError: Raised when the storage download fails.
    """
    bucket = settings.supabase_bucket
    url = f"{settings.supabase_url}/storage/v1/object/{bucket}/{storage_path}"
    r = requests.get(url, headers=rest_headers(settings), timeout=40)
    if not r.ok:
        raise RuntimeError(f"reference_download_failed:{r.status_code}:{r.text[:160]}")
    mime = (
        r.headers.get("Content-Type") or ""
    ).strip() or _guess_mime_from_storage_path(storage_path)
    return r.content, mime


def _extract_generation_inputs(cred_row: dict) -> tuple[str, str]:
    """Extract normalized gender and hair color values from a credential row.

    Args:
        cred_row (dict): Credential row loaded from Supabase.

    Returns:
        tuple[str, str]: Supported gender and hair-color values for prompt generation.
    """
    data = (
        cred_row.get("data_json") if isinstance(cred_row.get("data_json"), dict) else {}
    )
    gender = str((data or {}).get("gender") or "mulher").strip().lower()
    hair_color = str((data or {}).get("hair_color") or "castanho").strip().lower()
    if gender not in _ALLOWED_GENDERS:
        gender = "mulher"
    if hair_color not in _ALLOWED_HAIR_COLORS:
        hair_color = "castanho"
    return gender, hair_color


def _load_archetype(
    settings: Settings, experience_id: str, archetype_id: str
) -> dict | None:
    """Load a specific archetype for an experience.

    Args:
        settings (Settings): Runtime settings used for Supabase reads.
        experience_id (str): Experience identifier to scope the lookup.
        archetype_id (str): Archetype identifier to fetch.

    Returns:
        dict | None: Matching archetype row, or `None` when absent.
    """
    if not archetype_id:
        return None
    rows = get_json(
        settings,
        "archetypes",
        "id,name,image_prompt,text_prompt,use_photo_prompt",
        {"id": f"eq.{archetype_id}", "experience_id": f"eq.{experience_id}"},
        limit=1,
    )
    return rows[0] if rows else None


def _load_first_archetype(settings: Settings, experience_id: str) -> dict | None:
    """Load the first archetype configured for an experience.

    Args:
        settings (Settings): Runtime settings used for Supabase reads.
        experience_id (str): Experience identifier to scope the lookup.

    Returns:
        dict | None: First archetype row ordered by `sort_order`, or `None` when absent.
    """
    rows = get_json(
        settings,
        "archetypes",
        "id,name,image_prompt,text_prompt,use_photo_prompt",
        {"experience_id": f"eq.{experience_id}", "order": "sort_order.asc"},
        limit=1,
    )
    return rows[0] if rows else None


def _resolve_experience_gemini_key(
    settings: Settings, experience_id: str
) -> str | None:
    """Resolve the per-experience Gemini API key in strict mode.

    The worker intentionally does not fall back to the global `GEMINI_API_KEY`. This keeps
    generation behavior deterministic and aligned with experience-level configuration.

    Args:
        settings (Settings): Runtime settings used for Supabase reads.
        experience_id (str): Experience identifier whose API key should be resolved.

    Returns:
        str | None: Experience-specific Gemini API key, or `None` when unavailable.
    """
    try:
        rows = get_json(
            settings,
            "experiences",
            "id,gemini_api_key",
            {"id": f"eq.{experience_id}"},
            limit=1,
        )
        exp_key = (
            str((rows[0] or {}).get("gemini_api_key") or "").strip() if rows else ""
        )
        if exp_key:
            return exp_key
    except Exception:
        # If column/query is unavailable we keep behavior deterministic: no key resolved.
        pass
    return None


_VAR_TOKEN_RE = re.compile(r"\{\{\s*([a-zA-Z0-9_]+)\s*\}\}")
_WORD_RE = re.compile(r"\b[\wÀ-ÿ]+\b", re.UNICODE)
_LEGACY_VAR_PATTERNS = [
    re.compile(r"\[\[\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\]\]"),  # [[key]]
    re.compile(r"\{\[\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\]\}"),  # {[key]}
    re.compile(r"\[\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\]"),  # [key]
]

_PROMPT_EXACT_TRANSLATIONS = {
    "sim": "yes",
    "nao": "no",
    "não": "no",
    "masculino": "male",
    "feminino": "female",
    "homem": "man",
    "mulher": "woman",
    "loiro": "blond",
    "castanho": "brown",
    "preto": "black",
    "ruivo": "red",
    "grisalho": "gray",
    "solteiro": "single",
    "casado": "married",
    "divorciado": "divorced",
    "viuvo": "widowed",
    "viúvo": "widowed",
}

_PROMPT_WORD_TRANSLATIONS = {
    "anos": "years",
    "ano": "year",
    "empreendimento": "business",
    "empreendimentos": "businesses",
    "vendas": "sales",
    "venda": "sale",
    "corretor": "broker",
    "consultor": "consultant",
    "cliente": "client",
    "clientes": "clients",
    "premium": "premium",
    "iniciante": "beginner",
    "avancado": "advanced",
    "avançado": "advanced",
    "experiente": "experienced",
    "alto": "high",
    "media": "medium",
    "média": "medium",
    "baixo": "low",
}

_PROMPT_KEY_ALIASES = {
    "genero_para_criacao_do_avatar": [
        "genero_para_criacao_do_avatar",
        "genero",
        "sexo",
        "gender",
    ],
    "cor_do_seu_cabelo": [
        "cor_do_seu_cabelo",
        "cor_do_cabelo",
        "cor_cabelo",
        "hair_color",
        "cor_do_cabelo_participante",
    ],
}


def _resolve_prompt_variable_value(
    key: str, normalized_payload: dict[str, object]
) -> object | None:
    """Resolve a prompt variable value using direct keys and alias fallbacks.

    Args:
        key (str): Normalized placeholder key extracted from the template.
        normalized_payload (dict[str, object]): Normalized payload used for interpolation.

    Returns:
        object | None: Matching value to inject into the prompt template, if any.
    """
    if not key:
        return None

    direct = normalized_payload.get(key)
    if direct is not None:
        return direct

    for alias in _PROMPT_KEY_ALIASES.get(key, []):
        val = normalized_payload.get(alias)
        if val is not None:
            return val

    # Heuristic fallback for common semantic groups
    if "genero" in key or "sexo" in key or key == "gender":
        for alias in ("genero_para_criacao_do_avatar", "genero", "sexo", "gender"):
            val = normalized_payload.get(alias)
            if val is not None:
                return val

    if "cabelo" in key or "hair" in key:
        for alias in ("cor_do_seu_cabelo", "cor_do_cabelo", "cor_cabelo", "hair_color"):
            val = normalized_payload.get(alias)
            if val is not None:
                return val

    return None


def _gemini_max_attempts() -> int:
    """Return the configured maximum number of Gemini attempts.

    Returns:
        int: Maximum number of provider attempts before fallback behavior applies.
    """
    try:
        return max(1, int(os.getenv("QUIZ_GEMINI_MAX_ATTEMPTS", "3")))
    except Exception:
        return 3


def _gemini_retry_base_delay_seconds() -> float:
    """Return the configured base delay used for Gemini retry backoff.

    Returns:
        float: Base retry delay in seconds.
    """
    try:
        return max(0.1, float(os.getenv("QUIZ_GEMINI_RETRY_BASE_DELAY_SECONDS", "1.2")))
    except Exception:
        return 1.2


def _is_retryable_gemini_error_message(message: str) -> bool:
    """Determine whether a Gemini error message should be treated as retryable.

    Args:
        message (str): Error message raised during Gemini generation.

    Returns:
        bool: `True` when the worker should consider retrying the provider call.
    """
    text = (message or "").strip().lower()
    if not text:
        return False
    if "gemini_no_image_in_response" in text:
        return True
    if "gemini_empty_image" in text:
        return True
    if "gemini_http_429" in text:
        return True
    if re.search(r"gemini_http_5\d\d", text):
        return True
    retryable_tokens = (
        "timeout",
        "timed out",
        "temporarily unavailable",
        "service unavailable",
        "internal server error",
        "connection reset",
        "connection aborted",
        "connection error",
        "read error",
    )
    return any(token in text for token in retryable_tokens)


def _should_retry_gemini_error_message(
    message: str,
    *,
    attempt: int,
    max_attempts: int,
) -> bool:
    """Determine whether another Gemini retry should be attempted.

    Args:
        message (str): Error message raised during Gemini generation.
        attempt (int): Current attempt number, starting at one.
        max_attempts (int): Maximum number of attempts allowed.

    Returns:
        bool: `True` when the worker should sleep and retry.
    """
    return attempt < max_attempts and _is_retryable_gemini_error_message(message)


def _strip_accents(text: str) -> str:
    """Remove accent marks from a string while preserving its letters.

    Args:
        text (str): Text to normalize.

    Returns:
        str: Accent-free representation of the input.
    """
    return "".join(
        ch
        for ch in unicodedata.normalize("NFD", text or "")
        if unicodedata.category(ch) != "Mn"
    )


def _normalize_variable_key(raw: str) -> str:
    """Normalize template and payload keys into a shared slug format.

    Args:
        raw (str): Raw placeholder or payload key.

    Returns:
        str: Lowercased key containing only letters, numbers, and underscores.
    """
    key = (raw or "").strip()
    if not key:
        return ""
    key = re.sub(r"^\{\{\s*|\s*\}\}$", "", key)
    key = re.sub(r"^\[\[\s*|\s*\]\]$", "", key)
    key = re.sub(r"^\{\[\s*|\s*\]\}$", "", key)
    key = re.sub(r"^\[\s*|\s*\]$", "", key)
    key = re.sub(r"^\{\s*|\s*\}$", "", key)
    key = _strip_accents(key).lower()
    key = re.sub(r"[^a-z0-9_]", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key


def _normalize_template_placeholders(template: str) -> str:
    """Convert legacy placeholder syntaxes into the canonical `{{key}}` format.

    Args:
        template (str): Prompt template that may contain legacy placeholder styles.

    Returns:
        str: Prompt template with normalized placeholder markers.
    """
    normalized = template or ""
    for pattern in _LEGACY_VAR_PATTERNS:
        normalized = pattern.sub(
            lambda m: "{{" + _normalize_variable_key(str(m.group(1) or "")) + "}}",
            normalized,
        )
    return normalized


def _translate_prompt_value_to_english(value) -> str:
    """Translate dynamic prompt values to English before interpolation.

    Unknown words are preserved so the worker avoids losing user-provided information.

    Args:
        value: Value being interpolated into the prompt template.

    Returns:
        str: English-friendly representation of the provided value.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple)):
        return ", ".join(
            _translate_prompt_value_to_english(v) for v in value if v is not None
        )

    raw = str(value).strip()
    if not raw:
        return ""

    normalized = _strip_accents(raw).lower()
    if normalized in _PROMPT_EXACT_TRANSLATIONS:
        return _PROMPT_EXACT_TRANSLATIONS[normalized]

    def _replace_word(match: re.Match[str]) -> str:
        word = match.group(0)
        key = _strip_accents(word).lower()
        translated = _PROMPT_WORD_TRANSLATIONS.get(key)
        return translated if translated else word

    return _WORD_RE.sub(_replace_word, raw)


def _render_prompt_template(template: str, data: dict | None) -> str:
    """Render a prompt template with normalized payload values.

    Args:
        template (str): Prompt template containing `{{variable}}` placeholders.
        data (dict | None): Payload used to resolve template variables.

    Returns:
        str: Rendered prompt text with empty lines and whitespace normalized.
    """
    raw = _normalize_template_placeholders(str(template or "")).strip()
    if not raw:
        return ""
    payload = data if isinstance(data, dict) else {}
    normalized_payload = {
        _normalize_variable_key(str(k)): v
        for k, v in payload.items()
        if _normalize_variable_key(str(k))
    }

    def _replace(match: re.Match[str]) -> str:
        key = _normalize_variable_key(str(match.group(1) or ""))
        val = _resolve_prompt_variable_value(key, normalized_payload)
        if val is None:
            return ""
        if isinstance(val, dict) and str(val.get("kind") or "") == "prompt_image":
            label = str(val.get("label") or key).strip() or key
            return f"reference image of {label}"
        return _translate_prompt_value_to_english(val)

    rendered = _VAR_TOKEN_RE.sub(_replace, raw)
    # normalize whitespace while keeping line breaks readable
    rendered = "\n".join(line.strip() for line in rendered.splitlines() if line.strip())
    return rendered


def _extract_prompt_image_assets(data: dict | None) -> dict[str, dict[str, str]]:
    """Extract prompt-image metadata previously stored on the credential payload.

    Args:
        data (dict | None): Credential data payload.

    Returns:
        dict[str, dict[str, str]]: Prompt-image assets indexed by normalized field key.
    """
    payload = data if isinstance(data, dict) else {}
    raw_assets = (
        payload.get(_PROMPT_IMAGE_DATA_KEY)
        if isinstance(payload.get(_PROMPT_IMAGE_DATA_KEY), dict)
        else {}
    )
    assets: dict[str, dict[str, str]] = {}
    for raw_key, raw_asset in raw_assets.items():
        if not isinstance(raw_asset, dict):
            continue
        key = _normalize_variable_key(str(raw_key or ""))
        storage_path = str(raw_asset.get("storage_path") or "").strip()
        label = str(raw_asset.get("label") or key).strip() or key
        if not key or not storage_path:
            continue
        assets[key] = {
            "storage_path": storage_path,
            "label": label,
        }
    return assets


def _normalize_prompt_asset_selection(raw_value: object) -> list[str]:
    """Normalize a prompt-asset selection payload into unique asset keys.

    Args:
        raw_value (object): Raw single-select or multi-select payload value.

    Returns:
        list[str]: Distinct normalized asset keys selected by the user.
    """
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        raw_items = raw_value
    else:
        raw_items = [part.strip() for part in str(raw_value).split(",")]
    normalized: list[str] = []
    for item in raw_items:
        key = _normalize_variable_key(str(item or ""))
        if key and key not in normalized:
            normalized.append(key)
    return normalized


def _resolve_catalog_prompt_assets(
    data: dict | None,
    rows: list[dict] | None,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    """Resolve fixed and selected catalog prompt assets for the current credential.

    Args:
        data (dict | None): Credential data payload.
        rows (list[dict] | None): Prompt-asset catalog rows loaded from Supabase.

    Returns:
        tuple[list[dict[str, str]], dict[str, object]]: Deduplicated asset list to download
        and prompt payload additions describing selected asset labels.
    """
    payload = data if isinstance(data, dict) else {}
    asset_rows = rows if isinstance(rows, list) else []
    fixed_assets: list[dict[str, str]] = []
    selectable_assets: dict[str, dict[str, dict[str, str]]] = {}

    for row in asset_rows:
        storage_path = str(row.get("storage_path") or "").strip()
        label = str(row.get("label") or "").strip()
        asset_key = _normalize_variable_key(str(row.get("asset_key") or ""))
        if not storage_path or not label or not asset_key:
            continue
        asset = {
            "asset_key": asset_key,
            "label": label,
            "storage_path": storage_path,
        }
        if bool(row.get("required")):
            fixed_assets.append(asset)
            continue
        variable_key = _normalize_variable_key(str(row.get("variable_key") or ""))
        if not variable_key:
            continue
        selectable_assets.setdefault(variable_key, {})[asset_key] = asset

    selected_assets: list[dict[str, str]] = []
    prompt_payload: dict[str, object] = {}
    for variable_key, available_assets in selectable_assets.items():
        selected_keys = _normalize_prompt_asset_selection(payload.get(variable_key))
        if not selected_keys:
            continue
        matched_assets = [
            available_assets[key] for key in selected_keys if key in available_assets
        ]
        if not matched_assets:
            continue
        selected_assets.extend(matched_assets)
        labels = [asset["label"] for asset in matched_assets]
        prompt_payload[variable_key] = labels if len(labels) > 1 else labels[0]

    deduped_assets: list[dict[str, str]] = []
    seen_storage_paths: set[str] = set()
    for asset in [*fixed_assets, *selected_assets]:
        storage_path = str(asset.get("storage_path") or "").strip()
        if not storage_path or storage_path in seen_storage_paths:
            continue
        seen_storage_paths.add(storage_path)
        deduped_assets.append(asset)

    return deduped_assets, prompt_payload


def _build_catalog_asset_prompt_appendix(
    assets: list[dict[str, str]] | None,
    prompt_payload: dict[str, object] | None,
) -> str:
    """Build prompt instructions that force catalog assets to appear visibly.

    Args:
        assets (list[dict[str, str]] | None): Resolved fixed and selected catalog assets.
        prompt_payload (dict[str, object] | None): Prompt payload describing selected labels.

    Returns:
        str: Prompt appendix with composition rules for fixed and selected assets.
    """
    resolved_assets = assets if isinstance(assets, list) else []
    payload = prompt_payload if isinstance(prompt_payload, dict) else {}

    selected_labels: list[str] = []
    for value in payload.values():
        if isinstance(value, list):
            for item in value:
                label = str(item or "").strip()
                if label:
                    selected_labels.append(label)
            continue
        label = str(value or "").strip()
        if label:
            selected_labels.append(label)

    selected_labels = list(dict.fromkeys(selected_labels))
    fixed_labels = list(
        dict.fromkeys(
            [
                str(asset.get("label") or "").strip()
                for asset in resolved_assets
                if str(asset.get("label") or "").strip()
                and str(asset.get("label") or "").strip() not in selected_labels
            ]
        )
    )
    if not fixed_labels and not selected_labels:
        return ""

    fixed_prompt_labels = [
        _translate_prompt_value_to_english(label) for label in fixed_labels
    ]
    selected_prompt_labels = [
        _translate_prompt_value_to_english(label) for label in selected_labels
    ]

    instructions: list[str] = []
    if fixed_labels:
        instructions.append(
            "Mandatory fixed reference assets that must define the scene structure: "
            + ", ".join(fixed_prompt_labels)
            + "."
        )
    if selected_labels:
        instructions.append(
            "Mandatory selected visual elements that must be clearly visible in the final image: "
            + ", ".join(selected_prompt_labels)
            + "."
        )
        if len(selected_prompt_labels) == 1:
            instructions.append(
                "Do not omit, hide, or replace the selected visual element. Integrate it into the composition in an obvious and readable way."
            )
        elif len(selected_prompt_labels) <= 3:
            instructions.append(
                "Do not omit, hide, or replace any selected visual element. Integrate every selected element into the composition in an obvious and readable way, with each one individually recognizable."
            )
        elif len(selected_prompt_labels) <= 8:
            instructions.append(
                "There are multiple selected visual elements. Arrange them as a balanced collectible composition around the participant, using shelves, accessories, props, or layered product placement so every selected element remains individually visible."
            )
        else:
            instructions.append(
                "There are many selected visual elements. Use a structured collage or curated display system inside and around the packaging, such as miniature props, side compartments, icon cards, stickers, shelves, or printed inserts, so the final composition still represents all selected elements."
            )
        instructions.append(
            "Use the provided reference images for the selected assets as the source of truth for visual identity, shape, and object appearance."
        )
    instructions.append(
        "The participant must remain the main subject, but the fixed and selected assets must appear as explicit visual objects in the final composition."
    )
    instructions.append(
        "If the number of selected assets is high, reduce their size and distribute them intelligently, but do not silently remove selected assets from the scene."
    )
    return "\n".join(instructions)


def _build_prompt_template_payload(data: dict | None) -> dict[str, object]:
    """Prepare credential payload data for prompt-template interpolation.

    Args:
        data (dict | None): Credential data payload.

    Returns:
        dict[str, object]: Prompt payload enriched with prompt-image placeholders.
    """
    payload = dict(data) if isinstance(data, dict) else {}
    for key, asset in _extract_prompt_image_assets(payload).items():
        payload[key] = {
            "kind": "prompt_image",
            "label": str(asset.get("label") or key).strip() or key,
        }
    return payload


def _select_prompt_image_assets(
    template: str,
    available_assets: dict[str, dict[str, str]],
) -> list[tuple[str, dict[str, str]]]:
    """Select prompt-image assets referenced by the prompt template.

    Args:
        template (str): Prompt template used for generation.
        available_assets (dict[str, dict[str, str]]): Prompt-image assets available on the credential.

    Returns:
        list[tuple[str, dict[str, str]]]: Ordered prompt-image assets that should be downloaded.
    """
    if not available_assets:
        return []

    normalized_template = _normalize_template_placeholders(str(template or ""))
    referenced_keys: list[str] = []
    for match in _VAR_TOKEN_RE.finditer(normalized_template):
        key = _normalize_variable_key(str(match.group(1) or ""))
        if key in available_assets and key not in referenced_keys:
            referenced_keys.append(key)

    selected_keys = referenced_keys or list(available_assets.keys())
    return [
        (key, available_assets[key]) for key in selected_keys if key in available_assets
    ]


def _build_svg_card(job: Job, cred_row: dict) -> bytes:
    """Build a fallback SVG card when Gemini generation is unavailable.

    Args:
        job (Job): Claimed generation job.
        cred_row (dict): Credential row driving the generation.

    Returns:
        bytes: SVG document encoded as UTF-8 bytes.
    """
    data = cred_row.get("data_json") or {}
    name = html.escape(str(data.get("name") or "Participante"))
    city = html.escape(str(data.get("city") or ""))
    profession = html.escape(str(data.get("profession") or ""))
    subtitle = f"{city} {profession}".strip()
    subtitle = html.escape(subtitle) if subtitle else "EUVATAR Experience"
    ts = html.escape(dt.datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S UTC"))
    kind = html.escape(job.kind)
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="1080" height="1080">
<defs>
  <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
    <stop offset="0%" stop-color="#0b1f3a"/>
    <stop offset="100%" stop-color="#1f4f8a"/>
  </linearGradient>
</defs>
<rect width="1080" height="1080" fill="url(#bg)"/>
<rect x="80" y="80" width="920" height="920" rx="36" fill="#ffffff" opacity="0.93"/>
<text x="130" y="220" font-size="52" font-family="Arial, sans-serif" fill="#0b1f3a">EUVATAR CARD</text>
<text x="130" y="320" font-size="68" font-weight="700" font-family="Arial, sans-serif" fill="#10294a">{name}</text>
<text x="130" y="390" font-size="36" font-family="Arial, sans-serif" fill="#274c77">{subtitle}</text>
<text x="130" y="480" font-size="28" font-family="Arial, sans-serif" fill="#274c77">Generation kind: {kind}</text>
<text x="130" y="900" font-size="22" font-family="Arial, sans-serif" fill="#4c627d">Generated at {ts}</text>
</svg>"""
    return svg.encode("utf-8")


def _upload_output(
    settings: Settings,
    experience_id: str,
    generation_id: str,
    data: bytes,
    *,
    mime_type: str,
) -> str:
    """Upload generated output bytes to Supabase Storage.

    Args:
        settings (Settings): Runtime settings used for storage requests.
        experience_id (str): Experience identifier that owns the output.
        generation_id (str): Generation identifier used in the output path.
        data (bytes): Generated file contents to upload.
        mime_type (str): MIME type associated with the output bytes.

    Returns:
        str: Storage path of the uploaded output asset.

    Raises:
        RuntimeError: Raised when the storage upload fails.
    """
    bucket = settings.supabase_bucket
    ext = _ext_from_mime(mime_type)
    path = f"quiz/{experience_id}/generations/{generation_id}.{ext}"
    url = f"{settings.supabase_url}/storage/v1/object/{bucket}/{path}"
    r = requests.post(
        url,
        headers={
            **rest_headers(settings),
            "x-upsert": "true",
            "Content-Type": mime_type or "image/png",
        },
        data=data,
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"storage_upload_failed:{r.status_code}:{r.text[:160]}")
    return path


def _finish_job_done(settings: Settings, job: Job, duration_ms: int, output_path: str):
    """Mark a generation row as done and persist output metadata.

    Args:
        settings (Settings): Runtime settings used for Supabase requests.
        job (Job): Claimed generation job.
        duration_ms (int): Processing duration in milliseconds.
        output_path (str): Storage path of the generated output.
    """
    url = f"{settings.supabase_url}/rest/v1/generations?id=eq.{job.id}"
    body_with_cost = {
        "status": "done",
        "duration_ms": duration_ms,
        "output_path": output_path,
        "output_url": None,
        "cost_estimated_usd": _estimated_cost_usd(job),
        "cost_currency": "USD",
        "error_message": None,
        "updated_at": _now_iso(),
    }
    r = requests.patch(
        url,
        headers={**rest_headers(settings), "Content-Type": "application/json"},
        json=body_with_cost,
        timeout=20,
    )
    if r.ok:
        return
    # backward compatibility: environments without cost columns yet
    body_legacy = {
        "status": "done",
        "duration_ms": duration_ms,
        "output_path": output_path,
        "output_url": None,
        "error_message": None,
        "updated_at": _now_iso(),
    }
    requests.patch(
        url,
        headers={**rest_headers(settings), "Content-Type": "application/json"},
        json=body_legacy,
        timeout=20,
    )


def _finish_job_error(settings: Settings, job: Job, duration_ms: int, err: str):
    """Mark a generation row as errored and persist failure metadata.

    Args:
        settings (Settings): Runtime settings used for Supabase requests.
        job (Job): Claimed generation job.
        duration_ms (int): Processing duration in milliseconds.
        err (str): Error message associated with the failed job.
    """
    url = f"{settings.supabase_url}/rest/v1/generations?id=eq.{job.id}"
    body_with_cost = {
        "status": "error",
        "duration_ms": duration_ms,
        "cost_estimated_usd": _estimated_cost_usd(job),
        "cost_currency": "USD",
        "error_message": err[:1000],
        "updated_at": _now_iso(),
    }
    r = requests.patch(
        url,
        headers={**rest_headers(settings), "Content-Type": "application/json"},
        json=body_with_cost,
        timeout=20,
    )
    if r.ok:
        return
    body_legacy = {
        "status": "error",
        "duration_ms": duration_ms,
        "error_message": err[:1000],
        "updated_at": _now_iso(),
    }
    requests.patch(
        url,
        headers={**rest_headers(settings), "Content-Type": "application/json"},
        json=body_legacy,
        timeout=20,
    )


def _process_job(settings: Settings, job: Job):
    """Process a claimed generation job from credential load to final persistence.

    Args:
        settings (Settings): Runtime settings used for data access and provider calls.
        job (Job): Claimed generation job to process.
    """
    t0 = time.time()
    _write_generation_log(
        settings,
        job.id,
        level="info",
        event="job_started",
        message="Generation worker started processing job",
        payload={
            "kind": job.kind,
            "experience_id": job.experience_id,
            "credential_id": job.credential_id,
        },
    )
    try:
        cred = _load_credential_data(settings, job.credential_id)
        gender, hair_color = _extract_generation_inputs(cred)
        cred_data_for_log = (
            cred.get("data_json") if isinstance(cred.get("data_json"), dict) else {}
        )
        catalog_prompt_assets = _load_experience_prompt_assets(
            settings, job.experience_id
        )
        catalog_assets, catalog_prompt_payload = _resolve_catalog_prompt_assets(
            cred_data_for_log,
            catalog_prompt_assets,
        )
        selected_catalog_labels: list[str] = []
        for value in catalog_prompt_payload.values():
            if isinstance(value, list):
                selected_catalog_labels.extend(
                    str(item or "").strip() for item in value if str(item or "").strip()
                )
                continue
            label = str(value or "").strip()
            if label:
                selected_catalog_labels.append(label)
        selected_catalog_labels = list(dict.fromkeys(selected_catalog_labels))
        fixed_catalog_labels = list(
            dict.fromkeys(
                [
                    str(asset.get("label") or "").strip()
                    for asset in catalog_assets
                    if str(asset.get("label") or "").strip()
                    and str(asset.get("label") or "").strip()
                    not in selected_catalog_labels
                ]
            )
        )
        _write_generation_log(
            settings,
            job.id,
            level="info",
            event="credential_loaded",
            message="Credential row loaded",
            payload={
                "has_photo_path": bool(cred.get("photo_path")),
                "has_data_json": bool(cred.get("data_json")),
                "prompt_image_fields": len(
                    _extract_prompt_image_assets(cred_data_for_log)
                ),
                "catalog_prompt_assets": len(catalog_assets),
                "fixed_catalog_assets": fixed_catalog_labels,
                "selected_catalog_assets": selected_catalog_labels,
                "gender": gender,
                "hair_color": hair_color,
                "winner_archetype_id": str(
                    (cred_data_for_log or {}).get("winner_archetype_id") or ""
                ),
            },
        )
        out_path = ""
        photo_path = str(cred.get("photo_path") or "").strip()
        cred_data = (
            cred.get("data_json") if isinstance(cred.get("data_json"), dict) else {}
        )
        prompt_template_payload = _build_prompt_template_payload(
            {
                **cred_data,
                **catalog_prompt_payload,
            }
        )
        winner_archetype_id = str(
            (cred_data or {}).get("winner_archetype_id") or ""
        ).strip()
        archetype = (
            _load_archetype(settings, job.experience_id, winner_archetype_id)
            if winner_archetype_id
            else None
        )
        if not archetype:
            archetype = _load_first_archetype(settings, job.experience_id)
        raw_archetype_prompt = str((archetype or {}).get("image_prompt") or "")
        archetype_prompt = _render_prompt_template(
            raw_archetype_prompt, prompt_template_payload
        )
        catalog_asset_appendix = _build_catalog_asset_prompt_appendix(
            catalog_assets,
            catalog_prompt_payload,
        )
        if catalog_asset_appendix:
            archetype_prompt = (
                f"{archetype_prompt}\n\n{catalog_asset_appendix}".strip()
                if archetype_prompt
                else catalog_asset_appendix
            )
        prompt_image_assets = _select_prompt_image_assets(
            raw_archetype_prompt,
            _extract_prompt_image_assets(cred_data),
        )
        prompt_source = "archetype" if archetype_prompt else "fixed_default"

        # Preferred mode: Gemini generation. With photo when available; prompt-only when archetype allows it.
        effective_gemini_key = _resolve_experience_gemini_key(
            settings, job.experience_id
        )
        if not effective_gemini_key:
            raise RuntimeError("missing_experience_gemini_key")
        use_photo_prompt = bool((archetype or {}).get("use_photo_prompt"))
        has_prompt_image_assets = bool(prompt_image_assets)
        has_catalog_prompt_assets = bool(catalog_assets)
        can_prompt_only = bool(
            effective_gemini_key
            and (not photo_path)
            and archetype_prompt
            and (
                (not use_photo_prompt)
                or has_prompt_image_assets
                or has_catalog_prompt_assets
            )
        )
        if effective_gemini_key and (
            photo_path
            or has_prompt_image_assets
            or has_catalog_prompt_assets
            or can_prompt_only
        ):
            gemini_settings = replace(settings, gemini_api_key=effective_gemini_key)
            gemini = GeminiImageClient(gemini_settings)
            max_attempts = _gemini_max_attempts()
            retry_base_delay = _gemini_retry_base_delay_seconds()
            ref_bytes = b""
            ref_b64 = ""
            ref_mime = "image/jpeg"
            inline_images: list[dict[str, str]] = []
            prompt_applied = archetype_prompt or build_editorial_prompt(
                gender, hair_color
            )
            if photo_path:
                ref_bytes, ref_mime = _download_reference_image(settings, photo_path)
                ref_b64 = base64.b64encode(ref_bytes).decode("ascii")
                inline_images.append(
                    {
                        "data": ref_b64,
                        "mime_type": ref_mime,
                    }
                )
            for _, asset in prompt_image_assets:
                asset_bytes, asset_mime = _download_reference_image(
                    settings,
                    str(asset.get("storage_path") or ""),
                )
                inline_images.append(
                    {
                        "data": base64.b64encode(asset_bytes).decode("ascii"),
                        "mime_type": asset_mime,
                    }
                )
            for asset in catalog_assets:
                asset_bytes, asset_mime = _download_reference_image(
                    settings,
                    str(asset.get("storage_path") or ""),
                )
                inline_images.append(
                    {
                        "data": base64.b64encode(asset_bytes).decode("ascii"),
                        "mime_type": asset_mime,
                    }
                )
            if photo_path and (has_prompt_image_assets or has_catalog_prompt_assets):
                generation_mode = "reference_photo_plus_prompt_assets"
            elif photo_path:
                generation_mode = "reference_photo"
            elif has_prompt_image_assets or has_catalog_prompt_assets:
                generation_mode = "prompt_assets_only"
            else:
                generation_mode = "prompt_only"

            generated_bytes = b""
            generated_mime = "image/png"
            model_name = None
            latency_ms = None
            last_err = None
            _write_generation_log(
                settings,
                job.id,
                level="info",
                event="generation_inputs_resolved",
                message="Generation inputs resolved before Gemini call",
                payload={
                    "generation_mode": generation_mode,
                    "has_photo_path": bool(photo_path),
                    "prompt_image_asset_labels": [
                        str(asset.get("label") or "").strip()
                        for _, asset in prompt_image_assets
                        if str(asset.get("label") or "").strip()
                    ],
                    "fixed_catalog_assets": fixed_catalog_labels,
                    "selected_catalog_assets": selected_catalog_labels,
                    "inline_image_count": len(inline_images),
                    "prompt_source": prompt_source,
                    "prompt_preview": (prompt_applied or "")[:600],
                },
            )

            for attempt in range(1, max_attempts + 1):
                try:
                    if inline_images:
                        t_gem = time.time()
                        raw = gemini.generate_from_images_b64(
                            prompt=prompt_applied,
                            images=inline_images,
                        )
                        latency_ms = int((time.time() - t_gem) * 1000)
                        generated_bytes = raw.get("image_bytes") or b""
                        generated_mime = str(raw.get("mime_type") or "image/png")
                        model_name = raw.get("model")
                    else:
                        t_gem = time.time()
                        raw = gemini.generate_from_prompt(prompt_applied)
                        latency_ms = int((time.time() - t_gem) * 1000)
                        generated_bytes = raw.get("image_bytes") or b""
                        generated_mime = str(raw.get("mime_type") or "image/png")
                        model_name = raw.get("model")

                    if not generated_bytes:
                        raise RuntimeError("gemini_empty_image")

                    if attempt > 1:
                        _write_generation_log(
                            settings,
                            job.id,
                            level="info",
                            event="gemini_retry_recovered",
                            message="Gemini succeeded after retry",
                            payload={"attempt": attempt, "max_attempts": max_attempts},
                        )
                    break
                except Exception as exc:
                    last_err = exc
                    err_str = str(exc)
                    is_retryable = _is_retryable_gemini_error_message(err_str)
                    should_retry = _should_retry_gemini_error_message(
                        err_str,
                        attempt=attempt,
                        max_attempts=max_attempts,
                    )
                    _write_generation_log(
                        settings,
                        job.id,
                        level="warning" if is_retryable else "error",
                        event="gemini_attempt_failed",
                        message="Gemini generation attempt failed",
                        payload={
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "retryable": is_retryable,
                            "will_retry": should_retry,
                            "error": err_str[:1000],
                        },
                    )
                    if not is_retryable:
                        raise
                    if not should_retry:
                        break
                    sleep_s = retry_base_delay * (2 ** (attempt - 1))
                    time.sleep(sleep_s)

            if last_err is not None and not generated_bytes:
                last_err_str = str(last_err)
                if not _is_retryable_gemini_error_message(last_err_str):
                    raise last_err

                # Graceful fallback for transient provider outages: keep user flow alive
                # with the captured photo (if present) or SVG card output.
                if photo_path and ref_bytes:
                    generated_bytes = ref_bytes
                    generated_mime = ref_mime or "image/jpeg"
                    _write_generation_log(
                        settings,
                        job.id,
                        level="warning",
                        event="gemini_fallback_reference_image",
                        message="Gemini failed after retries; using reference photo fallback",
                        payload={
                            "error": last_err_str[:1000],
                            "max_attempts": max_attempts,
                            "mime_type": generated_mime,
                        },
                    )
                else:
                    generated_bytes = _build_svg_card(job, cred)
                    generated_mime = "image/svg+xml"
                    _write_generation_log(
                        settings,
                        job.id,
                        level="warning",
                        event="gemini_fallback_svg_on_retryable_error",
                        message="Gemini failed after retries; using SVG fallback",
                        payload={
                            "error": last_err_str[:1000],
                            "max_attempts": max_attempts,
                        },
                    )

            out_path = _upload_output(
                settings,
                job.experience_id,
                job.id,
                generated_bytes,
                mime_type=generated_mime,
            )
            _write_generation_log(
                settings,
                job.id,
                level="info",
                event="gemini_generated",
                message="Gemini generated and uploaded output image",
                payload={
                    "model": model_name,
                    "latency_ms": latency_ms,
                    "mime_type": generated_mime,
                    "output_path": out_path,
                    "prompt_source": prompt_source,
                    "prompt_chars": len(prompt_applied or ""),
                    "archetype_id": (archetype or {}).get("id"),
                    "archetype_name": (archetype or {}).get("name"),
                    "generation_mode": generation_mode,
                    "use_photo_prompt": use_photo_prompt,
                    "has_photo_path": bool(photo_path),
                    "gemini_key_source": "experience",
                },
            )
        else:
            # Fallback path keeps previous behavior for environments without Gemini or without reference image.
            svg = _build_svg_card(job, cred)
            out_path = _upload_output(
                settings,
                job.experience_id,
                job.id,
                svg,
                mime_type="image/svg+xml",
            )
            _write_generation_log(
                settings,
                job.id,
                level="warning",
                event="fallback_svg_output",
                message="Fallback SVG output used (gemini path not eligible)",
                payload={
                    "has_gemini_key": bool(effective_gemini_key),
                    "has_photo_path": bool(photo_path),
                    "use_photo_prompt": bool((archetype or {}).get("use_photo_prompt")),
                    "has_archetype_prompt": bool(archetype_prompt),
                    "output_path": out_path,
                },
            )
        _write_generation_log(
            settings,
            job.id,
            level="info",
            event="output_uploaded",
            message="Output uploaded to storage",
            payload={"output_path": out_path},
        )
        dur = int((time.time() - t0) * 1000)
        _finish_job_done(settings, job, dur, out_path)
        _write_generation_log(
            settings,
            job.id,
            level="info",
            event="job_done",
            message="Generation job completed",
            payload={
                "duration_ms": dur,
                "cost_estimated_usd": _estimated_cost_usd(job),
                "cost_currency": "USD",
            },
        )
    except Exception as exc:
        dur = int((time.time() - t0) * 1000)
        _finish_job_error(settings, job, dur, str(exc))
        _write_generation_log(
            settings,
            job.id,
            level="error",
            event="job_error",
            message="Generation job failed",
            payload={
                "duration_ms": dur,
                "cost_estimated_usd": _estimated_cost_usd(job),
                "cost_currency": "USD",
                "error": str(exc)[:1000],
            },
        )


def _fetch_pending(settings: Settings, limit: int) -> list[str]:
    """Fetch pending generation identifiers from the queue table.

    Args:
        settings (Settings): Runtime settings used for Supabase reads.
        limit (int): Maximum number of pending ids to fetch.

    Returns:
        list[str]: Pending generation identifiers ordered by creation time.
    """
    rows = get_json(
        settings,
        "generations",
        "id",
        {"status": "eq.pending", "order": "created_at.asc"},
        limit=limit,
    )
    return [str(r.get("id")) for r in rows if r.get("id")]


def main() -> int:
    """Run the generation worker loop until interrupted or a single batch completes.

    Returns:
        int: Process exit code for the worker command.
    """
    load_dotenv()
    parser = argparse.ArgumentParser(description="Run quiz generation worker")
    parser.add_argument("--max-workers", type=int, default=5, help="Concurrent jobs")
    parser.add_argument("--batch-size", type=int, default=20, help="Pending fetch size")
    parser.add_argument(
        "--once", action="store_true", help="Process one batch and exit"
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=2.0,
        help="Sleep interval when no pending jobs",
    )
    parser.add_argument(
        "--network-retry-base-seconds",
        type=float,
        default=2.0,
        help="Base backoff when pending fetch fails due to network/DNS",
    )
    parser.add_argument(
        "--network-retry-max-seconds",
        type=float,
        default=60.0,
        help="Max backoff when pending fetch fails due to network/DNS",
    )
    args = parser.parse_args()

    settings = Settings.load()
    max_workers = max(1, int(args.max_workers))
    batch_size = max(1, int(args.batch_size))
    net_retry_base = max(0.1, float(args.network_retry_base_seconds))
    net_retry_max = max(net_retry_base, float(args.network_retry_max_seconds))
    net_error_count = 0

    while True:
        try:
            pending_ids = _fetch_pending(settings, batch_size)
            net_error_count = 0
        except requests.exceptions.RequestException as exc:
            net_error_count += 1
            sleep_s = min(
                net_retry_max, net_retry_base * (2 ** max(0, net_error_count - 1))
            )
            print(
                f"[WORKER] network_fetch_pending_error attempt={net_error_count} "
                f"sleep_s={sleep_s:.1f} err={exc}",
                flush=True,
            )
            if args.once:
                return 1
            time.sleep(sleep_s)
            continue
        if not pending_ids:
            if args.once:
                break
            time.sleep(max(0.1, args.poll_seconds))
            continue

        claimed: list[Job] = []
        for pid in pending_ids:
            job = _claim_job(settings, pid)
            if job:
                _write_generation_log(
                    settings,
                    job.id,
                    level="info",
                    event="job_claimed",
                    message="Job claimed from pending queue",
                    payload={"kind": job.kind},
                )
                claimed.append(job)

        if claimed:
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                for job in claimed:
                    pool.submit(_process_job, settings, job)

        if args.once:
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

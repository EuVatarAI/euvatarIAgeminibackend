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
import io
import json
import logging
import os
import re
import sys
import time
import unicodedata
from collections import deque
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

logger = logging.getLogger(__name__)


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
        log_message = (
            "[GENERATION_LOG] "
            f"generation_id={generation_id} "
            f"level={level} "
            f"event={event} "
            f"message={message} "
            f"payload={json.dumps(payload or {}, ensure_ascii=True, sort_keys=True)}"
        )
    except Exception:
        log_message = (
            "[GENERATION_LOG] "
            f"generation_id={generation_id} "
            f"level={level} "
            f"event={event} "
            f"message={message}"
        )

    logger.log(
        getattr(logging, str(level or "INFO").upper(), logging.INFO), log_message
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


def _load_experience_config(settings: Settings, experience_id: str) -> dict:
    """Load the experience config JSON used to toggle generation behavior."""
    rows = get_json(
        settings,
        "experiences",
        "config_json",
        {"id": f"eq.{experience_id}"},
        limit=1,
    )
    if not rows:
        return {}
    config_json = rows[0].get("config_json")
    return config_json if isinstance(config_json, dict) else {}


def _avatar_cutout_enabled(config_json: dict | None) -> bool:
    """Return whether the experience requests cutout-based avatar generation."""
    config = config_json if isinstance(config_json, dict) else {}
    avatar_generation = (
        config.get("avatar_generation")
        if isinstance(config.get("avatar_generation"), dict)
        else {}
    )
    return (
        bool(avatar_generation.get("enabled"))
        and str(avatar_generation.get("background_mode") or "").strip()
        == "builder_fixed_png"
    )


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
    normalized_data = {
        _normalize_variable_key(str(key)): value for key, value in (data or {}).items()
    }

    gender = "mulher"
    for key in _GENERATION_GENDER_KEYS:
        raw_value = normalized_data.get(_normalize_variable_key(key))
        normalized = _normalize_generation_choice(
            raw_value,
            _GENDER_VALUE_ALIASES,
        )
        if normalized in _ALLOWED_GENDERS:
            gender = normalized
            break

    hair_color = "castanho"
    for key in _GENERATION_HAIR_COLOR_KEYS:
        raw_value = normalized_data.get(_normalize_variable_key(key))
        normalized = _normalize_generation_choice(
            raw_value,
            _HAIR_COLOR_VALUE_ALIASES,
        )
        if normalized in _ALLOWED_HAIR_COLORS:
            hair_color = normalized
            break

    return gender, hair_color


def _normalize_generation_choice(
    raw_value: object,
    aliases: dict[str, str],
) -> str:
    """Normalize a generation trait value using a canonical alias map."""
    text = str(raw_value or "").strip().lower()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return aliases.get(normalized, normalized)


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

_GENDER_VALUE_ALIASES = {
    "mulher": "mulher",
    "feminino": "mulher",
    "female": "mulher",
    "woman": "mulher",
    "homem": "homem",
    "masculino": "homem",
    "male": "homem",
    "man": "homem",
}

_HAIR_COLOR_VALUE_ALIASES = {
    "loiro": "loiro",
    "blond": "loiro",
    "blonde": "loiro",
    "castanho": "castanho",
    "brown": "castanho",
    "brunette": "castanho",
    "preto": "preto",
    "black": "preto",
    "ruivo": "ruivo",
    "red": "ruivo",
    "ginger": "ruivo",
    "grisalho": "grisalho",
    "gray": "grisalho",
    "grey": "grisalho",
}

_GENERATION_GENDER_KEYS = (
    "gender",
    "genero",
    "sexo",
    "genero_para_criacao_do_avatar",
)

_GENERATION_HAIR_COLOR_KEYS = (
    "hair_color",
    "cor_do_cabelo",
    "cor_do_seu_cabelo",
    "cor_cabelo",
    "cor_do_cabelo_participante",
)


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


def _avatar_cutout_max_attempts() -> int:
    """Return the stricter retry budget used for avatar cutout generations.

    Returns:
        int: Maximum number of attempts allowed for avatar-only cutout mode.
    """
    try:
        return max(1, int(os.getenv("QUIZ_GEMINI_AVATAR_CUTOUT_MAX_ATTEMPTS", "7")))
    except Exception:
        return 7


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
    if "avatar_cutout_quality_failed" in text:
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


def _strip_template_lines_with_keys(
    template: str, keys: list[str] | tuple[str, ...]
) -> str:
    """Remove template lines that reference placeholders for deferred generation data."""
    if not template or not keys:
        return str(template or "")
    normalized_keys = {
        _normalize_variable_key(str(key or ""))
        for key in keys
        if _normalize_variable_key(str(key or ""))
    }
    if not normalized_keys:
        return str(template or "")
    normalized_template = _normalize_template_placeholders(str(template or ""))
    kept_lines: list[str] = []
    for raw_line in normalized_template.splitlines():
        line = str(raw_line or "")
        referenced_keys = {
            _normalize_variable_key(str(match.group(1) or ""))
            for match in _VAR_TOKEN_RE.finditer(line)
        }
        if referenced_keys & normalized_keys:
            continue
        kept_lines.append(line)
    return "\n".join(kept_lines)


def _prepare_generation_prompt(
    base_prompt: str,
    appendix: str = "",
    *,
    enforce_photo_identity: bool = False,
    appearance_traits: str = "",
) -> str:
    """Compact and sanitize the final prompt sent to Gemini.

    Args:
        base_prompt (str): Rendered prompt body coming from the user or archetype.
        appendix (str): Additional composition instructions derived from catalog assets.

    Returns:
        str: Sanitized single-paragraph prompt optimized for image generation.
    """
    sections: list[str] = [
        "Create a purely visual image with zero readable text, letters, labels, captions, logos, signage, or watermarks anywhere."
    ]
    if enforce_photo_identity:
        sections.append(
            "Use the uploaded participant photo as the sole source of facial identity and recognizability. Do not invent a generic person, do not replace the face, and keep the participant clearly identifiable."
        )
        sections.append(
            "Prioritize exact facial resemblance to the uploaded participant over toy stylization, beauty enhancement, or generic doll features."
        )
        sections.append(
            "The final figure must look like the same real person from the uploaded photo, not an approximation."
        )
        sections.append(
            "Keep the head, face, and expression photorealistic and human. Apply the collectible-toy treatment only as a subtle material finish, not as facial redesign."
        )
        sections.append(
            "Do not create a caricature, doll face, cartoon face, oversized eyes, simplified nose, exaggerated jawline, exaggerated smile, or smoothed generic facial structure."
        )
        sections.append(
            "Keep the head size, neck width, shoulders, arms, hands, torso, and legs in believable human proportion. Do not enlarge the head or shrink the body to create a toy-like caricature silhouette."
        )
        sections.append(
            "Preserve the real facial proportions, eyelids, eye spacing, eyebrow shape, nose bridge, nostrils, lips, teeth visibility, cheek volume, jaw contour, and skin texture from the uploaded photo."
        )
        sections.append(
            "If the participant is smiling in the photo, preserve the same smile shape and intensity. Do not broaden, simplify, or replace the smile with a generic doll expression."
        )
        sections.append(
            "Do not invent personal attributes that are not visible in the uploaded photo. Do not add glasses, hats, jewelry, facial hair, tattoos, or clothing details unless they are clearly present in the participant photo."
        )
    if appearance_traits.strip():
        sections.append(
            "Requested appearance traits for the figure: "
            + appearance_traits.strip().rstrip(".")
            + "."
        )

    for raw_section in (base_prompt, appendix):
        text = str(raw_section or "").strip()
        if not text:
            continue
        text = re.sub(r"\{\{[^}]+\}\}", "", text)
        text = _dedupe_prompt_sentences(text)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            sections.append(text)

    return " ".join(section for section in sections if section).strip()


def _build_avatar_cutout_prompt_appendix() -> str:
    """Build strict instructions for the avatar-only cutout generation mode."""
    instructions = [
        "Generate only one full-body collectible figure avatar of the participant.",
        "Use a plain solid neutral background with smooth even lighting and no visible scene elements.",
        "The background must be a single clean studio backdrop designed for easy background removal, preferably a uniform light gray, light beige, or off-white color with no gradient, texture, or vignette.",
        "Do not generate any packaging, box, frame, pedestal, floor props, scenery, furniture, text, logos, accessories, or decorative objects.",
        "Keep the composition vertical 9:16 with the avatar standing upright and centered.",
        "Show the full body from head to toe and keep the complete silhouette fully visible inside the frame.",
        "Let the avatar occupy most of the image height while leaving a small clean margin around the silhouette.",
        "Keep the head size in realistic proportion to the torso, shoulders, hips, arms, and legs. Do not generate an oversized head or miniaturized body.",
        "Keep clear negative space around the head, arms, hands, torso, legs, feet, and between both legs so the full silhouette is easy to isolate.",
        "Generate both full feet completely, including ankles, heels, soles, toes, or shoes when present. Do not crop, hide, merge, blur, or distort the feet.",
        "Do not let the feet blend into the background or into any floor shadow. The bottom contour of each foot must be fully visible and clean.",
        "Do not add floor shadow, cast shadow, glow, smoke, reflections, fog, background blur bands, or any dark contact shadow touching the feet or legs.",
        "Do not generate any gray patch, gray band, background leak, washed-out area, or background-colored artifact on the hair, forehead, face, ears, or neck.",
        "The head, hairline, forehead, and face must be clean, fully rendered, and completely free of background-colored contamination.",
        "Do not generate gray seams, gray smudges, washed-out patches, cracks, straps, or background-colored artifacts on the shoulders, armpits, chest, torso, or biceps.",
        "The shoulders, chest, torso, arms, and underarm transitions must be fully rendered and free of gray contamination or cutout residue.",
        "Keep the outline of the hair, shoulders, elbows, hands, legs, ankles, and feet crisp and completely separated from the background.",
        "Preserve realistic human proportions while keeping the collectible-figure material finish subtle and premium.",
    ]
    return "\n".join(instructions)


def _build_avatar_cutout_recovery_prompt(
    *,
    enforce_photo_identity: bool,
    appearance_traits: str = "",
) -> str:
    """Return a shorter fallback prompt for avatar cutout recovery retries.

    This prompt is intentionally compact so the worker can fall back to it when
    Gemini repeatedly responds without an image (`IMAGE_OTHER` / no image
    payload) for the richer builder prompt.
    """
    base_prompt = (
        "Create a vertical 9:16 full-body collectible figure avatar of the participant "
        "on a plain solid neutral background. Show the full body from head to toe, "
        "standing upright and centered, with both full feet clearly visible. Keep "
        "realistic human proportions and a subtle premium collectible-figure material "
        "finish. Use a clean plain studio background with no gradient, no floor "
        "shadow, no props, no accessories, no packaging, and no text."
    )
    return _prepare_generation_prompt(
        base_prompt,
        "",
        enforce_photo_identity=enforce_photo_identity,
        appearance_traits=appearance_traits,
    )


def _dedupe_prompt_sentences(text: str) -> str:
    """Remove repeated prompt sentences while preserving the original order."""
    parts = re.split(r"(?<=[.!?])\s+|\n+", str(text or "").strip())
    seen: set[str] = set()
    deduped: list[str] = []
    for part in parts:
        sentence = re.sub(r"\s+", " ", str(part or "")).strip()
        if not sentence:
            continue
        normalized = sentence.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(sentence)
    return " ".join(deduped)


def _asset_matches_white_box_reference(asset: dict[str, str]) -> bool:
    """Return whether a resolved catalog asset represents the white box background."""
    candidates = [
        _normalize_variable_key(str(asset.get("asset_key") or "")),
        _normalize_variable_key(str(asset.get("label") or "")),
    ]
    return any(
        token in candidate
        for candidate in candidates
        for token in (
            "paredebranca",
            "parede_branca",
            "fundobranco",
            "fundo_branco",
            "caixabranca",
            "caixa_branca",
            "whitebox",
            "white_box",
            "whitebackground",
            "white_background",
            "whitepaddedbox",
        )
    )


def _build_appearance_traits(gender: str, hair_color: str) -> str:
    """Render a compact English appearance clause from normalized traits."""
    gender_text = _translate_prompt_value_to_english(gender)
    hair_text = _translate_prompt_value_to_english(hair_color)
    traits: list[str] = []
    if gender_text:
        traits.append(str(gender_text))
    if hair_text:
        traits.append(f"{hair_text} hair")
    return ", ".join(traits)


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
    prompt_payload: dict[str, object] = {}

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
            "required": "true" if bool(row.get("required")) else "false",
        }
        if bool(row.get("required")):
            fixed_assets.append(asset)
            # Required catalog assets are always available to the prompt
            # through their asset key so the builder can explicitly reference
            # them, e.g. {{paredebranca}}.
            prompt_payload[asset_key] = label
            continue
        variable_key = _normalize_variable_key(str(row.get("variable_key") or ""))
        if not variable_key:
            continue
        selectable_assets.setdefault(variable_key, {})[asset_key] = asset

    selected_assets: list[dict[str, str]] = []
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

    fixed_asset_keys = {
        str(asset.get("asset_key") or "").strip()
        for asset in resolved_assets
        if str(asset.get("required") or "").strip().lower() == "true"
    }

    selected_labels: list[str] = []
    for key, value in payload.items():
        if str(key or "").strip() in fixed_asset_keys:
            continue
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
                if str(asset.get("required") or "").strip().lower() == "true"
                and str(asset.get("label") or "").strip()
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
    has_white_box_reference = any(
        _asset_matches_white_box_reference(asset) for asset in resolved_assets
    )

    instructions: list[str] = [
        "Zero readable text anywhere in the image: no words, letters, labels, captions, logos, signage, subtitles, or watermarks."
    ]
    if has_white_box_reference:
        instructions.extend(
            [
                "Use the provided white box/background reference as the exact structural container of the scene.",
                "Keep the white box unchanged and let it fill the entire frame from top to bottom.",
                "The final image must be vertical 9:16. Do not generate square or horizontal compositions.",
                "Keep the participant and all accessories fully inside the box boundaries.",
                "Do not crop the participant or any accessory at the frame edges.",
            ]
        )
    if fixed_labels:
        instructions.append(
            "Required fixed scene references: " + ", ".join(fixed_prompt_labels) + "."
        )
    if selected_labels:
        instructions.append(
            "Required visible selected elements: "
            + ", ".join(selected_prompt_labels)
            + "."
        )
        instructions.append(
            "Place each selected element as its own distinct accessory or object. Do not merge, omit, abstract, or hide any selected element."
        )
        instructions.append(
            "Keep every selected element fully visible inside the frame and away from the image borders."
        )
        if len(selected_prompt_labels) == 1:
            instructions.append(
                "Show the selected element clearly as a visible object."
            )
        elif len(selected_prompt_labels) <= 3:
            instructions.append(
                "Show every selected element clearly, with each one individually recognizable and separated in the composition."
            )
        elif len(selected_prompt_labels) <= 8:
            instructions.append(
                "Arrange the selected elements as a balanced collectible composition using smaller accessory slots so each one remains individually visible."
            )
        else:
            instructions.append(
                "Use a structured collectible display so the final composition still represents all selected elements."
            )
        instructions.append(
            "Use the provided reference images as the source of truth for object identity and appearance."
        )
    instructions.append(
        "The participant must remain the main subject, but all required assets must appear as explicit visual objects."
    )
    instructions.append(
        "If many assets are selected, reduce their size and distribute them intelligently, but do not remove them from the scene."
    )
    return "\n".join(instructions)


def _filter_generation_catalog_assets(
    assets: list[dict[str, str]] | None,
    prompt_payload: dict[str, object] | None,
) -> tuple[list[dict[str, str]], dict[str, object], list[str]]:
    """Keep only structural catalog assets for Gemini generation.

    Returns generation assets, generation prompt payload, and deferred variable keys.
    """
    resolved_assets = assets if isinstance(assets, list) else []
    resolved_payload = prompt_payload if isinstance(prompt_payload, dict) else {}

    generation_assets = [
        asset for asset in resolved_assets if _asset_matches_white_box_reference(asset)
    ]
    generation_asset_keys = {
        _normalize_variable_key(str(asset.get("asset_key") or ""))
        for asset in generation_assets
    }

    generation_payload: dict[str, object] = {}
    deferred_variable_keys: list[str] = []
    for key, value in resolved_payload.items():
        normalized_key = _normalize_variable_key(str(key or ""))
        if normalized_key in generation_asset_keys:
            generation_payload[key] = value
            continue
        deferred_variable_keys.append(normalized_key)

    deferred_variable_keys = list(
        dict.fromkeys([key for key in deferred_variable_keys if key])
    )
    return generation_assets, generation_payload, deferred_variable_keys


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


def _resolve_generation_prompt_template(
    data: dict | None,
    archetype: dict | None,
) -> tuple[str, str]:
    """Resolve the prompt template source used for generation.

    Args:
        data (dict | None): Credential data payload.
        archetype (dict | None): Archetype row associated with the generation.

    Returns:
        tuple[str, str]: Raw prompt template and its source identifier.
    """
    del data
    return str((archetype or {}).get("image_prompt") or ""), "builder"


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


def _upload_cutout(
    settings: Settings,
    experience_id: str,
    generation_id: str,
    data: bytes,
) -> str:
    """Upload the transparent avatar cutout as a PNG asset."""
    bucket = settings.supabase_bucket
    path = f"quiz/{experience_id}/cutouts/{generation_id}.png"
    url = f"{settings.supabase_url}/storage/v1/object/{bucket}/{path}"
    r = requests.post(
        url,
        headers={
            **rest_headers(settings),
            "x-upsert": "true",
            "Content-Type": "image/png",
        },
        data=data,
        timeout=30,
    )
    if not r.ok:
        raise RuntimeError(f"cutout_upload_failed:{r.status_code}:{r.text[:160]}")
    return path


def _color_distance(rgb_a: tuple[int, int, int], rgb_b: tuple[int, int, int]) -> float:
    """Return Euclidean distance between two RGB colors."""
    return (
        (rgb_a[0] - rgb_b[0]) ** 2
        + (rgb_a[1] - rgb_b[1]) ** 2
        + (rgb_a[2] - rgb_b[2]) ** 2
    ) ** 0.5


def _keep_largest_alpha_component(alpha_image, *, threshold: int = 48):
    """Keep only the largest connected alpha component in a mask image."""
    width, height = alpha_image.size
    pixels = alpha_image.load()
    visited = [[False for _ in range(width)] for _ in range(height)]
    largest_component: set[tuple[int, int]] = set()

    for y in range(height):
        for x in range(width):
            if visited[y][x] or pixels[x, y] < threshold:
                visited[y][x] = True
                continue
            queue = [(x, y)]
            component: list[tuple[int, int]] = []
            visited[y][x] = True
            head = 0
            while head < len(queue):
                current_x, current_y = queue[head]
                head += 1
                component.append((current_x, current_y))
                for next_x, next_y in (
                    (current_x - 1, current_y),
                    (current_x + 1, current_y),
                    (current_x, current_y - 1),
                    (current_x, current_y + 1),
                ):
                    if (
                        next_x < 0
                        or next_x >= width
                        or next_y < 0
                        or next_y >= height
                        or visited[next_y][next_x]
                    ):
                        continue
                    visited[next_y][next_x] = True
                    if pixels[next_x, next_y] >= threshold:
                        queue.append((next_x, next_y))
            if len(component) > len(largest_component):
                largest_component = set(component)

    if not largest_component:
        return alpha_image

    cleaned = alpha_image.copy()
    cleaned_pixels = cleaned.load()
    for y in range(height):
        for x in range(width):
            if (x, y) not in largest_component:
                cleaned_pixels[x, y] = 0
    return cleaned


def _validate_avatar_cutout_quality(cutout_bytes: bytes) -> tuple[bool, str]:
    """Validate whether the generated cutout keeps the lower body intact."""
    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("avatar_cutout_dependency_missing:Pillow") from exc

    image = Image.open(io.BytesIO(cutout_bytes)).convert("RGBA")
    width, height = image.size
    if width < 16 or height < 16:
        return False, "image_too_small"

    pixels = image.load()
    alpha = image.getchannel("A")
    alpha_pixels = alpha.load()
    row_widths: list[int] = []
    soft_row_widths: list[int] = []
    for y in range(height):
        visible = 0
        soft_visible = 0
        for x in range(width):
            pixel_alpha = alpha_pixels[x, y]
            if pixel_alpha >= 160:
                visible += 1
            if pixel_alpha >= 48:
                soft_visible += 1
        row_widths.append(visible)
        soft_row_widths.append(soft_visible)

    lower_band_start = int(height * 0.90)
    leg_band_start = int(height * 0.72)
    leg_band_end = int(height * 0.86)
    lower_rows = row_widths[lower_band_start:]
    leg_rows = row_widths[leg_band_start:leg_band_end]
    if not lower_rows or not leg_rows:
        return False, "missing_lower_band"

    lower_avg = sum(lower_rows) / len(lower_rows)
    leg_avg = sum(leg_rows) / len(leg_rows)
    if leg_avg <= 0:
        return False, "missing_legs"
    if lower_avg / leg_avg < 0.22:
        return False, "incomplete_feet"

    bbox = alpha.getbbox()
    if bbox:

        def largest_suspicious_component(
            start_x: int,
            end_x: int,
            start_y: int,
            end_y: int,
        ) -> int:
            if start_x >= end_x or start_y >= end_y:
                return 0
            visited: set[tuple[int, int]] = set()
            largest = 0
            for region_y in range(start_y, end_y):
                for region_x in range(start_x, end_x):
                    if (region_x, region_y) in visited:
                        continue
                    pixel_alpha = alpha_pixels[region_x, region_y]
                    if pixel_alpha < 180:
                        continue
                    if (
                        _color_distance(
                            tuple(pixels[region_x, region_y][:3]), background_rgb
                        )
                        > 52.0
                    ):
                        continue
                    queue: deque[tuple[int, int]] = deque([(region_x, region_y)])
                    visited.add((region_x, region_y))
                    component_size = 0
                    while queue:
                        current_x, current_y = queue.popleft()
                        component_size += 1
                        for step_x, step_y in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                            next_x = current_x + step_x
                            next_y = current_y + step_y
                            if (
                                next_x < start_x
                                or next_x >= end_x
                                or next_y < start_y
                                or next_y >= end_y
                                or (next_x, next_y) in visited
                            ):
                                continue
                            next_alpha = alpha_pixels[next_x, next_y]
                            if next_alpha < 180:
                                continue
                            if (
                                _color_distance(
                                    tuple(pixels[next_x, next_y][:3]),
                                    background_rgb,
                                )
                                > 52.0
                            ):
                                continue
                            visited.add((next_x, next_y))
                            queue.append((next_x, next_y))
                    largest = max(largest, component_size)
            return largest

        edge_samples: list[tuple[int, int, int]] = []
        for x in range(width):
            for y in (0, height - 1):
                if alpha_pixels[x, y] < 24:
                    edge_samples.append(tuple(pixels[x, y][:3]))
        for y in range(height):
            for x in (0, width - 1):
                if alpha_pixels[x, y] < 24:
                    edge_samples.append(tuple(pixels[x, y][:3]))
        if edge_samples:
            sample_count = len(edge_samples)
            background_rgb = tuple(
                int(sum(sample[channel] for sample in edge_samples) / sample_count)
                for channel in range(3)
            )
            left, upper, right, lower = bbox
            bbox_width = max(1, right - left)
            bbox_height = max(1, lower - upper)
            head_top = upper
            head_bottom = min(lower, upper + max(24, int(bbox_height * 0.24)))
            head_left = left + int(bbox_width * 0.14)
            head_right = right - int(bbox_width * 0.14)
            suspicious_pixels = 0
            for y in range(head_top, head_bottom):
                for x in range(head_left, head_right):
                    if alpha_pixels[x, y] < 180:
                        continue
                    if _color_distance(tuple(pixels[x, y][:3]), background_rgb) <= 42.0:
                        suspicious_pixels += 1
            head_region_area = max(
                1, (head_bottom - head_top) * max(1, head_right - head_left)
            )
            if suspicious_pixels / head_region_area > 0.012:
                return False, "head_background_artifact"
            head_component = largest_suspicious_component(
                head_left,
                head_right,
                head_top,
                head_bottom,
            )
            if head_component > max(10, int(head_region_area * 0.0025)):
                return False, "head_background_artifact"

            torso_top = upper + int(bbox_height * 0.22)
            torso_bottom = min(lower, upper + int(bbox_height * 0.58))
            torso_left = left + int(bbox_width * 0.12)
            torso_right = right - int(bbox_width * 0.12)
            torso_suspicious_pixels = 0
            for y in range(torso_top, torso_bottom):
                for x in range(torso_left, torso_right):
                    if alpha_pixels[x, y] < 180:
                        continue
                    if _color_distance(tuple(pixels[x, y][:3]), background_rgb) <= 42.0:
                        torso_suspicious_pixels += 1
            torso_region_area = max(
                1, (torso_bottom - torso_top) * max(1, torso_right - torso_left)
            )
            if torso_suspicious_pixels / torso_region_area > 0.008:
                return False, "torso_background_artifact"
            torso_component = largest_suspicious_component(
                torso_left,
                torso_right,
                torso_top,
                torso_bottom,
            )
            if torso_component > max(16, int(torso_region_area * 0.002)):
                return False, "torso_background_artifact"

    # Reject residual floor shadows/halos that remain visible below the feet.
    bottom_nonzero_rows = row_widths[int(height * 0.94) :]
    if bottom_nonzero_rows:
        max_bottom_width = max(bottom_nonzero_rows)
        if max_bottom_width > max(18, int(width * 0.36)):
            return False, "residual_floor_shadow"

    bottom_soft_rows = soft_row_widths[int(height * 0.94) :]
    if bottom_soft_rows:
        max_bottom_soft_width = max(bottom_soft_rows)
        if max_bottom_soft_width > max(26, int(width * 0.48)):
            return False, "residual_floor_shadow"
    return True, "ok"


def _decontaminate_rgba_pixel(
    rgba: tuple[int, int, int, int],
    background_rgb: tuple[int, int, int],
) -> tuple[int, int, int, int]:
    """Remove background color contamination from a semi-transparent edge pixel."""
    red, green, blue, alpha = rgba
    if alpha <= 0 or alpha >= 255:
        return rgba
    alpha_ratio = alpha / 255.0
    restored_channels: list[int] = []
    for channel_value, background_value in zip(
        (red, green, blue), background_rgb, strict=True
    ):
        restored = (channel_value - background_value * (1.0 - alpha_ratio)) / max(
            0.01, alpha_ratio
        )
        restored_channels.append(max(0, min(255, int(round(restored)))))
    return restored_channels[0], restored_channels[1], restored_channels[2], alpha


def _build_avatar_cutout_png(image_bytes: bytes) -> bytes:
    """Remove a neutral edge background and return a transparent PNG cutout."""
    try:
        from PIL import Image, ImageFilter
    except ImportError as exc:  # pragma: no cover - exercised only without dependency
        raise RuntimeError("avatar_cutout_dependency_missing:Pillow") from exc

    image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    width, height = image.size
    pixels = image.load()

    if width < 8 or height < 8:
        raise RuntimeError("avatar_cutout_image_too_small")

    step_x = max(1, width // 24)
    step_y = max(1, height // 24)
    edge_samples: list[tuple[int, int, int]] = []
    for x in range(0, width, step_x):
        edge_samples.append(tuple(pixels[x, 0][:3]))
        edge_samples.append(tuple(pixels[x, height - 1][:3]))
    for y in range(0, height, step_y):
        edge_samples.append(tuple(pixels[0, y][:3]))
        edge_samples.append(tuple(pixels[width - 1, y][:3]))

    sample_count = max(1, len(edge_samples))
    background_rgb = tuple(
        int(sum(sample[channel] for sample in edge_samples) / sample_count)
        for channel in range(3)
    )
    edge_distances = [
        _color_distance(sample, background_rgb) for sample in edge_samples
    ]
    avg_edge_distance = sum(edge_distances) / max(1, len(edge_distances))
    background_threshold = max(26.0, min(60.0, avg_edge_distance + 18.0))
    soft_threshold = background_threshold + 16.0

    background_mask = [[False for _ in range(width)] for _ in range(height)]
    queue: list[tuple[int, int]] = []
    for x in range(width):
        queue.append((x, 0))
        queue.append((x, height - 1))
    for y in range(height):
        queue.append((0, y))
        queue.append((width - 1, y))

    head = 0
    while head < len(queue):
        x, y = queue[head]
        head += 1
        if background_mask[y][x]:
            continue
        rgba = pixels[x, y]
        if rgba[3] <= 8:
            background_mask[y][x] = True
        else:
            distance = _color_distance(tuple(rgba[:3]), background_rgb)
            if distance > background_threshold:
                continue
            background_mask[y][x] = True
        if x > 0:
            queue.append((x - 1, y))
        if x + 1 < width:
            queue.append((x + 1, y))
        if y > 0:
            queue.append((x, y - 1))
        if y + 1 < height:
            queue.append((x, y + 1))

    result = image.copy()
    result_pixels = result.load()
    for y in range(height):
        for x in range(width):
            rgba = result_pixels[x, y]
            if background_mask[y][x]:
                result_pixels[x, y] = (rgba[0], rgba[1], rgba[2], 0)
                continue
            distance = _color_distance(tuple(rgba[:3]), background_rgb)
            if distance < soft_threshold:
                alpha_ratio = max(
                    0.0,
                    min(
                        1.0,
                        (distance - background_threshold)
                        / max(1.0, soft_threshold - background_threshold),
                    ),
                )
                softened_alpha = int(rgba[3] * alpha_ratio)
                result_pixels[x, y] = (rgba[0], rgba[1], rgba[2], softened_alpha)

    alpha = result.getchannel("A")
    alpha = alpha.filter(ImageFilter.MinFilter(3))
    alpha = alpha.point(
        lambda value: 0 if value < 22 else (255 if value > 244 else int(value))
    )
    alpha = _keep_largest_alpha_component(alpha, threshold=52)
    alpha = alpha.filter(ImageFilter.MaxFilter(3))
    alpha = alpha.point(
        lambda value: 0 if value < 18 else (255 if value > 245 else int(value))
    )
    result.putalpha(alpha)
    strong_alpha = alpha.point(lambda value: 255 if value >= 210 else 0)
    strong_bbox = strong_alpha.getbbox()
    if strong_bbox:
        alpha_pixels = alpha.load()
        result_pixels = result.load()
        shadow_distance_threshold = soft_threshold + 10.0
        strong_alpha_pixels = strong_alpha.load()
        column_bottoms: list[int] = []
        for x in range(width):
            bottom_y = -1
            for y in range(height - 1, -1, -1):
                if strong_alpha_pixels[x, y] >= 210:
                    bottom_y = y
                    break
            column_bottoms.append(bottom_y)

        for x, bottom_y in enumerate(column_bottoms):
            if bottom_y < 0:
                continue
            for y in range(bottom_y, height):
                current_alpha = alpha_pixels[x, y]
                rgba = result_pixels[x, y]
                distance = _color_distance(tuple(rgba[:3]), background_rgb)
                if y >= bottom_y + 2:
                    alpha_pixels[x, y] = 0
                    continue
                if y == bottom_y + 1 and (
                    current_alpha < 240 or distance <= shadow_distance_threshold + 14.0
                ):
                    alpha_pixels[x, y] = 0
                    continue
                if y == bottom_y and (
                    current_alpha < 252 and distance <= shadow_distance_threshold + 8.0
                ):
                    alpha_pixels[x, y] = 0
                    continue
                if y > bottom_y + 1 and distance <= shadow_distance_threshold:
                    alpha_pixels[x, y] = 0
                    continue
                if current_alpha >= 180 and distance > shadow_distance_threshold:
                    continue
                if distance <= shadow_distance_threshold and current_alpha <= 245:
                    alpha_pixels[x, y] = 0
        result.putalpha(alpha)
    result_pixels = result.load()
    for y in range(height):
        for x in range(width):
            rgba = result_pixels[x, y]
            if 0 < rgba[3] < 255:
                result_pixels[x, y] = _decontaminate_rgba_pixel(
                    rgba,
                    background_rgb,
                )
    alpha = result.getchannel("A")
    bbox = alpha.getbbox()
    if bbox:
        padding = max(8, int(max(width, height) * 0.02))
        left = max(0, bbox[0] - padding)
        upper = max(0, bbox[1] - padding)
        right = min(width, bbox[2] + padding)
        lower = min(height, bbox[3] + padding)
        result = result.crop((left, upper, right, lower))

    output = io.BytesIO()
    result.save(output, format="PNG")
    return output.getvalue()


def _finish_job_done(
    settings: Settings,
    job: Job,
    duration_ms: int,
    output_path: str,
    *,
    cutout_path: str | None = None,
):
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
    if cutout_path:
        body_with_cost["cutout_path"] = cutout_path
        body_with_cost["cutout_url"] = None
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
    if cutout_path:
        body_legacy["cutout_path"] = cutout_path
        body_legacy["cutout_url"] = None
    r_legacy = requests.patch(
        url,
        headers={**rest_headers(settings), "Content-Type": "application/json"},
        json=body_legacy,
        timeout=20,
    )
    if r_legacy.ok or not cutout_path:
        return
    body_output_only = {
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
        json=body_output_only,
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
        experience_config = _load_experience_config(settings, job.experience_id)
        avatar_cutout_mode = _avatar_cutout_enabled(experience_config)
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
        if avatar_cutout_mode:
            generation_catalog_assets = []
            generation_catalog_prompt_payload = {}
            deferred_catalog_variable_keys = list(
                dict.fromkeys(
                    [
                        _normalize_variable_key(str(key or ""))
                        for key in catalog_prompt_payload.keys()
                        if _normalize_variable_key(str(key or ""))
                    ]
                )
            )
        else:
            (
                generation_catalog_assets,
                generation_catalog_prompt_payload,
                deferred_catalog_variable_keys,
            ) = _filter_generation_catalog_assets(
                catalog_assets,
                catalog_prompt_payload,
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
                "avatar_cutout_mode": avatar_cutout_mode,
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
        generation_catalog_labels = list(
            dict.fromkeys(
                [
                    str(asset.get("label") or "").strip()
                    for asset in generation_catalog_assets
                    if str(asset.get("label") or "").strip()
                ]
            )
        )
        deferred_catalog_labels = list(
            dict.fromkeys(
                [
                    label
                    for label in selected_catalog_labels
                    if label not in generation_catalog_labels
                ]
            )
        )
        prompt_template_payload = _build_prompt_template_payload(
            {
                **cred_data,
                **generation_catalog_prompt_payload,
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
        raw_prompt_template, prompt_source = _resolve_generation_prompt_template(
            cred_data,
            archetype,
        )
        generation_prompt_template = _strip_template_lines_with_keys(
            raw_prompt_template,
            deferred_catalog_variable_keys,
        )
        rendered_prompt = _render_prompt_template(
            generation_prompt_template, prompt_template_payload
        )
        catalog_asset_appendix = (
            _build_avatar_cutout_prompt_appendix()
            if avatar_cutout_mode
            else _build_catalog_asset_prompt_appendix(
                generation_catalog_assets,
                generation_catalog_prompt_payload,
            )
        )
        prompt_image_assets = _select_prompt_image_assets(
            generation_prompt_template,
            _extract_prompt_image_assets(cred_data),
        )
        archetype_prompt = _prepare_generation_prompt(
            rendered_prompt,
            catalog_asset_appendix,
            enforce_photo_identity=bool(photo_path),
        )
        if not archetype_prompt:
            prompt_source = "fixed_default"

        # Preferred mode: Gemini generation. With photo when available; prompt-only when archetype allows it.
        effective_gemini_key = _resolve_experience_gemini_key(
            settings, job.experience_id
        )
        if not effective_gemini_key:
            raise RuntimeError("missing_experience_gemini_key")
        use_photo_prompt = bool((archetype or {}).get("use_photo_prompt"))
        has_prompt_image_assets = bool(prompt_image_assets)
        has_catalog_prompt_assets = bool(generation_catalog_assets)
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
        cutout_path = ""
        if effective_gemini_key and (
            photo_path
            or has_prompt_image_assets
            or has_catalog_prompt_assets
            or can_prompt_only
        ):
            gemini_settings = replace(settings, gemini_api_key=effective_gemini_key)
            gemini = GeminiImageClient(gemini_settings)
            max_attempts = _gemini_max_attempts()
            if avatar_cutout_mode:
                max_attempts = max(max_attempts, _avatar_cutout_max_attempts())
            retry_base_delay = _gemini_retry_base_delay_seconds()
            ref_bytes = b""
            ref_b64 = ""
            ref_mime = "image/jpeg"
            inline_images: list[dict[str, str]] = []
            appearance_traits = _build_appearance_traits(gender, hair_color)
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
            for asset in generation_catalog_assets:
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
            if (
                photo_path
                and (has_prompt_image_assets or has_catalog_prompt_assets)
                and ref_b64
            ):
                inline_images.append(
                    {
                        "data": ref_b64,
                        "mime_type": ref_mime,
                    }
                )
            prompt_applied = _prepare_generation_prompt(
                rendered_prompt,
                catalog_asset_appendix,
                enforce_photo_identity=bool(photo_path),
                appearance_traits=appearance_traits,
            ) or build_editorial_prompt(gender, hair_color)
            recovery_prompt_applied = (
                _build_avatar_cutout_recovery_prompt(
                    enforce_photo_identity=bool(photo_path),
                    appearance_traits=appearance_traits,
                )
                if avatar_cutout_mode
                else ""
            )
            if photo_path and (has_prompt_image_assets or has_catalog_prompt_assets):
                generation_mode = "reference_photo_plus_prompt_assets"
            elif photo_path:
                generation_mode = "reference_photo"
            elif has_prompt_image_assets or has_catalog_prompt_assets:
                generation_mode = "prompt_assets_only"
            else:
                generation_mode = "prompt_only"
            if avatar_cutout_mode:
                generation_mode = f"{generation_mode}_avatar_cutout"

            generated_bytes = b""
            generated_mime = "image/png"
            cutout_bytes = b""
            model_name = None
            latency_ms = None
            last_err = None
            generation_succeeded = False
            no_image_retry_count = 0
            using_recovery_prompt = False
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
                    "generation_catalog_assets": generation_catalog_labels,
                    "deferred_catalog_assets": deferred_catalog_labels,
                    "avatar_cutout_mode": avatar_cutout_mode,
                    "max_attempts": max_attempts,
                    "inline_image_count": len(inline_images),
                    "identity_reference_image_count": (
                        2
                        if photo_path
                        and (has_prompt_image_assets or has_catalog_prompt_assets)
                        else (1 if photo_path else 0)
                    ),
                    "prompt_source": prompt_source,
                    "prompt_preview": (prompt_applied or "")[:600],
                },
            )

            for attempt in range(1, max_attempts + 1):
                try:
                    candidate_generated_bytes = b""
                    candidate_generated_mime = "image/png"
                    candidate_cutout_bytes = b""
                    candidate_model_name = None
                    candidate_latency_ms = None
                    attempt_prompt = prompt_applied
                    if (
                        avatar_cutout_mode
                        and no_image_retry_count > 0
                        and recovery_prompt_applied
                    ):
                        attempt_prompt = recovery_prompt_applied
                        if not using_recovery_prompt:
                            using_recovery_prompt = True
                            _write_generation_log(
                                settings,
                                job.id,
                                level="info",
                                event="gemini_prompt_recovery_activated",
                                message="Switched avatar cutout generation to the recovery prompt after no-image response",
                                payload={
                                    "attempt": attempt,
                                    "max_attempts": max_attempts,
                                    "no_image_retry_count": no_image_retry_count,
                                },
                            )
                    if inline_images:
                        t_gem = time.time()
                        raw = gemini.generate_from_images_b64(
                            prompt=attempt_prompt,
                            images=inline_images,
                        )
                        candidate_latency_ms = int((time.time() - t_gem) * 1000)
                        candidate_generated_bytes = raw.get("image_bytes") or b""
                        candidate_generated_mime = str(
                            raw.get("mime_type") or "image/png"
                        )
                        candidate_model_name = raw.get("model")
                    else:
                        t_gem = time.time()
                        raw = gemini.generate_from_prompt(attempt_prompt)
                        candidate_latency_ms = int((time.time() - t_gem) * 1000)
                        candidate_generated_bytes = raw.get("image_bytes") or b""
                        candidate_generated_mime = str(
                            raw.get("mime_type") or "image/png"
                        )
                        candidate_model_name = raw.get("model")

                    if not candidate_generated_bytes:
                        raise RuntimeError("gemini_empty_image")
                    if avatar_cutout_mode:
                        candidate_cutout_bytes = _build_avatar_cutout_png(
                            candidate_generated_bytes
                        )
                        is_valid_cutout, cutout_reason = (
                            _validate_avatar_cutout_quality(candidate_cutout_bytes)
                        )
                        if not is_valid_cutout:
                            raise RuntimeError(
                                f"avatar_cutout_quality_failed:{cutout_reason}"
                            )
                    generated_bytes = candidate_generated_bytes
                    generated_mime = candidate_generated_mime
                    cutout_bytes = candidate_cutout_bytes
                    model_name = candidate_model_name
                    latency_ms = candidate_latency_ms
                    generation_succeeded = True

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
                    generated_bytes = b""
                    cutout_bytes = b""
                    err_str = str(exc)
                    if "gemini_no_image_in_response" in err_str.lower():
                        no_image_retry_count += 1
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

            if not generation_succeeded:
                last_err_str = str(last_err or "gemini_generation_failed")
                if not _is_retryable_gemini_error_message(last_err_str):
                    raise (
                        last_err if last_err is not None else RuntimeError(last_err_str)
                    )

                last_err_str = str(last_err)
                _write_generation_log(
                    settings,
                    job.id,
                    level="error",
                    event="gemini_generation_failed_after_retries",
                    message="Gemini failed after retries; no fallback output will be used",
                    payload={
                        "error": last_err_str[:2000],
                        "max_attempts": max_attempts,
                        "generation_mode": generation_mode,
                        "has_photo_path": bool(photo_path),
                        "inline_image_count": len(inline_images),
                    },
                )
                raise RuntimeError(last_err_str)

            out_path = _upload_output(
                settings,
                job.experience_id,
                job.id,
                generated_bytes,
                mime_type=generated_mime,
            )
            if avatar_cutout_mode:
                try:
                    cutout_path = _upload_cutout(
                        settings,
                        job.experience_id,
                        job.id,
                        cutout_bytes,
                    )
                    _write_generation_log(
                        settings,
                        job.id,
                        level="info",
                        event="cutout_generated",
                        message="Avatar cutout generated and uploaded",
                        payload={
                            "cutout_path": cutout_path,
                            "cutout_bytes": len(cutout_bytes),
                        },
                    )
                except Exception as exc:
                    _write_generation_log(
                        settings,
                        job.id,
                        level="warning",
                        event="cutout_generation_failed",
                        message="Avatar cutout generation failed; keeping original output",
                        payload={"error": str(exc)[:1000]},
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
                    "avatar_cutout_mode": avatar_cutout_mode,
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
                    "avatar_cutout_mode": avatar_cutout_mode,
                    "output_path": out_path,
                },
            )
        _write_generation_log(
            settings,
            job.id,
            level="info",
            event="output_uploaded",
            message="Output uploaded to storage",
            payload={"output_path": out_path, "cutout_path": cutout_path or None},
        )
        dur = int((time.time() - t0) * 1000)
        _finish_job_done(
            settings,
            job,
            dur,
            out_path,
            cutout_path=cutout_path or None,
        )
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

    logging.basicConfig(
        level=getattr(
            logging, os.getenv("QUIZ_WORKER_LOG_LEVEL", "INFO").upper(), logging.INFO
        ),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        force=True,
    )

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
            logger.warning(
                "[WORKER] network_fetch_pending_error attempt=%s sleep_s=%.1f err=%s",
                net_error_count,
                sleep_s,
                exc,
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

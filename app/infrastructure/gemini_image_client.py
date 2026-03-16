"""HTTP client for multimodal Gemini image generation requests."""

from __future__ import annotations

import base64
import json

import requests

from app.core.settings import Settings


class GeminiImageClient:
    """Submit text and image prompts to Gemini and decode image responses.

    The client encapsulates request payload construction, HTTP transport, and response
    parsing for Gemini image models. It raises runtime errors when the provider response
    cannot be used by the worker.

    Attributes:
        _settings (Settings): Runtime settings that provide the model and API key.
        _model (str): Gemini image model name used for generation.
        _api_key (str): API key sent to the Gemini endpoint.
        _url (str): Full provider endpoint including the selected model and API key.
        _session (requests.Session): Reused HTTP session for outgoing Gemini calls.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        if not self._settings.gemini_api_key:
            raise RuntimeError("missing_GEMINI_API_KEY")
        self._model = (
            self._settings.gemini_image_model or "gemini-2.5-flash-image"
        ).strip()
        self._api_key = (self._settings.gemini_api_key or "").strip()
        self._url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{self._model}:generateContent?key={self._api_key}"
        )
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json"})

    def _request_generation(self, payload: dict) -> dict:
        """Send a generation payload to Gemini and extract the first image result.

        Args:
            payload (dict): Gemini `generateContent` request body.

        Returns:
            dict: Decoded provider payload with MIME type, image bytes, and usage data.

        Raises:
            RuntimeError: Raised when Gemini rejects the request or returns no image.
        """
        response = self._session.post(self._url, json=payload, timeout=90)
        if not response.ok:
            raise RuntimeError(
                f"gemini_http_{response.status_code}:{response.text[:400]}"
            )
        data = response.json() or {}

        image_part = None
        for candidate in data.get("candidates", []) or []:
            content = candidate.get("content") or {}
            for part in content.get("parts", []) or []:
                inline = part.get("inlineData") or part.get("inline_data")
                if inline and inline.get("data"):
                    image_part = inline
                    break
            if image_part:
                break

        if not image_part:
            raise RuntimeError(
                "gemini_no_image_in_response:"
                + self._summarize_missing_image_response(data)
            )

        return {
            "model": self._model,
            "mime_type": image_part.get("mimeType")
            or image_part.get("mime_type")
            or "image/png",
            "image_bytes": base64.b64decode(image_part.get("data") or ""),
            "usage_metadata": data.get("usageMetadata") or data.get("usage_metadata"),
        }

    def _summarize_missing_image_response(self, data: dict) -> str:
        """Build a compact diagnostic summary when Gemini returns no image part."""
        candidates = data.get("candidates", []) or []
        prompt_feedback = data.get("promptFeedback") or data.get("prompt_feedback") or {}
        finish_reasons: list[str] = []
        text_parts: list[str] = []
        for candidate in candidates[:3]:
            finish_reason = str(
                candidate.get("finishReason") or candidate.get("finish_reason") or ""
            ).strip()
            if finish_reason:
                finish_reasons.append(finish_reason)
            content = candidate.get("content") or {}
            for part in content.get("parts", []) or []:
                text = str(part.get("text") or "").strip()
                if text:
                    text_parts.append(text[:200])
        summary = {
            "candidate_count": len(candidates),
            "finish_reasons": finish_reasons,
            "prompt_block_reason": str(
                prompt_feedback.get("blockReason")
                or prompt_feedback.get("block_reason")
                or ""
            ).strip(),
            "text_parts": text_parts[:3],
            "usage_metadata": data.get("usageMetadata") or data.get("usage_metadata"),
        }
        return json.dumps(summary, ensure_ascii=True, sort_keys=True)[:1200]

    def generate_from_images_b64(
        self,
        prompt: str,
        images: list[dict[str, str]],
    ) -> dict:
        """Generate an image from a prompt plus multiple base64-encoded references.

        Args:
            prompt (str): Final prompt text sent to Gemini.
            images (list[dict[str, str]]): Reference images with `data` and `mime_type`.

        Returns:
            dict: Parsed generation result containing the generated image bytes.
        """
        parts = [{"text": prompt}]
        for image in images:
            data = str(image.get("data") or "").strip()
            if not data:
                continue
            parts.append(
                {
                    "inline_data": {
                        "mime_type": str(image.get("mime_type") or "image/jpeg"),
                        "data": data,
                    }
                }
            )
        payload = {
            "contents": [{"parts": parts}],
            "generation_config": {"response_modalities": ["IMAGE"]},
        }
        return self._request_generation(payload)

    def generate_from_reference_b64(
        self,
        prompt: str,
        image_b64: str,
        mime_type: str,
    ) -> dict:
        """Generate an image from a prompt and a single reference image.

        Args:
            prompt (str): Prompt text sent to Gemini.
            image_b64 (str): Base64-encoded reference image.
            mime_type (str): MIME type associated with the reference image.

        Returns:
            dict: Parsed generation result containing the generated image bytes.
        """
        return self.generate_from_images_b64(
            prompt=prompt,
            images=[
                {
                    "data": image_b64,
                    "mime_type": mime_type or "image/jpeg",
                }
            ],
        )

    def generate_from_prompt(self, prompt: str) -> dict:
        """Generate an image from text-only instructions.

        Args:
            prompt (str): Prompt text sent to Gemini.

        Returns:
            dict: Parsed generation result containing the generated image bytes.
        """
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generation_config": {"response_modalities": ["IMAGE"]},
        }
        return self._request_generation(payload)

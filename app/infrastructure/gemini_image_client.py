from __future__ import annotations

import base64

import requests

from app.core.settings import Settings


class GeminiImageClient:
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
            raise RuntimeError("gemini_no_image_in_response")

        return {
            "model": self._model,
            "mime_type": image_part.get("mimeType")
            or image_part.get("mime_type")
            or "image/png",
            "image_bytes": base64.b64decode(image_part.get("data") or ""),
            "usage_metadata": data.get("usageMetadata") or data.get("usage_metadata"),
        }

    def generate_from_images_b64(
        self,
        prompt: str,
        images: list[dict[str, str]],
    ) -> dict:
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
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generation_config": {"response_modalities": ["IMAGE"]},
        }
        return self._request_generation(payload)

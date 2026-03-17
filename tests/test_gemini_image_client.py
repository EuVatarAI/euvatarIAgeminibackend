from __future__ import annotations

import unittest

from app.core.config import Settings
from app.infrastructure.gemini_image_client import GeminiImageClient


class GeminiImageClientTests(unittest.TestCase):
    def test_generation_config_includes_default_vertical_aspect_ratio(self) -> None:
        client = GeminiImageClient(Settings(GEMINI_API_KEY="test-key"))

        self.assertEqual(
            client._build_generation_config(),
            {
                "response_modalities": ["IMAGE"],
                "image_config": {"aspect_ratio": "9:16"},
            },
        )

    def test_generation_config_uses_configured_aspect_ratio(self) -> None:
        client = GeminiImageClient(
            Settings(
                GEMINI_API_KEY="test-key",
                GEMINI_IMAGE_ASPECT_RATIO="1:1",
            )
        )

        self.assertEqual(
            client._build_generation_config(),
            {
                "response_modalities": ["IMAGE"],
                "image_config": {"aspect_ratio": "1:1"},
            },
        )


if __name__ == "__main__":
    unittest.main()

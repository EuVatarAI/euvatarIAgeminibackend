import unittest

from app.core.settings import Settings


class SettingsLoadTests(unittest.TestCase):
    def test_load_exposes_gemini_image_aspect_ratio(self) -> None:
        settings = Settings.load()

        self.assertTrue(hasattr(settings, "gemini_image_aspect_ratio"))
        self.assertEqual(settings.gemini_image_aspect_ratio, "9:16")

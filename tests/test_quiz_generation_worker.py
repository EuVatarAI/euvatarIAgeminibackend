"""Unit tests covering Gemini retry and fallback retryability helpers."""

from __future__ import annotations

import unittest

from scripts import quiz_generation_worker as worker


class QuizGenerationWorkerRetryTests(unittest.TestCase):
    """Verify retryability helpers used by the generation worker."""

    def test_no_image_response_is_retryable(self) -> None:
        """Treat missing Gemini image payloads as retryable failures."""
        self.assertTrue(
            worker._is_retryable_gemini_error_message("gemini_no_image_in_response")
        )

    def test_builder_prompt_is_always_used_for_generation(self) -> None:
        """Keep the generation prompt sourced from the configured builder prompt."""
        raw_prompt, prompt_source = worker._resolve_generation_prompt_template(
            {"_user_prompt_template": "ignorado"},
            {"image_prompt": "prompt do construtor"},
        )
        self.assertEqual(raw_prompt, "prompt do construtor")
        self.assertEqual(prompt_source, "builder")

    def test_photo_identity_clause_is_added_when_requested(self) -> None:
        """Force the generated image to preserve the participant identity."""
        prompt = worker._prepare_generation_prompt(
            "prompt base",
            "",
            enforce_photo_identity=True,
        )
        self.assertIn("sole source of facial identity", prompt)
        self.assertIn("Do not invent a generic person", prompt)

    def test_builder_prompt_is_used_without_dynamic_override(self) -> None:
        """Use the configured builder prompt even without extra credential fields."""
        raw_prompt, prompt_source = worker._resolve_generation_prompt_template(
            {},
            {"image_prompt": "prompt do arquétipo"},
        )
        self.assertEqual(raw_prompt, "prompt do arquétipo")
        self.assertEqual(prompt_source, "builder")

    def test_retryable_error_retries_before_last_attempt(self) -> None:
        """Retry retryable Gemini failures before the final allowed attempt."""
        self.assertTrue(
            worker._should_retry_gemini_error_message(
                "gemini_no_image_in_response",
                attempt=1,
                max_attempts=3,
            )
        )

    def test_retryable_error_does_not_retry_after_last_attempt(self) -> None:
        """Stop retrying once the final allowed attempt has been reached."""
        self.assertFalse(
            worker._should_retry_gemini_error_message(
                "gemini_no_image_in_response",
                attempt=3,
                max_attempts=3,
            )
        )
        self.assertTrue(
            worker._is_retryable_gemini_error_message("gemini_no_image_in_response")
        )


if __name__ == "__main__":
    unittest.main()

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

    def test_user_prompt_template_overrides_archetype_prompt(self) -> None:
        """Use the user's dynamic prompt when it is present on the credential."""
        raw_prompt, prompt_source = worker._resolve_generation_prompt_template(
            {"_user_prompt_template": "Use {{foto_do_neymar}} aqui"},
            {"image_prompt": "fallback"},
        )
        self.assertEqual(raw_prompt, "Use {{foto_do_neymar}} aqui")
        self.assertEqual(prompt_source, "user")

    def test_archetype_prompt_is_used_when_user_prompt_is_missing(self) -> None:
        """Fallback to the archetype prompt when there is no dynamic user prompt."""
        raw_prompt, prompt_source = worker._resolve_generation_prompt_template(
            {},
            {"image_prompt": "prompt do arquétipo"},
        )
        self.assertEqual(raw_prompt, "prompt do arquétipo")
        self.assertEqual(prompt_source, "archetype")

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

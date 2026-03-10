from __future__ import annotations

import unittest

from scripts import quiz_generation_worker as worker


class QuizGenerationWorkerRetryTests(unittest.TestCase):
    def test_no_image_response_is_retryable(self) -> None:
        self.assertTrue(
            worker._is_retryable_gemini_error_message("gemini_no_image_in_response")
        )

    def test_retryable_error_retries_before_last_attempt(self) -> None:
        self.assertTrue(
            worker._should_retry_gemini_error_message(
                "gemini_no_image_in_response",
                attempt=1,
                max_attempts=3,
            )
        )

    def test_retryable_error_does_not_retry_after_last_attempt(self) -> None:
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

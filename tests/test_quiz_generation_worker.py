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
        self.assertTrue(
            worker._is_retryable_gemini_error_message(
                'gemini_no_image_in_response:{"candidate_count": 1}'
            )
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
        self.assertIn("Do not add glasses", prompt)
        self.assertIn("photorealistic and human", prompt)
        self.assertIn("Do not create a caricature", prompt)
        self.assertIn("Preserve the real facial proportions", prompt)
        self.assertIn("Keep the head size", prompt)
        self.assertIn("same smile shape and intensity", prompt)

    def test_appearance_traits_are_injected_into_prompt(self) -> None:
        """Include requested appearance traits without inferring from the photo."""
        prompt = worker._prepare_generation_prompt(
            "prompt base",
            "",
            enforce_photo_identity=True,
            appearance_traits="woman, red hair",
        )
        self.assertIn("Requested appearance traits for the figure", prompt)
        self.assertIn("red hair", prompt)

    def test_white_box_asset_forces_vertical_structural_rules(self) -> None:
        """Reinforce 9:16 white-box composition when the white box asset is present."""
        appendix = worker._build_catalog_asset_prompt_appendix(
            [
                {
                    "asset_key": "paredebranca",
                    "label": "Parede branca",
                    "required": "true",
                    "storage_path": "x.png",
                }
            ],
            {"paredebranca": "Parede branca"},
        )
        self.assertIn("exact structural container", appendix)
        self.assertIn("vertical 9:16", appendix)
        self.assertIn("fill the entire frame", appendix)
        self.assertIn("Do not crop the participant", appendix)

    def test_generation_inputs_resolve_aliases_for_gender_and_hair_color(self) -> None:
        """Accept experience variable aliases when extracting canonical traits."""
        gender, hair_color = worker._extract_generation_inputs(
            {
                "data_json": {
                    "genero": "feminino",
                    "cor_do_cabelo": "ruivo",
                }
            }
        )
        self.assertEqual(gender, "mulher")
        self.assertEqual(hair_color, "ruivo")

    def test_prepare_generation_prompt_dedupes_repeated_sentences(self) -> None:
        """Collapse repeated prompt sentences to reduce conflicting redundancy."""
        prompt = worker._prepare_generation_prompt(
            "Keep the face. Keep the face.\nDo not crop. Do not crop.",
            "",
            enforce_photo_identity=False,
        )
        self.assertEqual(prompt.count("Keep the face."), 1)
        self.assertEqual(prompt.count("Do not crop."), 1)

    def test_generation_catalog_assets_keep_only_white_box_reference(self) -> None:
        """Only structural white-box assets should be sent to Gemini generation."""
        assets, payload, deferred_keys = worker._filter_generation_catalog_assets(
            [
                {
                    "asset_key": "paredebranca",
                    "label": "Parede branca",
                    "required": "false",
                    "storage_path": "white.png",
                },
                {
                    "asset_key": "gasometro",
                    "label": "Gasometro",
                    "required": "false",
                    "storage_path": "gasometro.png",
                },
            ],
            {
                "paredebranca": "Parede branca",
                "o_que_voce_mais_ama_em_porto_alegre": ["Gasometro"],
            },
        )
        self.assertEqual(len(assets), 1)
        self.assertEqual(str(assets[0].get("asset_key")), "paredebranca")
        self.assertEqual(payload, {"paredebranca": "Parede branca"})
        self.assertEqual(
            deferred_keys,
            ["o_que_voce_mais_ama_em_porto_alegre"],
        )

    def test_template_lines_with_deferred_asset_keys_are_removed(self) -> None:
        """Drop asset-specific template lines that should no longer reach Gemini."""
        template = (
            "Keep the face faithful.\n"
            "Use {{o_que_voce_mais_ama_em_porto_alegre}} as accessories.\n"
            "Show full body.\n"
        )
        stripped = worker._strip_template_lines_with_keys(
            template,
            ["o_que_voce_mais_ama_em_porto_alegre"],
        )
        self.assertIn("Keep the face faithful.", stripped)
        self.assertIn("Show full body.", stripped)
        self.assertNotIn("accessories", stripped)

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

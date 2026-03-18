"""Unit tests covering Gemini retry and fallback retryability helpers."""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import patch

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
        self.assertTrue(
            worker._is_retryable_gemini_error_message(
                "avatar_cutout_quality_failed:incomplete_feet"
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

    def test_avatar_cutout_flag_requires_enabled_builder_background_mode(self) -> None:
        """Enable cutout mode only for the explicit builder-fixed background config."""
        self.assertTrue(
            worker._avatar_cutout_enabled(
                {
                    "avatar_generation": {
                        "enabled": True,
                        "background_mode": "builder_fixed_png",
                    }
                }
            )
        )
        self.assertFalse(
            worker._avatar_cutout_enabled(
                {
                    "avatar_generation": {
                        "enabled": True,
                        "background_mode": "generated_scene",
                    }
                }
            )
        )

    def test_avatar_cutout_max_attempts_uses_stricter_default(self) -> None:
        """Use a higher retry budget for avatar cutout mode in production."""
        original = os.environ.get("QUIZ_GEMINI_AVATAR_CUTOUT_MAX_ATTEMPTS")
        try:
            os.environ.pop("QUIZ_GEMINI_AVATAR_CUTOUT_MAX_ATTEMPTS", None)
            self.assertEqual(worker._avatar_cutout_max_attempts(), 7)
        finally:
            if original is None:
                os.environ.pop("QUIZ_GEMINI_AVATAR_CUTOUT_MAX_ATTEMPTS", None)
            else:
                os.environ["QUIZ_GEMINI_AVATAR_CUTOUT_MAX_ATTEMPTS"] = original

    def test_avatar_cutout_prompt_appendix_forbids_scene_elements(self) -> None:
        """Avatar cutout mode should request only the isolated figure on neutral background."""
        appendix = worker._build_avatar_cutout_prompt_appendix()
        self.assertIn("plain solid neutral background", appendix)
        self.assertIn("Do not generate any packaging", appendix)
        self.assertIn("full body from head to toe", appendix)
        self.assertIn("vertical 9:16", appendix)
        self.assertIn("Do not add floor shadow", appendix)
        self.assertIn("Generate both full feet completely", appendix)
        self.assertIn("gray patch", appendix)
        self.assertIn("gray seams", appendix)

    def test_avatar_cutout_recovery_prompt_is_short_and_canonical(self) -> None:
        """Fallback prompt should stay compact for no-image Gemini recoveries."""
        prompt = worker._build_avatar_cutout_recovery_prompt(
            enforce_photo_identity=True,
            appearance_traits="woman, blonde hair",
        )
        self.assertIn("vertical 9:16 full-body collectible figure avatar", prompt)
        self.assertIn("plain solid neutral background", prompt)
        self.assertIn("both full feet clearly visible", prompt)
        self.assertIn("no floor shadow", prompt)
        self.assertIn("no props, no accessories, no packaging, and no text", prompt)
        self.assertIn("woman, blonde hair", prompt)

    def test_finish_job_done_persists_cutout_path_when_supported(self) -> None:
        """Persist cutout metadata when the database accepts the new columns."""
        settings = SimpleNamespace(
            supabase_url="https://example.supabase.co",
            supabase_service_role="service-role",
        )
        job = worker.Job(
            id="gen-1",
            experience_id="exp-1",
            credential_id="cred-1",
            kind="quiz_result",
        )

        with patch("scripts.quiz_generation_worker.requests.patch") as patch_request:
            patch_request.return_value = SimpleNamespace(ok=True)

            worker._finish_job_done(
                settings,  # type: ignore[arg-type]
                job,
                1234,
                "quiz/exp-1/generations/gen-1.png",
                cutout_path="quiz/exp-1/cutouts/gen-1.png",
            )

        self.assertEqual(patch_request.call_count, 1)
        self.assertEqual(
            patch_request.call_args.kwargs["json"]["cutout_path"],
            "quiz/exp-1/cutouts/gen-1.png",
        )

    def test_keep_largest_alpha_component_removes_small_residue(self) -> None:
        """Keep the main silhouette while dropping tiny detached alpha islands."""
        from PIL import Image

        alpha = Image.new("L", (6, 6), 0)
        pixels = alpha.load()
        for y in range(1, 5):
            for x in range(1, 3):
                pixels[x, y] = 255
        pixels[5, 5] = 255

        cleaned = worker._keep_largest_alpha_component(alpha, threshold=52)
        cleaned_pixels = cleaned.load()

        self.assertEqual(cleaned_pixels[5, 5], 0)
        self.assertEqual(cleaned_pixels[1, 1], 255)

    def test_validate_avatar_cutout_quality_detects_incomplete_feet(self) -> None:
        """Reject cutouts whose lower band collapses too much versus the leg band."""
        from PIL import Image

        image = Image.new("RGBA", (100, 200), (0, 0, 0, 0))
        pixels = image.load()
        for y in range(20, 160):
            for x in range(30, 70):
                pixels[x, y] = (255, 255, 255, 255)
        for y in range(160, 190):
            for x in range(44, 56):
                pixels[x, y] = (255, 255, 255, 255)

        import io

        output = io.BytesIO()
        image.save(output, format="PNG")
        is_valid, reason = worker._validate_avatar_cutout_quality(output.getvalue())

        self.assertFalse(is_valid)
        self.assertEqual(reason, "incomplete_feet")

    def test_validate_avatar_cutout_quality_detects_residual_floor_shadow(self) -> None:
        """Reject cutouts that still keep a wide visible band below the feet."""
        from PIL import Image

        image = Image.new("RGBA", (100, 200), (0, 0, 0, 0))
        pixels = image.load()
        for y in range(20, 188):
            for x in range(34, 66):
                pixels[x, y] = (255, 255, 255, 255)
        for y in range(188, 192):
            for x in range(30, 70):
                pixels[x, y] = (255, 255, 255, 255)
        for y in range(192, 197):
            for x in range(18, 82):
                pixels[x, y] = (220, 220, 220, 96)

        import io

        output = io.BytesIO()
        image.save(output, format="PNG")
        is_valid, reason = worker._validate_avatar_cutout_quality(output.getvalue())

        self.assertFalse(is_valid)
        self.assertEqual(reason, "residual_floor_shadow")

    def test_validate_avatar_cutout_quality_detects_head_background_artifact(
        self,
    ) -> None:
        """Reject cutouts with a background-colored opaque patch inside the head area."""
        from PIL import Image

        image = Image.new("RGBA", (100, 200), (232, 232, 232, 0))
        pixels = image.load()
        for y in range(20, 188):
            for x in range(30, 70):
                pixels[x, y] = (255, 220, 190, 255)
        for y in range(188, 192):
            for x in range(34, 66):
                pixels[x, y] = (255, 220, 190, 255)
        for y in range(22, 42):
            for x in range(38, 55):
                pixels[x, y] = (232, 232, 232, 255)

        import io

        output = io.BytesIO()
        image.save(output, format="PNG")
        is_valid, reason = worker._validate_avatar_cutout_quality(output.getvalue())

        self.assertFalse(is_valid)
        self.assertEqual(reason, "head_background_artifact")

    def test_validate_avatar_cutout_quality_detects_torso_background_artifact(
        self,
    ) -> None:
        """Reject cutouts with a background-colored opaque patch inside the torso area."""
        from PIL import Image

        image = Image.new("RGBA", (120, 220), (232, 232, 232, 0))
        pixels = image.load()
        for y in range(20, 208):
            for x in range(34, 86):
                pixels[x, y] = (210, 150, 96, 255)
        for y in range(208, 212):
            for x in range(40, 80):
                pixels[x, y] = (210, 150, 96, 255)
        for y in range(78, 118):
            for x in range(42, 82):
                pixels[x, y] = (232, 232, 232, 255)

        import io

        output = io.BytesIO()
        image.save(output, format="PNG")
        is_valid, reason = worker._validate_avatar_cutout_quality(output.getvalue())

        self.assertFalse(is_valid)
        self.assertEqual(reason, "torso_background_artifact")

    def test_decontaminate_rgba_pixel_removes_background_tint(self) -> None:
        """Recover a semi-transparent edge pixel from the sampled background tint."""
        restored = worker._decontaminate_rgba_pixel(
            (180, 180, 180, 128),
            (240, 240, 240),
        )

        self.assertEqual(restored[3], 128)
        self.assertLess(restored[0], 180)
        self.assertLess(restored[1], 180)
        self.assertLess(restored[2], 180)


if __name__ == "__main__":
    unittest.main()

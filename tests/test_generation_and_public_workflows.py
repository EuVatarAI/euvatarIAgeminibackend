from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from app.core.generations.engine.workflow import GenerationsWorkflow
from app.core.public_experiences.engine.workflow import PublicExperiencesWorkflow


class GenerationsWorkflowStatusTests(unittest.TestCase):
    def test_status_returns_signed_cutout_url_when_path_exists(self) -> None:
        workflow = GenerationsWorkflow(settings=object())  # type: ignore[arg-type]

        with patch(
            "app.core.generations.engine.workflow.get_json",
            return_value=[
                {
                    "id": "gen-1",
                    "status": "done",
                    "output_path": "quiz/output.png",
                    "output_url": None,
                    "cutout_path": "quiz/cutout.png",
                    "cutout_url": None,
                    "final_card_path": None,
                    "final_card_url": None,
                    "error_message": None,
                    "duration_ms": 1234,
                }
            ],
        ):
            with patch.object(
                workflow,
                "_build_signed_download_url",
                side_effect=lambda path: f"https://signed/{path}",
            ):
                result = asyncio.run(workflow.get_generation_status("gen-1"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["output_url"], "https://signed/quiz/output.png")
        self.assertEqual(result["cutout_url"], "https://signed/quiz/cutout.png")


class PublicExperiencesWorkflowLeadConfigTests(unittest.TestCase):
    def test_lead_config_exposes_avatar_generation_flag(self) -> None:
        workflow = PublicExperiencesWorkflow(settings=object())  # type: ignore[arg-type]

        with patch.object(
            workflow,
            "_load_active_experience_by_slug",
            return_value={
                "id": "exp-1",
                "type": "quiz_result",
                "status": "published",
                "config_json": {
                    "avatar_generation": {
                        "enabled": True,
                        "background_mode": "builder_fixed_png",
                    }
                },
            },
        ):
            with patch.object(workflow, "_require_experience_id", return_value="exp-1"):
                with patch.object(
                    workflow, "_load_experience_variables", return_value=[]
                ):
                    with patch.object(
                        workflow, "_load_experience_prompt_assets", return_value=[]
                    ):
                        with patch.object(
                            workflow,
                            "_group_selectable_assets_by_variable",
                            return_value={},
                        ):
                            result = asyncio.run(workflow.get_lead_config("slug-teste"))

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["lead_capture"]["avatar_generation"],
            {
                "enabled": True,
                "background_mode": "builder_fixed_png",
            },
        )


if __name__ == "__main__":
    unittest.main()

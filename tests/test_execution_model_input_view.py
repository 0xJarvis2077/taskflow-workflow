"""Model projections retain machine originals; byte comparisons are offline only."""

import copy
from pathlib import Path
import unittest

from taskflow import execution_runner as runner
from tests import test_execution_quality_initial_listing as quality_fixture
from tests import test_execution_role_session as session_fixture


class ModelInputViewTests(unittest.TestCase):
    @staticmethod
    def resumed_legacy_request(root: Path, config: dict, manifest: dict) -> bytes:
        """Reconstruct the old direct-input rendering, rather than estimate tokens."""
        request = ["Role: " + manifest["role"] + ". Intent: " + manifest["intent_id"] + "."]
        request += ["Input (" + entry["kind"] + "): " + str(root / "inputs" / entry["target"])
                    for entry in manifest["inputs"]]
        request += [entry["text"] for entry in config["request_sections"]]
        return ("\n\n".join(request) + "\n").encode("utf-8")

    @staticmethod
    def quality_legacy_initial(current: bytes) -> bytes:
        """Restore exactly the old deferred-reading paragraph in an actual render."""
        return current.decode("utf-8").replace(
            "只有完成保存和实际SHA读回后，才通过真实工具读取 ../runtime/inputs/execution/quality-instructions.md "
            "及 ../runtime/inputs/execution/role-inputs.md；它们提供完整后续操作和原件位置。"
            "检查点此后保持原字节；不要重复该阶段或改写检查点来追认顺序。"
            "按后续操作从 ../runtime/inputs/ 读取必要完整材料、对照并完成原报告与签发要求，",
            "只有完成保存和实际SHA读回后，才通过真实工具完整读取 ../runtime/quality-later-inputs.v1.json；"
            "它是锁定完整输入和后续操作的声明。检查点此后保持原字节；原公开先行前缀作为已完成步骤保留，"
            "不要重复该阶段或改写检查点来追认顺序。逐段读完其中 request_sections/request_text，"
            "并按 inputs 中的 target 从 ../runtime/inputs/ 读取必要材料、对照并完成原报告与签发要求，"
        ).encode("utf-8")

    def quality_case(self):
        case = quality_fixture.QualityInitialListingTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def session_case(self):
        case = session_fixture.RoleSessionTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def test_fresh_qa_reads_model_projection_while_full_machine_declaration_is_unchanged(self):
        case = self.quality_case()
        _, root, manifest = case.prepare()
        request = (root / "request.md").read_bytes()
        locations = (root / runner.MODEL_LOCATIONS).read_bytes()
        instructions = (root / runner.MODEL_QUALITY_INSTRUCTIONS).read_bytes()
        declaration = runner._read(root / "quality-later-inputs.v1.json")
        self.assertEqual(manifest["inputs"], declaration["inputs"])
        self.assertEqual(runner._model_quality_instructions(declaration), instructions)
        self.assertNotIn(b"quality-later-inputs.v1.json", request)
        self.assertNotIn(b"PRIVATE_", request)
        for item in manifest["inputs"]:
            self.assertIn((item["kind"] + "\t" + item["target"]).encode(), locations)
            self.assertNotIn(item["source"].encode(), locations)
            self.assertNotIn(item["sha256"].encode(), locations)
            self.assertEqual(runner._info(root / "inputs" / item["target"]),
                             {key: item[key] for key in ("sha256", "size")})
        before = self.quality_legacy_initial(request) + (root / "quality-later-inputs.v1.json").read_bytes()
        after = request + instructions + locations
        self.assertLess(len(after), len(before))
        self.assertIn(b"PRIVATE_PHASE2_FOCUS", instructions)
        self.assertEqual(manifest, runner._validate(root))
        files = runner._collection_files(root)
        self.assertIn(runner.MODEL_LOCATIONS, files)
        self.assertIn(runner.MODEL_QUALITY_INSTRUCTIONS, files)
        self.assertIn("quality-later-inputs.v1.json", files)

    def test_resume_request_uses_one_locator_and_preserves_every_input_and_archive(self):
        case = self.session_case()
        source, config = case.config()
        config["inputs"] = [{"path": str(case.public), "target": "units/unit-" + str(i) + ".md", "kind": "public"}
                            for i in range(40)]
        runner.prepare_attempt(source, config)
        self.assertTrue(runner.run_attempt(source)["technical_accepted"])
        root, following = case.config("resume_existing", session_fixture.reference(source / "collection.json"))
        following["inputs"] = copy.deepcopy(config["inputs"])
        following["request_sections"] = [{"kind": "operation", "text": "Only correct the current deterministic delivery error."}]
        manifest = runner.prepare_attempt(root, following)
        request = (root / "request.md").read_bytes()
        before = self.resumed_legacy_request(root, following, manifest)
        self.assertLess(len(request), len(before))
        self.assertNotIn(b"Input (public):", request)
        self.assertIn(str(root / runner.MODEL_LOCATIONS).encode(), request)
        self.assertEqual(40, len(manifest["inputs"]))
        self.assertIn(b"Only correct the current deterministic delivery error.", request)
        for item in manifest["inputs"]:
            self.assertEqual(case.public.read_bytes(), (root / "inputs" / item["target"]).read_bytes())
        self.assertTrue(runner.run_attempt(root)["technical_accepted"])
        destination = case.base / "complete-resume-archive"
        runner.archive_full_attempt(root, destination)
        self.assertEqual((root / "manifest.json").read_bytes(), (destination / "manifest.json").read_bytes())
        self.assertEqual((root / runner.MODEL_LOCATIONS).read_bytes(), (destination / runner.MODEL_LOCATIONS).read_bytes())
        self.assertIn(runner.MODEL_LOCATIONS, runner._read(root / "collection.json")["evidence_files"])

    def test_model_projection_tamper_or_business_input_collision_cannot_rewrite_originals(self):
        case = self.quality_case()
        _, root, manifest = case.prepare()
        declaration = (root / "quality-later-inputs.v1.json").read_bytes()
        view = root / runner.MODEL_QUALITY_INSTRUCTIONS
        view.chmod(0o644)
        view.write_text("Different model instructions", encoding="utf-8")
        with self.assertRaises(runner.RunnerError):
            runner._validate(root)
        with self.assertRaises(runner.RunnerError):
            runner._quality_locked_request(root, manifest)
        self.assertEqual(declaration, (root / "quality-later-inputs.v1.json").read_bytes())
        other = self.session_case()
        source, config = other.config()
        config["inputs"][0]["target"] = "execution/role-inputs.md"
        runner.prepare_attempt(source, config)
        self.assertTrue(runner.run_attempt(source)["technical_accepted"])
        following, config2 = other.config("resume_existing", session_fixture.reference(source / "collection.json"))
        config2["inputs"] = copy.deepcopy(config["inputs"])
        with self.assertRaisesRegex(runner.RunnerError, "cannot replace an input"):
            runner.prepare_attempt(following, config2)
        self.assertEqual(other.public.read_bytes(), (following / "inputs/execution/role-inputs.md").read_bytes())
        self.assertFalse((following / "dispatch.json").exists())


if __name__ == "__main__":
    unittest.main()

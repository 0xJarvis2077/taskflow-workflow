"""Complete initial rendering hides informed inputs while preserving locked bytes."""

import copy
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution_runner as runner
from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_flow import advance_workflow
from taskflow.execution_flow_contracts import request_has_native_signoff_protocol
from tests import test_execution_flow as fixture
from tests import test_execution_formal as formal_fixture


LISTING = {"scope": "whole_package", "index": "execution/public-inputs.json",
           "checkpoint": "public-responsibility-first.json"}


class QualityInitialListingTests(unittest.TestCase):
    def setUp(self):
        self.case = fixture.ContinuationTests()
        self.case.setUp()
        self.addCleanup(self.case.doCleanups)
        c = self.case
        self.definition = c.definition("neutral", outcome="accepted")
        self.definition["roles"]["quality"]["runner"]["public_first_initial_listing"] = LISTING
        self.definition["roles"]["quality"]["runner"]["request_sections"] = [
            {"kind": "operation", "text": "PRIVATE_PHASE2_FOCUS bounded_store/profile_limits/resource_envelope"}]
        self.extra = []
        for index, kind in enumerate(("source_metadata", "history", "submission", "trace")):
            path = c.root / ("PRIVATE_ORIGINAL_SOURCE_" + str(index) + ".txt")
            path.write_text("PRIVATE_CONTENT_" + str(index))
            target = "execution/PRIVATE_MEMBER_" + str(index) + ".txt"
            self.extra.append({"path": str(path), "kind": kind, "target": target})
        self.definition["roles"]["quality"]["runner"]["inputs"] = self.extra[:2]
        self.definition["roles"]["quality"]["predecessor_inputs"] = self.extra[2:]

    def prepare(self):
        c = self.case
        record = c.register(self.definition)
        record = advance_workflow(c.store, record["flow_id"])
        record = advance_workflow(c.store, record["flow_id"])
        self.assertEqual("active", record["state"]["status"], record["state"].get("exception"))
        root = Path(record["state"]["current_runtime"])
        return record, root, runner._validate(root)

    def test_entire_rendered_initial_is_public_only_and_all_original_inputs_remain_locked(self):
        record, root, manifest = self.prepare()
        text = (root / "request.md").read_text()
        for forbidden in ("PRIVATE_", "bounded_store", "profile_limits", "resource_envelope",
                          "execution/current.json", "execution/task.json", "source_metadata", "Input (teacher)",
                          str(self.case.root), record["flow_id"], record["state"]["current_work_id"],
                          "revised_candidate_id", "predecessor_archive", "public_archive"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, text)
        self.assertIn("PUBLIC_RESPONSIBILITY_FIRST_SHA256", text)
        self.assertIn("../runtime/inputs/execution/quality-instructions.md", text)
        self.assertIn("../runtime/inputs/execution/role-inputs.md", text)
        self.assertNotIn("quality-later-inputs.v1.json", text)
        self.assertNotIn("Intent:", text)
        candidate = self.case.store.get_candidate(self.definition["initial"]["candidate_id"])
        self.assertNotIn(candidate["content_digest"], text)
        self.assertNotIn(candidate["evaluation_digest"], text)
        for row in candidate["files"]:
            if row["role"] in {"prompt", "input", "template"}:
                self.assertIn("Public input: ../runtime/inputs/" + row["package_path"], text)
        declaration = read_json(root / "quality-later-inputs.v1.json")
        self.assertEqual(manifest["inputs"], declaration["inputs"])
        for entry in manifest["inputs"]:
            copied = root / "inputs" / entry["target"]
            self.assertEqual({key: entry[key] for key in ("sha256", "size")}, runner._info(copied))
        for extra in self.extra:
            self.assertIn(extra["target"], {entry["target"] for entry in manifest["inputs"]})
            self.assertEqual(Path(extra["path"]).read_bytes(), (root / "inputs" / extra["target"]).read_bytes())
        self.assertFalse(request_has_native_signoff_protocol(text.encode()))
        full = runner._quality_locked_request(root, manifest)
        self.assertTrue(request_has_native_signoff_protocol(full))
        self.assertIn(b"PRIVATE_PHASE2_FOCUS", full)
        self.assertIn("quality-later-inputs.v1.json", runner._collection_files(root))

    def test_default_request_remains_unchanged_and_existing_prepared_request_cannot_be_rewritten(self):
        _, root, manifest = self.prepare()
        original = {p.name: p.read_bytes() for p in (root / "manifest.json", root / "request.md", root / "quality-later-inputs.v1.json")}
        spec = read_json(root.parent / "spec.json")
        spec["runner"]["request_sections"].append({"kind": "operation", "text": "Changed later focus"})
        from taskflow.execution import enqueue_spec

        with self.assertRaises(TaskflowError):
            enqueue_spec(self.case.store, spec, root, resume_prepared=True)
        for name, raw in original.items():
            self.assertEqual(raw, (root / name).read_bytes())
        self.assertIn(b"PRIVATE_PHASE2_FOCUS", runner._quality_locked_request(root, manifest))

    def test_deferred_declaration_is_a_real_locked_dependency(self):
        _, root, manifest = self.prepare()
        path = root / "quality-later-inputs.v1.json"
        path.write_text(path.read_text() + " ")
        with self.assertRaises(runner.RunnerError):
            runner._validate(root)
        with self.assertRaises(runner.RunnerError):
            runner._quality_locked_request(root, manifest)

    def test_affected_and_serial_requests_cannot_enable_this_presentation(self):
        c = self.case
        self.definition["roles"]["quality"]["scope"] = "affected"
        record = c.register(self.definition)
        advance_workflow(c.store, record["flow_id"])
        failed = advance_workflow(c.store, record["flow_id"])
        self.assertEqual("exception", failed["state"]["status"])
        self.assertIn("fresh ordinary whole-quality", failed["state"]["exception"]["reason"])
        for change in ({"role": "repair"}, {"quality_session": {"index": 0}}, {"resume_sid": "old-sid"},
                       {"public_first_initial_listing": True}):
            config = {"role": "quality", "mode": "test_fixture", "candidate_binding": {},
                      "public_first_initial_listing": LISTING, **change}
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner._quality_initial_listing(config)

    def test_index_cannot_advertise_source_metadata_or_an_unlocked_member(self):
        _, root, manifest = self.prepare()
        index = root / "inputs/execution/public-inputs.json"
        original = read_json(index)
        index.chmod(0o644)
        config = {"cwd": str(root.parent / "work"), "candidate_binding": manifest["candidate_binding"], "outputs": manifest["outputs"]}
        for change in ({"predecessor": "PRIVATE_SOURCE"}, {"input_digest": "f"*64}):
            write_json(index, {**original, **change})
            with self.subTest(change=change), self.assertRaises(runner.RunnerError):
                runner._render_quality_initial(root, config, manifest["inputs"], LISTING)
        wrong = copy.deepcopy(original)
        wrong["files"][0]["sha256"] = "f"*64
        write_json(index, wrong)
        with self.assertRaisesRegex(runner.RunnerError, "complete locked public"):
            runner._render_quality_initial(root, config, manifest["inputs"], LISTING)

    def test_declaration_membership_and_neutral_directory_contract_cannot_be_substituted(self):
        _, root, manifest = self.prepare()
        config = {"cwd": str(root.parent / "other-work"), "candidate_binding": manifest["candidate_binding"]}
        with self.assertRaisesRegex(runner.RunnerError, "sibling runtime/work"):
            runner._render_quality_initial(root, config, manifest["inputs"], LISTING)
        declaration = root / "quality-later-inputs.v1.json"
        body = read_json(declaration)
        body["inputs"] = []
        write_json(declaration, body)
        updated = {**manifest, "quality_later_declaration": {"file": declaration.name, **runner._info(declaration)}}
        with self.assertRaisesRegex(runner.RunnerError, "differs from its original manifest"):
            runner._quality_locked_request(root, updated)
        with self.assertRaisesRegex(runner.RunnerError, "one locked public index"):
            runner._render_quality_initial(root, {"cwd": str(root.parent / "work")}, [], LISTING)

    def test_existing_native_signature_consumer_reads_exact_protocol_from_locked_declaration(self):
        source = formal_fixture.NativeQualityHandoffTests()
        source.setUp()
        self.addCleanup(source.doCleanups)
        root = source.material
        manifest = read_json(root / "manifest.json")
        original_request = (root / "request.md").read_text()
        (root / "request.md").write_text("Role: quality. Neutral public-first initial presentation.\n")
        declaration = {"schema_version": 1, "listing": LISTING, "inputs": manifest["inputs"],
                       "request_sections": [{"kind": "operation", "text": original_request}],
                       "request_text": None, "outputs": manifest["outputs"], "output_trees": []}
        write_json(root / "quality-later-inputs.v1.json", declaration)
        manifest.update(public_first_initial_listing=LISTING, output_trees=[], request=runner._info(root / "request.md"),
                        quality_later_declaration={"file": "quality-later-inputs.v1.json", **runner._info(root / "quality-later-inputs.v1.json")})
        manifest["dependencies"][str(root / "quality-later-inputs.v1.json")] = runner._info(root / "quality-later-inputs.v1.json")
        write_json(root / "manifest.json", manifest)
        write_json(root / "manifest-lock.json", runner._info(root / "manifest.json"))
        (root / "collection.json").unlink(); (root / "collection-lock.json").unlink()
        source.seal(root)
        from tests import test_execution_normative_certification as normative_fixture
        from taskflow.execution_formal import validate_normative_certification

        normative = normative_fixture.NormativeCertificationTests()
        normative.complete_source(source, root)
        cert = normative.certification(source)
        self.assertEqual(cert["native_quality_handoff"]["normative_sources"],
                         validate_normative_certification(candidate=source.candidate, certification=cert))
        self.assertEqual(cert["native_quality_handoff"]["normative_sources"],
                         validate_normative_certification(candidate=source.candidate, certification=cert, store=source.store))
        self.assertEqual(["draft/evaluation/answer.md", "draft/evaluation/rubric.md"], source.handoff()["scoring_source_allowlist"])
        changed = read_json(root / "quality-later-inputs.v1.json")
        changed["request_sections"] = []
        write_json(root / "quality-later-inputs.v1.json", changed)
        with self.assertRaises(TaskflowError):
            source.handoff()

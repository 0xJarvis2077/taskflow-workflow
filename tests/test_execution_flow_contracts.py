"""Routing consumes a bound role signature, never a free-form passed claim."""

import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from taskflow.contracts import TaskflowError
from taskflow.execution_flow_contracts import (
    NATIVE_SIGNOFF_PROTOCOL_SHA256, NATIVE_SIGNOFF_PROTOCOL_TEXT,
    parse_review, repair_instructions, review_instructions, validate_repair,
    render_native_signoff_protocol, request_has_native_signoff_protocol,
)


def digest(value):
    return hashlib.sha256(value).hexdigest()


class FlowContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.contents = {
            "draft/input/prompt.md": ("prompt", "Choose and justify a route."),
            "draft/evaluation/answer.md": ("answer", "One feasible route."),
            "draft/evaluation/rubric.md": ("rubric", "Accept supported alternatives."),
        }
        self.candidate = self.make_candidate("original", self.contents)
        self.intent = "review-intent"
        self.binding = {key: self.candidate[key] for key in (
            "candidate_id", "content_digest", "input_digest", "evaluation_digest")}
        self.binding["intent_id"] = self.intent
        self.report = {"execution_routing": {
            "schema_version": 1, **self.binding,
            "signer_role": "independent_quality",
            "independent_of_current_author": True,
            "participated_in_production": False, "scope": "whole_package",
            "outcome": "accepted", "blocking_issue_ids": [],
            "covered_defect_ids": [], "reason": "Current authorized scope examined.",
        }}
        self.issues = {"execution_binding": dict(self.binding), "issues": []}
        self.report_path = self.root / "adoption-readiness.json"
        self.issues_path = self.root / "issues.json"
        self.repair_path = self.root / "repair-record.json"

    def make_candidate(self, identity, contents, **extra):
        rows = []
        for relative, (role, text) in contents.items():
            path = self.root / identity / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
            rows.append({"package_path": relative, "path": str(path), "role": role,
                         "sha256": digest(text.encode())})
        encoded = json.dumps(rows, sort_keys=True).encode()
        return {"task_id": "neutral", "candidate_id": identity,
                "content_digest": digest(encoded),
                "input_digest": digest(b"public" + encoded),
                "evaluation_digest": digest(b"private" + encoded),
                "files": rows, **extra}

    def write(self, path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def parse(self, *, report=None, issues=None, candidate=None, scope="whole_package",
              metadata_aliases=None, observed_aliases=None):
        self.write(self.report_path, self.report if report is None else report)
        self.write(self.issues_path, self.issues if issues is None else issues)
        return parse_review(self.report_path, self.issues_path,
                            candidate=self.candidate if candidate is None else candidate,
                            intent_id=self.intent, scope=scope,
                            metadata_aliases=metadata_aliases,
                            observed_aliases=observed_aliases)

    def declaration_alias(self):
        path = self.root / "original" / "task.json"
        path.write_text('{"id":"neutral","files":[]}', encoding="utf-8")
        ref = {"path": str(path), "sha256": digest(path.read_bytes())}
        self.candidate["evidence"] = [{**ref, "location": "explicit package declaration"}]
        return {"execution/task.json": ref}

    def observation_aliases(self):
        result = {}
        for key, kind, body in (
            ("actual-public-work/deliverables/check.json", "submission", '{"observed":true}'),
            ("actual-public-scratch/check.py", "submission", "print('observation')"),
            ("actual-public-trace.jsonl", "trace", '{"type":"turn.completed"}\n'),
        ):
            path = self.root / "public-archive" / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
            result[key] = {"path": str(path), "sha256": digest(path.read_bytes()), "kind": kind}
        return result

    def observation(self, aliases):
        issue = self.issue("claim")
        issue["blocking"] = False
        issue["affected_paths"] = list(aliases)
        issue["evidence"] += [{"path": key, "sha256": row["sha256"],
                                "location": "actual observed output"}
                               for key, row in aliases.items()]
        self.issues["issues"] = [issue]
        return issue

    def issue(self, kind="material", path="draft/input/prompt.md", identity="defect-a"):
        item = next(row for row in self.candidate["files"] if row["package_path"] == path)
        return {"id": identity, "kind": kind,
                "responsibility": "A required decision lacks adequate evidence.",
                "affected_paths": [path],
                "evidence": [{"path": path, "sha256": item["sha256"],
                              "location": "line 1"}]}

    def revision(self, issue=None):
        issue = self.issue() if issue is None else issue
        self.report["execution_routing"].update(
            outcome="revision_required", blocking_issue_ids=[issue["id"]])
        self.issues["issues"] = [issue]

    def repair_fixture(self):
        contents = dict(self.contents)
        contents["draft/input/prompt.md"] = ("prompt", "A revised, complete request.")
        del contents["draft/evaluation/answer.md"]
        contents["draft/evaluation/new.md"] = ("teacher", "New support evidence.")
        child = self.make_candidate("revised", contents, parent_id="original")
        before = {x["package_path"]: x for x in self.candidate["files"]}
        after = {x["package_path"]: x for x in child["files"]}
        record = {"parent_id": "original", "candidate_id": "revised",
                  "defect_ids": ["defect-a"], "needs_business_decision": False,
                  "changes": [{"path": key,
                               "before_sha256": before[key]["sha256"] if key in before else None,
                               "after_sha256": after[key]["sha256"] if key in after else None}
                              for key in sorted(set(before) | set(after))
                              if before.get(key, {}).get("sha256") != after.get(key, {}).get("sha256")]}
        return child, record

    def repair(self, child, record, **kwargs):
        self.write(self.repair_path, record)
        return validate_repair(self.repair_path, parent=self.candidate,
                               candidate=child, defect_ids=["defect-a"], **kwargs)

    def test_bound_accepted_review_has_no_automatic_defects(self):
        value = self.parse()
        self.assertEqual("accepted", value["outcome"])
        self.assertTrue(value["whole_package"])
        self.assertEqual([], value["defects"])

    def test_material_evidence_becomes_exact_current_absolute_ref(self):
        self.revision()
        value = self.parse()
        self.assertEqual("revision_required", value["outcome"])
        self.assertEqual(["draft/input/prompt.md"], value["defects"][0]["affected_paths"])
        ref = value["defects"][0]["evidence"][0]
        self.assertEqual(self.candidate["files"][0]["path"], ref["path"])
        self.assertEqual(self.candidate["files"][0]["sha256"], ref["sha256"])
        self.assertEqual("line 1", ref["location"])

    def test_private_reference_rubric_or_claim_can_have_private_basis(self):
        for kind in ("reference", "rubric", "claim"):
            with self.subTest(kind=kind):
                self.revision(self.issue(kind, "draft/evaluation/answer.md"))
                self.assertEqual(kind, self.parse()["defects"][0]["kind"])

    def test_affected_acceptance_preserves_covered_ids_and_scope(self):
        self.report["execution_routing"].update(scope="affected", covered_defect_ids=["old-a"])
        value = self.parse(scope="affected")
        self.assertFalse(value["whole_package"])
        self.assertEqual(["old-a"], value["covered_defect_ids"])

    def test_explicit_inconclusive_is_not_accepted(self):
        self.report["execution_routing"]["outcome"] = "inconclusive"
        self.report["candidate_business_quality"] = {"status": "inconclusive"}
        self.assertEqual("inconclusive", self.parse()["outcome"])

    def test_nonblocking_issue_is_validated_but_not_auto_repair(self):
        self.issues["issues"] = [self.issue("claim")]
        self.assertEqual([], self.parse()["defects"])
        self.issues["issues"][0]["evidence"][0]["sha256"] = "0" * 64
        with self.assertRaises(TaskflowError):
            self.parse()

    def test_explicit_metadata_alias_validates_nonblocking_and_normalizes_refs(self):
        aliases = self.declaration_alias()
        issue = self.issue("reference", "draft/evaluation/answer.md")
        issue["evidence"].append({"path": "execution/task.json",
                                  "sha256": aliases["execution/task.json"]["sha256"],
                                  "location": "files declaration"})
        self.issues["issues"] = [issue]
        with self.assertRaises(TaskflowError):
            self.parse()
        self.assertEqual([], self.parse(metadata_aliases=aliases)["defects"])
        self.revision(issue)
        result = self.parse(metadata_aliases=aliases)
        self.assertEqual(aliases["execution/task.json"], {
            key: result["defects"][0]["evidence"][1][key] for key in ("path", "sha256")})

    def test_metadata_alias_rejects_wrong_sha_foreign_source_and_undeclared_alias(self):
        aliases = self.declaration_alias()
        issue = self.issue("reference", "draft/evaluation/answer.md")
        issue["evidence"].append({"path": "execution/task.json",
                                  "sha256": aliases["execution/task.json"]["sha256"],
                                  "location": "files declaration"})
        self.issues["issues"] = [issue]
        outside = self.root / "outside" / "task.json"
        outside.parent.mkdir()
        outside.write_bytes(Path(aliases["execution/task.json"]["path"]).read_bytes())
        bad_aliases = [
            {"execution/task.json": {**aliases["execution/task.json"], "sha256": "0" * 64}},
            {"execution/task.json": {**aliases["execution/task.json"], "path": str(outside)}},
            {"task.json": aliases["execution/task.json"]},
            {"other.json": aliases["execution/task.json"]},
        ]
        for invalid in bad_aliases:
            with self.subTest(aliases=invalid), self.assertRaises(TaskflowError):
                self.parse(metadata_aliases=invalid)
        issue["evidence"][1]["sha256"] = "0" * 64
        with self.assertRaises(TaskflowError):
            self.parse(metadata_aliases=aliases)
        issue["evidence"][1]["sha256"] = aliases["execution/task.json"]["sha256"]
        Path(aliases["execution/task.json"]["path"]).write_text("changed declaration")
        with self.assertRaises(TaskflowError):
            self.parse(metadata_aliases=aliases)

    def test_metadata_requires_captured_declaration_and_current_candidate_root(self):
        aliases = self.declaration_alias()
        declaration = self.candidate["evidence"][0]
        for evidence in ([], [{**declaration, "location": "external completed author trace"}],
                         [declaration, declaration]):
            with self.subTest(evidence=evidence), self.assertRaises(TaskflowError):
                self.parse(candidate={**self.candidate, "evidence": evidence},
                           metadata_aliases=aliases)
        outside = self.root / "foreign" / "task.json"
        outside.parent.mkdir()
        outside.write_bytes(Path(aliases["execution/task.json"]["path"]).read_bytes())
        foreign = {**declaration, "path": str(outside)}
        with self.assertRaises(TaskflowError):
            self.parse(candidate={**self.candidate, "evidence": [foreign]},
                       metadata_aliases={"execution/task.json": foreign})

    def test_metadata_is_never_public_basis_or_an_affected_candidate_path(self):
        aliases = self.declaration_alias()
        aliases["execution/task.json"]["role"] = "prompt"
        issue = self.issue("material", "draft/evaluation/answer.md")
        issue["evidence"] = [{"path": "execution/task.json",
                              "sha256": aliases["execution/task.json"]["sha256"],
                              "location": "files declaration"}]
        self.revision(issue)
        with self.assertRaisesRegex(TaskflowError, "public basis"):
            self.parse(metadata_aliases=aliases)
        issue["kind"] = "reference"
        issue["affected_paths"] = ["execution/task.json"]
        with self.assertRaisesRegex(TaskflowError, "affected candidate path"):
            self.parse(metadata_aliases=aliases)

    def test_explicit_observed_aliases_accept_nonblocking_claim_without_repair(self):
        aliases = self.observation_aliases()
        self.observation(aliases)
        with self.assertRaises(TaskflowError):
            self.parse()
        value = self.parse(observed_aliases=aliases)
        self.assertEqual("accepted", value["outcome"])
        self.assertEqual([], value["defects"])
        self.assertEqual([], value["covered_defect_ids"])

    def test_observed_aliases_reject_ineligible_blocking_claims_and_targets(self):
        aliases = self.observation_aliases()
        issue = self.observation(aliases)
        self.revision(issue)
        with self.assertRaisesRegex(TaskflowError, "not repair targets"):
            self.parse(observed_aliases=aliases)
        issue["affected_paths"] = ["draft/input/prompt.md"]
        with self.assertRaisesRegex(TaskflowError, "Observed evidence requires a nonblocking observation"):
            self.parse(observed_aliases=aliases)
        self.report["execution_routing"].update(outcome="accepted", blocking_issue_ids=[])
        issue["blocking"] = True
        with self.assertRaisesRegex(TaskflowError, "Observed evidence requires a nonblocking observation"):
            self.parse(observed_aliases=aliases)

    def test_submission_and_trace_are_never_material_public_basis(self):
        aliases = self.observation_aliases()
        issue = self.observation(aliases)
        issue.update(kind="material", affected_paths=["draft/input/prompt.md"])
        issue["evidence"] = issue["evidence"][1:]
        with self.assertRaisesRegex(TaskflowError, "Observed evidence requires a nonblocking observation"):
            self.parse(observed_aliases=aliases)

    def test_observed_aliases_reject_wrong_sha_mutation_or_unknown_typed_paths(self):
        aliases = self.observation_aliases()
        issue = self.observation(aliases)
        first = next(iter(aliases))
        invalid_aliases = []
        wrong_sha = copy.deepcopy(aliases)
        wrong_sha[first]["sha256"] = "0" * 64
        invalid_aliases.append(wrong_sha)
        for key, kind in (("outside.json", "submission"),
                          ("actual-public-work/check.json", "teacher"),
                          ("actual-public-work/check.json", "trace"),
                          ("actual-public-trace.jsonl", "submission"),
                          ("actual-public-work/../outside.json", "submission")):
            invalid_aliases.append({key: {**aliases[first], "kind": kind}})
        relative = copy.deepcopy(aliases)
        relative[first]["path"] = "relative-source.json"
        invalid_aliases.append(relative)
        for value in invalid_aliases:
            with self.subTest(aliases=value), self.assertRaises(TaskflowError):
                self.parse(observed_aliases=value)
        issue["evidence"][1]["sha256"] = "0" * 64
        with self.assertRaises(TaskflowError):
            self.parse(observed_aliases=aliases)
        issue["evidence"][1]["sha256"] = aliases[first]["sha256"]
        Path(aliases[first]["path"]).write_text("changed observation")
        with self.assertRaisesRegex(TaskflowError, "actual SHA"):
            self.parse(observed_aliases=aliases)

    def test_wrong_version_or_old_intent_rejected_in_either_file(self):
        for where in ("report", "issues"):
            for key in self.binding:
                with self.subTest(where=where, key=key):
                    report, issues = copy.deepcopy(self.report), copy.deepcopy(self.issues)
                    value = report["execution_routing"] if where == "report" else issues["execution_binding"]
                    value[key] = "0" * 64 if key.endswith("_digest") else "older-work"
                    with self.assertRaises(TaskflowError):
                        self.parse(report=report, issues=issues)

    def test_missing_bindings_are_not_inconclusive_fallbacks(self):
        for key in self.binding:
            with self.subTest(key=key):
                report = copy.deepcopy(self.report)
                del report["execution_routing"][key]
                with self.assertRaises(TaskflowError):
                    self.parse(report=report)

    def test_unknown_or_malformed_routing_is_rejected(self):
        invalid = [("schema_version", True), ("schema_version", 2),
                   ("signer_role", "author"), ("independent_of_current_author", "true"),
                   ("participated_in_production", True), ("outcome", "passed"),
                   ("outcome", []), ("scope", "unbounded"), ("reason", " "),
                   ("covered_defect_ids", "all"), ("covered_defect_ids", ["a", "a"]),
                   ("blocking_issue_ids", None)]
        for key, value in invalid:
            with self.subTest(key=key, value=value):
                report = copy.deepcopy(self.report)
                report["execution_routing"][key] = value
                with self.assertRaises(TaskflowError):
                    self.parse(report=report)
        with self.assertRaises(TaskflowError):
            self.parse(scope=[])

    def test_accepted_blocks_empty_revision_or_missing_issue_rejected(self):
        self.report["execution_routing"]["blocking_issue_ids"] = ["missing"]
        with self.assertRaises(TaskflowError):
            self.parse()
        self.report["execution_routing"].update(outcome="revision_required", blocking_issue_ids=[])
        with self.assertRaises(TaskflowError):
            self.parse()
        self.report["execution_routing"]["blocking_issue_ids"] = ["missing"]
        with self.assertRaises(TaskflowError):
            self.parse()

    def test_legacy_business_status_must_not_contradict_routing(self):
        self.report["candidate_business_quality"] = {"status": "needs_author_revision"}
        with self.assertRaises(TaskflowError):
            self.parse()
        self.revision()
        self.assertEqual("revision_required", self.parse()["outcome"])
        self.report["candidate_business_quality"]["blocking_issue_ids"] = []
        with self.assertRaises(TaskflowError):
            self.parse()
        self.report["candidate_business_quality"] = {"status": "custom passed sentence"}
        with self.assertRaises(TaskflowError):
            self.parse()

    def test_concrete_issue_fields_and_citations_required(self):
        bad = [("kind", []), ("kind", "environment"), ("responsibility", ""),
               ("affected_paths", []), ("affected_paths", ["../outside"]),
               ("affected_paths", ["missing.md"]), ("evidence", []),
               ("evidence", [{"path": "missing.md", "sha256": "a" * 64, "location": "line 1"}]),
               ("evidence", [{"path": "draft/input/prompt.md", "sha256": "a" * 64, "location": "line 1"}]),
               ("evidence", [{"path": "draft/input/prompt.md", "sha256": self.candidate["files"][0]["sha256"]}])]
        for key, value in bad:
            with self.subTest(key=key, value=value):
                issue = self.issue()
                issue[key] = value
                self.revision(issue)
                with self.assertRaises(TaskflowError):
                    self.parse()
        self.revision()
        self.issues["issues"].append(copy.deepcopy(self.issues["issues"][0]))
        with self.assertRaises(TaskflowError):
            self.parse()

    def test_material_private_only_basis_cannot_trigger_repair(self):
        self.revision(self.issue("material", "draft/evaluation/answer.md"))
        with self.assertRaisesRegex(TaskflowError, "public basis"):
            self.parse()

    def test_actual_candidate_drift_rejected_even_with_no_defects(self):
        Path(self.candidate["files"][0]["path"]).write_text("changed after capture")
        with self.assertRaisesRegex(TaskflowError, "bytes changed"):
            self.parse()

    def test_invalid_digest_and_symlinked_candidate_cannot_bind_review(self):
        with self.assertRaises(TaskflowError):
            self.parse(candidate={**self.candidate, "input_digest": "not-a-digest"})
        original = Path(self.candidate["files"][0]["path"])
        target = self.root / "same-content-outside-candidate.md"
        target.write_bytes(original.read_bytes())
        original.unlink()
        original.symlink_to(target)
        with self.assertRaisesRegex(TaskflowError, "regular candidate"):
            self.parse()

    def test_optional_task_binding_and_issue_list_have_strict_types(self):
        self.report["execution_routing"]["task_id"] = "another-task"
        with self.assertRaises(TaskflowError):
            self.parse()
        self.report["execution_routing"]["task_id"] = "neutral"
        self.issues["execution_binding"]["task_id"] = "neutral"
        self.assertEqual("accepted", self.parse()["outcome"])
        self.issues["issues"] = {}
        with self.assertRaisesRegex(TaskflowError, "issues: list"):
            self.parse()

    def test_bad_capture_shape_and_duplicate_package_path_rejected(self):
        for files in ([], "files", self.candidate["files"] + [self.candidate["files"][0]]):
            with self.subTest(files=files):
                candidate = {**self.candidate, "files": files}
                with self.assertRaises(TaskflowError):
                    self.parse(candidate=candidate)
        candidate = copy.deepcopy(self.candidate)
        candidate["files"][0]["path"] = "relative-file"
        with self.assertRaises(TaskflowError):
            self.parse(candidate=candidate)

    def test_json_passed_text_and_duplicate_keys_do_not_route(self):
        self.write(self.issues_path, self.issues)
        for text in ('{"passed":true}', '{"execution_routing":{},"execution_routing":{}}',
                     '{"bad":NaN}', '[]', 'not-json'):
            with self.subTest(text=text):
                self.report_path.write_text(text)
                with self.assertRaises(TaskflowError):
                    parse_review(self.report_path, self.issues_path,
                                 candidate=self.candidate, intent_id=self.intent,
                                 scope="whole_package")

    def test_symlink_report_and_fifo_never_become_valid_deliveries(self):
        self.write(self.issues_path, self.issues)
        target = self.root / "target.json"
        self.write(target, self.report)
        self.report_path.symlink_to(target)
        with self.assertRaises(TaskflowError):
            parse_review(self.report_path, self.issues_path, candidate=self.candidate,
                         intent_id=self.intent, scope="whole_package")
        self.report_path.unlink()
        os.mkfifo(self.report_path)
        code = (
            "import json,sys; from pathlib import Path; "
            "from taskflow.execution_flow_contracts import parse_review; "
            "from taskflow.contracts import TaskflowError\n"
            "try: parse_review(Path(sys.argv[1]),Path(sys.argv[2]),"
            "candidate=json.loads(sys.argv[3]),intent_id='review-intent',scope='whole_package')\n"
            "except TaskflowError: sys.exit(0)\n"
            "sys.exit(1)\n"
        )
        result = subprocess.run([sys.executable, "-c", code, str(self.report_path),
                                 str(self.issues_path), json.dumps(self.candidate)],
                                capture_output=True, timeout=3)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_review_render_contains_real_binding_without_unrelated_context(self):
        context = {**self.candidate, "intent_id": self.intent,
                   "defect_ids": ["defect-a"], "history": "DO_NOT_INJECT_HISTORY"}
        rendered = review_instructions(context, "affected")
        payload, _ = json.JSONDecoder().raw_decode(rendered[rendered.index("{"):])
        self.assertEqual(self.intent, payload["intent_id"])
        self.assertEqual(self.candidate["content_digest"], payload["content_digest"])
        self.assertEqual(["defect-a"], payload["covered_defect_ids"])
        self.assertNotIn("DO_NOT_INJECT_HISTORY", rendered)
        self.assertIn("adoption-readiness.json", rendered)
        with self.assertRaises(TaskflowError):
            review_instructions(context, "unknown")

    def test_native_protocol_requires_the_complete_exact_actual_request_body(self):
        self.assertEqual(digest(NATIVE_SIGNOFF_PROTOCOL_TEXT.encode("utf-8")), NATIVE_SIGNOFF_PROTOCOL_SHA256)
        protocol = render_native_signoff_protocol()
        context = {**self.candidate, "intent_id": self.intent, "defect_ids": []}
        request = review_instructions(context, "whole_package")
        self.assertTrue(request_has_native_signoff_protocol(request))
        self.assertTrue(request_has_native_signoff_protocol(request.encode("utf-8")))
        for invalid in (NATIVE_SIGNOFF_PROTOCOL_SHA256,
                        protocol.replace(NATIVE_SIGNOFF_PROTOCOL_TEXT, "{}"),
                        request.replace("taskflow-native-quality-signoff-v1", "different-contract-v1"),
                        request + protocol, b"\xff", None):
            with self.subTest(invalid_type=type(invalid).__name__):
                self.assertFalse(request_has_native_signoff_protocol(invalid))

    def test_native_protocol_preserves_affected_scope_and_does_not_force_affirmation(self):
        context = {**self.candidate, "intent_id": self.intent, "defect_ids": ["defect-a"]}
        affected = review_instructions(context, "affected")
        whole = review_instructions(context, "whole_package")
        self.assertIn("native_signoff.coverage必须为affected", affected)
        self.assertIn("不能升级为新整包签发", affected)
        self.assertIn("发现阻断时仅签unchanged_scope", whole)
        self.assertIn("不强迫affirmed=true", whole)
        contract = json.loads(NATIVE_SIGNOFF_PROTOCOL_TEXT)
        shape = contract["output_shape"]["native_signoff"]
        self.assertEqual({"business", "artifact", "calculation"}, set(shape["dimensions"]))
        self.assertTrue(all("true or false" in item["affirmed"] for item in shape["dimensions"].values()))
        self.assertEqual({"package_path", "sha256", "size", "role", "purpose", "basis"}, set(shape["normative"]["files"][0]))
        self.assertIn("needs_business_decision", shape["normative"]["status"])
        self.assertTrue(any("entire current list again" in rule for rule in contract["normative_rules"]))
        self.assertEqual({"public_evidence", "upstream_work_already_done", "remaining_judgment", "consequence",
                          "retained_necessary_conditions", "legitimate_branches", "disposition", "blocking_issue_ids"},
                         set(shape["responsibility_analysis"][0]))

    def test_manifest_request_digest_locks_native_protocol_without_model_execution(self):
        from taskflow import execution_runner as runner

        context = {**self.candidate, "intent_id": self.intent, "defect_ids": []}
        work = self.root / "native-contract-work"
        work.mkdir()
        root = self.root / "native-contract-attempt"
        manifest = runner.prepare_attempt(root, {
            "intent_id": self.intent, "policy_version": "neutral-protocol-v1", "role": "quality",
            "mode": "test_fixture", "model": "neutral", "reasoning_effort": "high",
            "cwd": str(work), "argv": [str(Path(sys.executable).resolve()), "-c", "pass"],
            "inputs": [], "outputs": ["adoption-readiness.json"],
            "request_sections": [{"kind": "operation", "text": review_instructions(context, "whole_package")}],
        })
        path = root / "request.md"
        raw = path.read_bytes()
        self.assertTrue(request_has_native_signoff_protocol(raw))
        self.assertEqual(digest(raw), manifest["request"]["sha256"])
        self.assertFalse((root / "dispatch.json").exists())
        path.write_bytes(raw.replace(NATIVE_SIGNOFF_PROTOCOL_TEXT.encode(), b"{}", 1))
        self.assertFalse(request_has_native_signoff_protocol(path.read_bytes()))
        with self.assertRaisesRegex(runner.RunnerError, "Actual request changed"):
            runner._validate(root)

    def test_cold_author_parent_path_imports_protocol_only_at_future_quality_boundary(self):
        code = r'''
import json,sys
import taskflow.execution_author
import taskflow.execution_lifecycle
import tools.isolated_quality
cold='taskflow.execution_flow_contracts' not in sys.modules
from taskflow.execution_flow_contracts import review_instructions,request_has_native_signoff_protocol,NATIVE_SIGNOFF_PROTOCOL_SHA256
context={'task_id':'neutral','candidate_id':'neutral-candidate','intent_id':'neutral-review','defect_ids':[],**{key:'a'*64 for key in ('content_digest','input_digest','evaluation_digest')}}
print(json.dumps({'cold_before_quality':cold,'future_request_has_complete_protocol':request_has_native_signoff_protocol(review_instructions(context,'whole_package')),'protocol_sha256':NATIVE_SIGNOFF_PROTOCOL_SHA256}))
'''
        process = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, timeout=10,
                                 cwd=Path(__file__).resolve().parents[1])
        self.assertEqual(0, process.returncode, process.stderr)
        observed = json.loads(process.stdout)
        self.assertTrue(observed["cold_before_quality"])
        self.assertTrue(observed["future_request_has_complete_protocol"])
        self.assertEqual(NATIVE_SIGNOFF_PROTOCOL_SHA256, observed["protocol_sha256"])

    def test_repair_records_real_modify_add_delete(self):
        child, record = self.repair_fixture()
        value = self.repair(child, record)
        self.assertEqual(3, len(value["changes"]))
        changes = {x["path"]: x for x in value["changes"]}
        self.assertIsNone(changes["draft/evaluation/new.md"]["before_sha256"])
        self.assertIsNone(changes["draft/evaluation/answer.md"]["after_sha256"])
        self.assertEqual("revised", value["candidate_id"])

    def test_repair_identity_business_decision_and_assignment_rejected(self):
        child, good = self.repair_fixture()
        for field, value in [("parent_id", "other"), ("candidate_id", "other"),
                             ("defect_ids", []), ("defect_ids", ["other"]),
                             ("defect_ids", ["defect-a", "defect-a"]),
                             ("needs_business_decision", True),
                             ("needs_business_decision", "false")]:
            with self.subTest(field=field):
                record = copy.deepcopy(good)
                record[field] = value
                with self.assertRaises(TaskflowError):
                    self.repair(child, record)
        for update in ({"task_id": "foreign"}, {"candidate_id": "original"},
                       {"parent_id": "other"}):
            with self.subTest(update=update), self.assertRaises(TaskflowError):
                self.repair({**child, **update}, good)

    def test_repair_cannot_omit_actual_change_or_declare_unmodified_path(self):
        child, good = self.repair_fixture()
        for rows in (good["changes"][:-1], good["changes"] + [good["changes"][0]],
                     good["changes"] + [{"path": "draft/evaluation/rubric.md",
                                          "before_sha256": "a" * 64, "after_sha256": "a" * 64}],
                     "all changed"):
            with self.subTest(rows=rows), self.assertRaises(TaskflowError):
                self.repair(child, {**good, "changes": rows})

    def test_repair_sha_fields_must_match_each_captured_side(self):
        child, good = self.repair_fixture()
        for field in ("before_sha256", "after_sha256"):
            for missing in (False, True):
                record = copy.deepcopy(good)
                if missing:
                    del record["changes"][0][field]
                else:
                    record["changes"][0][field] = "0" * 64
                with self.subTest(field=field, missing=missing), self.assertRaises(TaskflowError):
                    self.repair(child, record)

    def test_noop_or_actual_post_capture_drift_cannot_claim_repair(self):
        unchanged = self.make_candidate("same-bytes", self.contents, parent_id="original")
        record = {"parent_id": "original", "candidate_id": "same-bytes",
                  "defect_ids": ["defect-a"],
                  "needs_business_decision": False, "changes": []}
        with self.assertRaises(TaskflowError):
            self.repair(unchanged, record)
        child, record = self.repair_fixture()
        Path(child["files"][0]["path"]).write_text("external replacement after capture")
        with self.assertRaises(TaskflowError):
            self.repair(child, record)

    def test_role_reclassification_must_be_an_explicit_change(self):
        contents = dict(self.contents)
        contents["draft/evaluation/answer.md"] = ("teacher", contents["draft/evaluation/answer.md"][1])
        child = self.make_candidate("role-changed", contents, parent_id="original")
        old = self.candidate["files"][1]["sha256"]
        record = {"parent_id": "original", "candidate_id": "role-changed",
                  "defect_ids": ["defect-a"],
                  "needs_business_decision": False,
                  "changes": [{"path": "draft/evaluation/answer.md",
                               "before_sha256": old, "after_sha256": old}]}
        self.assertEqual(1, len(self.repair(child, record)["changes"]))
        with self.assertRaises(TaskflowError):
            self.repair(child, {**record, "changes": []})

    def test_repair_renderer_lists_actual_parent_and_assigned_defects(self):
        context = {**self.candidate, "defect_ids": ["defect-a"],
                   "revised_candidate_id": "next", "history": "DO_NOT_INJECT_HISTORY"}
        rendered = repair_instructions(context)
        payload, _ = json.JSONDecoder().raw_decode(rendered[rendered.index("{"):])
        self.assertEqual("original", payload["parent_id"])
        self.assertEqual("next", payload["candidate_id"])
        self.assertEqual(["defect-a"], payload["defect_ids"])
        self.assertNotIn("DO_NOT_INJECT_HISTORY", rendered)
        with self.assertRaises(TaskflowError):
            repair_instructions({**context, "defect_ids": []})
        del context["revised_candidate_id"]
        with self.assertRaises(TaskflowError):
            repair_instructions(context)


if __name__ == "__main__":
    unittest.main()

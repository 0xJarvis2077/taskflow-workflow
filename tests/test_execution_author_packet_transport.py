"""Real neutral author/source IO and SQLite; zero external models.

Only the native execution/probe adapter is converted to the existing fixture
subprocess. Native quality uses the existing explicitly native-shaped sealed
fixture and ledger boundary double, not a claimed real model signature.
"""

from contextlib import contextmanager
import copy
import json
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from taskflow import execution, execution_runner
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_author import start_author_from_packet
from taskflow.execution_author_packet import KIND, TARGET, author_packet_matches, validate_author_packet_transport
from taskflow.execution_experiment import _author_source, _native_handoff, _node, _packet
from taskflow.execution_formal import native_quality_handoff, projected_quality_reports, validate_normative_certification
from taskflow.execution_lifecycle import _lifecycle_lock, _receive, advance_lifecycle, register_lifecycle, resume_path_transport
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests import test_execution_author_factory as factory_fixtures
from tests import test_execution_experiment_author as experiment_fixtures
from tests import test_execution_normative_certification as normative_fixtures
from tools.execution_simulation import perform


REASON = "authorized author flow differs from its exact task/owner/policy/original business packet"


def make_packet_fixture(owner, *, target=TARGET, kind="source_metadata", duplicate=False, disposition="research_ended"):
    case = factory_fixtures.AuthorFactoryTests()
    case.setUp()
    owner.addCleanup(case.doCleanups)
    case.factory()
    def enqueue(store, spec, runtime, **options):
        spec = copy.deepcopy(spec)
        row = next(row for row in spec["runner"]["inputs"] if row["target"] == TARGET)
        row.update(target=target, kind=kind)
        # Same explicit native-probe/executable fixture conversion as the
        # existing author factory tests, with its neutral outcome parameter.
        converted = copy.deepcopy(spec)
        converted["runner"].update(mode="test_fixture", argv=[str(Path(sys.executable).resolve()),
            str(case.script), str(case.task), disposition, "0", str(Path(__file__).resolve().parents[1])])
        converted["runner"].pop("native_permissions")
        return execution.enqueue_spec(store, converted, runtime, **options)

    with patch("taskflow.execution_author.enqueue_spec", side_effect=enqueue):
        prepared = start_author_from_packet(case.store, **case.kwargs())
    if duplicate:
        # Deliberately construct a duplicate manifest row before any worker or
        # collection exists; the unchanged input file is read twice. This is an
        # adversarial prepared-source fixture, not a permitted factory output.
        runtime = case.directory / "runtime"
        manifest = read_json(runtime / "manifest.json")
        manifest["inputs"].append(copy.deepcopy(next(row for row in manifest["inputs"] if row["target"] == TARGET)))
        for name in ("manifest.json", "manifest-lock.json"):
            (runtime / name).chmod(0o600)
        write_json(runtime / "manifest.json", manifest)
        write_json(runtime / "manifest-lock.json", execution_runner._info(runtime / "manifest.json"))
    case.actual_run = execution_runner.run_attempt
    with patch("taskflow.execution_runner.run_attempt", side_effect=case.actual_fixture_process):
        case.attempt = execution.run_worker(case.store, "neutral-author", True, [prepared["work_id"]])
    owner.assertEqual("completed", case.attempt["state"])
    binding = {key: case.attempt[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}
    parent_definition = {"schema_version": 1, "kind": "author_quality_v1",
        "flow_id": case.attempt["intent_id"] + "-author-quality", "task_id": "neutral",
        "owner": "fixture-controller", "policy_id": "fixture-v1",
        "directory": str(case.root / "parent"), "authorization": case.packet_ref,
        "source_author_attempt": case.attempt["attempt_id"], "source_author_binding": binding,
        "source_author_work": str(case.directory / "work"), "role_template_ref": case.config["role_template_ref"],
        "business_item": case.packet_data["items"][0], "author_contract": case.packet_data["output_contract"],
        "next": case.packet_data["preauthorized_normal_successors"]}
    parent = register_lifecycle(case.store, parent_definition, [case.packet_ref])
    lifecycle_spec = case.directory / "lifecycle-spec.json"
    write_json(lifecycle_spec, {"definition": parent_definition, "evidence": [case.packet_ref]})
    helper = experiment_fixtures.AuthorExperimentTests()
    helper.root = case.root
    definition = helper.definition(case, parent)
    definition["technical"].update(formal_runtime_root=str(case.root / "formal"), publication_root=str(case.root / "publication"))
    authority = Path(definition["authorization"]["path"])
    body = read_json(authority)
    staged = case.directory / "runtime/inputs" / TARGET
    body["items"][0]["source_business_packet"] = str(staged)
    write_json(authority, body)
    authorization = evidence_ref(authority, "same original substantive experiment authority with staged source locator")
    definition.update(authorization=authorization, business_packet_ref=authorization)
    record = register_lifecycle(case.store, definition, [authorization])
    state = {**record["state"], "status": "exception", "exception": {"stage": "experiment_v1", "reason": REASON}}
    record = case.store.update_flow(record["flow_id"], record["revision"], state, {"kind": "neutral_actual_original_path_exception"})
    record = _receive(case.store, record)
    note = case.root / "metadata-adoption.json"
    write_json(note, {"controller": "fixture-controller", "scope": "Only adopt the original locked source/staged packet locator; business text unchanged."})
    spec = {"schema_version": 1, "kind": KIND, "source_author_flow_id": parent["flow_id"],
        "source_author_attempt": case.attempt["attempt_id"], "authorization": authorization,
        "lifecycle_spec": evidence_ref(lifecycle_spec, "unchanged original source lifecycle definition"),
        "manifest": evidence_ref(case.directory / "runtime/manifest.json", "actual managed original author manifest"),
        "source": case.packet_ref,
        "staged": evidence_ref(staged, "exact locked source_metadata staged alias"),
        "evidence": [evidence_ref(note, "explicit metadata-only adoption")]} if target == TARGET else None
    transport_path = case.root / "packet-transport.json"
    if spec is not None:
        write_json(transport_path, spec)
    return {"case": case, "store": case.store, "parent": parent, "record": record,
            "authority": authority, "authorization": authorization, "spec": spec,
            "transport_path": transport_path, "staged": staged, "note": note}


class AuthorPacketTransportTests(unittest.TestCase):
    def fixture(self, **kwargs):
        return make_packet_fixture(self, **kwargs)

    def reference(self, value):
        return evidence_ref(value["transport_path"], "explicit immutable author packet alias sidecar")

    def validate(self, value, *, offline=False):
        return validate_author_packet_transport(packet_transport=self.reference(value),
            authorization=value["authorization"], source_flow_id=value["parent"]["flow_id"],
            store=None if offline else value["store"], require_complete=True)

    def resolution(self, value):
        ref = self.reference(value)
        return {"action": "resume_author_packet_source_metadata_transport",
                "reason": "Adopt only the exact original locked source/staged metadata alias.",
                "packet_transport": ref, "evidence": [ref, evidence_ref(value["note"], "explicit metadata-adoption evidence")]}

    @contextmanager
    def edited_row(self, value, table, identity, change):
        store = value["store"]
        with store._db(True) as db:
            raw = db.execute("SELECT data FROM " + table + " WHERE id=?", (identity,)).fetchone()["data"]
            row = json.loads(raw)
            change(row)
            db.execute("UPDATE " + table + " SET data=? WHERE id=?", (canonical(row).decode(), identity))
        try:
            yield
        finally:
            with store._db(True) as db:
                db.execute("UPDATE " + table + " SET data=? WHERE id=?", (raw, identity))

    def test_exact_alias_online_offline_and_normal_wait_preserve_originals(self):
        value = self.fixture()
        case, store = value["case"], value["store"]
        paths = [value["authority"], value["staged"], Path(value["record"]["state"]["receipt"])]
        paths += [case.directory / "runtime" / name for name in ("manifest.json", "request.md", "collection.json", "trace.jsonl")]
        before = {str(p): p.read_bytes() for p in paths}
        count = len(store.dashboard()["attempts"])
        store.set_control("work:" + _node(value["record"], "solver"), "pause", "Existing business purpose remains held.")
        with store._db() as db:
            controls = [tuple(row) for row in db.execute("SELECT * FROM controls ORDER BY scope")]
        with patch("subprocess.run", side_effect=AssertionError("transport cannot launch a process")):
            witness = self.validate(value)
            self.assertEqual(witness, self.validate(value, offline=True))
            result = resume_path_transport(store, value["record"]["flow_id"], self.resolution(value))
            bound = _author_source(store, result, _packet(result))
        self.assertEqual("active", result["state"]["status"])
        self.assertEqual("experiment_v1", result["state"]["stage"])
        self.assertEqual(value["record"]["definition_digest"], result["definition_digest"])
        self.assertEqual(value["record"]["state"]["exception"], result["state"]["history"][-1]["previous_exception"])
        self.assertEqual("source_author_quality_not_received", bound["record"]["state"]["waiting"]["reason"])
        self.assertEqual(count, len(store.dashboard()["attempts"]))
        with store._db() as db:
            self.assertEqual(controls, [tuple(row) for row in db.execute("SELECT * FROM controls ORDER BY scope")])
        self.assertEqual(before, {str(p): p.read_bytes() for p in paths})
        with self.assertRaises(TaskflowError):
            resume_path_transport(store, result["flow_id"], self.resolution(value))

    def test_absent_explicit_transport_keeps_legacy_exact_path_fail_closed(self):
        value = self.fixture()
        packet = _packet(value["record"])
        self.assertFalse(author_packet_matches(value["store"], parent=value["parent"], packet=packet,
                                              authorization=value["authorization"]))
        with self.assertRaisesRegex(TaskflowError, "original business packet"):
            _author_source(value["store"], value["record"], packet)
        explicit = self.reference(value)
        self.assertFalse(author_packet_matches(value["store"], parent=value["parent"],
            packet={**packet, "source_business_packet": str(value["case"].root / "foreign-peer.json")},
            authorization=value["authorization"], packet_transport=explicit))
        original = value["parent"]["definition"]["authorization"]
        direct = {**packet, "source_business_packet": original["path"]}
        self.assertTrue(author_packet_matches(value["store"], parent=value["parent"], packet=direct,
                                             authorization=value["authorization"]))

    def test_foreign_equal_bytes_alias_wrong_target_kind_and_duplicate_are_rejected(self):
        value = self.fixture()
        peer = value["case"].root / "same-byte-peer.json"
        peer.write_bytes(value["staged"].read_bytes())
        original = copy.deepcopy(value["spec"])
        for key in ("source", "staged", "manifest", "lifecycle_spec"):
            spec = copy.deepcopy(original)
            source = Path(spec[key]["path"])
            peer.write_bytes(source.read_bytes())
            spec[key] = evidence_ref(peer, "same-byte foreign peer is not original provenance")
            write_json(value["transport_path"], spec)
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                self.validate(value)
        for options in ({"target": "protocol/wrong-packet.json"}, {"kind": "teacher"}, {"duplicate": True}):
            with self.subTest(options=options):
                other = self.fixture(**options)
                if other["spec"] is None:
                    spec = copy.deepcopy(original)
                    spec.update(source_author_flow_id=other["parent"]["flow_id"], source_author_attempt=other["case"].attempt["attempt_id"],
                        authorization=other["authorization"], lifecycle_spec=evidence_ref(other["case"].directory / "lifecycle-spec.json", "original"),
                        manifest=evidence_ref(other["case"].directory / "runtime/manifest.json", "original"), source=other["case"].packet_ref,
                        staged=evidence_ref(other["case"].directory / "runtime/inputs/protocol/wrong-packet.json", "wrong target"))
                    write_json(other["transport_path"], spec)
                with self.assertRaises(TaskflowError):
                    self.validate(other)

    def test_actual_binding_or_staged_drift_cannot_be_concealed_by_equal_sha(self):
        value = self.fixture()
        case = value["case"]
        for table, identity, change in (
            ("attempts", case.attempt["attempt_id"], lambda row: row.update(policy_id="foreign-policy")),
            ("attempts", case.attempt["attempt_id"], lambda row: row.update(cli_sid="foreign-cli")),
            ("candidates", case.attempt["candidate_id"], lambda row: row.update(evaluation_digest="0" * 64)),
        ):
            with self.subTest(table=table), self.edited_row(value, table, identity, change), self.assertRaises(TaskflowError):
                self.validate(value)
        raw = value["staged"].read_bytes()
        mode = value["staged"].stat().st_mode & 0o777
        value["staged"].chmod(0o600)
        try:
            value["staged"].write_bytes(raw + b" ")
            with self.assertRaises((TaskflowError, execution_runner.RunnerError)):
                self.validate(value)
        finally:
            value["staged"].write_bytes(raw)
            value["staged"].chmod(mode)

    def test_exact_resume_guards_hold_receipt_locks_cas_and_zero_formal_boundary(self):
        value = self.fixture()
        store, record = value["store"], value["record"]
        before = store.events()
        with _lifecycle_lock(store, record["flow_id"]), self.assertRaises(TaskflowError):
            resume_path_transport(store, record["flow_id"], self.resolution(value))
        self.assertEqual(before, store.events())
        bad = self.resolution(value)
        bad["evidence"] = [evidence_ref(value["note"], "transport ref intentionally absent")]
        with self.assertRaisesRegex(TaskflowError, "explicit resolution evidence"):
            resume_path_transport(store, record["flow_id"], bad)
        receipt = Path(record["state"]["receipt"])
        original = receipt.read_bytes()
        try:
            payload = read_json(receipt)
            payload["controller"] = "foreign-controller"
            write_json(receipt, payload)
            with self.assertRaisesRegex(TaskflowError, "original received exception"):
                resume_path_transport(store, record["flow_id"], self.resolution(value))
        finally:
            receipt.write_bytes(original)
        self.assertEqual(before, store.events())

        for field, changed in (("pending_business_scope", {}), ("run_index", 1), ("current_work_id", "some-work")):
            state = {**record["state"], field: changed}
            forged = {**record, "state": state}
            from taskflow.execution_lifecycle import _resume_author_packet_transport
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                _resume_author_packet_transport(store, forged, self.resolution(value), self.resolution(value)["evidence"])
        root = Path(record["definition"]["technical"]["formal_runtime_root"])
        root.mkdir()
        try:
            with self.assertRaisesRegex(TaskflowError, "formal preparation"):
                resume_path_transport(store, record["flow_id"], self.resolution(value))
        finally:
            root.rmdir()
        self.assertEqual(before, store.events())
        actual = ExecutionStore.update_flow
        def conflict(self, identity, revision, state, event):
            if event.get("kind") == "lifecycle_author_packet_transport_resumed":
                return actual(self, identity, revision + 1, state, event)
            return actual(self, identity, revision, state, event)
        with patch.object(ExecutionStore, "update_flow", new=conflict), self.assertRaises(TaskflowError):
            resume_path_transport(store, record["flow_id"], self.resolution(value))
        self.assertEqual(record, store.get_flow(record["flow_id"]))
        self.assertEqual(before, store.events())


    def test_original_owner_sidecar_authority_and_existing_work_or_decision_cannot_be_replaced(self):
        value = self.fixture()
        store, record = value["store"], value["record"]
        original = copy.deepcopy(value["spec"])
        for key, replacement in (("kind", "unknown_alias"), ("schema_version", True),
                                 ("source_author_attempt", "foreign-attempt"), ("source_author_flow_id", "foreign-parent")):
            spec = {**original, key: replacement}
            write_json(value["transport_path"], spec)
            with self.subTest(key=key), self.assertRaises(TaskflowError):
                self.validate(value)
        write_json(value["transport_path"], original)
        parent = value["parent"]
        for field, replacement in (("pending_business_scope", {}), ("exception", {"reason": "actual unresolved choice"})):
            state = {**parent["state"], field: replacement}
            source = store.update_flow(parent["flow_id"], parent["revision"], state, {"kind": "neutral_source_hold"})
            before = store.events()
            with self.subTest(field=field), self.assertRaises(TaskflowError):
                resume_path_transport(store, record["flow_id"], self.resolution(value))
            self.assertEqual(before, store.events())
            parent = store.update_flow(parent["flow_id"], source["revision"], value["parent"]["state"], {"kind": "neutral_restore_source"})
        target = _node(record, "solver")
        store.record_decision(_node(record, "experiment-decision"), "neutral", value["case"].attempt["candidate_id"],
            action="research", rationale="Existing exact purpose ID, not a new experiment authority.",
            evidence=[value["authorization"]])
        before = store.events()
        with self.assertRaisesRegex(TaskflowError, "formal work/attempt/decision"):
            resume_path_transport(store, record["flow_id"], self.resolution(value))
        self.assertEqual(before, store.events())
        store.enqueue(target, "neutral", value["case"].attempt["candidate_id"], "quality", "fixture-v1",
            intent_id=target + "-intent", purpose="Preexisting neutral ready work must not be replaced.",
            request_digest="0" * 64, evidence=[value["authorization"]], dispatch_owner="fixture-controller")
        for claim in (False, True):
            if claim:
                self.assertIsNotNone(store.claim("neutral-existing-owner", [target]))
            before = store.events()
            with self.subTest(claim=claim), self.assertRaisesRegex(TaskflowError, "formal work/attempt/decision"):
                resume_path_transport(store, record["flow_id"], self.resolution(value))
            self.assertEqual(before, store.events())
        self.assertEqual(record, store.get_flow(record["flow_id"]))
    def test_native_handoff_projection_and_offline_certificate_use_the_same_transport(self):
        value = self.fixture()
        maker = normative_fixtures.NormativeCertificationTests()
        maker.addCleanup = self.addCleanup
        native = maker.fixture()
        parent = {**value["parent"], "state": copy.deepcopy(native.parent["state"])}
        parent["state"]["child_flow_id"] = native.child_id
        child = copy.deepcopy(native.child)
        child["definition"]["owner"] = parent["definition"]["owner"]
        path = Path(child["state"]["receipt"])
        payload = read_json(path)
        payload["controller"] = parent["definition"]["owner"]
        write_json(path, payload)
        actual_store = value["store"]
        base_attempt = native.store.get_attempt
        proxy = SimpleNamespace(
            get_flow=lambda identity: parent if identity == parent["flow_id"] else child,
            get_attempt=lambda identity: actual_store.get_attempt(identity) if identity == value["case"].attempt["attempt_id"] else base_attempt(identity),
            get_work=actual_store.get_work,
            get_candidate=lambda identity: actual_store.get_candidate(identity) if identity == value["case"].attempt["candidate_id"] else native.candidate,
            quality_gate=native.store.quality_gate, assert_review_current=native.store.assert_review_current)
        with self.assertRaisesRegex(TaskflowError, "actual accepted author parent"):
            native_quality_handoff(proxy, candidate_id=native.candidate["candidate_id"],
                authorization=value["authorization"], source_flow_id=parent["flow_id"])
        handoff = native_quality_handoff(proxy, candidate_id=native.candidate["candidate_id"],
            authorization=value["authorization"], source_flow_id=parent["flow_id"], packet_transport=self.reference(value))
        self.assertEqual(self.validate(value), handoff["certification"]["author_packet_transport"])
        record = {"definition": {"authorization": value["authorization"], "source_author_flow_id": parent["flow_id"]},
                  "state": {"source_business_packet_transport": self.reference(value), "native_quality_handoff": handoff}}
        self.assertEqual(handoff, _native_handoff(proxy, record, native.candidate["candidate_id"]))
        reports = projected_quality_reports(proxy, candidate_id=native.candidate["candidate_id"],
            source_refs=handoff["source_refs"], protocol_map=handoff["protocol_map"], destination=native.root / "alias-projection")
        self.assertEqual({"business", "artifact", "calculation"}, set(reports))
        cert = {"schema_version": 1, "kind": "native_quality_handoff", "candidate_binding": {
            key: native.candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")},
            "source_author_flow_id": parent["flow_id"], "authorization": value["authorization"], "native_quality_handoff": handoff}
        self.assertEqual(handoff["normative_sources"], validate_normative_certification(candidate=native.candidate, certification=cert))
        self.assertEqual(handoff["normative_sources"], validate_normative_certification(candidate=native.candidate, certification=cert, store=proxy))
        bad = copy.deepcopy(cert)
        bad["native_quality_handoff"]["certification"]["author_packet_transport"]["policy_id"] = "foreign"
        with self.assertRaisesRegex(TaskflowError, "author packet transport"):
            validate_normative_certification(candidate=native.candidate, certification=bad)
        missing = copy.deepcopy(cert)
        del missing["native_quality_handoff"]["protocol_map"]["native_signoff"]["packet_transport"]
        with self.assertRaisesRegex(TaskflowError, "no explicit author packet transport"):
            validate_normative_certification(candidate=native.candidate, certification=missing)

    def accepted_revised_source(self):
        value = self.fixture(disposition="candidate_complete")
        case, store = value["case"], value["store"]
        parent = advance_lifecycle(store, value["parent"]["flow_id"])
        parent = advance_lifecycle(store, parent["flow_id"])
        self.assertEqual("public_prepare", parent["state"]["stage"])
        initial = store.get_candidate(parent["state"]["candidate_id"])
        public = perform(store, case.root / "source-public", initial["candidate_id"], "quality", "source-public", {
            "public.md": "Neutral actual public predecessor."})
        revision_author = perform(store, case.root / "revision-author", initial["candidate_id"], "author", "private-revision", {
            "answer.md": "Actual authored private revision, before fresh independent quality.\n"})
        product = case.root / "private-revision-product"
        shutil.copytree(Path(parent["state"]["candidate_root"]), product)
        shutil.copyfile(Path(revision_author["runtime_path"]) / "artifacts/answer.md", product / "draft/evaluation/answer.md")
        revised = execution.capture_candidate(store, product, "current-revised-source", initial["candidate_id"], revision_author["attempt_id"])
        intent = "current-revised-quality"
        binding = {"intent_id": intent, **{key: revised[key] for key in
                   ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}}
        route = {"schema_version": 1, **binding, "signer_role": "independent_quality",
            "independent_of_current_author": True, "participated_in_production": False,
            "scope": "whole_package", "outcome": "accepted", "blocking_issue_ids": [],
            "covered_defect_ids": [], "reason": "Neutral complete current revision inspected."}
        quality = perform(store, case.root / "current-quality", revised["candidate_id"], "quality", intent, {
            "adoption-readiness.json": json.dumps({"execution_routing": route}),
            "issues.json": json.dumps({"execution_binding": binding, "issues": []}), "intake.md": "Neutral current whole fixture."},
            extra={"inputs": [{"path": str(product / "task.json"), "kind": "source_metadata", "target": "execution/task.json"}]})
        refs = [evidence_ref(Path(quality["runtime_path"]) / "artifacts" / name, "actual neutral current source")
                for name in ("adoption-readiness.json", "issues.json")]
        review = store.record_review("current-source-whole", revised["candidate_id"], quality["attempt_id"],
            verdict="accepted", scope="whole neutral revised source", evidence=refs, whole_package=True)
        history = [{"kind": "independent_review", "review_id": review["review_id"], "attempt_id": quality["attempt_id"],
                    "candidate_id": revised["candidate_id"], "cli_sid": quality["cli_sid"], "outcome": "accepted"}]
        child_id = "actual-revised-child"
        receipt = case.root / "actual-revised-child-result.json"
        write_json(receipt, {"flow_id": child_id, "controller": "fixture-controller", "status": "accepted",
                            "candidate_id": revised["candidate_id"], "history": history})
        child_state = {"status": "accepted", "candidate_id": revised["candidate_id"], "candidate_root": str(product),
            "accepted_review_id": review["review_id"], "history": history, "receipt": str(receipt)}
        child = store.register_flow(child_id, {"task_id": "neutral", "owner": "fixture-controller", "policy_id": "fixture-v1",
            "initial": {"candidate_id": initial["candidate_id"], "expected_digests": {key: initial[key] for key in
                ("input_digest", "content_digest", "evaluation_digest")}, "predecessor_attempt": public["attempt_id"],
                "predecessor_binding": {key: public[key] for key in ("work_id", "intent_id", "candidate_id", "task_id")}}}, refs, child_state)
        state = {**parent["state"], "status": "accepted", "stage": "child_quality", "current_work_id": None,
            "child_flow_id": child_id, "public_attempt_id": public["attempt_id"], "result": {
                "outcome": "candidate_quality_accepted", "candidate_id": revised["candidate_id"], "candidate_root": str(product),
                "accepted_review_id": review["review_id"], "child_flow_id": child_id, "child_receipt": str(receipt)}}
        parent = store.update_flow(parent["flow_id"], parent["revision"], state, {"kind": "neutral_current_revision_received"})
        parent = _receive(store, parent)
        self.assertEqual(initial["candidate_id"], parent["state"]["candidate_id"])
        self.assertNotEqual(parent["state"]["candidate_id"], parent["state"]["result"]["candidate_id"])
        effective = {**value["record"], "state": {**value["record"]["state"], "source_business_packet_transport": self.reference(value)}}
        bound = _author_source(store, effective, _packet(effective), require_candidate=True)
        self.assertEqual(child, bound["source"])
        return {**value, "parent": parent, "revised": revised}

    def test_actual_revised_result_candidate_foreign_work_and_decision_are_not_missed(self):
        for mode in ("solver", "decision", "child_hold"):
            with self.subTest(mode=mode):
                value = self.accepted_revised_source()
                store, revised = value["store"], value["revised"]
                if mode == "solver":
                    purpose = "Existing source candidate solver belongs to another managed work."
                    store.record_decision("original-authority-for-foreign-solver", "neutral", revised["candidate_id"],
                        action="experiment", rationale="Preexisting exact neutral solver purpose.",
                        experiment_purpose=purpose, authorized=True, evidence=[value["authorization"]])
                    store.enqueue("foreign-current-solver", "neutral", revised["candidate_id"], "solver", "fixture-v1",
                        intent_id="different-solver-intent", purpose=purpose,
                        request_digest="0" * 64, evidence=[value["authorization"]], dispatch_owner="fixture-controller")
                elif mode == "decision":
                    store.record_decision("foreign-current-experiment-decision", "neutral", revised["candidate_id"], action="experiment",
                        rationale="Existing source current-version experiment purpose.", experiment_purpose="A separate previously authorized purpose.",
                        authorized=True, evidence=[value["authorization"]])
                else:
                    child = store.get_flow(value["parent"]["state"]["child_flow_id"])
                    store.update_flow(child["flow_id"], child["revision"],
                        {**child["state"], "pending_business_scope": {}}, {"kind": "neutral_child_business_hold"})
                before = store.events()
                with self.assertRaisesRegex(TaskflowError, "business hold" if mode == "child_hold" else "formal work/attempt/decision"):
                    resume_path_transport(store, value["record"]["flow_id"], self.resolution(value))
                self.assertEqual(before, store.events())
                self.assertEqual(value["record"], store.get_flow(value["record"]["flow_id"]))


if __name__ == "__main__":
    unittest.main()

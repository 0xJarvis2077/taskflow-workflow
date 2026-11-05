"""A real neutral accepted first intake may precede one own-run recovery.

Workers, sealed two-stage intake, native records, exact SQLite/CAS and recovery
proof IO are real local fixtures. Formal execution and the host probe gate use
the existing named neutral adapters; no provider/model or native permission claim.
"""

import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from taskflow import execution
from taskflow.contracts import TaskflowError, canonical, read_json, sha256, write_json
from taskflow.execution_experiment import _node
from taskflow.execution_lifecycle import _confirmation_first_run_footprint, _receive, resume_path_transport
from taskflow.execution_runner import _info
from taskflow.execution_store import evidence_ref
from tests import test_execution_experiment as experiments
from tests import test_execution_formal_public_probe_recovery as recovery_fixtures
from tools.execution_simulation import fixture_policy, perform


def make_confirmation_recovery_fixture(owner):
    f = recovery_fixtures.make_public_probe_recovery_fixture(owner)
    case = experiments.ExperimentBoundaryTests()
    case.root, case.task, case.candidate, case.store = f.root, f.task, f.candidate, f.store
    case.snapshot = Path(f.release["snapshot"]["path"])
    policy = fixture_policy()
    policy["roles"]["solver"].update(model=f.config["model"], reasoning_effort=f.config["reasoning_effort"], timeout_seconds=f.config["timeout_seconds"])
    f.store.register_policy(f.config["policy_version"], policy)
    f.store.register_policy("fixture-v1", fixture_policy())
    quality = perform(f.store, f.root / "fixture-quality", f.candidate["candidate_id"], "quality", "fixture-quality",
                      {"intake.md": "Neutral original quality source; not a native/business signature."})
    release_binding = read_json(Path(f.release["receipt"]["path"]))["binding"]
    f.store.record_review(release_binding["accepted_review_id"], f.candidate["candidate_id"], quality["attempt_id"],
        verdict="accepted", scope="neutral fixture source only", whole_package=True, evidence=quality["evidence"])
    flow_id = "one-confirmation-recovery"
    packet = {"formal": {"first_run_id": "neutral-original"}, "release": {"version_id": release_binding["version"]},
        "business_investment": {"action": "one neutral original and its conditional confirmation",
                                "unresolved_business_question": "Preserve the actual neutral first run and one own-run preparation."},
        "independent_review": {"score_id": "neutral-original-score", "teacher_file_ids": ["answer", "rubric"]},
        "single_confirmation": {"authorized_if": "First signed original <=3/7 with exact local evidence only.",
                                "max_additional_attempts": 1, "run_id": f.config["run_id"], "score_id": "neutral-confirmation-score"}}
    definition = {"schema_version": 1, "kind": "experiment_v1", "flow_id": flow_id, "task_id": f.candidate["task_id"],
        "owner": "fixture-controller", "policy_id": f.config["policy_version"], "authorization": f.config["authorization"],
        "business_packet_ref": f.config["authorization"], "directory": str(f.root / "lifecycle"),
        "technical": {"formal_runtime_root": str(f.runtime.parent), "review_runner": {}, "real_repository": str(f.root),
                      "formal_config_base": {k:v for k,v in f.config.items() if k not in ("run_id", "intent_id", "policy_version")}}}
    case.definition = definition
    case.record = f.store.register_flow(flow_id, definition, [f.config["authorization"]], {
        "status": "active", "stage": "formal_prepare", "run_index": 0, "current_work_id": None,
        "candidate_id": f.candidate["candidate_id"], "candidate_root": str(f.task),
        "accepted_review_id": release_binding["accepted_review_id"], "release": f.release,
        "runs": [], "history": [], "archives": [], "excluded_sids": [quality["cli_sid"]], "waiting": None})
    original_write = experiments.write_json
    def write_fixture(path, value):
        path = Path(path)
        if path.name == "first_review_index.json":
            configuration = path.parent / "actual-configuration.json"
            original_write(configuration, {"requested_model": f.config["model"], "reasoning_effort": f.config["reasoning_effort"], "nature": "test_fixture"})
            value = {**value, "allowed_direct_artifacts": [*value["allowed_direct_artifacts"],
                {"id": "actual_configuration", "incoming_path": "indexes/actual-configuration.json",
                 "visibility": "actual_configuration", **_info(configuration)}]}
        if path.name == "run.json" and "requested_config" in value:
            value = {**value, "requested_config": {**value["requested_config"], "model": f.config["model"]},
                "observed_config": {**value["observed_config"], "model": f.config["model"], "client_resolved": f.config["model"],
                                    "client_resolved_reasoning": {"effort": f.config["reasoning_effort"], "context": f.config["reasoning_context"]}},
                "artifacts": [*value["artifacts"], {"id":"actual_configuration","path":"indexes/actual-configuration.json","kind":"trace"}]}
        if path.name == "payload.json" and "score.json" in value:
            score = json.loads(value["score.json"])
            run = read_json(Path(case.store.get_flow(flow_id)["state"]["imported_run"]["run_path"]) / "run.json")
            score.update(total_score=2, execution_binding={key:run[key] for key in ("run_digest", "snapshot_digest", "input_digest", "evaluation_digest")},
                experiment_validity={"local_profile":"current_registered_local_profile","local_eligible":True,
                    "served_identity_unknown_disclosed":True,"substantive_disputes":[],"reason":"Exact local neutral configuration only.",
                    "evidence":[{"path":"original/actual_configuration/actual-configuration.json","locator":{"type":"json_pointer","pointer":"/requested_model"}}]},
                artifact_status={"artifacts_complete":True,"reason":"Actual neutral submission is complete.",
                    "evidence":[{"path":"original/answer/answer.md","locator":{"type":"lines","start":1,"end":1}}]})
            score["criteria"][0].update(score=2, reason="Neutral bounded partial score; no actual business finding.")
            value = {**value, "score.json":json.dumps(score)}
        return original_write(path, value)
    def prepare_first(**kwargs):
        owner.assertEqual("neutral-original", kwargs["config"]["run_id"])
        work = kwargs["runtime"] / "work";work.mkdir(parents=True)
        script = kwargs["runtime"] / "fixture-first.py"
        script.write_text("import pathlib,json\npathlib.Path('answer.md').write_text('Neutral actual answer.')\n" +
            "\n".join("print(json.dumps(" + repr(event) + "))" for event in [
                {"type":"thread.started","thread_id":"00000000-0000-4000-8000-000000000001"},
                {"type":"item.completed","item":{"type":"agent_message","text":"Neutral first answer."}}, {"type":"turn.completed"}]))
        return {"runner":{"intent_id":kwargs["config"]["intent_id"],"policy_version":definition["policy_id"],"role":"solver",
            "mode":"test_fixture","model":f.config["model"],"reasoning_effort":f.config["reasoning_effort"],"timeout_seconds":f.config["timeout_seconds"],
            "cwd":str(work),"argv":[f.config["launcher_python"],str(script)],"dependencies":[str(script)],"inputs":[],
            "request_sections":[{"kind":"operation","text":"Neutral first role, not a provider call."}],"outputs":["answer.md"]}}
    actual_enqueue = execution.enqueue_spec
    def enqueue_review(store, spec, runtime, **kwargs):
        spec = copy.deepcopy(spec)
        spec["runner"]["policy_version"] = spec["work"]["policy_id"]
        return actual_enqueue(store, spec, runtime, **kwargs)
    with patch("taskflow.execution_experiment._packet",return_value=packet), \
         patch("taskflow.execution_experiment.prepare_formal",side_effect=prepare_first), \
         patch("taskflow.execution_experiment.import_formal",side_effect=case.import_original_fixture), \
         patch("taskflow.execution_experiment.prepare_facts_runner",side_effect=case.fixture_role), \
         patch("taskflow.execution_experiment.prepare_score_runner",side_effect=lambda **kw:case.fixture_role(scoring=True,**kw)), \
         patch.object(experiments,"write_json",side_effect=write_fixture), \
         patch.object(execution,"enqueue_spec",side_effect=enqueue_review):
        record = case.advance()
        owner.assertEqual("formal_wait", record["state"]["stage"], record["state"])
        for label in ("solver", "facts", "score"):
            owner.assertIsNotNone(record["state"].get("current_work_id"),record["state"].get("exception",record["state"]["stage"]))
            execution.run_worker(f.store,"neutral-"+label,False,[record["state"]["current_work_id"]])
            record=case.advance()
            if label != "score":record=case.advance()
        owner.assertEqual("evaluation_intake",record["state"]["stage"],record["state"])
        record=case.advance()
    owner.assertEqual("formal_prepare",record["state"]["stage"],record["state"].get("exception",record["state"].get("result")))
    owner.assertEqual(1,record["state"]["run_index"])
    # Rebind the existing full zero-model proof fixture to this exact second run.
    f.config["intent_id"] = _node(record,"solver") + "-intent"
    f.binding["config_digest"] = sha256(canonical(f.config))
    write_json(Path(f.proof["intent"]["path"]),f.binding)
    token=Path(f.proof["recovery_token"]["path"]);body=read_json(token);body["binding"]=f.binding;write_json(token,body)
    retained=f.runtime.with_name(f.runtime.name+".failed-preparation-"+_info(token)["sha256"][:12]);f.retained.rename(retained)
    f.proof["previous_runtime"]["retained_path"]=str(retained);f.proof["recovery_token"]=f.ref(token)
    accepted=read_json(Path(f.proof["acceptance"]["path"]));accepted["preparation_binding"]=f.binding;write_json(Path(f.proof["acceptance"]["path"]),accepted)
    f.proof["acceptance"]=f.ref(Path(f.proof["acceptance"]["path"]));f.proof["binding"]=f.binding;f.proof["intent"]=f.ref(Path(f.proof["intent"]["path"]))
    write_json(f.proof_path,f.proof);f.recovery=f.ref(f.proof_path)
    state={**record["state"],"status":"exception","exception":{"stage":"formal_prepare","reason":"existing formal preparation failed; original runtime/reservation retained: neutral missing public target"}}
    record=f.store.update_flow(flow_id,record["revision"],state,{"kind":"neutral own confirmation preparation failure"});record=_receive(f.store,record)
    resolution={"action":"resume_validated_public_probe_preparation","reason":"Use only this confirmation own completed zero-model source recovery.","preparation":f.recovery,"evidence":[f.config["authorization"]]}
    return f,record,packet,resolution


class ConfirmationPreparationResumeTests(unittest.TestCase):
    def test_only_actual_completed_original_is_excluded_and_this_confirmation_own_proof_resumes_once(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


    def test_authorization_or_first_identity_changes_never_admit_a_confirmation(self):
        f,record,packet,resolution=make_confirmation_recovery_fixture(self)
        changes=[{"run_index":0},{"run_index":True},{"confirmation_authorized":False},
                 {"confirmation_basis":{"eligible":False}},{"runs":[]},{"runs":record["state"]["runs"]*2}]
        for field,value in (("solver_sid","foreign-sid"),("solver_attempt_id","foreign-attempt"),("accepted",False)):
            rows=copy.deepcopy(record["state"]["runs"]);rows[0][field]=value;changes.append({"runs":rows})
        for change in changes:
            wrong={**record,"state":{**record["state"],**change}}
            with self.subTest(change=change),self.assertRaises(TaskflowError):
                _confirmation_first_run_footprint(f.store,wrong,packet,f.candidate)
        wrong=copy.deepcopy(packet);wrong["single_confirmation"]["max_additional_attempts"]=True
        with self.assertRaises(TaskflowError):_confirmation_first_run_footprint(f.store,record,wrong,f.candidate)
        wrong=copy.deepcopy(packet);wrong["single_confirmation"]["authorized_if"]="First signed <=1/7"
        with self.assertRaisesRegex(TaskflowError,"one-confirmation authorization"):
            _confirmation_first_run_footprint(f.store,record,wrong,f.candidate)
        self.assertEqual(record,f.store.get_flow(record["flow_id"]))

    def test_actual_failed_uncertain_owner_purpose_or_version_drift_is_not_excluded(self):
        f,record,packet,resolution=make_confirmation_recovery_fixture(self)
        first=record["state"]["runs"][0]
        first_id=_node({**record,"state":{**record["state"],"run_index":0}},"solver")
        for table,identity in (("work",first_id),("attempts",first["solver_attempt_id"])):
            with f.store._db() as db:original=db.execute("SELECT data FROM "+table+" WHERE id=?",(identity,)).fetchone()["data"]
            for field,value in (("state","failed"),("state","uncertain"),("dispatch_owner","foreign-owner"),
                                ("purpose","another purpose"),("candidate_id","foreign-candidate"),("policy_id","foreign-policy"),
                                ("intent_id","foreign-intent"),("runtime_path",str(f.root/"foreign-runtime"))):
                changed=json.loads(original);changed[field]=value
                with f.store._db(True) as db:db.execute("UPDATE "+table+" SET data=? WHERE id=?",(json.dumps(changed),identity))
                try:
                    with self.subTest(table=table,field=field,value=value),self.assertRaises(TaskflowError):
                        _confirmation_first_run_footprint(f.store,record,packet,f.candidate)
                finally:
                    with f.store._db(True) as db:db.execute("UPDATE "+table+" SET data=? WHERE id=?",(original,identity))
        self.assertEqual(record,f.store.get_flow(record["flow_id"]))

    def test_unsealed_receipt_native_record_score_or_archive_drift_preserves_the_exception(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_foreign_or_current_solver_footprints_including_same_data_alias_are_never_excluded(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)

    def test_old_host_action_remains_strict_and_confirmation_recovery_cas_is_real(self):
        # The former recovery execution contract was explicitly removed.
        from tests.helpers import assert_legacy_recovery_retired

        assert_legacy_recovery_retired(self)


if __name__ == "__main__":unittest.main()

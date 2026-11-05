"""Neutral source-bound profile migration; no CLI, database of real work or inference."""

import copy
import json
from pathlib import Path
import runpy
import tempfile
import unittest
from unittest.mock import patch

from taskflow.contracts import TaskflowError, read_json, write_json
from taskflow.execution_batch import confirmation_digests, freeze_batch_plan
from taskflow.execution_batch_factory import compile_batch_configuration
from taskflow.execution_solver_profile import source_policy_id, validated_profile
from taskflow.execution_store import ExecutionStore, evidence_ref
from tests.test_execution_batch import make_confirmed_plan
from tools.build_formal_profile_adapter import build_formal_profile_adapter


class SolverProfileTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir="/private/tmp", prefix="solver-profile-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        plan, path = make_confirmed_plan(self.root / "choices")
        self.adapter = self.root / "old-adapter"
        self.adapter.mkdir()
        (self.adapter / "runtime_gate.py").write_text('''import json
from pathlib import Path
HERE = Path(__file__).resolve().parent
REQUESTED_MODEL = 'old-profile'
def validate_configuration(plan):
    argv = plan['argv']
    return argv
def matches(observation):
    return (True
            and observation.get('model') == REQUESTED_MODEL
            )
''')
        (self.adapter / "prepare_diagnostic.py").write_text('''def plan():
    return {'reasoning_setting': 'not_explicitly_specified',}
def check(observations):
    if observations[0]['model'] != gate.REQUESTED_MODEL:
        raise ValueError('model mismatch')
''')
        for name in ("execute_diagnostic.py", "file_evidence.py"):
            (self.adapter / name).write_text("# Synthetic template; never executed.\n")
        write_json(self.adapter / "baseline-plan.json", {"argv":["neutral-cli","exec","-m","old-profile"]})
        self.original_adapter = evidence_ref(self.adapter / "prepare_diagnostic.py", "synthetic original adapter")
        self.original_adapter["files"] = {name:evidence_ref(self.adapter / name, "synthetic")['sha256']
            for name in ("runtime_gate.py","execute_diagnostic.py","file_evidence.py","baseline-plan.json")}
        infra = {"schema_version":1,"policy_id":"original-policy","policy":{"concurrency":2},
            "experiment_factory":{"schema_version":1,"repository":str(self.root),
                "formal_runtime_root":str(self.root / "formal"),"lifecycle_root":str(self.root / "lifecycles"),
                "publication_root":str(self.root / "publications"),"technical":{
                    "real_repository":str(self.root),"formal_config_base":{"adapter":self.original_adapter},"review_runner":{}}}}
        ip = self.root / "infrastructure.json";write_json(ip,infra)
        old = compile_batch_configuration(path,ip,self.root / "original")
        self.old = read_json(Path(old["policy"]["path"]))['arguments']
        updated = copy.deepcopy(plan)
        updated['roles']['solver']['runtime'].update(reasoning_effort='minimal',reasoning_context=None)
        updated['user_model_confirmation'].update(confirmation_digests(updated))
        up = self.root / "updated-plan.json";write_json(up,updated)
        frozen = freeze_batch_plan(up,self.root / "updated/confirmation")
        adapter = build_formal_profile_adapter(adapter_ref=self.original_adapter,snapshot_ref=frozen['snapshot'],destination=self.root / "new-adapter")
        infra['policy_id']='updated-policy';infra['policy']=copy.deepcopy(self.old['config'])
        infra['experiment_factory']['technical']['formal_config_base']['adapter']=adapter
        nip=self.root/'updated-infrastructure.json';write_json(nip,infra)
        new = compile_batch_configuration(up,nip,self.root/'updated')
        self.new = read_json(Path(new['policy']['path']))['arguments']
        self.store = ExecutionStore(self.root/'state.sqlite3')
        self.store.register_policy(self.old['policy_id'],self.old['config'])
        self.store.register_policy(self.new['policy_id'],self.new['config'])
        packet={'schema_version':1,'kind':'future_solver_profile_update_v1','changed_role':'solver','task_id':'neutral-task',
            'source_policy_record':old['policy'],'target_policy_record':new['policy'],
            'source_factory':old['experiment_factory'],'target_factory':new['experiment_factory']}
        self.packet=self.root/'profile-update.json';write_json(self.packet,packet)
        self.ref=evidence_ref(self.packet,'synthetic explicit solver-only update')
        self.old_factory=read_json(Path(old['experiment_factory']['path']))

    def test_generated_argv_and_capture_gate_require_explicit_selected_effort(self):
        gate=runpy.run_path(str(self.root/'new-adapter/runtime_gate.py'))
        baseline=read_json(self.root/'new-adapter/baseline-plan.json')
        self.assertEqual('other-provider/solver',gate['REQUESTED_MODEL'])
        self.assertIn('model_reasoning_effort="minimal"',baseline['argv'])
        good={'argv':baseline['argv'],'reasoning_setting':'explicit','request_reasoning_override':'minimal'}
        gate['validate_configuration'](good)
        with self.assertRaises(ValueError):gate['validate_configuration']({**good,'request_reasoning_override':'deep'})
        self.assertTrue(gate['matches']({'model':'other-provider/solver','reasoning':{'effort':'minimal'}}))
        self.assertFalse(gate['matches']({'model':'other-provider/solver','reasoning':{'effort':'deep'}}))

    def test_new_experiment_source_policy_is_exact_and_old_definition_stays_unchanged(self):
        profile=validated_profile(self.ref)
        definition={"task_id":"neutral-task","policy_id":"updated-policy",
            "factory_config_ref":profile["packet"]["target_factory"],
            "technical":copy.deepcopy(profile["factory"]["technical"]),"solver_profile_update":self.ref}
        original=copy.deepcopy(definition)
        self.assertEqual("original-policy",source_policy_id(self.store,definition))
        self.assertEqual(original,definition)
        with self.assertRaisesRegex(TaskflowError,"experiment differs"):
            source_policy_id(self.store,{**definition,"task_id":"foreign-task"})
        with self.assertRaisesRegex(TaskflowError,"experiment differs"):
            source_policy_id(self.store,{**definition,"policy_id":"original-policy"})
        self.assertEqual("updated-policy",source_policy_id(self.store,{"policy_id":"updated-policy"}))

    def test_normal_factory_transports_explicit_source_update_without_starting_consumer(self):
        from taskflow.execution_experiment_factory import start_experiments_from_packet

        profile=validated_profile(self.ref)
        packet=self.root/"experiment-business.json"
        write_json(packet,{"schema_version":1,"controller":"neutral-owner","batch_id":"neutral-batch",
            "solver_profile_update":self.ref,"items":[{"task_id":"neutral-task","source_author_quality_flow":"neutral-source"}]})
        captured=[]
        def register(store,definition,evidence):
            captured.append(copy.deepcopy(definition))
            return {"flow_id":definition["flow_id"],"revision":1}
        with patch("taskflow.execution_experiment_factory.register_lifecycle",side_effect=register), patch(
                "taskflow.execution_experiment_factory.launch_lifecycle_consumer",side_effect=AssertionError("no model start")):
            result=start_experiments_from_packet(self.store,packet_ref=evidence_ref(packet,"synthetic business packet"),
                factory_config_ref=profile["packet"]["target_factory"],allow_model=False)
        self.assertFalse(result["attention"])
        self.assertEqual(0,result["consumers_started"])
        self.assertEqual(1,len(captured))
        self.assertEqual("updated-policy",captured[0]["policy_id"])
        self.assertEqual(self.ref,captured[0]["solver_profile_update"])
        self.assertEqual("original-policy",source_policy_id(self.store,captured[0]))
        self.assertEqual([],self.store.list_flows())

    def test_non_solver_factory_additions_deletions_and_changes_are_rejected(self):
        packet=read_json(self.packet)
        original=read_json(Path(packet["target_factory"]["path"]))
        variants=[]
        changed=copy.deepcopy(original)
        changed["technical"]["review_runner"]["request_sections"]=[{"kind":"operation","text":"NEW_OPERATION"}]
        changed["technical"]["review_runner"]["native_permissions"]={"filesystem":{":root":"write"}}
        variants.append(changed)
        changed=copy.deepcopy(original);changed["technical"]["extra_tool"]={"unbounded":True};variants.append(changed)
        changed=copy.deepcopy(original);changed["repository"]="/different-repository";variants.append(changed)
        changed=copy.deepcopy(original);del changed["technical"]["review_runner"];variants.append(changed)
        changed=copy.deepcopy(original);changed["technical"]["local_profile_policy"]={"new_rule":"changed"};variants.append(changed)
        changed=copy.deepcopy(original);changed["new_root_capability"]=True;variants.append(changed)
        for index,factory in enumerate(variants):
            with self.subTest(index=index):
                path=self.root/("changed-factory-"+str(index)+".json");write_json(path,factory)
                proof=self.root/("changed-proof-"+str(index)+".json")
                write_json(proof,{**packet,"target_factory":evidence_ref(path,"synthetic changed factory")})
                with self.assertRaises((TaskflowError,KeyError)):
                    validated_profile(evidence_ref(proof,"synthetic broadened source-bound update"))

    def test_adapter_tampering_is_rejected_without_new_registration(self):
        path=self.root/'new-adapter/runtime_gate.py';path.write_text(path.read_text()+'\n# changed\n')
        with self.assertRaisesRegex(TaskflowError,'deterministic profile'):
            validated_profile(self.ref)
        with self.store._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='solver_profile_updates'").fetchone())

    def test_unsupported_template_creates_no_output(self):
        path=self.adapter/'runtime_gate.py';path.write_text(path.read_text()+'\nREQUESTED_MODEL = "other"\n')
        # Preserve exact new source reference to exercise template validation rather than hash drift.
        source=copy.deepcopy(self.original_adapter);source['files']['runtime_gate.py']=evidence_ref(path,'synthetic')['sha256']
        snapshot=self.new['config']['batch_snapshot'];destination=self.root/'unsupported'
        with self.assertRaisesRegex(TaskflowError,'one original model declaration'):
            build_formal_profile_adapter(adapter_ref=source,snapshot_ref=snapshot,destination=destination)
        self.assertFalse(destination.exists())

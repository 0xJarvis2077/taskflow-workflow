"""Future operation order and exact diff guidance, without a new execution gate."""

import copy
import json
import unittest

from taskflow.contracts import TaskflowError
from taskflow.execution_flow_contracts import (
    public_first_instructions, request_has_native_signoff_protocol, review_instructions,
)


class ReviewInstructionTests(unittest.TestCase):
    def setUp(self):
        self.context = {"task_id": "neutral", "candidate_id": "neutral-current", "intent_id": "neutral-quality",
            "content_digest": "1" * 64, "input_digest": "2" * 64, "evaluation_digest": "3" * 64,
            "defect_ids": ["neutral-defect"], "author_history": "NEVER_DUMP_AUTHOR_CONTEXT",
            "public_first": {"index": "execution/public-inputs.json", "checkpoint": "public-responsibility-first.json",
                             "input_digest": "2" * 64}}

    def test_public_only_checkpoint_and_actual_tool_response_precede_teacher_phase(self):
        before = copy.deepcopy(self.context)
        request = public_first_instructions(self.context)
        phases = ["阶段1：仅读公开原件", "阶段2：保存独立责任检查点", "阶段3：真实工具打印检查点摘要", "阶段4：随后读取教师"]
        self.assertEqual(sorted(request.index(value) for value in phases), [request.index(value) for value in phases])
        for value in ("PUBLIC_ROLES", "execution/public-inputs.json", "先不读完整 execution/task.json", "scoring", "旧审查", "监控",
                      "来源适用性", "上游已完成", "业务后果", "必要条件", "合法分支", "public-responsibility-first.json",
                      "PUBLIC_RESPONSIBILITY_FIRST_SHA256=<实际SHA256>", "可见工具响应", "trace/item/行号",
                      "不是新模型、审批或全局passed门", "不能事后补打印", "intake.md", "issues.json", "adoption-readiness.json"):
            with self.subTest(value=value):
                self.assertIn(value, request)
        self.assertNotIn("NEVER_DUMP_AUTHOR_CONTEXT", request)
        self.assertEqual(before, self.context)

    def test_whole_report_keeps_native_protocol_and_references_observed_sequence(self):
        rendered = review_instructions(self.context, "whole_package")
        self.assertTrue(request_has_native_signoff_protocol(rendered))
        for value in ("public-responsibility-first.json", "PUBLIC_RESPONSIBILITY_FIRST_SHA256", "随后读取教师", "不靠hash", "收窄/inconclusive"):
            self.assertIn(value, rendered)
        routing, _ = json.JSONDecoder().raw_decode(rendered[rendered.index("{"):])
        self.assertEqual(self.context["intent_id"], routing["intent_id"])
        self.assertEqual(self.context["input_digest"], routing["input_digest"])
        legacy = {key: value for key, value in self.context.items() if key != "public_first"}
        self.assertIn("不凭空宣称已有公开先行顺序锚点", review_instructions(legacy, "whole_package"))

    def test_observed_attempt_is_distinguished_from_candidate_repair_scope(self):
        for scope in ("whole_package", "affected"):
            rendered = review_instructions(self.context, scope)
            for value in ("独立试解观察，不是当前题包文件", "不得把它们作为阻断问题的affected_paths",
                          "试解出错本身不证明题包有缺陷", "非阻断claim", "当前题包或绑定元数据依据",
                          "revision_required的阻断清单", "status=open_nonblocking",
                          "无法确定真实归属或必要依据时签inconclusive"):
                with self.subTest(scope=scope, value=value):
                    self.assertIn(value, rendered)

    def test_affected_review_reads_historical_parent_and_complete_actual_changes(self):
        context = {**self.context, "repair_diff": {"index": "execution/repair-diff.json", "available": True,
                    "parent_candidate_id": "neutral-parent", "candidate_id": "neutral-current"}}
        rendered = review_instructions(context, "affected")
        for value in ("execution/repair-diff.json", "parent_binding/current_binding", "before_staged_path", "current_staged_path",
                      "kind=history", "不是当前teacher规范", "old_sha256/new_sha256", "old_role/new_role",
                      "defect_resolution.evidence", "题包相对path", "当前sha256", "非空location", "defect_resolution.affected_paths",
                      "coverage.changed_files", "新增的old为null", "删除的new为null", "仅记provenance", "不能升级为新整包签发"):
            with self.subTest(value=value):
                self.assertIn(value, rendered)
        self.assertNotIn("PUBLIC_RESPONSIBILITY_FIRST_SHA256", rendered)
        self.assertNotIn("public-responsibility-first.json", rendered)
        self.assertTrue(request_has_native_signoff_protocol(rendered))

    def test_unavailable_parent_does_not_request_fabricated_old_sha_or_closure(self):
        context = {**self.context, "repair_diff": {"index": "execution/repair-diff.json", "available": False}}
        rendered = review_instructions(context, "affected")
        self.assertIn("available=false", rendered)
        self.assertIn("不要求伪造old/new字段", rendered)
        self.assertIn("必要证据不足时签inconclusive", rendered)
        context.pop("repair_diff")
        self.assertIn("未提供actual repair_diff索引", review_instructions(context, "affected"))

    def test_index_paths_and_public_digest_are_bound_without_dumping_context(self):
        for key, value in (("index", "../unsafe.json"), ("checkpoint", "/absolute/checkpoint.json"),
                           ("input_digest", "f" * 64), ("input_digest", "short")):
            context = copy.deepcopy(self.context)
            context["public_first"][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(TaskflowError):
                public_first_instructions(context)
        with self.assertRaises(TaskflowError):
            review_instructions({**self.context, "repair_diff": {"index": "execution/diff.json", "available": 1}}, "affected")


if __name__ == "__main__":
    unittest.main()

"""The future repair request distinguishes payload changes from declaration copies."""

import json
import unittest

from taskflow.execution_flow_contracts import repair_instructions


class RepairPromptTests(unittest.TestCase):
    def test_payload_diff_excludes_only_same_byte_declaration_materialization(self):
        request = repair_instructions({"candidate_id": "neutral-parent", "revised_candidate_id": "neutral-revision",
                                       "defect_ids": ["neutral-reference-defect"]})
        example, _ = json.JSONDecoder().raw_decode(request[request.index("{"):])
        self.assertEqual("neutral-parent", example["parent_id"])
        self.assertEqual("neutral-revision", example["candidate_id"])
        self.assertEqual(["neutral-reference-defect"], example["defect_ids"])
        self.assertFalse(example["needs_business_decision"])
        self.assertIn("candidate.files 清单中真实 payload", request)
        self.assertIn("新增/修改/删除及角色归属变化", request)
        self.assertIn("未改变的文件不列", request)
        self.assertIn("锁定的 execution/task.json", request)
        self.assertIn("同字节物理化", request)
        self.assertIn("不是 candidate.files 中声明的 payload", request)
        self.assertIn("单独 provenance 字段", request)
        self.assertIn("不列入 changes", request)
        self.assertIn("不以 before_sha256=null", request)
        self.assertIn("交新独立复检", request)


if __name__ == "__main__":
    unittest.main()

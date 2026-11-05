# 主控接收与下一投入（现有 record_decision）

填写真实原件引用和已确认授权后，保存本地 JSON，经 `execution apply --record` 登记。模板占位不能运行；不加入公开作答输入。

```json
{
  "operation": "record_decision",
  "arguments": {
    "decision_id": "replace-decision-id",
    "task_id": "replace-task-id",
    "candidate_id": "replace-candidate-id",
    "action": "experiment",
    "rationale": "主控选择及具体依据",
    "evidence": [],
    "experiment_purpose": "与 solver purpose 完全相同的本次用途",
    "authorized": false,
    "intake": {
      "purpose_kind": "formal_experiment",
      "quality": {"conclusion": "unknown", "evidence": [], "limitations": "真实独立原签及适用范围"},
      "difficulty": {"status": "unknown", "conclusion": "目前证据及重要未知", "evidence": [], "limitations": "实际观察、原因假设、未覆盖部分分开"},
      "experiment": {
        "important_unknown": "本次调用要解决的重要未知",
        "outcomes": [
          {"observation": "结果一", "next_action": "retain", "reason": "对应后续选择及原因"},
          {"observation": "结果二", "next_action": "research", "reason": "怎样改变下一投入"}
        ]
      }
    }
  }
}
```

非正式实验明确选择其他 purpose_kind、原有 action，并将 experiment 设 null。质量结论、难度证据、下一投入分别留证。正式实验须真实 accepted quality 和授权；不能从占位或预选推断同意。证据为普通文件 path、SHA256、location，支持 size。

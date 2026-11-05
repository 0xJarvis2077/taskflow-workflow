# 明确材料修订派工

本模板用于现行`execution revision submit --spec`，不恢复旧author/lifecycle。主控填写业务目的及投入选择，工程只补确切运行绑定。原请求、原卷、原评和原分不改。

definition保留现有flow_id/task_id/owner/policy_id/directory/candidate_id/candidate_root/revised_candidate_id，并补：

```json
{
  "purpose": "当前要解决的具体业务问题及用途",
  "purpose_kind": "mechanism_diagnosis",
  "author_id": "本题稳定逻辑作者ID",
  "source_revision_task_id": "确实产出当前父版的原修订任务ID",
  "expected_changes": [{"path": "实际相对路径", "reason": "为什么需要改"}],
  "responsibilities": [{"id": "责任ID", "description": "主要业务责任及现存未知", "paths": ["关联完整业务单元"]}],
  "diagnosis": {"mode": "post_run", "history_inventory": [
    {"source_role": "code", "status": "does_not_exist", "reason": "原运行没有代码交付"}
  ]},
  "sources": [],
  "runner": {"role_session": {
    "mode": "resume_existing", "home": "/原实际专属home",
    "previous": {"path": "/原调用/runtime/collection.json", "sha256": "原件SHA", "size": 0, "location": "精确原作者终态"}
  }}
}
```

示意路径和摘要必须替换；其余原生运行字段遵守[修订合同](../../docs/material-revisions.md)。purpose_kind四选一：quality_repair/mechanism_diagnosis/business_reconstruction/formal_experiment。sources逐项用既有evidence path/SHA/location（可加size），明确source_role及history/下target；覆盖最近相关请求、工具调用和返回、代码、中间成果、完整终稿、封存事实、原评。优先引用原归档，不复制历史正文进请求。缺失保持unknown；已归档但未提供则history_inventory列archived_not_provided并给originals精确引用，不能填不存在。

不能合法resume时改fresh_persistent，移除source_revision_task_id，previous=null，并填写session_reason。首次initial_author只适用没有已登记或external父作者的材料；其余用original_unrecoverable/original_ephemeral/author_changed，均须session_reason_detail实际原因。original_ephemeral还须有精确captured父作者manifest证明原mode确为fresh_ephemeral；不能用该标签代替缺失来源。不得用latest、别的角色SID或拼接轨迹冒充原上下文。

作者接收请求时读取当前revision-brief/original-index、真实差异和当前受影响完整业务单元。repair-record沿现有changes/declaration_change/impact_analysis，再逐责任补：

```json
{
  "responsibility_analysis": [{
    "id": "责任ID", "result": "success",
    "public_work": {"status": "supported", "summary": "公开依据已完成哪些工作", "evidence": [{"target": "parent/实际文件", "location": "L/B或业务单元位置"}]},
    "processing": {"status": "supported", "summary": "可见实际处理如何完成这项责任", "evidence": [{"target": "history/实际工具轨迹", "location": "实际返回及采用位置"}]},
    "final_result": {"status": "supported", "summary": "完整终稿的实际结果", "evidence": [{"target": "history/实际终稿", "location": "实际位置"}]},
    "business_consequence": {"status": "supported", "summary": "结果怎样改变业务选择或交付", "evidence": [{"target": "parent/实际依据", "location": "实际位置"}]},
    "next_action": {"disposition": "retained", "reason": "正常方法仍承担哪些主要责任以及为何保留", "changed_paths": []}
  }]
}
```

每项result可为success/failure/mixed/unknown，未知不补造。changed须列真实变更路径，retained/unknown为空。成功终稿有证但过程缺失时保留成功结果与processing unknown。另须success_method_coverage={status:supported/unknown,summary,covered_responsibilities:[文字说明],evidence:[{target,location}]}，即使本次只委托返修失分责任，也分析正常成功与合法短法仍覆盖哪些主要责任；证据缺失保留未知，不要求每版使旧方法失效。source_usage如实引用实际trace行及analysis_ids，不能把文件齐全声称已使用。主控接收质量、难度和下一投入三项；正式实验还须说明重要未知及不同结果如何改变下一选择，质量通过不自动投测。

# 独立质检任务

质检入口处理一个明确的独立角色任务，复用 SQLite 工作队列和 Codex 载体。Python 不安排后续返修或复检，也不根据金额、措辞或模型结论推导新的研发任务。

## 命令

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 quality submit --spec /absolute/quality-task.json
python3 -m taskflow execution --db /absolute/state.sqlite3 quality prepare quality-id
python3 -m taskflow execution --db /absolute/state.sqlite3 quality run quality-id --allow-model
python3 -m taskflow execution --db /absolute/state.sqlite3 quality collect quality-id
python3 -m taskflow execution --db /absolute/state.sqlite3 quality status quality-id
python3 -m taskflow execution --db /absolute/state.sqlite3 quality correct quality-id --record /absolute/correction-evidence.json
```

`submit` 只登记；`prepare` 锁定请求、资料与运行配置；`collect` 观察和接收原尝试；`status` 只查询。`run` 最多执行当前明确工作项一次，真实模型必须带 `--allow-model`。`correct` 只登记下一次有界纠错，不派发；执行该次纠错仍使用 `run`。

当执行已完成而交付不合法时，`collect` 和 `run` 返回结构化结果及退出码 2。合法的 `revision_required` 或 `inconclusive` 是正常角色结论，退出码 0，不自动创建返修、试解或下一质检任务。

## 明确任务说明

`--spec` 使用对象 `{"definition": {...}, "evidence": [...]}`。definition 提供 `schema_version=1`、`flow_id`（本次质检任务的稳定 ID）、`task_id`、`owner`、`policy_id`、绝对 `directory`、`candidate_id`、绝对 `candidate_root`、`purpose`、`scope=whole_package`或`affected`、明确已完成的 `predecessor_attempt` 及其 `predecessor_archive`、`runner` 和 `correction_limit`。

runner 使用本批已确认的模型、推理设置、CLI/provider、权限和角色输入；不会从其他批次补默认模型。前驱答卷和运行记录按实际原归档绑定，候选、参考及规则按当前版本绑定。未经来源核验的任意观察文件不能加入输入。

`correction_limit` 默认为 0；有明确授权时才设置纠错额度。每次新增 Codex turn 都是可计费调用，与 Codex 内部网络重连分别计数。注册和准备不会替用户授予费用或资料外发许可，实际系统审批仍独立适用。

默认独立整包质检；scope=affected使用明确修订来源和覆盖合同。coverage显式提供revision_task_id、parent_quality_task_id、inherited_paths、recheck_paths、business_units、related_checks及reopened_evidence。继承与复检必须完整分区当前和删除成员，变化文件不能继承。business_units和related_checks各项为{id,responsibility,paths}，正常独立质检确认完整业务单元、跨文件关联及隔离依据；不能隔离时另提交整包任务。新证据各项为{evidence,responsibility,paths}，涉及的未改文件也必须进入recheck。作者建议来自修订原件，不单设范围审批角色。

父版合法业务结论为revision_required仍可作为复检的上下文和原问题来源。只有父版accepted或明确签发、各维度肯定的unchanged_scope可以提供未变范围继承；没有这类旧签时，inherited_paths必须为空，并重新检查完整当前/删除成员。程序不会把父版改为accepted。原问题、原签、实际差异、父版真实结论及新版业务责任进入同一复检请求，关闭问题还须有作者真实修订和新版独立签发。

task.json声明有变时还须coverage.declaration_check={id,responsibility,paths}，明确关联复检责任；前后声明都作为原件提供，不只给SHA。adoption-readiness原签保留revision_coverage及plan_sha256、确认范围和实检责任ID，逐business_unit_checks、related_check_evidence及declaration_check_evidence给出原件SHA与实际L/B位置；coverage.declaration_change与实际前后声明一致，并引用双方声明。真实差异、改动证据和当前规范逐一核验。旧原签保留原版本身份，程序不把affected改写成whole_package。新版solver和质量门消费同一有效来源，未闭缺陷或后发反证仍阻断。具体字段和原件使用见[材料修订](material-revisions.md)。

公开完全相同时可加public_predecessor_reuse，指向显式适用记录的evidence引用，predecessor_attempt/archive仍指原执行。记录使用既有identical_public_predecessor_reuse_v1合同，绑定controller、task_id、原predecessor身份、source_candidate_binding、target_candidate_binding、quality_transfer=false、reason和evidence；完整成员/路径/角色/SHA/size逐一相等才可复用，不转移原质量或伪造新调用。

## 会话

runner 的 `role_session` 明确指定：

```json
{
  "mode": "fresh_persistent",
  "home": "/absolute/task-owned/codex-home",
  "previous": null
}
```

整包首审使用fresh_persistent。affected子版本可使用resume_existing，并精确引用parent_quality_task_id当前完成调用的collection path/SHA/size，保留原逻辑QA、owner、policy、模型、provider、实际home及权限。准备入口从已验真的revision-diff与完整typed input差异生成quality_recheck_v1版本迁移；不直接放宽通用resume身份检查。作者、公开重建、solver或其他题目的会话不得转为QA。使用精确SID，不查全局latest，不扫描其他角色的会话。

原QA已见教师材料时，新复检明确属于informed_recheck。它保留execution/parent-public-checkpoint.json与旧问题，在当前受影响公开业务单元重读后保存新检查点；检查点与报告分别声明review_mode/mode=informed_recheck、parent_checkpoint_sha256。当前工具顺序证明当前重读，不冒称新的公开盲读。需要新盲审时另建独立fresh上下文。公开重建成果的版本适用复用仍是成果复用，不是QA会话续接。

首审提供完整质量协议；原QA业务复检和结构纠错仅追加当前身份、问题、差异、新证据及必要原件入口，不重贴整套review_instructions/native协议。所有新调用都保留同内容的execution/quality-protocol.md入口供必要时回读，续接请求仅给定位，不要求全文重读。caller自定义Skill/操作段仅在结构完全相同、且该渲染正文已出现在精确原QA锁定请求链时省去；新增或改变的段落保留，原definition仍完整保存。新affected合同只补首次需要的范围字段与引用规则，纠错沿用原范围。接收端沿精确previous collection链核验先前已实际提供的完整协议、同QA/home/SID，不把旧请求拼接进新请求。机器manifest、完整hash和归档继续保留。离线请求字节对比只证明模型可见文本变化，不等于token、缓存或实际账单节省。

同角色结构纠错在原实际上下文进行，沿用相同输入视图，新增请求仅给出当前调用身份和确定的合同错误。每次纠错都有独立 work/intent/runtime/attempt、请求和采集记录；原终态文件不重开、不覆写。ephemeral、技术失败、调用未知或业务返修结论均不能据此恢复成一份“已修正”的报告。

## 三种结果

返回的 `state` 分别表示：

- `execution`：是否启动、运行、技术完成或失败/未知。
- `delivery`：交付是否待收集、合法或不合法。
- `outcome`：原角色签发的业务结论。

交付不合法保留原报告和可定位错误，不能把原结论改成通过。交付合法且原签 accepted 后，仍需通过当前候选质量准入，未闭缺陷或后发冲突不会被一个新的格式检查覆盖。

公开先行、检查点实际读回、公私资料分离及独立原签继续由质检合同约束。只读共享预检给出结构/路径问题，由当前 Codex 角色独立处理；它不输出预填的业务答案，也不决定是否通过。

## 入口调整

旧 `flow-register`、`flow-run`、`flow-resume`、`flow-inconclusive-*`、`flow-rejected-quality-*` 和 `flow-repair-capacity-prepare` 不再暴露为质检命令。旧任务不做运行兼容；历史原件与状态读取仍保留。其他角色的旧编排不被这一条质检任务隐式启动。

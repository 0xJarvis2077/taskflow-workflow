# 真实角色运行的本机配置

空模板展示字段，不是可直接运行的部署配置。模型、档位、路径、身份、权限、授权和摘要必须来自本批用户选择及实际本机文件。Python核确定结构和来源，Codex提供Agent、工具和上下文能力。

## 先确认，再准备

先完成[批次模型表](../knowledge/templates/task-batch-plan.template.json)，核启用角色、用户选择范围和本机支持。不能猜模型名或从旧模板代填。事实与评分共享reviewer模型、provider、policy、home及实际SID。

使用batch-check、batch-freeze、batch-config核验、冻结并编译本批配置。保留不可覆盖的policy及真实path/sha256/location来源；null与空模板仍是未配置，不授予模型调用权。各启用角色的runtime、用户选择来源、模型与范围摘要必须对应当前批次。

## 明确角色任务

| 工作 | 当前入口与规格 |
| --- | --- |
| 材料修订 | [revision submit/prepare/run/collect](material-revisions.md)，父版与新版独立，runner和本批policy显式一致 |
| 独立质量及受影响复检 | [quality submit/prepare/run/collect](quality-tasks.md)，公开先行，独立签发；复用公开原件须核完整相同关系 |
| 独立正式作答 | [solver submit/prepare/run/collect](formal-review-tasks.md)，仅当前公开资料，明确有效quality来源与业务授权 |
| 全卷事实、封存、同会话评分 | [review](formal-review-tasks.md)，facts不见标准，封存后同actual reviewer/SID追加当前标准 |
| 新公开重建 | 底层enqueue/worker/runner及existing_isolated公开适配器，须提供完整公开输入/权限/输出与policy，不恢复旧自动父子链 |

runner明确mode、CLI/provider、model、reasoning_effort、timeout、role_session、权限和输出。角色任务派生实际输入，不接随意追加的历史材料。原生权限和当前实际工具能力仍经正常零模型预检；填配置或文件哈希不证明隔离。

run需要显式--allow-model。准备可能写任务目录和执行零模型预检，不能把它当只读预览；登记、准备或配置本身不授予付费与资料外发许可。实际系统审批拒绝不得绕过。

## 会话及资料

任务使用独立home。fresh_persistent允许保存该角色的实际SID，fresh_ephemeral结束后不能追认恢复。质量交付纠错以及facts到score仅走合法同角色/阶段通道，新turn有新work/intent/attempt；未知或失败不重放。材料继承通过独立文件副本，不依赖旧作者SID。

正式作答、公开重建和首facts不能读取作者Skill、教师参考、历史答卷、旧分或私有修订分析。质量公开先行后才读教师资料；修订角色可以按显式来源读取完整历史诊断。来源和当前规范按各自实际版本绑定，不能迁旧签为新签。

## 退役与限制

旧author-start、teacher-successor-start、batch-author-start、batch-baseline-quality-start、experiment/round和lifecycle运行/恢复CLI已移除，不做旧任务执行兼容。历史工厂和packet示例仅保留原合同及证据用途，不能作为当前可执行命令。历史状态与原件仍可只读核验。

完整不可变运行依赖bundle尚未实现；源码安装仍需全部注册执行域的实际锁与自然空闲窗口。聊天模型选择另核实际载体；请求模型不等于served身份。工具探针、中性测试、业务质量和正式成绩分别留证，不为连接检查例行调用模型。

普通native quality会追加教师资料，不能作为公开重建载体；真实public_rebuild slot对此在准备及派发前拒绝，使用现有公开隔离适配器。没有把尚未迁移的native公开专用入口当成已实现能力。

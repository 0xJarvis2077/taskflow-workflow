# 工作流知识库

本知识库收录专业业务任务设计、增难、材料审查、独立压测、完整读卷和返修方法，以及相关经验的整理与适用范围。具体题包、附件、答案、真实答卷和运行资料不在分享范围。

## 使用顺序

1. 新批次先读[协作架构与模型确认](../docs/task-collaboration-architecture.md)，确认交付范围、角色模型与本批规则。
2. 按下面的主题检索方法，不每次加载整库。设计结论须回到当前业务事实、实际公开材料与成果；经验不能代替证据。
3. 使用[来源经验索引](experience/index.md)核对每条经验的出处类型、期次和适用限制。历史阈值、模型与截止时间不能自动成为本批规则。
4. 真实运行、质量通过、目标难度与平台结果分别留证；模拟和教师推演不当作正式成绩。

## 方法

| 主题 | 入口 |
| --- | --- |
| 选岗、系列题规划与实质去重 | [方向组合](methods/topic-portfolio.md) |
| 业务任务、必要条件、公开预解题、参考与评分 | [设计与质量审查](methods/design-review.md) |
| 完整岗位任务、正常短解、增难与投入选择 | [难度设计](methods/difficulty-design.md) |
| 隔离运行、模型身份、事实封存与独立评分 | [压测](methods/stress-testing.md) |
| 全卷正确／错误／遗漏、归因和返修 | [读卷复盘](methods/post-run-review.md) |
| 历史自测方法及其适用范围 | [自测协议](methods/selftest-protocol.md) |
| 来源整理、冲突和未决项 | [材料入库](ingestion.md) |

## 模板与规则

- [完整角色交接模板](templates/role-handoffs.md)
- [单题工作记录](templates/task-record.md)
- [空白角色模型表](templates/task-batch-plan.template.json)
- [未决问题模板](open-questions.template.md)
- [规则快照使用说明](policy-notes/README.md)
- [快速工作方法](../docs/workflow-methods.md)、[简明角色边界](../docs/role-handoffs.md)
- [单题工具契约](../docs/task-contract.md)、[执行层](../docs/execution-layer.md)、[运行记录](../docs/record-examples.md)

## 本地扩展

来源原件、当前项目授权、私人规则和实际运行可在自己的工作副本内维护，并保留独立来源与权限边界。若本机存在私有 `index.local.md`，按其实际项目范围补读；它不随此知识库发布，也不替代公开方法。历史来源、他人经验、本项目口述、构造题设、建议与实测观察不能混为同一层证据。

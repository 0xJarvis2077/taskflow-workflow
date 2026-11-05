# Taskflow：专业业务任务与模型执行管理

用于设计专业业务任务、保存独立作答与评阅原件，并通过 Codex 执行相互隔离的角色任务。

Python 管理明确任务的准备、唯一领取、启动、采集和必要校验；Codex 负责阅读、推理、工具、上下文和角色内纠错。题目质量与评分由真实业务角色签发，调度成功或结构检查通过均不等于业务通过。

## 入口

| 工作 | 文档 |
| --- | --- |
| 设计与审查专业任务 | [工作方法](docs/workflow-methods.md) / [主 Skill](skills/high-value-task-workflow/SKILL.md) |
| Python 与 Codex 的职责 | [执行层重设计](docs/python-execution-redesign.md) |
| 独立质检任务与同会话纠错 | [质检执行](docs/quality-tasks.md) |
| 冻结父材料继承、针对性修订与受影响复检 | [材料修订](docs/material-revisions.md) |
| 完整复盘、主控接收与下一投入、离线决策回放 | [迭代接收](docs/iteration-intake.md) |
| 正式作答、全卷事实封存与同会话评分 | [正式与评阅](docs/formal-review-tasks.md) |
| 每轮压测计量、逐项原分与证据报告 | [压测报告](docs/stress-reports.md) |
| 角色与资料边界 | [协作架构](docs/task-collaboration-architecture.md) / [角色交接](docs/role-handoffs.md) |
| 任务建档、冻结与分包 | [单题契约](docs/task-contract.md) |
| 一题一目录、生成物归属与分类索引 | [目录规范](docs/task-layout.md) |
| 模型/provider/CLI 配置 | [运行配置](docs/runtime-configuration.md) / [Codex 角色管理](docs/codex-role-management.md) |
| 执行原语、正式作答和评阅 | [执行层](docs/execution-layer.md) / [隔离运行](docs/isolated-quality.md) |
| 答卷、原分与修订证据 | [记录示例](docs/record-examples.md) |
| 知识库 | [知识库入口](knowledge/index.md) |

## 质检链路

材料迭代使用 `revision submit/prepare/run/collect/status`：从冻结父版建立独立副本，只改需要变化的内容，程序保留未变文件并核对完整实际差异。公开成员、角色及字节完全相同可显式复用原公开重建；新版质量可由独立签发的继承范围、受影响业务单元复检和关联检查组成，solver同步消费有效来源。旧签保留旧身份，未闭阻断项仍阻止准入。详见[修订委托与合同](docs/material-revisions.md)。

同题作者修订默认沿精确父子链续接同一逻辑作者的持久会话；原 QA 可对修后子版继续受影响复检，包括原结论为 revision_required 的合法审查。版本迁移绑定原 collection、SID、owner、模型/provider/home、真实差异和资料权限；通用 resume 仍校验同版身份。不能恢复时明确记录 fresh 原因。各次调用均有独立 work/attempt 及封存记录，角色之间继续隔离。

质检是一项明确提交的角色任务。管理层分别记录执行状态、交付有效性和原角色业务结论；交付错误提供给同一 Codex 角色处理。可用的持久会话和明确纠错额度是追加调用的条件。

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 quality submit --spec /absolute/quality-task.json
python3 -m taskflow execution --db /absolute/state.sqlite3 quality prepare quality-id
python3 -m taskflow execution --db /absolute/state.sqlite3 quality run quality-id --allow-model
python3 -m taskflow execution --db /absolute/state.sqlite3 quality collect quality-id --brief
python3 -m taskflow execution --db /absolute/state.sqlite3 quality status quality-id --brief
```

登记、准备和采集不启动模型；真实执行须明确 `--allow-model`。只有原技术成功、交付不合法且会话和额度均有效，才能通过 `quality correct` 准备同审查者的下一次纠错调用。公开先行、独立性、资料权限和原签仍按质检合同核验。

`revision_required` 和 `inconclusive` 返回协调者，Python 不自动选择返修、公开重建、复检或新一轮研究。旧 author/lifecycle、`flow-*` 质量/返修循环与事故型接续命令已退出执行CLI；旧任务不做运行兼容，历史原件继续保留。

主控通过既有 `record_decision` 接收质量结论、难度证据与下一投入；新正式作答须提供重要未知及不同结果的后续选择，管理侧决定锁入调用依赖并与公开输入隔离。完整历史按来源索引提供，资料缺失保持未知；责任分析随真实作者交付接收。离线决策回放使用 `tools.execution_replay --decisions`，不启动模型。

正式作答与评阅也采用明确任务，按 `solver submit/run/collect`、`review run --stage facts`、`review seal`、`review run --stage score` 分别操作。solver 只见公开输入；reviewer 在全卷事实封存后，沿同一实际 reviewer/model/provider/home/SID 追加冻结标准评分。共享结构预检交回 Codex 自己纠错，Python不代分、不自动启动后续阶段。旧自动 experiment/round CLI 已退役。详见[正式与评阅](docs/formal-review-tasks.md)。

代码、文件哈希和分包本身不证明模型隔离或业务质量。

作答及评分终态收件会附带教师侧 `report.json/report.md` 路径，汇总实际运行计量、原评阅事实、逐项得分与明确未知项。可用 `python3 -m tools.stress_report --db /absolute/state.sqlite3 --solver solver-id --output /absolute/private-reports` 只读补生成；不重新调用模型或改变原分。

## 本地使用与验证

Python 3.10+，核心只用标准库。

```sh
python3 -m taskflow --help
python3 -m taskflow init my-task --topic my-topic
python3 -m taskflow check tasks/my-topic/packages/my-task --no-write
python3 -m unittest discover -s tests
```

测试使用中性任务、受控事件和临时目录。真实外部模型联调需要明确用途及授权，不能把模拟报告为真实调用。角色模型、档位与资料范围来自本批显式选择，不从其他批次或 CLI 默认推测。

## 目录

```text
taskflow/   任务、执行原语、Codex 载体和领域合同
tools/      离线检查、模拟与受控适配
tests/      标准库测试与中性夹具
docs/       架构、合同及使用方式
skills/     角色工作方法
knowledge/  可复用方法、经验和空模板
```

题包、私有参考、真实答卷、成绩、配置、数据库、凭据及运行痕迹保留在本机，不进入公开提交。[GitHub 同步](docs/github-sync.md)只在明确授权和配置下发布经过检查的公共文件。

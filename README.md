# Taskflow：专业业务任务出题与评测工作流

一套可在本地使用的工作方法、角色协作规范和 Python 工具，用于设计有真实业务价值的任务、审查公开材料是否提前完成关键判断、保存独立作答与评阅证据，并按实际结果返修。

难度来自证据关系、业务约束与可观察后果。材料数量、文件哈希、调度成功或低分都不能单独证明题目质量。

## 知识库

[知识库总入口](knowledge/index.md)包含完整方法、角色与单题模板、来源经验笔记、规则适用范围和材料入库方法。可复用知识随仓库分享；具体题目、附件、答案、真实答卷和成绩留在本地。

## 从这里开始

| 你要做什么 | 入口 |
| --- | --- |
| 理解出题、增难、读卷与返修 | [工作方法](docs/workflow-methods.md) |
| 组织角色、命名会话、确认每个角色的模型 | [协作架构](docs/task-collaboration-architecture.md) |
| 派发作者、审查、返修和评阅任务 | [角色交接](docs/role-handoffs.md) |
| 给 agent 加载工作方法 | [主 skill](skills/high-value-task-workflow/SKILL.md) |
| 建档、检查、冻结与分包 | [单题契约](docs/task-contract.md) |
| 保存答卷、原评与纠错记录 | [记录示例](docs/record-examples.md) |
| 独立角色、依赖与自动接续 | [执行层](docs/execution-layer.md) |
| 首次配置本机真实角色与正式适配器 | [配置说明与空模板](docs/runtime-configuration.md) |
| 准备只见公开材料的独立质量求解 | [隔离运行](docs/isolated-quality.md) |
| 分享流程与知识库、同步已验证迭代 | [GitHub 同步](docs/github-sync.md) |

## 协作方式

主控负责业务目标、投入选择和研发接续；作者设计完整业务任务和私有参考；独立角色分别进行公开重建、质量检查、正式作答与评阅。内容检查者专注公开预解题及材料声明清理，工程只在具体技术问题发生时介入。Python 负责状态、互斥、依赖和已授权节点的接续。

原卷、全卷事实和评分接收后，主控可用明确的教师后继绑定登记下一研究作者，按同一意图自动接续；已在途作者只接收其原身份。新正式实验仍需独立的具体用途与授权。绑定、恢复与执行入口见[执行层](docs/execution-layer.md)。

新批次先建立[角色模型表](knowledge/templates/task-batch-plan.template.json)。所有模型初始为空，缺项必须问用户。用户可以分别指定，也可以明确“作答用 X，其余全部用 Y”。同一已确认批次内正常迭代不反复询问；换模型、新增角色或超出确认范围时补问。

新批次使用有模型确认门的 `batch-*` 入口。可先检查空模板；退出码 2 和 `awaiting_user_models` 表示仍需用户选择，不是可调用模型的配置：

```sh
python3 -m taskflow execution batch-check --plan knowledge/templates/task-batch-plan.template.json
```

补齐本批范围、用户模型选择、实际 CLI 支持及聊天配置证据后，再按[执行层说明](docs/execution-layer.md#新批次模型确认入口)冻结确认并生成本批工厂。检查与配置编译不访问执行库、不调用模型。

会话可命名为 `出题·项目或批次·研发主控`、`出题·项目或批次·内容检查`。真实身份由角色与 thread/work/attempt/SID 的绑定确定，不能靠标题猜路由。逻辑角色不要求各自常驻一个聊天。

## 本地使用

需要 Python 3.10+。核心工具使用标准库，可直接在仓库根目录运行，无需配置模型账号即可建档、检查和执行中性模拟。

```sh
python3 -m taskflow --help
python3 -m taskflow init my-task
python3 -m taskflow check tasks/my-task --no-write
```

新建草稿有意留空业务内容和评分依据，首次检查可能失败；按[单题契约](docs/task-contract.md)补齐后，完成真实业务审查，再冻结与导出。

```sh
python3 -m taskflow freeze tasks/my-task --version v1
python3 -m taskflow export tasks/my-task/versions/v1 --audience solver
python3 -m taskflow export tasks/my-task/versions/v1 --audience evaluator
python3 -m taskflow status tasks/my-task
```

以下只运行中性模拟和离线测试，不调用真实模型：

```sh
python3 -m taskflow execution simulate --output /tmp/taskflow-demo-new
python3 -m unittest discover -s tests
python3 -m tools.validate_local
```

输出目录必须尚不存在。本机可选集成测试需要显式提供对应 adapter 或 CLI；例如 `TASKFLOW_TEST_FORMAL_ADAPTER=/path/to/adapter`。未启用时明确跳过，显式配置无效会失败。`tools.validate_local` 使用标准库 trace，报告可执行行覆盖率，不是分支覆盖率。

真实模型执行另需配置本机 CLI、权限探针、角色模型、策略和运行适配器。先完成用户模型确认、[本机配置](docs/runtime-configuration.md)及[执行层契约](docs/execution-layer.md)要求，不能把模拟运行、字段齐全或文件分包当成真实隔离、实际成绩或平台通过。

## 仓库结构

```text
taskflow/       单题契约、冻结分包、执行状态与角色接续
tools/         中性模拟、回放、库存检查与离线验收
tests/         标准库测试与中性夹具
docs/          方法、角色和命令契约
skills/        可供 agent 使用的工作流 skill
knowledge/
  methods/       设计、增难、质检、压测与复盘
  experience/    来源经验与采用范围
  policy-notes/  历史规则的适用边界
  templates/     角色、单题和空白模型配置
```

工作中的题包、真实答卷、数据库、配置和来源材料保存在本机，并由忽略规则排除提交。仓库不附带个人历史案例或现成的私人服务配置。

用户明确授权持续发布后，可配置 `tools.github_sync`，通过私有镜像同步已验证的公开文件；默认只做本地预览。具体配置、检查和失败恢复见 [GitHub 同步](docs/github-sync.md)。

目录发布与文件权限涉及操作系统能力；具体已支持的行为和限制以契约及实际探针为准。模型请求值、可证客户端配置和服务端实际身份分别记录，未知保持未知。

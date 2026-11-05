# Python 执行管理层

Python 3.10+，核心仅用标准库。管理层负责明确任务的准备、唯一领取、Codex 进程、采集和确定性校验。阅读、推理、业务判断、工具循环与角色内纠错由 Codex 完成。

当前材料修订、独立质检、正式作答和两阶段评阅分别使用 `execution revision`、`execution quality`、`execution solver`、`execution review`。旧author/lifecycle/flow执行入口已退役，不兼容旧任务运行。历史后端仅保留内部依赖与证据读取，不是隐式业务后继。

## 入口与职责

| 入口 | 作用 | 启动模型 |
| --- | --- | --- |
| `capture` | 捕获并登记实际候选字节 | 否 |
| `revision submit/prepare/collect/status` | 独立继承、准备、完整差异采集与查询 | 否 |
| `revision run` | 执行一次针对性修订 | 仅显式 `--allow-model` |
| `quality submit/prepare` | 登记明确整包质检或受影响复检，锁定来源与运行配置 | 否 |
| `quality run` | 执行当前唯一工作项 | 仅显式 `--allow-model` |
| `quality collect/status` | 接收原尝试或查询三种结果 | 否 |
| `quality correct` | 登记有额度的同审查者交付纠错 | 否 |
| `enqueue/worker` | 底层唯一派发原语 | worker 需 `--allow-model` |
| `reconcile` | 采集原进程与终态证据 | 否 |
| `status/events` | 当前投影及追加事件查询 | 否 |
| `release` | 校验质量准入并冻结、导出候选 | 否 |

参数以 `python3 -m taskflow execution --help` 及子命令帮助为准。质检 spec、明确会话与操作示例见 [独立质检任务](quality-tasks.md)。

## 质检是一个角色任务

协调者提供确切候选、用途、模型、权限、已完成的独立公开尝试、来源归档与授权证据。登记不启动模型，准备不授予费用或资料外发许可。

管理层复用同一 SQLite 队列、worker 和 runner；不再建立“质检→返修→重新试解→复检”的自动业务循环。合法 `revision_required` 和 `inconclusive` 返回协调者作下一步选择。原签缺陷保留为未闭责任，后续 accepted 报告不会自动关闭它们。

质检结果有三个独立字段：

- `execution`：未启动、运行、完成、失败或未知。
- `delivery`：待收集、合法或不合法。
- `outcome`：原角色的 accepted、revision_required、inconclusive 声明，或尚无声明。

`completed + invalid` 保留原文件与可定位结构诊断；不能当作业务失败或接受。只有交付有效、原签 accepted 且当前候选通过质量准入，才满足后续业务条件。执行失败或未知不会自动重派。

共享只读预检在模型交付前检查原签结构、调用绑定和修复目标归属。它不给出业务答案、不修改报告、不写数据库、不联网。Codex 可以在当前 turn 内根据明确错误修正自己的交付，或者保留未决。最终收件还核实际来源、SID、公开先行及原签证据，不能以预检通过替代。

## 会话与执行身份

新质检明确使用独立 task-owned Codex home。runner 支持 fresh persistent、fresh ephemeral 和精确 collection 续接；独立整包入口要求持久会话，以便必要时在同角色中纠错。

每次 `exec resume` 都创建新的 work、intent、runtime、attempt、请求和终态记录。原 SID 来自技术成功的原 collection 和 trace，不能从全局 latest、聊天显示名或调用方自填值推导。通用续接要求候选、policy、provider、模型、输入及等价权限一致；作者父子版本与原 QA 的子版复检只通过明确 role_transition 迁移，绑定真实 revision diff、同题版本关系、逻辑角色/owner及全部实际输入差异。权限和模型等稳定身份仍不能改变。

同一原 collection 只有一个后继消费；路径别名或归档副本不能制造额外额度。role home 互斥覆盖实际 CLI 生命周期；启动未知保持占用，不能靠重新准备重派。provider 与实际 `CODEX_HOME` 必须相同，模型工具不能读取 role home。

纠错只允许已完成持久会话中的确定性交付错误。原公开检查点保持原字节，新的调用签署新 intent。没有原会话、技术失败、费用或执行未知、业务返修结论、额度耗尽，均不能进入结构纠错。

业务复检使用新的 quality affected 任务，不借 correct 入口。父审查 revision_required 可以续接，但未通过范围不能继承；已经接触教师资料的 QA 标为知情复检，当前公开业务单元必须重新读取，不能宣称再次全新盲读。作者链和 QA 链的持久复用分别独立；新公开重建、正式作答、新答卷首 facts 均保持独立。详见[材料修订](material-revisions.md)、[质量合同](quality-tasks.md)和[主控接收](iteration-intake.md)。

## 原件与资料边界

候选捕获绑定公开输入、私有参考和评阅规则的实际内容。公开尝试只能看到公开资料；质检首先收到中性公开索引，保存原检查点并实际读回 SHA，之后才按锁定声明读取知情资料。

收件核原 trace 中唯一成功的检查点与教师资料输出锚及先后关系。该证据只证明可见顺序，不证明隐藏上下文或操作系统工具域隔离；报告不得把输出标记当自我认证。

独立公开尝试的全 work、scratch 和 trace 按完整归档原件运输。来源或归档发生漂移则拒绝；暂不支持的 indexed 原件显式拒绝，不能静默漏卷。

进程结束、输出文件、采集、完整归档和接收记录分别保存。纯观察或重新接收沿原 attempt，不额外调用模型。原 attempt、collection、原签和原分不可覆盖。宿主或依赖漂移不能通过改写旧 manifest 修复。

## 正式作答与评阅

正式作答只见公开输入。评阅先读实际完整答卷、运行原件和公开输入，封存全卷事实；之后由同一实际 reviewer、模型和上下文追加冻结标准评分，接收唯一原分。作者、独立试解、质检、solver 和 reviewer 的上下文保持分离。

正式作答与评阅已使用[明确角色任务](formal-review-tasks.md)，协调者显式提交真正 accepted quality 后的 solver，并在原卷完成后提交 reviewer；不是旧 parent 的自动接续。事实封存和评分分别显式请求，评分沿同实际上下文。历史任务和成绩只保留证据，不因重构重新运行。

## 已有执行原语

`apply` 接收确定类型的本地记录；`enqueue` 准备具体 work；worker 只领取符合暂停、唯一性和策略条件的项。真实调用须显式允许，授权与实际系统审批不能由本地记录自行取代。

SQLite 接收暂时繁忙时保留原终态 receipt，后续重采集接收同一 attempt；不会为了写入回执再次调用模型。不能证明进程终止或结果完整时标记未知，不假定零费用或未执行。

新批次的模型与角色选择仍走显式确认及 `batch-check/freeze/config`。新建公开重建暂无专用任务CLI，需提供完整明确公开角色spec及existing_isolated公开适配器交现有enqueue/worker；新建作者专用任务未迁移，旧author真实派发已退役，不能恢复旧链。材料继承与公开原件适用关系见[材料修订](material-revisions.md)。

不可变运行依赖包仍是后续工作。本次没有宣称活动源码可热切换：安装仍必须核实际消费者、锁与队列状态，不能取消就绪项、终止业务或伪造空窗。

## 验证

```sh
python3 -m unittest tests.test_execution_quality tests.test_execution_role_session tests.test_execution_quality_cli
python3 -m unittest discover -s tests
```

普通测试使用中性临时目录、假 CLI 与离线事件。必要的外部模型联调需明确输入、目的地、调用范围及费用授权，单独保存真实 SID 和用量证据，不能混报为业务质量或原分。

普通native quality会追加教师资料，不能作为公开重建载体；真实public_rebuild slot对此在准备及派发前拒绝，使用现有公开隔离适配器。没有把尚未迁移的native公开专用入口当成已实现能力。

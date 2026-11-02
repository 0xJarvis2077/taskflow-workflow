# 真实角色运行的本机配置

这里说明现有执行层的接入边界。空模板只展示需要填写的字段，不是可直接运行的部署配置；模型、路径、身份、授权和摘要必须来自本批用户选择及真实本机文件。核心工具及中性模拟可直接使用，真实模型调用还需要实际 CLI、权限探针和适配器。

## 先确认，再准备

先完成[批次模型表](../knowledge/templates/task-batch-plan.template.json)，核启用角色、用户选择范围和本机支持。不能把一个模型名称猜成另一个，也不能从旧模板代填。事实与评分必须共享 reviewer 模型和实际 SID。

然后准备新的不可覆盖策略和本机文件。引用文件须有实际 `path/sha256/location`；不要填写虚构 SHA 或照抄他人的机器路径。模板中的 null 和空数组仍是未配置，不能据此准备真实角色。

新批次通过 `batch-check`、`batch-freeze` 和 `batch-config` 核验并锁定配置，再使用 `batch-author-start`／`batch-experiment-start`。每个启用角色须补齐其 `runtime`、用户直接选择的原件与范围／模型摘要；CLI 角色提供实际支持观察，四个聊天角色提供真实 thread/host/model/provider 配置证据。程序只核结构化来源与字节，不解释自然语言同意，也不替用户选择或切换模型。完整字段与现有推理设置限制见[新批次入口](execution-layer.md#新批次模型确认入口)。

## 作者工厂与业务委托

[作者工厂形状](../config/examples/author-factory.example.json)的字段：

| 字段 | 含义 |
| --- | --- |
| `schema_version` | 当前格式版本 |
| `policy_id` | 本批已注册的不可覆盖角色策略 |
| `template_spec_ref` | 已审查作者运行配置的真实文件引用 |
| `role_template_ref` | 公开重建、质量、返修和复检模板的真实文件引用 |
| `research_root` / `lifecycle_root` | 本机为新工作分配的根目录 |

作者模板的 runner 至少明确 mode、role、CLI、model、reasoning_effort、timeout_seconds、cwd、native_permissions，以及类型化输入和输出。原生运行需要实际权限探针；仅把字符串填好不证明环境可用。

[业务 packet 形状](../config/examples/author-packet.example.json)一次交一个确切业务 item。`controller` 是真实协调者身份；item 绑定 task、intent、candidate、结构完整的 baseline、实际方向及相关来源；`author_work` 交代完整岗位工作、输入范围、正常对照、必交成果及权限。完整作者输出契约见[执行层](execution-layer.md)。

`init` 生成的草稿有意不完整，不能直接用作工厂要求的 baseline。首次题目须先按[单题契约](task-contract.md)完成可解析、task ID 一致的初稿，再进入受管作者研发；不能靠复制题目历史来填空。

后继模板有 `quality/repair/recheck/public_rebuild/recheck_public` 五个键，分别绑定各自运行配置。公开重建使用只见公开输入的隔离入口；配置需包含实际 prepare adapter、其 SHA、launcher Python、prepare config 与检查。其他审查保留独立上下文和真实候选绑定。

```sh
python3 -m taskflow execution batch-author-start --packet /path/to/author-packet.json --factory /path/to/batch-configuration/author-factory.json
```

省略 `--allow-model` 仍可能准备目录、执行零模型探针并入队，不是只读预览。先读实际帮助和契约，再按已有授权启动。

示例中的本批 factory 由 `batch-config` 结合已确认计划及实际基础设施生成；前述空模板说明基础设施的形状，不能直接替代带确认绑定的编译结果。业务 packet 必须携带同一 `batch_id`，题目须在用户确认范围内。

## 实验工厂与正式适配器

[实验工厂形状](../config/examples/experiment-factory.example.json)绑定 policy、repository、生命周期／正式运行／发布根目录，以及 `technical.real_repository/formal_config_base/review_runner`。

`formal_config_base` 至少包含：实际 adapter 脚本及伴随文件摘要；中性公开请求；launcher Python；已确认模型及推理／限时；必要拒读探针和输出要求。实验 packet 另须绑定 task、源作者质量 flow、运行 ID、冻结版本、实际用途与授权。评阅 runner 则按[两阶段契约](execution-layer.md)完成事实封存与原 SID 评分。

```sh
python3 -m taskflow execution batch-experiment-start --packet /path/to/experiment-packet.json --factory /path/to/batch-configuration/experiment-factory.json
```

正式桥是显式 adapter 接口。本仓库不把某个项目的题目探针、历史答卷、本机账户或固定型号适配器当成通用默认。自备适配器必须符合 `taskflow.execution_formal.prepare_formal` 的调用和回执契约；运行前的探针、输入锁定、单次启动与失败留证不能省略。

若适配器使用基线方案，[基线字段模板](../config/examples/baseline-plan.template.json)只说明 argv、隔离 root 与 observed CLI 的路径／摘要。真实 argv 必须由当前环境生成并核可见文件、能力和自动上下文；空模板不证明隔离。脚本所依赖的 Codex home、解释器、系统依赖、中性规则和拒读目标都应为本机配置，不能要求所有业务使用固定答案文件名或别人的题目。

## 当前实现限制

新批入口已将公开重建、整包质量、返修、复检、正式作答和评阅按七个 CLI 模型槽分别绑定本批确认；来源核验绑定原版本策略及确认快照，不以型号品牌替代来源身份。事实和评分仍使用同一 reviewer 配置及实际 SID。实际模型能力或推理配置不兼容时拒绝继续，不静默映射成其他角色的选择。

旧 `author-start/experiment-start` 只承接显式历史工厂；低层手工策略、旧导入和恢复 API 保留兼容，不能宣称它们全局实施了新批门禁。新的委托应走 `batch-*`，不能借兼容入口绕过确认。模型支持观察、权限探针、实际服务端身份和正式表现分别留证；中性模拟不证明某个新服务已经实测可用。

聊天模型的选择还须核实际载体配置。配置表不能自动切换已有聊天，模型请求值也不能证明服务端实际身份。新服务、费用和资料外传另按本批授权处理。

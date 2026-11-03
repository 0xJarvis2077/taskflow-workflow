# 持久执行层

Python 3.10+，标准库，无新服务或依赖。`python3 -m taskflow execution` 是显式接入入口；旧 `check/freeze/export/run/evaluation/status` 保留兼容。非夹具角色均由现有worker执行；`worker`、`flow-run`、`lifecycle-run`、`author-start`与`experiment-start`的`--allow-model`允许各自已授权范围内的调用。实际运行以不可覆盖的packet、尝试、原件与接收回执为准。

新出题批次先遵守[架构与模型确认](task-collaboration-architecture.md)：复用职责及自动接续，按批绑定角色身份；模型未经用户明确选择不准备或启动真实角色。已有批次的 policy/factory 不能当作下批默认。工程接入状态与验证证据应单列；显式 `--model`、旧 policy 一致或模型目录支持均不能代替本批用户确认。

真实服务首次接入先读[本机配置与适配器](runtime-configuration.md)，其中的空模板不代表已经完成环境验证。

## 新批次模型确认入口

新批次使用 `batch-*`。入口先核模型确认和批次范围，再创建执行库；实际 runner 在准备和派发前逐次核验，不合格准备保留具体错误而不调用模型。空模板返回 `awaiting_user_models`、逐角色缺项和退出码 2；确认范围、证据、模型或实际 runner 不匹配也拒绝继续。主控据实际缺项向用户集中确认，程序不解释自然语言同意、不选型号、不切换聊天。

```sh
python3 -m taskflow execution batch-check --plan batches/batch-id/plan.json
python3 -m taskflow execution batch-freeze --plan batches/batch-id/plan.json --output batches/batch-id/frozen-confirmation
python3 -m taskflow execution batch-config --plan batches/batch-id/plan.json --infrastructure batches/batch-id/infrastructure.json --output batches/batch-id/configuration
python3 -m taskflow execution --db execution/batch-id.sqlite3 batch-author-start --packet batches/batch-id/author-business.json --factory batches/batch-id/configuration/author-factory.json
python3 -m taskflow execution --db execution/batch-id.sqlite3 batch-experiment-start --packet batches/batch-id/experiment-business.json --factory batches/batch-id/configuration/experiment-factory.json
```

这些是填好本批真实配置与业务授权后使用的入口。`batch-check`、`batch-freeze`、`batch-config` 不访问执行库、不调用模型；后两条默认只做准备和登记，真实调用仍须原业务授权及 `--allow-model`。实验模型确认不能代替正式实验用途授权。

启动记录在空模板基础上补充以下机器字段：

- 每个启用角色保留稳定 key、`enabled` 和用户选择的 `model`。CLI 的 `runtime` 包含 `carrier=cli`、`provider`、绝对 `cli` 路径、`reasoning_selection`、`reasoning_effort`、可空 `reasoning_context`、正数 `timeout_seconds` 和 `support_evidence`。不支持 `auto/default` 或从旧模板填型号。
- `user_model_confirmation` 包含 `origin=direct_user`、带 `path/sha256/size/location` 的真实人类消息来源，以及 `scope`、`expanded_model_map`、`model_mapping_digest`、`scope_digest`。`confirmation_digests(plan)` 生成需归因给人的展开映射和范围；模型、启用角色、runtime 或范围变化必须重新确认。程序仅核结构化归因及字节，启动者仍须核原话确由用户作出。
- 主控、内容检查、工程、健康巡检的 `runtime` 使用 `carrier=chat` 和 `provider`。`role_bindings` 按角色保存真实 `thread_id/host/actual_model`、配置证据引用和 JSON pointer；证据须对应实际 ID、型号、服务和批次范围。缺证保持未知并阻止冻结；表内填值不代表聊天已切换。标题、展示名及命名模式不参与路由。
- CLI 支持证据绑定同一可执行文件、实际目录查询 argv、退出码、原 JSON 输出引用，以及同一模型条目的型号／推理能力定位。推理等级可为字符串数组，或对象数组中的 `effort`／app-server `reasoningEffort`；同一项的两个别名冲突时拒绝。默认 effort 定位支持 `default_reasoning_level`、`default_effort` 和 app-server `defaultReasoningEffort`，仍须与能力列表属于同一模型条目。保留原 JSON，不改写目录响应。真实批次要求原 CLI 观察；`test_fixture` 证据只供模拟，不能准备真实角色。模型目录支持不证明服务端实际型号，服务端未知继续保持未知。
- `verified_client_default` 的批次检查将同一模型目录条目的默认 effort 作为声明和预期，并锁定预期 context；目录不含 context 时无需伪造该字段。旧 `observation_kind` 字段可保留，但标签本身不能证明客户端解析。正式准备仍须对当前计划执行下文的固定响应解析核验，实际 model、effort、context 或请求不匹配即拒绝调用。`explicit` 模式的非空 context 仍须有同模型目录支持证据。
- 已冻结 v1 快照回读时兼容原合同允许的其他默认字段定位，但须重验原标记、同模型条目、原字符串等级及 context 证据和全部封存字节；活动计划不能通过旧标记启用兼容分支。文件及哈希不能单独证明快照的历史真实性，仍须核原策略与来源。
- 新批业务 packet 必须显式携带相同 `batch_id`，所有 `items[].task_id` 必须在确认范围。未发生的 CLI work、attempt 和 SID 保持空，不预造身份。

基础设施文件为 `schema_version=1`，含新 `policy_id`、`policy` 的并发等参数，可按需要提供 `author_factory` 和 `experiment_factory`。两类工厂沿原字段提供工具、权限、模板引用和输出根；模板仍需真实可用，不提供模型默认。编译产生不可覆盖的确认快照、`policy-record.json` 和相应新工厂，模型与推理参数只来自本批确认。快照留存来源证据字节，后续原路径或模板变化不改已锁定请求；同 ID 的不同策略拒绝覆盖，同批相同策略可幂等复用。

底层 `quality` 运行器通过独立 `model_slot` 区分 `public_rebuild`、`quality`、`recheck`；返修单列 `repair`。整包签发取 `quality`，受影响复检取 `recheck`，公开重建不能签知情质量结论。`review-facts` 与 `review-score` 共用 `reviewer` 的原策略、型号、推理设置及同一实际 SID，先封存事实再交参考，换模型或换确认绑定均拒绝。来源认证核原版本策略／快照和完整原件链，不查最新策略，也不将型号品牌当作来源身份证明。

准备和实际派发都校验原确认；独立 supervisor 同时校验锁定请求、worker、快照及捕获证据的依赖摘要。当前 native 和公开重建适配器只支持显式 effort、空 context；不支持的推理配置直接拒绝。正式作答继续核其隔离适配器实际计划与客户端观察，不静默改服务或型号。

现有 `author-start/experiment-start` 保留当前已授权历史批次的显式工厂兼容，`--factory` 已无默认；已登记 `flow-run/lifecycle-run`、旧策略、冻结请求和原 SID 沿原身份恢复。手工 legacy policy、低层 API 和旧外部导入仍是历史兼容界面，不能宣称这些路径已经全局实施新批确认；未来新批走上述 `batch-*` 入口。批次治理、用户来源与模型表留在内部，不加入 solver 输入。

## 状态与文件归属

`execution/state.sqlite3` 是本执行层唯一可修改的调度当前源；SQLite事务保存策略、候选、工作意图、尝试、审查、缺陷、决定、所有权与事件。`execution/dashboard.md` 是可重建投影。原题、冻结、大文件、原卷和评分继续使用既有文件体系；历史JSON只读导入观测，不迁移覆盖。旁路观测明确保留来源时点、采集时点、来源字节及原主控，不等于领取或接管。迟到的旧来源不能覆盖新来源，实际受管尝试优先。

一次工作项只对应一个意图和一次尝试；独立同版确认采用新的工作项/意图。领取、启动、存活、模型完整响应、产物完整、技术验收、业务质量、实验有效性、难度和平台状态分别保存。未决尝试占用并发；领取到期不释放为新调用。SQLite与外部进程不构成原子事务，不承诺 exactly-once。

每题作者/返修自动取得修改者互斥；审查与作答使用捕获副本。公开件改变后，旧审查不能转移到新候选；新问题可重开同字节结论。缺陷修订需实际材料或私有依据变化，按缺陷对象及必要关联独立复检后才能关闭。保留原整包签发与当前受影响复检时，程序只可按真实改动、未变字节和已闭责任组合覆盖，不把受影响签发改称新整包审查。合法上游成果保留后的声明调整单独记载，不能冒称材料已移除或难度已提高。程序不判断证据的业务充分性。

## 接收已接受版本上的新公开问题

控制器已明确接收新的公开问题时，可在原作者质量链的自然结束点追加，不修改原独立角色的签发或原结果：

```sh
python3 -m taskflow execution --db execution/batch-id.sqlite3 controller-finding-reopen --record controls/controller-finding.json
```

记录参数为 `parent_flow_id`、`controller`、`finding_ref`、三条原 flow 的预期 revision、`expected_candidate_binding` 五字段和 `expected_review_id`。`finding_ref` 指向控制器已接收的原报告，必须带源 flow、候选、实验、明确处置与归属，且 `exact_public_findings` 逐项绑定当前公开件路径、SHA 和真实原句。程序只核来源、范围、字节和状态，不代控制器裁定业务。

入口要求原 parent/child 已 accepted 且没有在途角色、实验仍处初始零派发边界、确切未来 solver 工作项已 pause；同时取得原生命周期与工作流消费者的现有非阻塞锁。一次 SQLite 事务登记带 `finding_origin` 的新 material defect，并以双 CAS 将原 parent/child 交回普通 repair→公开重建→整包复检。旧 accepted review 保留原义，新发现单独归控制器；旧 `result.json` 保留，后续结果追加 `result-<hash>.json`。精确同次重入幂等，旧 accepted 游标须匹配原 receipt、状态摘要和真实重开事件，不能覆盖当前流程。

该操作不清实验异常、不解除 target pause、不启动模型。只做运输修复时仍须独立保留真实业务待修；消费者启动前须核原范围与唯一归属。历史负评只能以它自己的实际修订、独立闭合、原事件顺序和完整祖先副本重新核验；当前新 whole 或 affected 签发不能伪充旧修订，受影响覆盖也不能升为整包。正式作答仍等待实际修复、独立质量及控制 owner 的明确闭合。

## 命令

在仓库根目录查看实际帮助、注册只对新工作生效的版本策略、生成当前看板：

```sh
python3 -m taskflow execution --help
python3 -m taskflow execution --db /private/tmp/new-execution.sqlite3 apply --record /path/to/policy-record.json
python3 -m taskflow execution status --format markdown --output execution/dashboard.md
python3 -m taskflow execution events
```

策略ID不可覆盖；重复注册会拒绝。政策变化另建新ID，旧请求和在途锁不改。模型及推理设置须由所选CLI随附目录证明本地支持；请求、客户端可证配置、服务端身份分别记录。服务端未知保持未知。

中性全流程只创建新目录，不调用模型，不登记真实成绩：

```sh
python3 -m taskflow execution simulate --output /private/tmp/new-taskflow-simulation
python3 -m tools.validate_local
```

`apply --record` 接收 `{"operation": "...", "arguments": {...}}`，仅允许帮助所列执行层操作白名单。参数以 `ExecutionStore` 的公共签名为准，所有证据必须有真实文件、SHA与位置。生产候选用 `capture TASK --candidate-id ID` 从 `check_task(write_report=False)` 取得实际摘要；已完成外部作者迁移用 `--external-author-sid/--external-author-trace` 绑定原CLI完成轨迹，不能重新冒认成执行层发起的尝试。

已有独立质量使用`import_external_review`导入原签发来源，核真实CLI、配置、归档索引、签署时序、当前冻结指纹和已声明的关联复用。受影响复检不追认为新全包独立审查；只在同冻结范围承接既有组合证据，不制造engine attempt或重复调用质量模型。新建受管修订均核作者实际交付，不仅缺陷返修时才核；旧审查不能换ID重新盖章。

`enqueue --spec SPEC --runtime NEW_DIR` 接收 `work` 与 `runner`。work需工作项/题目/候选/策略/意图/角色/用途；已观测在原主控下的题目，还须匹配范围交接回执及 `dispatch_owner`。runner需同意图、角色、策略，明确模型、推理、单次限时、工作目录、类型化输入/请求段落和必交成果。完成准备不会调用模型。

成果可声明具体`outputs`，或`output_trees`目录。后者保持题面允许的任意文件名，至少有一份非空普通文件；树内合法空辅助文件可保留。目录为空、符号链接或特殊文件均不算交付完成。采集封存实际成员及字节；中断后的新增、删除或改变不得混入原成果。

worker每次最多领取一项，多个独立worker可并行，数据库负责并发与写者互斥。以下会真实启动已就绪的非夹具角色，只在交接归属及既有业务用途已满足后执行：

```sh
python3 -m taskflow execution worker --worker-id local-01 --allow-model
```

没有就绪工作时返回waiting。缺权限开关、配置漂移或启动不明会保留该尝试待核证，不自动退回队列。`release TASK --candidate-id ID --version VERSION` 核当前摘要、独立质量与未闭缺陷，复用既有冻结和分包；发布后再核摘要，冲突保留原件并拒绝后续导出。

## 运行器及资料边界

`execution_runner.py`准备独立请求、复制自身监督进程、锁CLI/解释器/输入/依赖，启动后实时回传PID与启动身份，保存SID、完整trace及不可覆盖回执。非做题角色按本批授权读取项目；工作目录与产物单独归属。正式solver禁止直接用普通native模式，必须桥接既有隔离运行器的实际probe、validate、冻结及授权记录。已有质量和正式运行器均有桥接契约；未执行真实正式桥调用不能宣称服务环境已验证。

native角色必须显式提供`native_permissions`，经`probe_native_attempt`核项目只读、自有工作及scratch可写后才能运行；不沿默认workspace-write的受限网络。模型请求保留live web与managed proxy，原生裸TCP/UNIX直连按该CLI边界拒绝，不能把两者混为禁网或为过探针扩大权限。当前实际权限探针不等于真实外网调用已验证。

正式桥支持显式推理设置，或`verified_client_default`。后者只认可同argv本机固定响应捕获的模型、effort/context、请求与配置绑定；没有实际解析证据就拒绝，不用默认名或模型自报补证。每个角色的模型及推理设置按其已确认的版本策略保存，不将某一批次的配置设为全局默认。请求参数、客户端解析和真实服务端身份分列。

运行器须逐项核当前系统所需的只读依赖及实际自动上下文，不能以关闭搜索或跳过发现开关推断原件未进入上下文。固定响应、权限、工具和正式运行的证据分别保存；一次探针不能证明所有调用或未来配置都已验证。

作答请求只接public/operation；首评只接public/submission/trace/operation。teacher、target、history类型会拒绝。类型标签是可信交接契约，不能识别将教师内容谎标为operation的恶意操作者，也不是无泄题证明。仍须审计实际请求、自动上下文和可见读取。普通角色不套solver文件拒读模板。

第二阶段必须有第一阶段完整成果封存，并使用相同SID；事实原件、封存摘要、同版参考与规则锁定。评分产物和下一投入决定分别验收。高分、低分或环境失败均不自动授予改题重跑权限；solver派发须有同候选、有用途、有授权的实验决定。packet可事先明确授权同版单次确认；只有实际原签分和全部已登记的配置、有效性及成果条件均满足，才按新意图自动接续该次确认，未决条件不当作通过。

独立质量可通过已封存的`native_quality_handoff`明确哪些私有文件属于当前规范；角色标签为answer/reference的作者观察不因此自动成为评分标准。评分准备仅在typed`normative_certification`的当前候选三摘要、真实受管独立来源、原签规范选择、授权及冻结均通过核对时消费其精确清单；无证书保持原answer/reference/rubric覆盖要求。证书随`scoring_projection.normative_certification`和原review-binding封存，不作为额外teacher材料交给评阅者；后续消费者离线重验实际manifest/collection/trace及原签选择，不能只相信“已验证”标记或扩名单来过检查。

原native签发的三维`affirmed`保留严格布尔值。当前候选已被独立接收且只有明确有界的非阻断观察时，接收结论与“试解所有数字均正确”分别运输：原false、完整观察、证据和限制不得改写。投影若按旧包合同使用`status=passed`，须明确`status_basis=candidate_routing_acceptance`并携带原维度值；它只表示有限候选准入，不是补签三维true。仍有阻断、未决责任、未定规范或来源不符时拒绝。

非阻断观察保留原`kind`。旧`claim`路径继续适用；`reference`或`rubric`只有在明确`blocking=false`、`status=open_nonblocking`且`nonblocking_reason`非空，并且无需业务决定、无未决disposition时，才可引用本次已锁定的submission/trace补充证据。使用补充观察引用时，还须至少一条当前candidate或已绑定metadata依据，不能只引observed来源。其`affected_paths`仍须来自当前候选，补充观察路径不能成为该类问题的返修目标。该消费适配不修改native协议正文、SHA或已锁请求，不关闭观察、不增加教师来源，也不把原false维度改为true。

阻断`reference`/`rubric`也可引用同一锁定公开前驱的补充观察，但须明确`blocking=true`、ID在原routing的阻断清单中且原结论为`revision_required`，同时保留当前candidate或绑定metadata依据及当前候选中的全部受影响路径。接收后保留真实不通过与open缺陷，进入原返修和独立复检。闭合时重验原签，仍按原工作流独立审查历史绑定原公开前驱档案，不用后续公开重建档案替换其来源，也不把补充观察当成返修目标或独立通过。

原签证据的`locator`可保留文字，或使用明确类型对象：`{"type":"lines","start":1,"end":3}`及`{"type":"json_pointer","pointer":"/items/0"}`。传输时机械投影为`artifact_id#lines 1-3`或`artifact_id#json_pointer:/items/0`；空根指针`""`投影为`json_pointer:`，不能替换为表示空键的`/`。原对象、理由和完整原签JSON仍保留。行范围和指针在已绑定原件上核验；路径/别名仍须来自实际锁定输入，并唯一映射到同SHA、同大小的原生run artifact，类型适配不增加教师、历史或外部来源权限。

新准备的同 SID 评分请求对 criteria、experiment_validity、artifact_status 的全部 evidence 列表统一声明类型化定位：JSON 使用一个实际存在的 `json_pointer`；UTF-8 文本、CSV、JSONL 使用一个闭合的、从1开始的整数 `lines` 范围。`normative/task-scoring.json`、`sealed/facts.json`、`local-profile-definition.json` 三个 JSON 别名只接受指针，且只用于 experiment_validity.evidence 或 artifact_status.evidence。类型化语法不扩大来源权限；criterion 的 public_evidence、submission_evidence 仍须引用首阶段锁定原件，stage1_evidence 保留在原签 JSON 中，不能替代必要的 criterion 原生证据。每个 `{path,locator}` 只表示一处；多处引用拆成多项，可重复同一路径，说明写入既有 `reason` 字段，不能拼进 locator。JSON 整体（包括根数组）用空指针，键内 `~`、`/` 分别转义为 `~0`、`~1`。

该生成契约只作用于未来准备；已有 ready 工作冷恢复复用原锁定请求。仅磁盘准备完成而尚未登记工作项的中断，在请求配置变化时仍拒绝复用并保留旧字节；本次不新增恢复迁移。历史原签定位、理由、成绩和资格原样保留，唯一候选映射不等于已通过现有定位校验。

就绪请求和执行前均核工作项/角色/意图、当前候选公私摘要。quality的teacher输入必须是该候选参考与规则，其他历史来源另标history，不能把异版评分冒充当前标准。事实封存只接受原CLI实际采集的交付；同SID不能让A版事实进入B版评分。作答与首评请求不渲染教师侧意图、用途或策略标识。

原签明确写`all N criteria`时，只有实际选入同版冻结规范的JSON根数组、实际元素数精确为N且manifest/source/当前冻结SHA及大小全匹配，才能严格解为全数组。解析使用同一份重新校核摘要的字节；不接受含糊计数、错误N或跨版本来源。原文字、资格布尔和原分保留；已complete流程只追加经过严格重验的元数据接收原件，不重开评分或派确认。

明确的`JSON Pointers /a, /b`可投影为逐个真实JSON指针；仅同SID封存facts可追加`; facts F01, F02`式事实ID选择，每个ID在真实facts数组中必须唯一并记录实际指针。明确的JSONL行集合、区间、全文条数及指定turn.completed行也逐项核验；不从自然语言猜位置。所有新解析均使用与已绑定SHA/size重新一致的同一份字节，保留原locator。旧array/typed定位和已接收元数据输出保持兼容。

## 恢复、暂停及回退

### 经明确授权的返修容量重试

当原独立返修确已启动并以已封存的单一容量错误终止、无完整交付时，主控可对原 child 精确登记有界重试。授权须另有可回读的人类原话证据和结构化文件，显式绑定 controller、child flow、原失败 attempt、候选四摘要、原 policy 及允许的额外次数；不得从其他角色、旧批次或默认值推断。数量是本次授权，不是全局默认。用户已授权重试时不另发模型调用来制造可用性证据。

`flow-repair-capacity-prepare --parent-flow-id ID --flow-id ID --record FILE` 只复核和保存原失败完整归档、原 parent/child 接收及原实验授权，登记预算与第一新工作项的 future-claims pause，不派模型或改缺陷。record 必须是 `reason`、`authority`（path/SHA/location 引用）和 `failed_attempt_id`；authority 必须包含 `schema_version: 1`、`kind: bounded_repair_capacity_v1`、`owner`、`flow_id`、`failed_attempt_id`、`candidate_binding`、`policy_id`、`action: prepare_bounded_same_profile_repairs`、正整数 `maximum_additional_attempts` 和 `direct_user_evidence` 引用。该引用及模型确认仍需人工核实际来源，文件与哈希不是人类身份签名。

原 parent 在整个 child 返修/复检期间保留其异常和原回执；任何关联原实验的授权和业务 hold 不自动恢复。确认原直接授权、未来领取控制及在途边界后，可通过 `apply` 中的 `set_control` 对 `work:<ID>` 精确 resume，ID 取返回的 `state.repair_capacity_continuation.new_work_id`；再用 `flow-run --flow-id CHILD_ID --allow-model` 运行该 child。只有首个新工作项预先暂停；放行后，在预算内遇到已验证的容量失败时，flow-run 会自动依序接续后续重试。每次新工作与尝试保持相同确认批次、模型、推理参数、输入和职责，仅更换节点/runtime 与新独立 SID；每次失败的原 trace、collection、完整归档、预算 ordinal 和事件逐项回读。只有当前一次确已终止且唯一真实容量失败时才可继续下一次；达到授权次数、非容量错误、超时、不完整证据或不明状态均停止，不覆盖原件、不猜测未启动。完整返修交付按原缺陷和版本契约验证，再交独立复检；完成后不再消耗该预算。上述流程不解除正式实验的业务 hold，也不新建实验或授权正式作答。

`execution reconcile ATTEMPT_ID` 只核原进程/启动身份、监督进程回执、原trace、占位与成果，从不启动模型。启动前已占位、PID登记前中断或进程无法核验时保持uncertain；重新启动worker不会重派同一意图。已证明终止后按实际响应及成果分别收集，不以退出0计完成。采集与归档逐文件原子发布，保留中断意图；源文件漂移拒绝覆盖原件。

正常作者、公开重建、质量及实验消费者也会检查原 ACTIVE 尝试的实际终止证据，并沿同一 `reconcile` 入口接收该尝试。没有终止证据仍等待原角色；不明启动不会变成新尝试。消费者在模型结束后、成果收集前中断时，恢复原消费者即可继续原件接收和后继独立检查。读取某工作项的尝试或归属使用窄状态查询；真正交接时的独立质量门仍照常验证，不因读取看板而重验所有无关候选。

原模型已结束且成果封存后，`mark_collecting`／终态接收遇到 SQLite `BUSY`／`LOCKED` 时，消费者保留完成者互斥锁，只对同一 attempt 的元数据收件有限等待；不会再次领取、启动进程或调用模型。默认重试预算为90秒，每次数据库调用的忙等待不超过1秒及剩余预算，BEGIN 与 COMMIT 共用该次调用的预算；其他路径仍保留原15秒等待。原文件校验不跳过，也不被这个预算中断，因此这不是整个命令的90秒墙钟上界。已提交的终态先复读核对，部分 `collecting` 提交不会造成重复接收。

持续锁耗尽后，运行目录追加 `receipt-write-pending-<id>.json`，绑定原 attempt／work／intent／role／SID 以及 manifest、manifest-lock、termination、collection、collection-lock 五件原件的确切路径和SHA。CLI以 `terminal_receipt_pending` 和该证据引用退出2；原 attempt 不被改成模型失败或自动重派。同一自动消费者及作者监督线程不会在下一tick重开等待预算；多flow消费者只停该工作，继续其他就绪flow，最终报告未收件状态。数据库可用后，明确执行 `python3 -m taskflow execution --db execution/state.sqlite3 reconcile ATTEMPT_ID` 重验原封存并接收同一 attempt，追加确定性的收口记录，不覆盖 pending 原件。收口记录落盘异常单独报告，不能把已经提交的原终态再写成 uncertain。这里的 pending 仅是技术诊断证据，调度当前状态仍以 SQLite 为准。

未能证明未启动的异常类型仍保留 uncertain，CLI 报告 `original_result_uncertain`，不发布声称终态已接收的收口记录，也不重复记同一未决观察。标准库内建 `OSError` 子类仍须连同同一意图、启动阶段和无进程记录等原证据一起核验，才能证明未启动。

出分后停止使用 `original_score_pause_v1` 控制生命周期，仍由既有 `lifecycle-register`／`lifecycle-run` 执行，不派模型。主控预登记它所跟随的确切 `source_flow_id`、`source_definition_digest` 和 `first_run_purpose`；授权文件须同 controller／task_id／policy_id／这三项，并明确 `action: pause_after_original_score`、`round_scope: this_round_original`、`first_run_only: true`。来源只能是该主控、题目、策略和用途一致的原实验；一个来源只能登记一个停止后继。没有授权的旧基线不会因分数高低被自动选入。

消费者等待首卷真正完成独立事实封存、同一评阅SID评分和唯一原评价接收，验证原 run／冻结／签分及三段真实 attempt 的封存绑定后，原子写入 `task:<task_id>` 的 future-claims 暂停和该后继的 accepted 状态。原始实验即使处于旧确认等待，也只以首卷为依据，且不会改写原包、原分或终态；应在本轮首次派发前另行暂停确定性 solver-1，避免旧确认授权抢跑。暂停不终止已经在途的角色。停止后继完成后不会再次写控制，因此后来直接人类授权的 resume 不会被旧后继反复暂停。标准启动器对这个控制生命周期不传 `--allow-model`，其启动意图也保存 `allow_model: false`。

未来的新实验 packet 可在顶层明确声明 `"original_score_pause": {"round_scope": "this_round_original", "first_run_only": true}`，可选 `authority_evidence` 为实际授权引用。`experiment-start` 只对这一严格类型的授权派生停点：先正常登记来源，再按来源实际 definition digest 和首卷用途生成不可变 `original-score-pause-authority.json`，引用原 packet，登记唯一停止后继。它不会读 `after_score` 文字、分数或目标阈值来推断授权，也不会把旧基线自动纳入本轮。

生产者先在同一 SQLite 事务检查后继是否已接收或已准备确认控制，只首次暂停该来源确定性的 solver-1 并记录一次准备标记；首次准备时，如果该精确工作已被领取、已有尝试或不再处于待领取状态，则拒绝准备，不能把未来领取暂停说成已就绪。已有 pause 保留原字节，已完成或已准备的旧生产者重入不能覆盖后来的人类 resume。随后启动普通零模型停止消费者，即使实验入口只作 register-only，也会运行这个明确授权的控制消费者；正式实验消费者仍只在 `--allow-model` 下启动，并在控制消费者需要 attention 时保留登记等待。返回的 `original_pause.control_consumer` 与 `control_consumers_started` 分列控制进程，`consumers_started` 仍只计正式实验消费者。没有该显式字段的旧 packet、已登记 definition 和已锁请求完全沿原行为；若要为旧原卷增加停点，须另有精确控制授权并沿现有生命周期登记，不能回写旧 packet。

新工作进程与接收入口共享每个 runtime 的成果收集互斥，覆盖实际运行直至成果封存和数据库接收。终止回执先于原工作进程收尾出现时，其他接收者只等待；原工作进程完成收尾或退出并释放互斥后才能接收同一尝试。升级前已在途的进程不会自动取得新锁或热加载新代码，须沿原消费者收尾，或在其确已退出的自然边界恢复。

Markdown看板将原主控的R&D观察与已交接的工作项逐行展开，显示各自负责人；未派发、运行、待核证及技术完成分别呈现。进程已结束、模型响应完成和产物技术接收不能从主控文档的`running`文字互相推断。JSON保留完整工作项、尝试、质量及来源时间；非受管角色的实际终止与trace观察仅作旁路证据，不替原主控接收或签业务结论。

`apply`调用`set_control(scope, action, reason)`：scope为all、task:ID或work:ID；pause/cancel仅约束后续领取，cancel仅取消ready，不杀在途CLI；resume不复活已取消工作。对不明尝试先核证，技术重试另建有说明的新意图，保留原尝试。没有“自动无限重试”。

回退时先暂停新领取、逐项核对在途，不终止无关进程；导出status/events及原件，使用范围明确的transfer_owner回执交回原主控。交回后旧ready意图被所有权门阻止；在途请求不回写。同一工作项有在途写者时不转移写入权。两端确认唯一派发者后，由原主控继续旧入口。不要直接删除SQLite或占位来冒充调用未发生。

现阶段未安装后台服务，未验证睡眠/系统重启下持续运行、跨主机或服务端exactly-once。stdlib trace报告可执行行覆盖，不是分支覆盖；故障注入及负例另由行为测试证明。

## 已授权正常节点自动接续

后续授权的质量、实际缺陷返修及独立复检由同一SQLite保存不可替换的flow定义和CAS游标。主控一次登记分支、用途、来源、角色与边界，由普通消费者接续节点；消费者可在独立会话中持久运行，没有新增后台服务。模型并发仍由现有全局并发和所有权门控制，worker只领取此flow的确切工作ID。

可以在确切`initial.predecessor_attempt`仍运行时提前登记后继，`predecessor_binding`锁定其work/intent/candidate/task。消费者保持active并记`waiting_predecessor`，相同等待不重复追加事件；只读核绑定和manifest，不reconcile、采集、归档或重领前驱。原worker将状态置completed且collection封存可核后，才追加全档、绑定原独立SID并一次准备后继。failed/uncertain/cancelled只停该flow，其他就绪flow继续；新质量SID不得复用公开前驱SID。无需全库动态发现，单次`flow-run`显式列出本次预登记的ID即可。

```sh
python3 -m taskflow execution flow-register --spec WORKFLOWS.json
python3 -m taskflow execution flow-run --flow-id FLOW_ID --allow-model
```

质量角色沿现有三项成果提供显式`execution_routing`版本、候选摘要、scope、结果、具体缺陷及已覆盖执行缺陷ID。程序同时校验实际独立SID/作者归属，不从“通过”等文字推断业务结果。实际独立缺陷才进入返修；修订清单必须与真实公私字节差异一致。私有修订复用绑定的公开成果，公开字节改变才走已登记的隔离公开重建及新独立复检。接受前再核当前版本与后发否定；程序接收回执明确不冒认人工阅读，不自动派发正式作答或判定难度。

已闭合缺陷的修复归属沿同一quality scope核定：同task的三摘要完全相同别名及其祖先可承接原闭合，但当前候选仍须有自己的当前独立整包准入。经别名承接时重验范围内原件、文件成员的id/路径/角色/SHA/大小、实际作者交付及原闭合签发和时序；晚于当前签发的闭合、后发否定或来源漂移仍拒绝。此规则不让旧坏字节克隆取得修复，也不要求新whole重新关闭已经闭合的旧缺陷。

历史闭合的真实性与当前候选freshness分别核验。历史闭合之后、当前接收之前的负评，只有作为当前深验canonical组合的精确whole来源，或由当前原始whole签发及其直接实际修复逐项闭合全部原始问题时，才可在历史别名承接中被识别为已处理。后一条重核原签/当前签的实际封存、全部原始问题与唯一缺陷的对应、原作者交付、repair-record、全部变化和闭合时序。旧闭合自身的`assert_review_current`仍报告过时；不相关或未闭合的负评、仅有更新accepted、伪闭合以及晚于当前接收的新负评仍阻断。

未来尚未准备的whole质量节点先取得从当前`PUBLIC_ROLES`实际字节生成的`execution/public-inputs.json`，仅列公开路径、角色、SHA和大小。请求先要求只读这些公开原件，保存同一上下文的`public-responsibility-first.json`，在真实tool响应打印其SHA后才读完整task声明、teacher/scoring或旧审查；最终三份原报告引用先行文件及可见顺序位置。先行文件由既有work/full-archive保留，不是新审批表或模型。SHA只绑定字节，实际责任判断和是否先读教师仍须按原trace核验；缺顺序证据或已预读教师不可追认无锚定整包。已准备的request/spec/manifest不重渲染。

未来affected请求同时提供从真实父/子`candidate.files`核定的`execution/repair-diff.json`，旧改动原件按不可变副本以history输入`execution/parent-inputs/`，不冒充当前teacher规范。每个真实改动须在同一原报告的`defect_resolution.evidence`列当前相对path、实际SHA及非空位置；额外必要关联还须列`affected_paths`、关联依据和`coverage.changed_files`的path/old/new。角色/成员变化如实记录；声明原样物理化不算payload差异，缺父源或新增/删除边界不可猜造摘要。消费端原有逐件门槛保持。

累计返修跨过公开改版时，不能把最后一个affected签发用于全部旧缺陷。`close_defect_with_coverage_chain`只接真实连续修订链：新公开版本上的whole原签逐项承接旧修复，后续同该公开输入的私有修复由最终affected原签明确闭合，并重验双方完整来源、身份、时序和逐件必要关联。原`close_defect`的material/公开摘要守卫保持；中间whole仍可为revision_required，最终签仍是affected。组合以当前直接父候选上实际whole finding为基准；授权initial到该基准的连续来源、当代阻断与显式继承闭合链分别核定，不能偷换为最初旧公开输入的whole。

原whole以whole_package签发并给出false和真实阻断时，原值与问题保留。只有经store深验的完整修复覆盖图及当前affected闭合，才可形成带`prior_whole_findings`/`repaired_candidate_acceptance`的机械准入；不补造unchanged_scope肯定，不把旧whole改accepted，也不把当前affected改whole。未闭合、未决或来源/范围不符仍拒。

同一whole业务路由的原native签发若为`unchanged_scope`且有false，仅在原排除路径均关联实际阻断、责任分析明确链接全部阻断、每个false均有排除路径证据，并经既有canonical图与当前affected三维明确肯定精确闭合时承接findings。在线与离线证书均保留原coverage、排除路径、coverage_basis、false及局限；不证明未改范围被重新肯定。缺少上述业务依据返回`needs_business_decision`，不自动全审或补签。初始零派发实验的原精确business显式肯定运输异常可沿`resume_validated_source_quality_transport`追加恢复，仍须原异常history/hash、同作者/子flow/候选、当前gate/native handoff和零formal足迹均通过，业务hold或complete仍拒。

私有修复组合还可严格接收原独立claim的`scope_adjustment`：原affected范围须全部为已声明私有材料，公开输入、真实修订与闭合身份及时序门槛仍保留；不支持的类型/处置与身份时序错误分别报错。新增私有验证材料仅限teacher/verification，须有原作者真实采集、声明old=null/new实际SHA、当前逐项证据及必要关联覆盖；原公件、既有文件角色、评分及其他声明字段不可趁机改变。已有声明行的sha256/bytes只可按对应实际私有改动同步，原声明和差异另存。重解析历史签发时，每一份`execution/task.json`别名都绑定其自身候选和锁定manifest，不把父版SHA替换成当前版。

已完成affected复检确有额外独立窄补签时，`compose_repaired_quality(..., supplement=SPEC)`可显式绑定新attempt、report/issues、原recheck报告及review ID。运输层重核实际独立上下文、采集封存、当前声明文件集合的实际差异、原签复用指针和新必要关联；绝对路径只在实际manifest/current候选一致时按其`package_path`归一。组合图保留旧whole/recheck/closure，不把补签当新whole或重选规范；每次复验重新核全部三源及限制。业务前提未决时，技术组合恢复不能消除`pending_business_scope`。

质量依据可引用工厂提供的`execution/task.json`声明别名：仅在原manifest的source_metadata源、暂存字节、原候选声明及当前三摘要均一致时归一为声明证据。它不能作为公开业务依据或受影响文件，也不放开其他外部引用。已完成角色遇到这类技术消费异常时保留原签发和归档，经明确恢复消费同一attempt，不重新调用模型。

每个完成节点追加全原件归档，包含工作与scratch目录、未在窄成果白名单中的普通文件及空目录；旧collection、manifest和原始来源不改。前驱已完成公开作答由原attempt承接，全部已声明成果进入追加全档，不重跑该作答。

真实异常保留原记录并停在本flow，其他就绪flow继续。技术原因修复后，用带`reason`与`evidence`引用的JSON追加恢复决定：

```sh
python3 -m taskflow execution flow-resume --flow-id FLOW_ID --record RESOLUTION.json
python3 -m taskflow execution flow-run --flow-id FLOW_ID --allow-model
```

恢复核对原节点、实际工作及尝试；有活动/不明尝试先核原进程，未证明完成或实际失败均不重派。未入队的零模型探针可对登记的确切失败报告SHA集合再探一次；出现新失败报告后旧授权失效，原报告不删。原定义、候选、已完成归档、旧异常回执和完成节点均保留。Unix长工作路径使用自有helper进程在原scratch内绑定短basename，主进程不切换cwd，不跳过IPC探针或扩大角色权限。

isolated公开准备的inner须在仓库外，外层节点的current/spec/状态证据仍沿原位置保存。首次公开重建与返修后的`public_rebuild`复用同一准备器及稳定短路径；真正launcher路径取已验证的isolated绑定。已有准备原件和工作身份不因新路径规则重建或换绑；未准备节点才可选择新的仓库外inner。`recheck_public`教师复检仍走自己的原生角色，不套用公开运行器。

返修清单的`changes`只列父/新`candidate.files`中真实payload差异及角色变化。同字节复制锁定的`execution/task.json`为完整候选声明时，单独记录provenance，不把未声明为payload的`task.json`当新增材料。若既有completed返修原件已因唯一这条声明复制记录触发`Repair changes duplicate or do not reflect actual differences`，`flow-resume`恢复JSON可追加`repair_record_projection={"original":REF,"projected":REF}`。每个REF均含真实path/SHA/location；派生记录只移除该唯一`task.json`行，其余字段与原记录保持一致。程序重核原封存attempt、原锁定声明与实际交付声明同字节，再用原严格payload差异校验；原件、异常与投影分别保留，随后仍由新独立角色复检，不重跑返修作者。

## 业务packet与完整生命周期

完整作者工厂使用本机通过 `--factory FILE` 明确提供的配置，锁定已验证原生作者模板、条件质量角色模板、策略、研究目录和生命周期目录。本机服务配置不随仓库提供。作者工厂用于已有结构完整 baseline 的研发：先按单题契约形成可解析且 task ID 一致的初稿，不能直接把 `init` 的空草稿当成已满足此前置条件。CLI一次接受一个确切业务item；packet给出controller、task/intent/candidate、实际baseline、最相关来源、作者工作、五份交付及`candidate_complete/research_ended/needs_business_decision`正常后继。

新批作者在入队前的 native 零模型探针失败后，可在修复实际宿主条件并核对原始失败报告后，使用原 batch packet、factory 和数据库重入 `batch-author-start`，对每份原失败报告重复指定 `--retry-failed-native-probe-sha256 SHA256`。入口只接受精确的全部失败报告摘要，核原 manifest、同批绑定、工厂意图，拒绝已有 work、attempt 或成功 seal；不接 `--allow-model`。授权留在不可覆盖的私有回执，原失败报告和已锁请求保留。只有当前宿主的同一探针通过才入队；新失败须重新核证并显式包括新增报告。入队后仍用原批次入口正常接续；探针本身不证明正式模型或服务端身份。

```sh
python3 -m taskflow execution author-start --packet PACKET.json --factory /path/to/author-factory.json --allow-model
python3 -m taskflow execution author-start --help
```

历史兼容入口必须显式传 `--factory FILE`，CLI 不再提供旧工厂默认值；新批次使用 `batch-author-start`。省略`--allow-model`会捕获当前baseline、准备新work/scratch、执行零模型权限探针并入队ready，不调用模型，也不是只读dry-run。带开关的同一入口使用确切work ID领取作者，在实际claim后立即登记条件生命周期，完成前保持等待；后台作者和前台生命周期共享原台账。原作者的运行/采集窗口不被另行reconcile或重领。请求只带本轮packet及通用契约，旧模板业务文案和旧history输入不会沿用；知情作者按本批授权读取所需项目材料，只能写自己的work/scratch。

未来说明更新只用于新意图。同一已发布factory意图重入时，先核原packet、配置与完整spec其余字段，再复用已绑定的原请求；已有准备/队列仍核manifest、request和原工作绑定。不能用新说明覆盖旧spec或request，也不能把packet、模型、权限或其他spec漂移当作说明升级接受。

作者必须实际交付`research.md`、`candidate-decision.md`、`handoff.md`、`handoff.json`和`issues.json`。候选目录是可选成果，研究结束不因此被判缺交。完成后先封完整原件，再按原签`handoff.json`分支：研究结束只归档并接收具体结论；业务选择未决只停该生命周期；完整候选经真实作者/完整来源绑定后才准备新的严格公开重建及独立整包质量、必要返修和复检。最终仅接收研究质量资产，不自动派发正式作答。相同packet重入复用原意图；修改packet、替换模板或不明尝试不能暗中变成第二次调用。

### 原评接收后的明确教师后继

`teacher-successor-start` 消费主控明确选择的有界后继包，连接已接收的原卷／事实／评分与唯一下一研究作者。包使用 `schema_version: 1`、`kind: explicit_bounded_post_original_teacher_business_successors`、`owner`、`batch_scope`、`author_factory: REF` 及非空 `items`。每项绑定 `predecessor_run`、`predecessor_receipt: REF`、`registered_original: REF`、`author_intent` 和 `author_packet: REF`。原分如有提供，仅用于核对原件，不触发作者或改题规则。

```sh
python3 -m taskflow execution teacher-successor-start --packet TEACHER-BUSINESS.json --directory execution/teacher-successors
python3 -m taskflow execution teacher-successor-start --packet TEACHER-BUSINESS.json --directory execution/teacher-successors --allow-model
```

省略开关只登记并准备，机械 tick 不派模型。已有接收器与完整归档验证实际原卷、评价摘要、事实封存、同一评阅 SID 的评分及其先后关系，并在当前状态中绑定唯一已接收前驱。每个作者意图只能有一个不可替换的教师后继绑定。新作者沿原 author factory 准备、原 worker 领取及原独立质量链继续；已在途作者只按原 factory-intent、spec、request、manifest 和 attempt 身份接收，不启动第二个消费者或重写原请求。失败、不明尝试或缺失原件保留待处理。

该入口延续同一已授权批次，原模型确认门保持；不为新批次代选模型。作者交付仍须由正常独立角色检查真实改稿，绑定或请求准备不等于内容问题已关闭。正式实验需要主控另行登记具体用途与授权；研究结束及真实业务未决仍返回其已登记主控。

完整候选的未审报告使用`task.reviews`中对应的`id/kind/path`；报告路径须在`reports/`且不重复声明为payload。作者自检、原始观察和归属说明保存在私有provenance，不冒充独立签发。严格unsigned占位须有`schema_version=1`、对应`review_id/kind`、`status=inconclusive`、`independent_quality_passed=false`和`new_formal_score=null`，其余字段仍受接收器允许集约束。已有原报告不符合此元数据形状时，原JSON、路径、SHA、作者和异常先保留，显式派生包装再接收；不回写原作者输出或已锁请求。

`accept_declaration_only_candidate_transport`另接明确的`declaration_transport={schema_version:1,kind,original:REF,projected:REF,evidence:[REF]}`，逐次重核原completed作者及完整采集归档。`referenced_rubric_role_v1`仅能更正已被`scoring.rubric_file`唯一引用、与当前评分完整对应的私有teacher角色为rubric；`declared_unsigned_report_layout_v1`仅能把三个原私有未审报告同字节移入作者provenance，并将对应`reviews.path`指向`reports/`中的严格unsigned包装。包装须保留完整原JSON、原相对路径和SHA。其余题包、公开内容、评分和payload字节保持原样。后者允许对原已存在的私有零字节JSONL追加[合同规定的`empty_content`](task-contract.md)，须有真实生产/解析依据；它证明零记录格式声明，不证明业务控制正确。

experiment release 在 canonical kind producer 与严格 required-ID consumer 之间按捕获声明选择：每个 required kind 必须唯一，未知 producer kind、缺失、错实际 kind 或重用路径均拒；原 projection、原签和 task.json 字节保留。optional ID 不进入输出；未选的已验证 canonical 维度可保留为证据，calculation 不增加全局必需门。`prepare_release` 仍只接受精确的 required ID 集合。

`lifecycle-resume`接收明确的技术恢复记录，不派发替代模型。原作者已经completed、原异常回执已落盘、后继尚未领取/入队的`author_handoff`或`public_prepare`异常，沿用`accept_path_only_candidate_transport`、`accept_unsigned_author_placeholders`或`resume_unclaimed_public_preparation`。恢复JSON须给出具体`action`、`reason`和`evidence`；若提供派生候选目录，必须保持原交付的`task.json`和全部声明payload同路径、同角色、同SHA及大小，只核未审或current inconclusive，不造质量通过。另有以下有限恢复：

| action | 已有证据及边界 |
| --- | --- |
| `resume_failed_public_capacity_once` | 仅接原作者完成后第一次严格公开重建的已采集容量失败。明确给出`failed_attempt_id`、`availability_attempt_id`与`evidence`，重验原收到的异常、实际 failed attempt／锁定公开来源／完整归档，以及失败之后同 CLI、模型、推理配置和策略的真实正常完成证据。外层 exit 0 不代替模型完成。保留旧失败，追加唯一接续身份与独立 runtime，恢复原父 flow 到`public_prepare`；动作不 prepare、不派模型，不重做作者。重复、未知尝试、业务 hold、已有 child/formal 或别的领取均拒。 |
| `resume_resolved_public_capacity_dependency` | 只恢复上述原失败在原唯一实验收到的精确传播异常。父 flow 的一次接续证据绑定零个或唯一原实验的 ID、定义、授权、原异常与事件；多个用途不猜选，后加的其他实验不能借此恢复。按原作者／SID、持久化历史、失败／可用性及已有题包运输侧证重验；父 flow 必须仍在未准备新公共重建的`public_prepare`。先完成父 flow 和依赖的零模型恢复，再启动原正常消费者。保留原正式用途、原回执和控制，不新建实验、换模型或循环 retry。 |
| `resume_author_packet_source_metadata_transport` | 仅接初始零 formal 实验的原题包路径绑定异常。`packet_transport: REF`须同时列入`evidence`，指向严格`author_packet_source_metadata_alias_v1`侧证：原作者定义、实际 work/attempt、原授权、锁定 manifest 中唯一`source_metadata`输入和源／暂存字节均须吻合。只显式采纳该原输入的路径，保留原正文、模型、SID、异常和用途；不按同 SHA 搜索别名，不创建第二实验。当前已接受的修订候选与初始候选均检查 solver/decision 足迹。在线、离线规范证书使用同一验证器；无侧证时沿用原路径精确匹配。 |
| `resume_unclaimed_formal_preparation` | 仅处理`formal_prepare`原精确授权映射异常；尚无该solver work/attempt、runtime或准备意图。重新核原packet精确item、当前候选/质量、冻结发布及配置，不放宽模型、超时或baseline。 |
| `resume_resolved_quality_dependency` | 子质量flow已显式解决原精确`repair defect_ids: list required`字段传递异常（原repair角色`covered_defect_ids`与assigned集合相同），或已用明确repair-record投影解决原精确技术异常，或已显式解决零派发`public_rebuild`的仓库内runtime错误后，核原作者、公开前驱、同一子flow/节点和原异常回执，恢复其传播到作者父flow或尚无solver工作的后继实验的依赖异常。不能跳过未解决的质量缺陷、失败/不明尝试或独立业务等待条件。 |
| `resume_validated_evaluation_intake` | 处理 `evaluation_intake` 的原精确错误 `signed evidence path/location is missing`，或 `signed source has no unique same-byte native run artifact: <signed source>`。恢复JSON另给`receipt: REF`，指向该flow固定`evaluations/<run_id>/receipt.json`的已成功接收原件。前者保留原 typed-locator 验证；后者核原 `source_path_map` 的每项公开包路径、角色、首阶段输入、selector、唯一同名同字节run原件、全部引用投影与完整归档。均核同SID、当前候选四摘要、原封score/facts/规范与原生评价后仅恢复同stage。 |
| `resume_validated_host_formal_preparation` | 仅处理原run在宿主监听EPERM处停止、尚无solver work/attempt或执行痕迹的零模型准备。另给`preparation: REF`，绑定原intent/reservation、完整失败目录封存、同路径准备、全部原隔离/上下文/正常工具探针及完成回执；验证后恢复原`formal_prepare`，不重新准备或claim。 |
| `resume_validated_public_probe_preparation` | 仅处理原准备器把公开正向读取探针写死为不存在路径的零模型失败。另给 `preparation: REF`，完整保留原九文件失败目录、intent、reservation、request 与用途；显式新准备源码只允许把该公开目标改为已锁定公开树中的真实成员。新 source-lock 如实记录新字节，其他源码、全部负向隔离、上下文及正常工具探针不变。只有实际完成的最终验收侧证能恢复同一 `formal_prepare`；准备引导的 draft 不能用于恢复。动作不 prepare、claim 或重派模型，旧 EPERM 分支保持原边界。 |
| `resume_validated_score_preparation` | 原精确规范角色覆盖错误已由有效独立规范证书解决后，另给`preparation: REF`指向原`prepare_score_runner`返回原件；仅接同facts SID/seal、同冻结run的ready评分工作且score attempt为0，保留异常回执，恢复原`score_prepare`。动作不准备、不重封facts、不claim。 |
| `resume_validated_source_quality_transport` | 仅处理原初始experiment_v1已接收源的明确运输异常；先重验同一原作者/候选、原签或已登记canonical组合、当前native handoff、原异常回执及零solver准备/工作/尝试，再恢复原stage。`different/later revision`仅在确有上述同字节别名闭合、原接收history/hash一致且当前原签为独立whole时可恢复。精确`later independent rejection/conflict covers the accepted scope; old acceptance is not current`仅用于同一原接收history/hash、当前别名gate及原作者子flow的既有canonical组合全部通过，并且无原实验decision、任何formal足迹或源业务hold的情况。resolution只追加，动作不创建组合、不签业务通过、不补三true，不消除pending_business_scope或独立业务hold，不重开complete。部署须使用重新导入当前源码的新消费者，旧进程不会热加载。 |
| `resume_unpublished_release_metadata` | 仅接原初次release的`snapshot file manifest mismatch`异常，原终态异常回执、来源与三份投影均重验；publication、暂存、锁、formal及solver足迹须确实不存在。只恢复同packet/候选/版本的release首次原子提交，不adopt或覆盖已存在发布，也不派发模型。 |
| `resume_unpublished_release_report_binding` | 仅接已收到原回执的初次 release required-ID 集合错误；按未改声明逐个唯一 required kind 绑定原 canonical projection。须提供 `expected_revision` 与含 path/SHA/location 的 `original_receipt`，重验 receipt 原字节、同源作者/child/候选/原签/native handoff/授权/版本与全部原投影；业务 hold、任何 publication/staging/lock/formal/solver/decision 足迹均拒。只追加一次 history/CAS 并恢复原 release，不发布、不 prepare、不签发、不派模型；重复请求拒绝且不再追加。 |
| `resume_resolved_declaration_dependency` | 原作者父flow已在`author_handoff`接收严格声明运输、尚未捕获候选时，核同一原作者和运输证据，恢复仅因该声明异常而停下的初始实验依赖；已有solver准备、work或attempt均拒绝。 |
| `resume_resolved_author_preparation_dependency` | 原作者已用`accept_unsigned_author_placeholders`接收同字节候选别名时，重核完整unsigned包装、原报告保存及原依赖异常，只恢复尚无solver准备的初始等待。原实验本来active等待时直接沿正常消费者，不重复resume。 |

源路径歧义先使用 `ingest_signed_evaluation(config=..., destination=...)` 已有的 `source_path_map` 配置，显式将原签引用路径映射至该run的实际 `source_path`；同字节对象不能代替命名原件；首次显式地图接收也须在归档/原生评价写入前验证这些绑定。须成功原生接收后才能恢复，且不能清除待决业务范围。普通stage再入在所有原配置一致时保留已接收地图并正常重验，复用同一evaluation；不把地图加入producer、原请求或原签。未知路径、跨scope、教师扩展、歧义及源漂移拒绝恢复。恢复本身不启动模型、不决定业务hold/complete，旧回执保留。

快照manifest的文件元数据按原声明与实际payload SHA/size完整重建，作JSON类型精确对账。声明自带sha256/size键不取代实际计算值，原task.json及其他元数据仍保留并绑定；不能先剥计算字段再与含同名键的原声明错配，也不能放过文件、角色、元数据或字节变化。原子发布失败且暂存已清理时，如实记录“尚无快照”；恢复是第一次成功提交，不伪称复用了未产生的冻结。

上述操作均在生命周期锁内追加历史，保留原异常、回执、定义、尝试和原签分。validated intake恢复不重新导入或调用模型；正常消费者幂等接收同一evaluation，active状态或已消费恢复不能再次使用。

历史负面结论的旧修订图可作为来源证据，但不能因此成为当前通过。接收器须重建该负面结论自己的实际返修、独立 affected 闭合及完整祖先别名；当前独立整包签发或持久化派生图均须通过原始验证，再逐项证明后继重叠负面结论已闭合。空图和伪造摘要不能代替真实当前来源。默认调用仍拒绝失效旧图；上下文不放宽 whole/affected 范围，不修改原签或补充 covered IDs。初始实验收到`historical negative is outside the current repaired quality graph`时，也只能在上述当前图验证及原有零足迹、回执和业务 hold 检查通过后，使用`resume_validated_source_quality_transport`恢复。

原独立整包负评若明确声明 whole，非空 `affected_paths` 只有在恰为原 blocking findings 的实际目标时，才能作为缺陷定位运输。侧证保留原 coverage、路径、false 维度和逐目标原件／暂存字节绑定；旧负评仍不是通过，当前返修闭合图必须另外有效。初始实验的精确范围字段错误可沿同一 source-quality 恢复入口接收该侧证；未知范围、正面 whole 附带局部路径、定位不符或业务 hold 均不能借此清除。

公开探针准备恢复侧证仅绑定其原 run。原评分实际接收且符合既有唯一确认授权时，活跃侧证退役入历史，不随新 run 自动使用。确认 run 若自身出现同类零模型准备失败，须有它自己的完整侧证；只有该 flow 已完整封存并接收原卷、事实及评分的唯一首跑，才可从本次重复派发检查中精确排除。当前 run、其它用途、失败、不明或未封存的 solver 足迹仍阻止恢复。

宿主准备恢复须先按明确授权核正确宿主权限，独占同run恢复标记并完整保留旧inner，原intent和reservation路径及字节不变；仅在原canonical路径执行批准准备程序的`--in-runtime`分支及全部原探针。不得再stage抢预约、跳过隔离检查、放宽目标profile或重抽失败/不明模型。恢复操作不自动重试；任何已有solver派发或证据漂移均停止。完成回执经只读验证后，既有生命周期仍须照常验证prepared plan，才可沿原授权唯一派发。

统一入口为：

```sh
python3 -m taskflow execution lifecycle-resume --flow-id LIFECYCLE_ID --record RESOLUTION.json
python3 -m taskflow execution lifecycle-run --flow-id LIFECYCLE_ID --allow-model
```

已有独立业务投资可一次登记完整生命周期，JSON为`{"definition": {...}, "evidence": [...]}`：

```sh
python3 -m taskflow execution lifecycle-register --spec LIFECYCLE.json
python3 -m taskflow execution lifecycle-run --flow-id LIFECYCLE_ID --allow-model
```

`flow-run`消费质量/返修flow，完整生命周期kind使用`lifecycle-run`；二者不能互换。工厂会自动登记作者后继，不要求再逐节点运行上述登记命令。实验生命周期须另外绑定业务packet及确切用途，随后机械接续独立发布副本、冻结双包、既有正式隔离runner、全work/scratch/trace归档与原卷导入、全卷事实封存、原SID续接冻结标准评分及唯一原签分接收。首次事实输入只带实际公开/答卷/trace和经过筛选的实际配置事实；教师、授权、设计历史及目标不随整包控制文件注入。未知served身份和原签有效性未决保持原样。

另行授权的作者结果实验也可一次使用共享入口；本机 `--factory FILE` 配置锁定既有正式适配器、客户端配置、评阅角色、策略及各根目录，packet逐项锁定确切task和source author-quality flow：

```sh
python3 -m taskflow execution experiment-start --packet EXPERIMENT-PACKET.json --factory /path/to/experiment-factory.json --allow-model
```

历史兼容入口必须显式传 `--factory FILE`，CLI 不再提供旧工厂默认值；新批次使用 `batch-experiment-start`。省略`--allow-model`只登记独立`experiment_v1`定义；带开关启动每项自己的`lifecycle-run --allow-model`普通消费者。新实验等待对应作者质量结果并核确切候选、来源与授权，不修改旧作者的no-auto实验定义，不全库动态发现。启动先保存不可替换的意图，使用`start_new_session`、`stdin=DEVNULL`及独立stdout/stderr日志，随后保存PID、启动身份和进程组；同一生命周期还有进程锁。已证明存活的同源消费者复用，完成结果复用；意图缺PID收据、身份不明或已退出时默认返回attention，不自动重拉原模型。持久状态与日志可供接续，系统睡眠/重启后持续运行仍不在已验证范围。

工程显式调用 `launch_lifecycle_consumer(..., resume_dead=True, recovery=...)` 可恢复已证明退出的原消费者。`recovery` 绑定经过验收的源码发布回执和当前完整源码清单；重验原意图、定义、用途、授权、PID 启动身份及空闲生命周期锁。恢复只追加 `consumer-generations/000001/` 等新一代意图、PID 收据和日志，每代指向原锚及上一代，旧文件保留。任一代缺少收据、启动身份不明、原业务 hold、锁被占用或证据漂移均拒绝；原工作项和尝试仍按其原状态接收，不新造模型重试。旧启动源码清单保留为当时记录，不冒充本次实际源码。


原 repair 缺陷字段与依赖恢复：`validate_repair` 可消费原repair角色的
`covered_defect_ids`，只将同一assigned集合返回为机械字段`defect_ids`。
两个字段同时出现时，各自必须是精确、非空、唯一字符串集合；冲突和未知业务决定拒绝。
兼容不改写原记录，不借用task.json物理化投影，不代表独立质量通过。
未来未prepare的repair请求明确输出`role=repair`及`defect_ids`；已锁定请求不重渲染。

原精确字段异常仍通过既有`flow-resume`显式解决。消费前只读重验原completed
repair、同一节点/author/父子候选、当前源、实际完整payload差异、collection和既有归档；
恢复事件追加`repair_defect_identity`，原异常回执与原作者字节保留。
随后用既有`resume_resolved_quality_dependency`依次恢复作者父flow及其精确绑定的初始实验，
不创建repair、solver或评分尝试。parent/experiment只能承接同一显式解决记录；
新业务hold、未知reason、当前失败/不明工作以及已存在的publication/formal runtime/intent/work/attempt拒绝。
后续仍须实际新独立affected或完整必要范围复检及原有store/closure门；实验恢复仅返回可继续等待的active状态。

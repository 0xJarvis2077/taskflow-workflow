# 正式作答与两阶段评阅

这三个阶段使用明确角色任务，复用现有 SQLite、worker 和 Codex runner。Python 验证来源、结构、资料范围、封存与唯一接收；Codex 独立作答、读卷并按冻结标准签发原分。Python 不选择业务答案、补分、改尺或自动启动下一阶段。

## 顺序与命令

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 solver submit --spec /absolute/solver.json
python3 -m taskflow execution --db /absolute/state.sqlite3 solver prepare solver-01
python3 -m taskflow execution --db /absolute/state.sqlite3 solver run solver-01 --allow-model
python3 -m taskflow execution --db /absolute/state.sqlite3 solver collect solver-01
python3 -m taskflow execution --db /absolute/state.sqlite3 review submit --spec /absolute/reviewer.json
python3 -m taskflow execution --db /absolute/state.sqlite3 review run review-01 --stage facts --allow-model
python3 -m taskflow execution --db /absolute/state.sqlite3 review seal review-01
python3 -m taskflow execution --db /absolute/state.sqlite3 review prepare review-01 --stage score
python3 -m taskflow execution --db /absolute/state.sqlite3 review run review-01 --stage score --allow-model
python3 -m taskflow execution --db /absolute/state.sqlite3 review collect review-01 --stage score
```

`submit`、`prepare`、`collect`、`seal` 和 `status` 不启动模型；`run` 必须明确 `--allow-model` 才能执行非夹具请求。facts 完成不会自动封存或评分，solver 完成也不会自动建立 reviewer。每个执行 turn 具有新的 work、intent、runtime、attempt 和不可覆盖的原件。

`solver status ID` 与 `review status ID` 只查询，正常查询退出码为 0。返回分别记录 execution、delivery 和 outcome。run/collect 遇结构错误、执行失败或未知返回退出码 2；合法的事实未完成或评分未决作为角色结论保留，不被 Python 改成通过。

## 正式作答

solver spec 使用 `{"definition": {...}, "evidence": [...]}`。definition 的必需字段：

- `schema_version=1`、`flow_id`、`task_id`、`owner`、`policy_id`、`purpose`。
- 绝对 `directory`、`candidate_root` 及确切 `candidate_id`。
- `quality_task_id`：真正完成且 delivery valid、原签 accepted、当前质量准入通过的独立 quality 任务。可以是整包原签，或明确继承范围、新版受影响复检及关联检查共同覆盖当前版的独立affected原签；旧签不改版、不升格成新版whole原签。
- `decision_id`：协调者通过正常 `record_decision` 登记的明确授权，action 为 experiment、authorized 为 true，candidate/task/purpose 完全相同；intake 分别保留质量结论、难度证据、重要未知及不同结果的下一选择，见[迭代接收](iteration-intake.md)。质量通过本身不触发正式调用。
- `runner`：本批已确认的 CLI/provider、model、effort、timeout、权限、独立 home 与明确 outputs 或 output_trees。

真实 native solver 不能消费模拟质检作为真实业务来源。原质量结论、当前候选字节、未闭责任和来源原签均重核，不接旧 author parent 或 formal packet。

quality、solver 和 review 分别使用各自明确登记的 policy_id，可以不同；来源记录保留其实际原 policy，不改写成消费角色的 policy。同一个 review 的 facts 与 score 使用该 review 注册时的同一 policy，不能在阶段间切换。

runner 只用 `native_role` 或明确的中性 `test_fixture`。真实 solver 直接通过受限原生 Codex 执行，不再经过旧 inner runner。role_session 只能 fresh_persistent 或 fresh_ephemeral，不复用作者、试解、质量或评阅 SID。

管理层生成仅含实际公开 payload 的 `directory/public`，保留相同路径与字节。native repository 必须为 `{{public_root}}`；不得授予 candidate 原目录、task.json、质量控制目录或其他批次资料读取权限。模型最终所见 public inputs 仍绑定捕获候选的实际公开来源。

收件保存完整 work、scratch、trace、请求、进程及终止证据，并生成不可覆盖的 solver-receipt.json。该 receipt 只证明实际交付与来源，outcome 为 null，不表示答卷正确，也不是分数。

作答终态及评分终态的 run/collect 返回教师侧报告路径。报告依原件列出计量、逐项原分、原评理由和未知项；生成失败不会改变原收件结果。详见[每轮压测报告](stress-reports.md)。

## 评阅规格

review spec 同样包含 definition 与 evidence。共有身份字段同 solver，新增 `solver_task_id`、`facts_runner`、`score_runner`、`standards`、`criteria` 和可选 `correction_limit`（默认 0）。同一 solver 原卷只登记一个原评阅任务；不同 flow ID 不能制造多个原分。

两份 runner 必须显式确认同一模型、推理档位、provider、CLI 和 persistent home；mode 相同。模型选择不从旧任务或默认配置猜测。native reviewer 不接受模拟 solver 升格为真实答卷来源。

`standards` 是接受质量原签 native_signoff.normative.files 的完整名单；不能加入其他作者研究、历史答卷或删掉必要参考。`criteria` 显式给出每项 `id`、`max_points` 和 rubric `source={path,sha256,location}`，其 ID/上限必须与当前 task.json.scoring 一致。

完整 task.json.scoring 仅通过纯 JSON 投影 normative/task-scoring.json 运输，字节与原冻结声明独立比对，不携带整个 task.json 或控制字段。首 facts 不得到标准、criteria、投影或 source-binding 的正文；这些只在事实封存后的 score 阶段追加。

facts 与 score 使用不同的阶段 role，共用合法 reviewer model_slot；实际 reviewer/model/provider/effort/home/profile/candidate/run 必须不变。manager-only run_binding 绑定确切 solver 原件及已冻结标准，不成为事实阶段的模型输入。

## 原生权限

两个新入口的模型工具只能读取自身公开视图、当前 staged inputs、自有 work/scratch，以及明确的实际安装运行时。不能把任意外部目录标作 runtime 后获得读取。

native_permissions.python 必须为当前管理解释器；允许其实际 sys.prefix/base_prefix 安装视图。文件系统基线为 :root deny、:minimal read、:tmpdir deny、:slash_tmp deny、/private/tmp deny；显式 home deny。未知 special scope 拒绝。scratch 必须是本次 invocation 的 canonical 普通目录，不能指向其他项目或 symlink。

runner 支持 `{{public_root}}`、`{{work}}`、`{{scratch}}`、`{{runtime}}` 和 `{{inputs_root}}` 等当前角色上下文占位符，字典键和值均展开；未知变量或展开后键冲突拒绝。probe_sources 和 readonly_probe 指向公开视图中确实存在的原件，不依赖仅首 turn 存在的声明文件。

实际模型启动前仍通过正常零模型权限/工具预检。配置、目录和哈希不等于平台隔离证明；权限或系统审批拒绝不能绕过。

## 交付与封存

facts.json 使用唯一 review_facts_v1 结构：binding、status（complete/incomplete）、coverage、facts、limitations。coverage 覆盖全部公开输入和完整实际答卷/运行来源，每个来源标实际 SHA、read/inspected/unresolved、位置和限制。facts 为原角色陈述与引用。不得预评分或加入教师判断。

score.json 使用 review_score_v1：binding、outcome（scored/inconclusive）、facts_sha256、validity、criteria、total、limitations。模型给出每项 points/max_points/reason/evidence，Python仅核冻结项目、证据位置及原分加总；local_eligible 保留角色的 true/false/null，不推成官方认证。

引用使用实际 `L1[-L2]` 行或半开 `B0-B1` 字节范围。共享只读 preflight 与收件校验同用一份纯合同，错误带结构化字段位置。模型在当前 turn 内自行处理错误，不由 Python 修报告。

每次评阅调用把完整阶段context保存于自有work的`.taskflow-assessment-context.json`，作为不可覆盖、哈希锁定的运行依赖；preflight用`--context`读取该文件。请求只给必要新阶段结构和当前定位，不把整份JSON同时塞进协议及命令。facts文件仅含公开/实际答卷来源，criteria、scoring和教师定位只在封存后的score文件出现；typed inputs及facts→score身份校验不变。修改该依赖会在运行/采集时拒收，不能据修改后的参数收分。

只有 complete 且结构/来源有效的事实可显式 seal。封存重复调用幂等；已有有效 seal 不改写。正常 post-collection seal 是单独核验的追加附件，旧归档及 collection 不回填。score 沿最后被封存的实际 facts SID 续接，材料只增加同版冻结 teacher 和原封存 facts。

完整阶段参数保存为本次 work 的 `.taskflow-assessment-context.json` 并加入运行依赖哈希，预检通过 `--context` 读取；请求仅给入口，不重复内联完整 JSON。同阶段纠错只追加本次错误、绑定与必要定位。typed inputs 和 facts/score 身份规则不变，首 facts 的参数仍无教师资料。文件被改写或删除会拒绝采集，不用新调用掩盖原尝试。日常 collect/status 可用 `--brief`；默认机器返回及完整原件保留，`--full` 按需展开。

## 有界结构纠错

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 review correct review-01 --stage facts --record /absolute/correction-authority.json
python3 -m taskflow execution --db /absolute/state.sqlite3 review run review-01 --stage facts --allow-model
```

record 是 `{"evidence": [...]}`。correct 仅登记明确授权额度内的同阶段下一 turn，不调用模型。仅技术完成、交付不合法且问题确实可纠正时可用。每次继续保留原 JSON、原结论、错误及来源，签署新 intent。

原已合法的 criterion 原分与有效业务签发字段不可借结构纠错变更；不完整/未决业务结论不能强改成完整/有分。Python 不代算分数或预设模型应通过。score valid 后只写唯一原 score receipt，不能再用 correct 覆盖原分。failed/unknown 不自动重派。

同阶段结构纠错只追加当前错误、新调用身份、context定位和preflight，不重复已提供的整套facts/score说明；上下文中的business_lock保留原已合法业务字段。完整参数、原交付与归档继续保留，模型必要时可回读当前自有文件。请求字节减少不等于固定token或费用节省。

## 退役边界

旧 experiment-start、batch-experiment-start、round-bind、round-start 不再暴露。旧 experiment/evaluation-tail/bounded-round/original-score-pause lifecycle 不能从 CLI 运行或恢复；历史证据仍能查询。旧 formal/review 包装后端仅保留尚未拆除的内部依赖，不是新任务兼容入口。

此改造不宣称完整运行依赖 bundle 已实现。每次新调用仍锁其运行代码和依赖，source 安装须真实自然锁窗；不为旧任务撤销身份锁、热换源码或重放未知调用。

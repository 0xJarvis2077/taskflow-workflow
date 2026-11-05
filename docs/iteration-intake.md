# 迭代接收与下一投入

作者委托沿 `revision submit/prepare/run/collect`，独立复检沿 `quality`，主控接收沿已有 `execution apply` 的 `record_decision`。记录继续保存在现有 flow、work/attempt、decisions 及不可覆盖原件中；没有额外审批者或自动研究循环。

`purpose_kind` 明确选择 `quality_repair`、`mechanism_diagnosis`、`business_reconstruction`、`formal_experiment`。具体目的仍写 `purpose`，责任和预期变化仍写 `responsibilities`、`expected_changes`。分类不能替代具体委托。

主控通过[下一投入模板](../knowledge/templates/next-investment.md)向现有入口提交选择：

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 apply --record /absolute/next-investment.json
```

`record_decision` 的 `action`、`rationale`、`evidence` 保留下一投入决定。当前 `execution apply` 的所有新主控决定必须提供 `intake`，包括 research/repair/retain；旧已存记录继续可读。`intake` 分别保存：

- `purpose_kind`：本次用途类别。
- `quality={conclusion,evidence,limitations}`：当前质量的真实结论、原件及范围；conclusion 为 accepted、revision_required、inconclusive 或 unknown。
- `difficulty={status,conclusion,evidence,limitations}`：status 为 unknown、observed 或 inferred，区分未知、实际观察和原因假设。没有原件证据只能 unknown，不能由质量通过、总分变化或文件齐全推定。
- `experiment`：非投测为 null；正式实验提供 `important_unknown` 及至少两个 `outcomes=[{observation,next_action,reason}]`，说明不同结果如何影响下一选择。next_action 使用原有 research、repair、reconstruct、experiment、retain、environment。

新的明确 solver 入口必须取得完整 intake、精确用途与授权，且当前独立质量真正通过。旧 decisions 仍可读，仅有“quality passed”的旧决定不能直接进入新的正式作答。准备时将决定冻结为 `solver-decision.json` 并锁入运行依赖；它是管理资料，不进入 solver 的公开请求或 inputs。实际调用仍须显式允许模型。

合法短法、存在脚本、质量试解正确和高分均不构成程序自动停止条件。正常方法仍可能覆盖主要责任，也可能留下值得验证的重要未知；主控根据完整证据作选择。没有新重要未知时可以保留成果、转诊断或重构。Python只核解释及证据入口是否齐备，不替主控判断内容或扩展调用范围。

## 离线行为回放

现有回放工具增加 decision 模式：

```sh
python3 -m tools.execution_replay --decisions /absolute/decision-replay.json --output /absolute/new-offline-replay
```

spec 为 `schema_version=1`、`cases`。每项含唯一 id、来源 `sources`、before/after（record_decision 参数，身份和 evidence 由工具在临时库隔离）、expected_changed 和 expected_before_accepted。主控提供历史决定的可定位重建及正常对照；回放调用当前接收和正式决策合同，保存前后是否接收、实际动作及原因。工具使用中性 fixture 质量门，不启动模型，不操作真实执行库，不改历史。

必须同时检查应调整的决定与应保留的正常实验。回放仅证明流程对明确提案的约束与决定行为；不证明模型效果、增难成功或费用收益。业务成果变化另由真实修订 diff、责任分析和独立复检留证。

## 观察实际用量

每次新调用保留自己的 request、manifest、trace、collection、archive 和终态。精确 collection/SID 续接是上下文复用证据；不代表只按新增文字计费。`runtime_metrics` / 压测报告保留原 usage，并由 `usage_summary` 分别汇总 `input_tokens`、`cached_input_tokens`、`output_tokens`。缺失或不完整计数保持未知。按角色、版本、请求范围、模型/provider及实际 turn 对比；管理调用数不是 HTTP 请求数。

上下文复用、provider报告的缓存命中、实际账单分别核验。缓存计数来自实际 trace；实际费用以对应服务账单为准，不承诺固定节省比例。本工程只做离线验证。

## 模型文本与简短回执

模型侧保留业务任务、来源绑定及原件入口；完整 manifest、SHA、归档清单仍由机器保存和校验。来源索引是已锁定机器数据的确定性投影，不做语义摘要或伪造缺失内容。同会话续接只追加本次问题、版本差异、新证据和必要原件入口，完整协议按需可读；回读当前受影响业务单元仍是职责。

人或模型日常观察使用简短视图：

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 revision collect revision-01 --brief
python3 -m taskflow execution --db /absolute/state.sqlite3 quality status quality-01 --brief
python3 -m taskflow execution --db /absolute/state.sqlite3 solver status solver-01 --brief
python3 -m taskflow execution --db /absolute/state.sqlite3 review collect review-01 --stage facts --brief
```

brief 保留状态、原业务结论、错误数与前三条定位、当前调用身份及原件路径，并给出 `status --full` 的完整展开命令。默认 CLI 输出和 Python API 返回保持现有机器格式；revision 默认仍保留原有精简投影，显式 `--full` 才展开存储记录。brief/full 只改变输出视图，不裁剪数据库、manifest、哈希、trace或归档，不改变采集行为和失败退出码。

离线比较实际 UTF-8 请求和回执字节，同时核完整输入、机器索引及归档原件。字节下降不等于 token、缓存或账单按比例下降；没有为此启动模型调用。

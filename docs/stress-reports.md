# 每轮压测报告

报告面向教师与研发协调者，把已有原件整理为 JSON 和 Markdown。它不调用模型，不推进任务，不改原分，也不是新增评分或质量关卡。Python 统计可观测数据并排版；原评阅者的事实、得分、理由和证据原样保留，业务归因不由统计程序补写。

## 生成与查看

正式作答终态的 `solver run/collect`，以及评分终态的 `review run/collect --stage score`，返回 `report`。报告保存在相应角色任务的 `directory/reports/<content_digest>/report.json` 和 `report.md`。作答后尚无评分时明确显示未评分；评分后保留原总分和全部逐项结果。报告生成失败只返回 `report.status=unavailable` 与原因，已接收的原卷或原分不回滚。

也可独立读取已有任务，生成一个新派生快照：

```sh
python3 -m tools.stress_report --db /absolute/state.sqlite3 --solver solver-01 --output /absolute/private-reports
python3 -m tools.stress_report --db /absolute/state.sqlite3 --solver solver-01 --review review-01 --output /absolute/private-reports
```

该入口使用 SQLite `mode=ro` 事务，不创建数据库、不调用 collector、不重跑旧任务。仅原任务已接收的唯一 score receipt 可作为原分来源；存在但未受理的 score.json 不会成为正式成绩。已受理来源若哈希或跨卷绑定变化则报错。

快照按内容追加，相同内容重入保留原生成时间与字节，JSON/Markdown 缺一份时可补同一派生快照。输出不得进入 candidate、public、sources、invocations、runtime、work 或 archive 等受保护来源目录；它只属于教师侧私有交付，不发给作答或首阶段评阅。

## 固定展示内容

| 内容 | 来源与边界 |
| --- | --- |
| 总分与分母、逐项满分/得分/失分 | 原评分与冻结 criteria，不默认换成百分制；未决和未收件不是零分 |
| 得失分理由、完整评阅事实、证据 | 原评阅文档逐项展开，不按关键词猜业务归因 |
| 起止与墙钟时间 | 进程原始 termination；包含其中的网络等待 |
| 可见回答、命令、工具、文件事件 | 单位分列，只报告真实可观察计数 |
| 重连、原始用量、收费 | 原 trace 的可见事件与 usage；CLI 用量不等于已核实账单 |
| 请求模型/档位、实际 SID、served 身份 | 实际 manifest/collection/trace；未提供 served attestation 时保持未知 |
| 测试有效性 | 保留原 reviewer 的 local_eligible、理由与证据；专家/官方确认另列未知 |
| 教师访问、公开预填 | 不凭 public 输入或新 SID 判为无泄漏；原事实全文与未单独裁定边界分别展示 |
| 得分比较、稳定性 | 得分比较须有明确来源；可比样本不足时注明单次观察 |

新报告仅保留可统计指标，移除不可统计的官方步数、有效耗时和 M，JSON 也不输出对应未知占位。总耗时仍按真实进程起止计算，包含其中的等待。重复 completed item ID 只计一次；损坏行与无 ID 事件的计数限制另列。

## 显式得分比较与复测

需要比较原分与本批所选分数上限时，通过 `--rules FILE` 明确来源和分母：

```json
{
  "source": {"path": "/absolute/selected-rule.md", "sha256": "ACTUAL_SHA256", "location": "L1-L8"},
  "score_max": 50,
  "score_denominator": 100
}
```

此处仅比较 `原分≤score_max`。分母不同时不换算，模拟或本地资格未决/否时不作符合性结论；得分比较不代表整体难度达标或官方通过。

`--compare another-solver-id` 可重复指定同库已有原卷。报告保守比较题包摘要、策略、完整角色 runner 配置、实际模型/profile 与评分来源，角色自有 home 仅作为运行载体归一。配置不同、不具本地有效资格、未出原分或模拟夹具均单列排除原因。两份及以上可比原卷仅给分数范围、均值和总体标准差，不自动裁定稳定，更不把五个迭代版本当五次同版复测。实际 served 版本未知的限制继续保留。

# 运行与评价记录示例

这些是本地记录格式示例，没有执行模型、产生真实成绩或提交平台。命令在仓库根目录执行；将示例中的 `my-task`、版本、考点 ID 和路径替换成实际对象。完整流程见根 README，这里只说明运行与评分留证。

## 先回读实际交付的作答目录

`actual_input_digest` 应来自实际交付副本的文件回读。它与冻结快照的 `input_digest` 使用相同算法：按 `package.json.files` 原有顺序，将每个公开文件的 `id/path/role/size/sha256` 组成列表，再对其 canonical JSON 计算 SHA-256。下面的命令校验目录成员并重新读取文件字节；内容改变时输出实际摘要与 `matches: false`，成员缺失或额外混入文件时保留问题并将实际摘要标为未知。

```sh
python3 - tasks/my-task/exports/v1-solver <<'PY'
import json
import sys
from pathlib import Path
from taskflow.contracts import canonical, parse_json, safe_read, sha256

root = Path(sys.argv[1])
package = parse_json(safe_read(root, "package.json"))
if package.get("audience") != "solver":
    raise SystemExit("expected a solver package")
expected = {"package.json"} | {item["path"] for item in package["files"]}
actual = set()
problems = []
for path in root.rglob("*"):
    if path.is_symlink():
        problems.append(f"symlink: {path.relative_to(root)}")
    elif path.is_file():
        actual.add(path.relative_to(root).as_posix())
    elif not path.is_dir():
        problems.append(f"special file: {path.relative_to(root)}")
if actual != expected:
    problems.append(f"unexpected/missing members: {sorted(actual ^ expected)}")
observed = []
if not problems:
    for item in package["files"]:
        data = safe_read(root, item["path"])
        observed.append({
            "id": item["id"], "path": item["path"], "role": item["role"],
            "size": len(data), "sha256": sha256(data),
        })
digest = sha256(canonical(observed)) if not problems else None
print(json.dumps({
    "scope": "local delivered-directory byte readback",
    "actual_input_digest": digest,
    "declared_input_digest": package["input_digest"],
    "matches": digest is not None and digest == package["input_digest"],
    "problems": problems,
}, ensure_ascii=False, indent=2))
PY
```

保存此输出作为运行证据，并记录接收侧读取或上传回执。此命令只能证明所读取的本地副本状态；远端模型是否收到这些文件、是否另有可读资料，仍需对应环境证据。`run import` 保存传入的摘要与证据，不代替服务侧传递核验。无法取得实际输入证据时填 `null`，不直接复制快照摘要冒充实际接收结果。

## 导入一次运行

为每次尝试使用独立 `run_id`，包括失败、超时与部分交付。准备一个收件目录，例如 `incoming/local-fixture-001/`，放入 `run.json`、实际收到的 `answer.md`、`trace.jsonl`、`input-readback.json` 和 `isolation-review.md`。所有 `artifacts[].path` 相对 `run.json` 所在目录，禁止绝对路径、`..` 和符号链接。只复制清单列出的文件；收到的其他产物也要逐项列出。

下面是测试夹具模板。时间、模型和证据位置均为格式示例，不能据此报告实测；`actual_input_digest` 的字符串必须换成上一步实际回读结果，未知则换成 JSON `null`。

```json
{
  "schema_version": 1,
  "run_id": "local-fixture-001",
  "nature": "test_fixture",
  "actual_input_digest": "REPLACE_WITH_READBACK_DIGEST",
  "status": "completed",
  "requested_config": {"model": "local-fixture", "tools": []},
  "observed_config": {"model": "local-fixture", "tools": []},
  "started_at": "2026-10-29T10:00:00+09:00",
  "ended_at": "2026-10-29T10:01:00+09:00",
  "interventions": [],
  "exceptions": [],
  "input_evidence": ["readback#actual_input_digest", "trace#delivery"],
  "isolation_evidence": ["isolation#review-scope"],
  "artifacts": [
    {"id": "answer", "path": "answer.md", "kind": "answer"},
    {"id": "trace", "path": "trace.jsonl", "kind": "trace"},
    {"id": "readback", "path": "input-readback.json", "kind": "evidence"},
    {"id": "isolation", "path": "isolation-review.md", "kind": "evidence"}
  ]
}
```

```sh
python3 -m taskflow run import tasks/my-task/versions/v1 --record incoming/local-fixture-001/run.json
python3 -m taskflow status tasks/my-task
```

`nature: test_fixture` 用于工具测试与模拟记录，始终保持测试性质。只有实际发生且具有原始答卷或轨迹的模型运行才使用 `nature: observed_run`；没有足够证据时保留未知值，不补造配置和时间。

| 字段或情形 | 如何记录与解读 |
| --- | --- |
| 起止时间 | `started_at/ended_at` 写实际运行时间，使用带时区的 ISO 时间；不明填 `null`。工具另写 `imported_at`，不会将导入时间当作运行时间。 |
| 请求配置与可观察配置 | 分别填 `requested_config/observed_config`；模型或工具未知可填 `null`。未记录工具能力不能默认视为关闭或可用。 |
| 干预与异常 | 已确认没有时填空列表；未知填 `null`。排队、故障、人工等待及中途补充材料写入原始记录。工具不自行推断有效耗时或执行步数。 |
| 超时或未完成 | `status` 可用 `timeout/failed/cancelled/incomplete`，保留已经收到的答卷与产物，缺失项写明；不把部分交付改成 `completed`。 |
| 输入摘要不符 | 仍保存原记录和可读取产物，输出 `input_binding: mismatched`；该答卷不能据此计为所选版本的有效压测。 |
| 产物缺失或路径不安全 | 保留原始 JSON，跳过无法安全读取的产物，将具体问题列入 `gaps`；其他合法产物继续归档。 |
| 字段齐全 | `evidence_complete` 只表示这些字段与引用结构齐全；`model_validity` 仍为 `not_determined`。证据内容与隔离需独立审查。 |

导入目录保存 `record.original.json` 原始字节、所选快照描述以及捕获产物的大小和哈希。重复 `run_id` 不覆盖。无效 JSON、重复 JSON 键、非法 ID 或无法校验的快照会使导入失败，原收件目录仍需保留；不要将失败命令视为已归档。

## 原评、同标准纠错与新标准复判

先从冻结快照的 `scoring.criteria` 取得实际考点 ID 和 `max_score`。评价必须列全考点，分值在各自范围内，总分按题目配置加总，不统一改成 100 分。每项 `evidence` 引用该运行已归档的产物 ID，例如 `answer#section-2`；程序检查 ID 能否对应，具体定位是否支持结论仍需读卷核验。

下面仅示范一个总分 28、`c1` 上限 12、`c2` 上限 16 的中性夹具。其他题目需换成对应标准，不能直接导入这两个示例考点冒充实际评分。

`evaluation-original.json`：

```json
{
  "schema_version": 1,
  "evaluation_id": "eval-original-001",
  "kind": "original",
  "input_digest": "REPLACE_WITH_OBSERVED_RUN_INPUT_DIGEST",
  "evaluator": "local-fixture-reviewer",
  "review_status": "unreviewed",
  "items": [
    {"id": "c1", "score": 12, "evidence": ["answer#section-1"]},
    {"id": "c2", "score": 16, "evidence": ["answer#section-2"]}
  ],
  "total": 28
}
```

```sh
python3 -m taskflow evaluation import tasks/my-task/runs/local-fixture-001 --record incoming/local-fixture-001/evaluation-original.json
```

同一冻结标准下发现原评分错计，另存 `evaluation-correction.json`。保留原评文件，使用新 ID、父评价 ID 与 `kind: correction`：

```json
{
  "schema_version": 1,
  "evaluation_id": "eval-correction-001",
  "kind": "correction",
  "parent_evaluation_id": "eval-original-001",
  "input_digest": "REPLACE_WITH_OBSERVED_RUN_INPUT_DIGEST",
  "evaluator": "local-fixture-reviewer",
  "review_status": "reviewed",
  "items": [
    {"id": "c1", "score": 10, "evidence": ["answer#section-1"]},
    {"id": "c2", "score": 16, "evidence": ["answer#section-2"]}
  ],
  "total": 26
}
```

```sh
python3 -m taskflow evaluation import tasks/my-task/runs/local-fixture-001 --record incoming/local-fixture-001/evaluation-correction.json
```

采用新评分标准时，先冻结新版本；另写一个完整评价 JSON，使用 `kind: rejudgment`、新的 `evaluation_id`、原评价或纠错评价的 `parent_evaluation_id`，并增加实际新快照路径：

```json
{
  "evaluation_id": "eval-rejudgment-001",
  "kind": "rejudgment",
  "parent_evaluation_id": "eval-correction-001",
  "rubric_snapshot": "/path/to/project/tasks/my-task/versions/v2"
}
```

上方只是需要替换的字段片段，不能单独作为完整评价；还须填写新标准的全部 `items/total/evaluator/review_status`，`input_digest` 继续指向这份旧答卷实际收到的输入。省略 `rubric_snapshot` 时使用该运行的原快照；相对路径只能在评价 JSON 所在目录内引用，跨目录时使用明确的绝对快照路径。

```sh
python3 -m taskflow evaluation import tasks/my-task/runs/local-fixture-001 --record incoming/local-fixture-001/evaluation-rejudgment.json
```

只修改评分资料、公开输入不变，可以对旧卷按新标准单独记录。新标准依赖旧卷未见的公开材料时，记录标为 `exploratory` 并保存输入差异，不能回填成旧运行原成绩。原评、纠错和复判各有目录与原始 JSON，所有评价 ID 均不可覆盖；已存在原评后再次以 `original` 导入会保存并指出重复原评。评分范围、证据、父评价或汇总有误时，问题保存在 `validation_issues`，不能把“已保存”当成“评分有效”。

本地 macOS/Unix 用常驻 `.evaluation-sequence` 文件与操作系统协作锁，串行检查同一运行的原评和父评价关系；文件存在不表示正在运行，不应手工删除锁 inode。它不进入模型产物，也不会被 `status` 报为中断操作。`.lock` 和 `.staging-*` 则按 `pending_or_interrupted` 展示，先核实进程与残留内容，不自动删除。

## 每次回看

`status` 回读当前题目、冻结快照、归档答卷及评价，列出哈希异常、`gaps`、评价问题、过期检查和可能中断的操作。草稿变化会使旧检查标为 `stale`，不会改写旧冻结版本。文件哈希锁定的是被记录的字节；本地目录仍可被修改，不是防篡改存储。

作答包与评判包分离只限定导出成员。真实运行还要证明作答环境没有答案历史、不能读取评判目录或共享工作库，且保留输入传递证据。字段完整、程序检查通过、评分已保存和平台审核通过是分别需要证据的状态。

# 单题契约与本地命令

版本1；Python包0.1.0。唯一默认业务入口仍为根AGENTS和主skill。此文件说明程序字段，不另建出题指南。所有命令在仓库根目录执行，使用 Python 3.10+ 和标准库。核心建档、检查和离线测试不需要网络或模型服务。

独立角色的持久派发、缺陷返修和中断恢复使用[执行层契约](execution-layer.md)。本页的原包/归档入口保持兼容；已接管工作项通过`execution capture/release`绑定当前候选和独立质量，不用裸`freeze`绕开其未闭缺陷。旁路观测与实际接管分别留证。

## 建档与目录

```sh
python3 -m taskflow init my-task --topic my-topic
python3 -m taskflow check tasks/my-topic/packages/my-task --no-write
```

`init --topic` 创建 `tasks/my-topic/packages/my-task/`，含 README.md、task.json、record.md、draft/input、draft/evaluation、checks、reports、versions、runs、exports。已有归并集合必须显式题目归属；平铺集合和显式 `--root <题目>/packages` 保持兼容。分类导航和保留约定见[目录规范](task-layout.md)。下文 `tasks/my-task` 代表实际题包路径，已有包不迁移。草稿有意缺标题、项目和评分配置，首次check失败是待填写提示。已存在目标不覆盖。`check`默认追加 `reports/check-<随机ID>/report.json`；日常浏览用 `--no-write`。CLI退出码0表示操作完成，check发现结构错误为1，契约/IO拒绝为2；run/evaluation导入成功仍可能有证据缺口，应读返回JSON。

下面是一个中性结构示例，业务内容必须自行填写和核验：

```json
{
  "schema_version": 1,
  "task_id": "my-task",
  "title": "业务决策与交付名称",
  "project": {"name": "本项目", "phase": null, "rule_sources": ["local-project-rules"]},
  "sources": [{"id": "S1", "kind": "constructed", "reference": "题设构造记录及依据位置"}],
  "files": [
    {"id": "prompt", "path": "draft/input/prompt.md", "role": "prompt", "references": ["S1"]},
    {"id": "answer", "path": "draft/evaluation/answer.md", "role": "answer", "references": ["prompt"]},
    {"id": "rubric", "path": "draft/evaluation/rubric.md", "role": "rubric", "references": ["prompt"]},
    {"id": "record", "path": "record.md", "role": "teacher", "references": ["S1"]}
  ],
  "scoring": {
    "total": 7, "rubric_file": "rubric",
    "criteria": [{"id": "C1", "dimension": "决策", "max_score": 7,
      "condition": "按公开依据得出结论并给出证据",
      "allowed": "接受同等有效的替代推导", "tolerance": "按题目业务定义",
      "evidence": ["prompt", "S1"], "root_cause_group": "R1"}]
  },
  "reviews": [
    {"id": "business", "path": "reports/business.json", "kind": "business", "required": true},
    {"id": "artifact", "path": "reports/artifact.json", "kind": "artifact", "required": true}
  ]
}
```

文件/来源/考点ID是稳定ASCII标识；相对路径可含中文。文件必须位于draft/、checks/或record.md；拒绝绝对路径、上级越界、反斜线、NUL、符号链接、重复路径和公开/私有同一文件。普通文件使用目录描述符逐级读取，拒绝特殊文件；文件中的代码、宏不执行。files/references及criteria/evidence必须指向已声明文件或来源ID；公开文件不能引用私有文件ID。来源文字的真实性和自然语言引用是否准确仍由业务审查判断。

公开角色为prompt、input、template；私有角色为answer、rubric、teacher、verification。所有题目至少声明prompt、answer、rubric。公开空白模板保留template身份。评分总分为单题配置，criteria的max_score正数且加总相符；condition、allowed、tolerance、evidence不能空。allowed可写解释文字或文字/有限数字列表，含义由该题说明，不自动把它推断成可机器判定的答案规则；root_cause_group辅助人工复核，工具不自动按根因扣分。

声明文件默认拒绝零字节。仅私有`teacher`或`verification`的`.jsonl`控制样本可在该文件条目显式填写`"empty_content":{"kind":"zero_record_jsonl","reason":"无记录样本的具体含义","evidence":["S1"]}`；对象仅含这三个字段，reason非空，evidence是非空、唯一且已声明的文件/来源ID列表。此声明要求实际文件存在且恰为零字节，换行、空白或任何记录均不符合；它不允许空prompt、input、template、answer或rubric，也不改变模型交付的非空要求。结构检查不裁定无记录样本在业务上是否合理，仍由独立质量审查判断。

`empty_content`进入现有内容/私有评价摘要，并在冻结快照和evaluator导出的相应文件元数据中保留；evidence ID通过同包文件及既有sources解析。快照和导出回读仍核同一声明与实际零字节，删掉声明不能使空文件通过。solver仅导出公开文件和原五项文件元数据，不携带此私有声明。现有未声明空内容的文件继续严格拒绝。

## 审查绑定与冻结

正式签发业务审查前的独立公开质量试解，使用[实际隔离入口](isolated-quality.md)的 `prepare-draft --task <题目目录>`。它不要求已有 passed 报告，不发布 versions/exports，仅捕获带摘要的候选公开副本；原生权限探针通过后才允许单次执行。已有冻结包用同一入口的 `prepare`。实际成果仍须业务复核，来源草稿过期与模型收到的副本是否一致分别记录，不能把一次质量试解当作目标模型难度成绩。

`check`返回三个摘要：

- `content_digest`：task.json原始字节哈希，以及全部声明文件元数据与实际字节哈希。修改JSON空白也会使审查过期。
- `input_digest`：有序公开文件描述列表，每项只有id、path、role、size、sha256。与solver包的files对应，可从实际接收文件重算。
- `evaluation_digest`：私有文件元数据及评分配置。可区分相同公开输入下的评分修改。

reports内容、冻结时间和派生manifest不进入content_digest，避免自引用。不要把含content_digest的报告列入files，也不要把包括record.md自身的content/snapshot摘要回填record.md。计算明细如果只含公开文件逐个哈希，可作为verification文件；也可完整嵌入核算review报告，由最终manifest锁定。

审查报告由真实审查或显式可信核算生成；工具不会替作者填“通过”。从check返回复制当前摘要，填写：

```json
{
  "schema_version": 1, "review_id": "business", "kind": "business", "status": "inconclusive",
  "content_digest": "当前check的content_digest", "input_digest": "当前check的input_digest",
  "scope": "本次实际检查的内容和文件范围",
  "limitations": ["未覆盖或仍未决的部分"],
  "evidence": ["文件/来源ID及具体位置、推导和检查结果"]
}
```

status为passed/failed/inconclusive。每题必须配置required business与artifact报告；存在确定性核算时再配置calculation。check可以在审查缺失时结构passed；freeze要求所有required报告的身份、摘要与当前字节一致且status=passed。limited scope通过只支持报告写明的范围，不能扩称整题本质质检通过或完整Office视觉验收。

business报告的人工签发按[模型可见材料整包复审](workflow-methods.md#逐项检查公开材料是否提前完成判断)：evidence包含当前版核心判断的公开原句/单元格/字段位置、输入已完成与仍待作答的工作、处置及保留必要条件/修后公开推导的证据，或指向已声明为teacher/verification的对应record/底稿。核心预给与能力声明未协调、缺必要依据或尚无复检证据时使用failed/inconclusive，不能因计算和字节检查通过签发passed。该要求是业务审查的内容契约，不增加JSON字段；当前程序不会自动识别语义预解题，也不会判断上述证据是否实质充分。旧冻结与旧原评不回填新检查。

`check.reviews`同时展示绑定状态`state`与原报告的`reported_status/scope/limitations/evidence_count`，完整依据由`path`指向原件。过期报告可仍显示原先的passed，但其`state: stale`继续阻止冻结；缺失或无法读取的报告不会补造范围。摘要不展开完整evidence，也不自动判断范围是否覆盖全部业务责任。

```sh
python3 tasks/my-task/checks/verify.py --input tasks/my-task/draft/input --report /tmp/my-task-calculation.json
python3 -m taskflow freeze tasks/my-task --version v1
```

上述verify.py是单题作者显式编写的脚本，init不生成它。核算应从最终公开文件独立推导，再与标答比较，并包含正常、边界和故意错误验证；不要用隐藏答案数字代替推导。

freeze再次捕获/复核源字节，暂存里生成task.json、实际files、review报告、结构检查和manifest；核对完成后一次发布 `versions/v1/`。SHA锁定字节和关联，不能提供签名或不可写存储；协调篡改所有文件和哈希无法仅靠SHA证明历史真伪。导出、运行导入和status会重新验快照。

目录发布使用协作锁及内核no-replace rename；本机macOS已实测，Linux需要libc/内核支持renameat2，未在本轮实测，其他平台拒绝发布。普通异常/KeyboardInterrupt清理未发布暂存；强退可能保留隐藏lock/staging，status列为pending_or_interrupted。确认无进程操作并检查其中内容后再手动处理，不自动删锁。不承诺断电后的存储耐久性或对绕过工具修改文件者的权限防护。

## 显式分包与回读

```sh
python3 -m taskflow export tasks/my-task/versions/v1 --audience solver
python3 -m taskflow export tasks/my-task/versions/v1 --audience evaluator
python3 -m taskflow status tasks/my-task
```

导出目录默认 `exports/v1-solver` / `v1-evaluator`，可以`--output`另指定新目录，已存在则拒绝。solver只含公开files及重新构造的package.json，不含完整task.json、私有文件名、评分、审查或目标。evaluator含同版公开files和全部私有files、评分元数据；审查报告保存在冻结目录中供审计。导出前后复核快照，并按逐文件哈希回读拷贝。它不自动打ZIP、不递归加入未声明文件，也不建立模型权限隔离。

ZIP附件必须给出archive_members，逐个列path/role，包括嵌套ZIP本身及其中成员，嵌套用 `inner.zip!/table.csv`。成员必须完全对应；真实成员名不能含保留分隔符`!/`。拒绝私有成员混入公开ZIP、越界、符号链接、加密、同名/文件目录冲突和不支持的嵌套压缩格式。技术展开限额为3层嵌套、2000成员、累计128MiB；超限先拆包再显式登记。RAR/7z/tar/gz不在通用首版支持范围，先用可信工具检查后解包、逐文件声明，不能直接通过冻结。

OOXML容器自动检查ZIP安全与CRC；文本检查UTF-8。PDF和其他格式自动层只锁字节，内容/公式/分页/视觉由artifact工具和人工审查另记。自动检查通过不证明文件语义正确、无答案泄漏或业务任务成立。

运行/评价字段、示例和留证边界见[记录示例](record-examples.md)。状态基于证据展示当前错误、报告新鲜度、冻结完整性、运行缺口和可能中断；平台状态没有回执即未知。

出题、作答与独立评阅的角色/上下文及交接、过程采集范围和逐关键判断证据链，按[压测方法](workflow-methods.md#先完整读卷再评分与归因)、[复盘方法](workflow-methods.md#用证据驱动下一轮)保存于现有record或评阅报告。可将原请求、trace、工具结果、产物版本和事后映射以现有run artifacts/evaluation evidence追加关联，不新增必填JSON字段。程序归档只核对应文件/摘要，不验证全部工具响应已进入模型、不还原内部思维、不自动证明独立评阅。补提取索引标注父文件、事件/行号与生成时点；历史未采集的内容留缺，不能伪补进旧run。运行中的缺口先旁路审计，采集器或依赖变更留待下一运行前核验，不能改变已锁运行。

`status`中的运行显式显示`version_id/input_binding/input_digest/actual_input_digest`、快照/评分摘要和完成状态；评价显示父评价、`classification`、所用`rubric_version_id`和评分/输入摘要。它们来自归档，用来辨认旧卷原评、纠错与探索性复判，不把新版本的存在推成已有新测试。未知字段保持未知。当前`task.json`解析或安全读取失败时，`check.status`为failed并说明错误，仍继续独立校验历史快照和运行；当前摘要未知，历史检查标为`unknown_current_input`。此时`status`成功返回审计结果的退出码仍为0，不能据此称草稿通过；直接`check`的既有退出码规则不变。

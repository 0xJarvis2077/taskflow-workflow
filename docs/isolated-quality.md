# 公开质量试解的实际隔离入口

`tools/isolated_quality.py` 为**新公开质量试解**提供可重复的准备、原生权限探针和单次执行入口。它不修改既有题目、冻结、旧卷或评分，不将质量试解计为目标模型难度压测。Python 3.10+、标准库；当前原生权限配置来自本机已验证的 Codex CLI，其他系统须重新验证，未通过探针不得执行。

## 使用

在仓库根目录，以将来运行 copied runner 的同一个 Python 解释器准备。`--output` 必须是仓库外的新目录，其父目录已存在。

**投测前的新题/返修质量试解使用 `prepare-draft`**，不必先取得 business passed，也不必正式冻结：

```sh
python3 -m tools.isolated_quality prepare-draft \
  --task tasks/<id> \
  --output /private/tmp/<new-quality-runtime> \
  --cli /absolute/path/to/codex \
  --model '<catalog-model-slug>' \
  --reasoning-effort '<supported-effort>' \
  --python /absolute/path/to/tool-python \
  --dependencies /absolute/path/to/bundled/dependencies \
  --rtk /absolute/path/to/rtk \
  --repository /absolute/path/to/repository
```

此模式使用既有 `taskflow check` 的安全捕获与结构/角色契约，以 `write_report=False` 前后核对同一 content/input 摘要，并从实际安全读取字节重算。需要结构完整、公开/私有角色明确；**审查缺失或尚未通过不妨碍准备**。只复制显式公开文件和中性六字段 package；`version_id` 明标 `candidate-*`，不带教师评分、私有文件清单或研发目标。content 摘要、完整声明来源与新鲜度记录只留在教师侧计划。不会发布 `versions/`、生成 `exports/`、回填 passed 或修改原 task/report。

已冻结版本的事后核验仍使用原 `prepare`：

```sh
python3 -m tools.isolated_quality prepare \
  --solver tasks/<id>/exports/<version>-solver \
  --snapshot tasks/<id>/versions/<version> \
  --output /private/tmp/<new-quality-runtime> \
  --cli /absolute/path/to/codex \
  --model '<catalog-model-slug>' \
  --reasoning-effort '<supported-effort>' \
  --python /absolute/path/to/tool-python \
  --dependencies /absolute/path/to/bundled/dependencies \
  --rtk /absolute/path/to/rtk \
  --repository /absolute/path/to/repository
```

该模式核验整个冻结快照及 solver export 的精确公开清单/摘要。两种模式复用同一准备/权限/探针/执行实现，只将公开字节复制到 `work/`。复制的输入保留 POSIX 644；只读由实际原生权限执行。教师参考、旧卷、知识索引、会话或自定义配置不会进入公开工作目录。可重复添加 `--deny-read /existing/private/path`，增加真实存在的拒读目标。

两种准备命令均须显式传 `--model` 和 `--reasoning-effort`，不接受空值、首尾空白、`auto` 或 `default`。准备时只运行所选 CLI 的 `--version` 和 `--no-daemon debug models --bundled`；后者跳过刷新，不请求模型。模型 slug 及对应推理等级必须在该 CLI 随附目录中匹配，不内置目标模型名单。可先用相同的 `debug models --bundled` 命令查看 `slug` 和 `supported_reasoning_levels[].effort`，替换示例占位值。目录/版本命令失败、格式不可用或组合不支持时，保留 `cli-evidence.json` 后拒绝生成可执行计划；不回退到默认值或在线刷新。

候选源与模型副本分别核对：投测前 `validate` 发现原 task.json 或任一声明来源改变时拒绝启动，要求重新准备新候选；实际运行中若原草稿后来变了，`attempt.json` 会记录 `source_freshness: stale`、`current_source_eligible: false`，而 `input_readback` 仍按模型实际收到的锁定副本判定。这样可保留本次原卷证据，同时明确其不能支持当前已改草稿的放行，不把源过期说成模型拿错输入。

接着使用准备时的同一个 launcher Python，执行保存的独立 runner。前三步均无模型调用；只有最后一条启动一次模型请求：

```sh
python3 /private/tmp/<new-quality-runtime>/runner.py probe /private/tmp/<new-quality-runtime>
python3 /private/tmp/<new-quality-runtime>/runner.py validate /private/tmp/<new-quality-runtime>
python3 /private/tmp/<new-quality-runtime>/runner.py execute /private/tmp/<new-quality-runtime>
```

`probe` 调用真实 `codex sandbox`，在与正式工具相同的 profile 下通过 shell/Python 逐件读取公开文件、比对 SHA，并验证公开文件及目录拒写；教师文件、仓库、旧运行（存在时）、索引、Codex 配置/会话（存在时）和指向禁止目标的符号链接拒读。只将 EPERM/EACCES 认作权限拒绝；缺文件或其他 IO 错误不能获得 ready。输出目录和 scratch 同时有正常读写对照。

`execute` 再核验输入/runner/解释器/CLI/依赖锁及探针，使用新 `exec --ephemeral --ignore-user-config --ignore-rules -m <model> -c 'model_reasoning_effort="<effort>"'`。禁用宿主技能发现、skill search、记忆、插件、hooks、apps 和多代理；保留正常 shell/Python/分析及网络工具配置。`validate` 在使用前同时核对 config、`model.requested` / `model.reasoning_effort`、完整 argv 和锁定的 CLI 目录/版本证据，缺失、默认选择、不一致、重复覆盖参数均拒绝启动。`plan.json` 和成功或失败的 `attempt.json` 均保留明确请求值及 CLI 版本证据引用。

本机 `codex-cli 0.165.0-alpha.1.2` 的已检查帮助入口未提供同一次 `exec` 最终配置的无模型请求导出方式，因此 `model.client_resolved` 记为 `null`，范围写在 `client_resolution_scope`。本地目录校验只证明该 CLI 声明支持该组合，不证明实际请求已解析、账户可用或服务端已采用；`served` / `revision` 均为未知，不向模型索取身份自报。不会新增模型探测调用。

此修复只影响新准备的 runtime。当前工具会拒绝缺少上述字段/证据的旧计划；应使用新输出目录重新准备。既有 runtime 中复制的 `runner.py`、计划、运行和评分保持原字节，其历史默认模型不能据此追认成明确模型。质量用途仍与正式目标模型难度运行分开。

全局 `AGENTS.md` / `AGENTS.override.md` 只能缺失、为空或仅引用已核中性的 RTK 文件。默认批准摘要对应当前短 RTK 执行约定；政策变更时，主控须先阅读确认，再显式传 `--neutral-rtk-sha256 <digest>`。所有自动指令路径的**存在性和字节**都会在验证/执行前重查。公开包使用 `AGENTS.md`、`AGENTS.override.md` 或 `.git/.codex/.agents` 自动发现保留名时拒绝准备，不默默修改冻结材料。

## 留证和失败行为

- `plan.json`、`plan-lock.json`：公开输入、argv、请求、权限边界及 `source_binding`（冻结或 draft candidate 来源）。
- `cli-evidence.json`：无模型请求的 CLI 版本及随附目录命令、退出状态和原始 stdout/stderr；受 source lock 和计划证据摘要约束。目录不是服务端身份证据。
- `source-lock.json`、`prepared-lock.json`：独立 runner、launcher Python、CLI、工具及已载入依赖的字节锁。执行不导入可变项目模块。
- `native-probe-*.json`、`ready.json`：原生探针原始 stdout/stderr、逐项预期、实测及锁定引用。
- `claim.json`：一次性执行占位。启动失败、超时或结果不完整都保留，不自动或原身份重试。
- `run/request.md`、`argv.json`、`trace.jsonl`、`stderr.txt`：实际请求/命令及原始可见输出。
- `run/trace-summary.json`、`answer.md`、`artifacts/`、`attempt.json`：唯一新 thread、完整 turn 状态、最后可见答复、最终普通文件副本与输入回读。进程退出 0 本身不代表完成。

交付仍须主控阅读实际成果和 `quality-note.md`。本入口不产生评分或自动签发业务通过。保存最终文件与可见 trace，不宣称覆盖每次文件修改、全部网页返回、完整生产请求或隐藏思维；不修补历史缺证。符号链接、特殊文件、单文件超过 16 MiB、总文件超过 128 MiB 或 2000 件会拒绝输入/产物捕获，错误留在记录中；不会展开附件 ZIP。

探针只是对所列操作和路径的技术证据。SHA 不是签名或不可变存储；拥有主机写权限的人协调改写所有数据/锁不在此工具的证明范围内。执行前检查与 CLI 真正读取之间仍有主机并发修改窗口，因此运行目录应由单一主控持有。

## 离线验证

```sh
python3 -m unittest tests.test_isolated_quality -v
python3 -m unittest discover -s tests -v
```

中性测试不调用模型，也不把 mocked 原生结果当作实际隔离证据；实际使用必须独立运行 `probe`。测试覆盖缺审查的草稿准备、无冻结/导出副作用、候选来源/副本分离、捕获间源漂移、公开/私有分包、冻结不匹配、自动指令及依赖读取例外、摘要/输入/解释器漂移、缺失目标与错误 errno、符号链接、未完成 turn、启动失败、超时、单次占位和产物留存。

新增回归覆盖两种准备入口的参数必填和传递、空值/默认值/本地目录不支持的组合、目录命令失败留证、模型/推理元数据与 argv 一致性及篡改拒绝。按项目现有 stdlib `trace` 可执行行覆盖规范检查本工具，门槛80%；`trace` 不提供分支覆盖，CLI 目录与版本测试夹具也不能代替真实服务端核验。

CLI 参数的依据为本机 `codex exec --help`、`codex sandbox --help` 及 [OpenAI Docs：非交互模式](https://learn.chatgpt.com/docs/non-interactive-mode)。主入口见 [README](../README.md) 和 [单题契约](task-contract.md)。`attempt.exceptions` 仅记录运行器异常；模型自行编写脚本的失败与修正保留在原始 trace 中，不能由该列表为空推定全程无工具错误。

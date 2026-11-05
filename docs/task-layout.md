# 题目目录与生成物

一个题目使用一个根目录，同题的新包不能继续散落到 `tasks/` 根。题目身份由操作者明确指定，不从包名、成绩或修改时间猜测。

```text
tasks/<topic>/
  README.md                    人工维护的题目说明
  INDEX.md                     可重建的分类导航，不是质量或成绩签发
  packages/<task-id>/           各自有 task.json 的题包
    draft/input/               公开题面与附件
    draft/evaluation/          私有参考与评分
    checks/                    本题核算
    reports/                   审查报告与核算证据
    versions/<version>/        冻结版本
    exports/<version>-<role>/   显式分包
    runs/<run>/evaluations/    原卷与原评、纠错、复判
  research/<research-id>/       教师研究
  prototypes/<prototype-id>/    尚未采用的试作
```

新包使用明确题目归属：

```sh
rtk proxy python3 -m taskflow init capacity-02 --topic Q1
rtk proxy python3 -m taskflow check tasks/Q1/packages/capacity-02 --no-write
```

`--root` 是集合根，`--topic` 创建 `ROOT/TOPIC/packages/TASK_ID`。已有归并题根的集合未指定 `--topic` 时拒绝创建，避免再次散落。显式 `--root tasks/Q1/packages` 及尚未归并的平铺集合保持兼容。已有目录仍拒绝覆盖；题包内部冻结、导出、原卷与原评的路径契约不变。

## 导航与日常检查

```sh
rtk proxy python3 -m tools.task_catalog --root tasks
rtk proxy python3 -m tools.task_catalog --root tasks --write
```

默认只读；`--write` 仅更新工具自己生成的总 `INDEX.md` 和题目根 `INDEX.md`，不覆盖人工同名文件，不进入冻结版本或执行副本写文件。原有 README 与历史正文保留原身份。导航依据实际目录分类，目录存在不证明采用、通过、原分或当前状态，不能从修改时间推断最新有效候选。

日常读取用 `check --no-write` 或 `status`，无需为浏览目录追加 `check-*` 报告。需要保存本次检查证据时使用原有 `check` 存档入口；历史报告不会自动去重或删除。

## 运行材料与交付

角色执行的 `runtime/work/scratch/archive` 由显式角色任务目录管理，不在 `reports/` 新建完整题包或运行工作树。执行库、配置、会话及外部交付仍由原位置管理，题目入口可以引用它们；目录导航不改变运行绑定或授权。

当前原卷、原评、冻结输入、有效候选与其必要证据保持完整。旧试作与可再生中间产物的清理按明确保留范围生成删除清单，核对没有在途或正式来源引用后执行；不能将重复字节、旧修改时间或没有通过单独当作可删证明。不得自动清理锁、staging 或执行未知的工作。

`INDEX.md` 与教师侧目录不能直接作为公开求解输入。正式作答仍仅接收显式导出的公开文件。

## 大型派生文件的冷存与恢复

已结束研究中不属于正式输入或原卷的巨大展开文件，可显式逐文件冷存。默认只读；`--apply` 才写入 gzip 和原路径/SHA/大小/权限/时间清单，并在流式解压校验成功后删除展开副本：

```sh
rtk proxy python3 -m tools.task_storage pack --package <题包路径> --path reports/<派生文件> --apply
rtk proxy python3 -m tools.task_storage restore --package <题包路径> --path reports/<派生文件> --apply
```

工具拒绝声明文件、正式版本、原卷、导出及运行工作树；不负责判断任意文件在业务上是否可冷存，操作者仍须核用途和依赖。`cold-storage/` 保留压缩原件与恢复清单，恢复拒绝覆盖已有不同内容。需要该展开文件的历史核算应先恢复，正式原卷与原评保持普通目录可读。题目根可用 `STORAGE.md` 记录具体恢复要求，并由分类导航链接。

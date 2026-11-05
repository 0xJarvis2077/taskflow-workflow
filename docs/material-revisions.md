# 材料修订与受影响复检

修订从已捕获并核验的冻结父材料派生独立可写副本。Python负责准备、唯一执行、完整实际差异、原件及签发来源；Codex负责诊断、实际修改、影响判断与独立审查。同一逻辑作者的合法父子版本默认续接精确原会话，每次仍建立独立调用记录；原件副本和成果复用本身不等于会话续接。

## 明确修订

```sh
python3 -m taskflow execution --db /absolute/state.sqlite3 revision submit --spec /absolute/revision.json
python3 -m taskflow execution --db /absolute/state.sqlite3 revision prepare revision-01
python3 -m taskflow execution --db /absolute/state.sqlite3 revision run revision-01 --allow-model
python3 -m taskflow execution --db /absolute/state.sqlite3 revision collect revision-01
python3 -m taskflow execution --db /absolute/state.sqlite3 revision status revision-01
```

submit、prepare、collect和status不调用模型。run只执行该任务已准备的一次明确调用，必须显式允许真实模型。不会安排质量、新solver或下一研发。重复准备、执行和采集消费原work/attempt；failed/unknown不重派。旧任务不兼容执行。

spec为`{"definition": {...}, "evidence": [...]}`。definition显式提供schema_version=1、flow_id、task_id、owner、author_id（稳定逻辑作者）、policy_id、绝对directory、candidate_id（父版）、candidate_root（父版原件目录）、revised_candidate_id（预留新版身份）、purpose、purpose_kind、expected_changes、responsibilities、sources和runner。purpose_kind明确选择quality_repair、mechanism_diagnosis、business_reconstruction或formal_experiment；分类不授权正式调用。本批模型与档位来自显式runner/policy，不从旧批次或CLI默认推断。可复制[修订派工模板](../knowledge/templates/material-revision.md)。

- expected_changes为`[{"path":"evaluation/reference.md","reason":"本轮具体目的"}]`，空列表明确表示本次无需修改。
- responsibilities为`[{"id":"reference-consistency","description":"需同步核验的业务责任","paths":["input/facts.csv","evaluation/reference.md"]}]`。责任来自业务委托，Python不按文件类型或关键词推断影响。
- sources沿现有归档入口提供实际请求、可见工具调用与返回、代码、中间成果、完整终稿、封存事实和原评的evidence引用，含path、sha256、location、source_role（request/tool_calls/tool_results/tools/code/intermediate/answer/facts/review/other）、可选size和唯一target（`history/`下）。原件仅进入修订角色的私有资料视图，正文不重复塞入请求。diagnosis.mode=post_run表示实际运行后的复盘；尚无投测的修订使用material_revision。sources可以明确为空；缺失历史保留未知，不能伪造过程或自动重跑。
- diagnosis.history_inventory可逐source_role说明status=unknown、does_not_exist、archived_not_provided或provided及reason。archived_not_provided须给originals精确归档引用；provided必须有实际sources输入。没有声明且未提供的类别自动保持unknown，不推断不存在。准备产生的original-index同时保留完整原件位置和以上状态。
- runner使用native_role或中性test_fixture，指定当前已确认CLI/provider/model/effort及role_session。repository使用独立只读父副本`{{parent_root}}`，工作区使用`{{work}}`，编辑目录为`{{revision_root}}`（即work/candidate），仅授权自身工作/资料和实际运行时；父目录禁止写入。readonly_probe及probe_sources使用该只读父副本内真实文件，不能指向可写副本。原父目录不授权，独立父副本也不得配置写权限。

resume_existing明确提供source_revision_task_id及role_session.previous的精确collection path/SHA/size，home沿原实际home。原修订须为合法完成、确实产出当前父版且属于同author_id/task/policy的持久作者；模型、provider、权限与资料边界由底层再次核验。执行层生成role_lineage及role_transition，绑定真实父子差异、完整typed input变化与只读父副本位置变化，不删除通用同身份校验。每次新请求、work、intent、attempt和归档独立，原件不覆盖。会话锁持续到CLI结束，同一原collection只能产生一个后继，unknown占位不会自动释放或重派。

首次作者使用fresh_persistent并记录session_reason=initial_author；已存在captured或external父作者不能冒称首次。不能恢复、原载体为ephemeral或明确换作者时分别记录original_unrecoverable、original_ephemeral或author_changed；以上例外都须有session_reason_detail。original_ephemeral还须核精确captured父作者manifest确为fresh_ephemeral，不能把persistent或来源未知改标为ephemeral；无法核源时使用original_unrecoverable并说明。fresh不填写previous/source_revision_task_id。fresh_ephemeral只用于明确不保留上下文的载体，不能伪造续接。已有父作者未迁移到本合同或身份缺证时，保留未知并用有据fresh例外，不扫描其他角色会话。

父版、管理侧独立原件和作者可写副本不共享可写硬链接。模型从简短revision-context读取本次目的、责任、预期改动、真实父差异、新证据和不含整套SHA的路径清单；完整revision-brief/original-index及其摘要仍保留给机器绑定和按需核验。首次提供必要交付合同，同作者续接只追加本次变化，精确相同的原operation段不重复附加；新增指令照常进入本次锁定请求。完整协议留在execution/revision-contract.md，原材料仍全部可读，不用正文拼接冒充会话。Codex先定位，再读取当前受影响的完整业务单元；记得父版不能省略必要核验。历史完整答卷、事实和原评用于检查正确成果、合法短法、重要遗漏及材料/参考/评分/环境/能力归因，不能只改扣分点。

作者直接修改副本，仅输出repair-record.json及修改原因、影响和建议复核范围，不重新输出整包。runner仅为repair接受绑定全部初始成员与锁定原件字节的seeded_output_trees，并在派发前重验普通独立文件，陈旧、额外成员、硬链接或种子篡改均拒绝；其他角色仍拒绝预存输出。程序保留未变文件，计算成员、路径、角色、字节和task.json声明的完整差异。与作者声明不符时保留原交付，返回具体差异；Python不替作者改业务结论。需要新调用时新建明确任务/intent/attempt，不重开终态或复活ephemeral SID。

作者记录复用已有repair合同：role=repair、parent_id、candidate_id、defect_ids（本次responsibilities IDs）、needs_business_decision、changes、declaration_change和impact_analysis。changes各项含path、before_sha256、after_sha256、reason、impact；新增/删除用null。task.json独立放declaration_change，同样给before_sha256、after_sha256、reason、impact。impact_analysis包含summary、非空文字建议recommended_scope及associated_paths；建议仅供人审阅，不自动选择质量范围。独立quality任务的scope仍须显式指定。

同一repair-record还须含responsibility_analysis，逐本次责任ID连接public_work→processing→final_result→business_consequence→next_action。前四项各为{status:supported/unknown,summary,evidence:[{target,location}]}，target定位实际提供的typed input，supported不能只有空引用。result区分success、failure、mixed或unknown；成功终稿有证而过程缺失时仍保留成功结果与处理未知，不能用总分变化或“读过日志”补造处理。next_action提供disposition=changed/retained/unknown、reason及changed_paths；changed只能对应真实差异，保留正常方法无须修改。另须success_method_coverage={status:supported/unknown,summary,covered_responsibilities:[文字说明],evidence:[{target,location}]}，即使本次assigned都是失分责任，也检查合法成功方法仍覆盖哪些主要责任；没有原件则写明未知，不强迫每版有失败或旧方法失效。Python检查证据合同与实际变化，业务结论仍由角色负责。接收返回责任分析、成功方法覆盖、历史可用性和原件引用，主控据此分别保留质量结论、难度证据和下一投入决定，不把交付有效或质量通过等同于投测授权。

source_usage可列{target,trace_lines,analysis_ids}。只有保留trace中对应实际typed input的已完成工具调用、成功返回内容，以及关联责任分析同时存在，接收才记录used；单纯provided不会升级成使用证据。这只支持可见访问与有据分析，不推断隐藏思维或唯一因果。

空修改委托在副本实测无变化时不创建队列工作、不调用模型、不创建候选。模型执行后实际未改也不制造新版候选；已经发生的调用仍如实记账。已知缺陷修订另显式提供defect_ids与逐项defect_repairs=[{defect_id,disposition}]；defect_ids属于本次responsibilities IDs，disposition明确选择material_repair、private_repair或scope_adjustment，程序不按缺陷类型代选。真实采集后仅登记这些缺陷的repair；后续正常独立quality原签须明确覆盖，才通过既有close_defect关闭，不能以作者影响说明自动关缺陷。

真实变化生成带parent_id的新候选及revision-diff.json，原卷、原签、原分和父版字节不变。只改新标准也形成独立版本，不追改旧分。

## 公开原件与质量

公开重建、新solver和新首facts不能收到历史答卷、旧分或私有修订分析。新版solver保持独立公开作答；首facts仍完整读卷，封存后由同一实际reviewer/SID按该版冻结标准评分。材料修订、同标准纠错、新标准复判和新版投测分别记录。

公开成员、路径、角色及字节完全相同，才可显式引用父版公开重建原件。复用只是新版适用关系，保留原候选、原attempt/SID和原时间；不伪造新版重新执行。公开任一成员、角色或字节变化会拒绝完全相同复用，需另行明确安排当前公开重建。新公开任务仍使用底层明确enqueue/worker/runner与现有existing_isolated公开适配器，不恢复旧author/lifecycle链。

公开适用记录通过quality spec的public_predecessor_reuse={path,sha256,location}引用。记录kind=identical_public_predecessor_reuse_v1、schema_version=1，提供controller、task_id、predecessor（原attempt_id/work_id/intent_id/candidate_id/task_id/cli_sid）、source_candidate_binding、target_candidate_binding、quality_transfer=false、reason及evidence。两个binding含candidate_id/task_id/input_digest/content_digest/evaluation_digest，均为各自实际版本。

默认仍可提交整包quality。受影响复检由作者提出业务范围，独立quality在正常公开先行审查中确认或扩大范围；不增加范围审批角色。当前质量必须同时指明合法继承范围、新版复检及必要跨文件关联检查。文件未改但有推翻旧审查的新证据，也要重开相关责任。影响不能隔离时提交整包审查，Python不通过文件数量、扩展名或文字猜测业务范围。

## 用量与限制

原始trace、usage、原报告和完整归档保存在各invocation；CLI返回实际变化、合同错误及原件路径，不复述完整历史。runtime_metrics保留usage原值，派生输入、缓存输入、输出计数；缺失计数保持未知。调用数是管理层Codex执行次数，不等于HTTP请求数，CLI用量不是已核实账单。不实现Python摘要、上下文裁剪、压缩或缓存；Codex维护自身上下文，缓存语义以实际provider为准。

已取消的重复运行、未进入请求正文的历史报告及不再要求重新输出的未变材料，分别作为调用、阅读与生成优化说明。复制文件或减少文件数本身不证明token下降。真实token及耗时收益需同口径实测，本工程离线验证不预报节省比例，也不启动新业务轮。

旧author-start、teacher-successor-start、batch-author-start、batch-baseline-quality-start、lifecycle-register/run/resume和事故恢复写入口已从CLI移除。status/events保留历史只读查询，历史模块仍有未拆除的内部证据校验依赖；不得将其视为旧任务执行兼容。安装仍须覆盖全部已注册执行域实际锁、源基线和自然空闲窗口，禁止热改活动源码。

普通native quality会追加教师资料，不能作为公开重建载体；真实public_rebuild slot对此在准备及派发前拒绝，使用现有公开隔离适配器。没有把尚未迁移的native公开专用入口当成已实现能力。

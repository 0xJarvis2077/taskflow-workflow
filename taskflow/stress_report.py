"""Teacher-facing reports derived from original runs, never a scoring gate."""

import copy
from datetime import datetime, timezone
import fcntl
import hashlib
import html
import json
import math
from pathlib import Path
import statistics

from taskflow.contracts import TaskflowError, canonical
from taskflow.stress_metrics import runtime_metrics
from taskflow.stress_report_sources import load_report_sources


def _finite(value):
    return type(value) in (int, float) and (type(value) is int or math.isfinite(value))


def _rules(value):
    if value is None:
        return None
    keys = {"source", "score_max", "score_denominator"}
    if not isinstance(value, dict) or set(value) != keys:
        raise TaskflowError("report rules require an exact source and explicit score parameters")
    for name in ("score_max", "score_denominator"):
        if not _finite(value[name]) or value[name] < 0:
            raise TaskflowError("invalid report rule number: " + name)
    if value["score_denominator"] <= 0 or value["score_max"] > value["score_denominator"]:
        raise TaskflowError("report rule score denominator/maximum is invalid")
    source = value["source"]
    if (not isinstance(source, dict) or set(source) != {"path", "sha256", "location"}
            or not all(isinstance(source.get(key), str) and source[key].strip() for key in source)):
        raise TaskflowError("report rules need their actual source path/SHA/location")
    path = Path(source["path"])
    if not path.is_absolute() or path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
        raise TaskflowError("report rule source must be an ordinary bounded absolute file")
    if hashlib.sha256(path.read_bytes()).hexdigest() != source["sha256"]:
        raise TaskflowError("report rule source SHA differs")
    return copy.deepcopy(value)


def _evaluation(sources):
    review = sources.get("assessment")
    if not review:
        return {"status": "not_reviewed", "total": None, "maximum": None,
                "criteria": [], "validity": None, "facts": [], "limitations": ["尚无原评阅任务。"]}
    record = review["record"]
    declared = record["definition"].get("criteria", [])
    maximum = sum(row["max_points"] for row in declared)
    score = review.get("score")
    facts = review.get("facts") or {}
    scoring_manifest = review.get("score_manifest") or {}
    reviewer_model = scoring_manifest.get("model", {})
    return {"status": score["outcome"] if score else "not_received",
            "total": score["total"] if score else None, "maximum": maximum,
            "criteria": copy.deepcopy(score["criteria"]) if score else [],
            "validity": copy.deepcopy(score["validity"]) if score else None,
            "facts_status": facts.get("status"), "facts": copy.deepcopy(facts.get("facts", [])),
            "limitations": list(score.get("limitations", []) if score else ["尚无已接收的原评分；不采用未受理草稿。"]),
            "facts_limitations": list(facts.get("limitations", [])),
            "reviewer": {"requested_model": reviewer_model.get("requested"),
                "effort": reviewer_model.get("reasoning_effort"), "served": reviewer_model.get("served"),
                "sid": (review.get("receipt") or {}).get("sid"),
                "same_sid_as_sealed_facts_verified": True if review.get("receipt") else None},
            "phase_results": copy.deepcopy(record["state"].get("results", {}))}


def _thresholds(metrics, evaluation, rules):
    if rules is None:
        return {"status": "not_configured", "reason": "未指定本批得分比较标准与来源。"}
    points, maximum = evaluation["total"], evaluation["maximum"]
    score_ok = None if points is None or maximum != rules["score_denominator"] else points <= rules["score_max"]
    eligible = (evaluation.get("validity") or {}).get("local_eligible")
    verdict = score_ok if eligible is True and metrics.get("nature") == "real_native" else None
    return {"status": "score_at_or_below_limit" if verdict is True else "score_above_limit" if verdict is False else "unknown",
            "rules": rules, "score": score_ok,
            "reason": "仅比较原分与明确得分上限；分母不同不换算，不据此裁定整体难度或官方通过。"}


def _cohort_signature(sources, metrics):
    solver = sources["solver"]
    manifest = solver["manifest"] or {}
    definition = solver["record"]["definition"]
    model = manifest.get("model", {})
    model = model if isinstance(model, dict) else {}
    def runner_config(runner):
        selected = copy.deepcopy(runner)
        if isinstance(selected, dict) and isinstance(selected.get("role_session"), dict):
            selected["role_session"]["home"] = "<OWNED_ROLE_HOME>"
        return selected
    review = sources.get("assessment") or {}
    review_definition = review.get("record", {}).get("definition", {})
    def actual_profile(value):
        return {key: value.get(key) for key in ("mode", "model", "native", "policy_version")} if isinstance(value, dict) else None
    return canonical({"candidate": manifest.get("candidate_binding"),
        "policy_id": definition["policy_id"], "requested": metrics.get("requested_model"),
        "effort": metrics.get("effort"), "cli_version": metrics.get("cli_version"),
        "catalog": model.get("catalog_evidence"), "mode": manifest.get("mode"),
        "public_inputs": [{key: row.get(key) for key in ("target", "kind", "sha256", "size")}
                          for row in manifest.get("inputs", [])],
        "solver_runner": runner_config(definition.get("runner")), "solver_profile": actual_profile(manifest),
        "review_policy": review_definition.get("policy_id"),
        "facts_runner": runner_config(review_definition.get("facts_runner")),
        "score_runner": runner_config(review_definition.get("score_runner")),
        "facts_profile": actual_profile(review.get("facts_manifest")),
        "score_profile": actual_profile(review.get("score_manifest")),
        "criteria": review_definition.get("criteria")})


def _protected_roots(sources):
    paths = set()
    for role in (sources["solver"], sources.get("assessment")):
        if not role:
            continue
        record = role["record"]
        definition, state = record["definition"], record["state"]
        paths.update((definition["candidate_root"], str(Path(definition["directory"]) / "public"),
                      str(Path(definition["directory"]) / "sources"), str(Path(definition["directory"]) / "invocations")))
    return sorted(paths)


def _stability(sources, metrics, evaluation, peers):
    identity = sources["solver"]["record"]["flow_id"]
    signature = _cohort_signature(sources, metrics)
    accepted, excluded, seen = [], [], set()
    for source, observed, grade in [(sources, metrics, evaluation), *peers]:
        selected = source["solver"]["record"]["flow_id"]
        if selected in seen:
            raise TaskflowError("stability cannot count the same original solver twice")
        seen.add(selected)
        if _cohort_signature(source, observed) != signature:
            excluded.append({"solver_task_id": selected, "reason": "题包/策略/请求配置/评分来源不同，不混算。"})
        elif grade["status"] != "scored" or grade["total"] is None:
            excluded.append({"solver_task_id": selected, "reason": "没有已接收原分。"})
        elif observed.get("nature") == "test_fixture":
            excluded.append({"solver_task_id": selected, "reason": "模拟夹具不计真实复测。"})
        elif (grade.get("validity") or {}).get("local_eligible") is not True:
            excluded.append({"solver_task_id": selected, "reason": "本地评阅资格为未决或否，不计有效复测样本。"})
        else:
            accepted.append({"solver_task_id": selected, "score": grade["total"]})
    values = [row["score"] for row in accepted]
    result = {"status": "insufficient_samples" if len(values) < 2 else "descriptive_only",
              "sample_count": len(values), "runs": accepted, "excluded": excluded,
              "reason": "只统计明确选择且同题包、策略、请求配置及评分来源的原卷；不把跨版迭代当同版复测，不自动宣称稳定。",
              "served_identity_limit": "请求配置相同不证明未知的实际服务端模型版本相同。"}
    if len(values) >= 2:
        result.update(minimum=min(values), maximum=max(values), mean=statistics.mean(values), population_stddev=statistics.pstdev(values))
    return result


def assemble_report(sources, metrics, *, rules=None, peers=()):
    """Project original results; do not infer business claims or replace scores."""
    selected = _rules(rules)
    evaluation = _evaluation(sources)
    definition = sources["solver"]["record"]["definition"]
    review = sources.get("assessment")
    return {"schema_version": 2, "kind": "stress_test_report_v2",
        "generated_at": datetime.now(timezone.utc).isoformat(), "purpose": "教师侧派生报告，不是新增评分或验收关卡。",
        "identity": {"solver_task_id": sources["solver"]["record"]["flow_id"],
            "assessment_task_id": review["record"]["flow_id"] if review else None,
            "task_id": definition["task_id"], "candidate_id": definition["candidate_id"],
            "policy_id": definition["policy_id"]},
        "execution": copy.deepcopy(metrics), "evaluation": evaluation,
        "thresholds": _thresholds(metrics, evaluation, selected),
        "stability": _stability(sources, metrics, evaluation, peers),
        "validity_boundaries": {"official_confirmation": "unknown", "expert_confirmation": "unknown",
            "teacher_material_access": "not_separately_determined", "public_preprocessing": "not_separately_determined",
            "reason": "公开输入清单、文件哈希及新 SID 不单独证明无教师访问或公开答案预填；业务判断请看原评阅事实与原资格理由。"},
        "references": copy.deepcopy(sources["references"]),
        "protected_source_roots": _protected_roots(sources),
        "limitations": list(sources["limitations"])}


def build_report(database, solver_id, *, assessment_id=None, rules=None, compare=()):
    """Read a snapshot of registered originals. No DB/runtime write or model call."""
    sources = load_report_sources(Path(database), solver_id, assessment_id)
    metrics = runtime_metrics(Path(sources["solver"]["runtime"]))
    _consistent_measurements(sources, metrics)
    peers = []
    for peer in compare:
        data = load_report_sources(Path(database), peer)
        measured = runtime_metrics(Path(data["solver"]["runtime"]))
        _consistent_measurements(data, measured)
        peers.append((data, measured, _evaluation(data)))
    return assemble_report(sources, metrics, rules=rules, peers=peers)


def _consistent_measurements(sources, metrics):
    originals = {ref["path"]: ref for ref in sources["references"]}
    for ref in metrics.get("evidence", []):
        original = originals.get(ref["path"])
        if original and any(original[key] != ref[key] for key in ("sha256", "size")):
            raise TaskflowError("report source changed between receipt authentication and measurement")


def _text(value):
    if value is None:
        return "未知／未取得"
    return html.escape(str(value)).replace("|", "&#124;").replace("\n", "<br>")


def _label(value):
    if not isinstance(value, str):
        return value
    return {"completed": "已完成", "running": "进行中", "failed": "失败", "unknown": "未知",
        "not_started": "未启动", "real_native": "真实原生运行", "test_fixture": "模拟夹具",
        "other": "未核定运行性质", "insufficient_samples": "样本不足，稳定性待测",
        "descriptive_only": "仅提供同配置样本描述，未裁定稳定性", "not_configured": "未指定适用门槛",
        "score_at_or_below_limit": "原分不高于所选上限", "score_above_limit": "原分高于所选上限"}.get(value, value)


def _citations(rows):
    return "; ".join(_text(row.get("path")) + " @ " + _text(row.get("location")) + " [" + _text(row.get("sha256")) + "]" for row in rows)


def _duration(seconds):
    return None if seconds is None else f"{seconds / 60:.2f} 分钟（{seconds:.3f} 秒）"


def render_markdown(report):
    """Render complete original criteria, findings, measurable values and gaps."""
    metrics, grade = report["execution"], report["evaluation"]
    timing, counts = metrics.get("timing", {}), metrics.get("counts", {})
    result = "尚无已接收原分" if grade["total"] is None else f"{grade['total']} / {grade['maximum']}"
    lines = ["# 压测结果报告", "", "结果：**" + _text(result) + "**", "",
        "原卷：" + _text(report["identity"]["solver_task_id"]), "生成时间（UTC）：" + _text(report["generated_at"]),
        "", "## 运行与计量", "", "| 指标 | 观测值 |", "| --- | --- |"]
    rows = [("执行状态", _label(metrics.get("completion", {}).get("status"))), ("运行性质", _label(metrics.get("nature"))),
        ("请求模型／推理档位", str(metrics.get("requested_model")) + " / " + str(metrics.get("effort"))),
        ("实际 served 模型", metrics.get("served")), ("真实 SID", metrics.get("sid")),
        ("总耗时", _duration(timing.get("gross_seconds"))), ("可见回答消息数", counts.get("agent_messages")),
        ("命令执行次数", counts.get("command_executions")), ("工具调用次数", counts.get("tool_calls")),
        ("文件改动事件数", counts.get("file_changes")), ("重连事件数", metrics.get("reconnections"))]
    lines += ["| " + label + " | " + _text(value) + " |" for label, value in rows]
    lines += ["", "声明输入范围：" + _text(metrics.get("inputs_by_kind")),
        "原始用量（CLI回执，非收费）：" + _text(metrics.get("usage")),
        "实际收费：未知。", "计数单位分别保留；已识别工具调用含命令/MCP/搜索，不含文件改动事件。"]
    lines += ["", "## 逐项评分", "", "| 考点 | 满分 | 原得分 | 失分 |", "| --- | ---: | ---: | ---: |"]
    for row in grade["criteria"]:
        lines.append("| " + " | ".join((_text(row["id"]), _text(row["max_points"]), _text(row["points"]), _text(row["max_points"] - row["points"]))) + " |")
    if not grade["criteria"]:
        lines.append("| 未取得已接收逐项评分 | — | — | — |")
    lines += ["", "## 原评理由与证据", ""]
    for row in grade["criteria"]:
        lines += ["- **" + _text(row["id"]) + "**：" + _text(row["reason"]), "  证据：" + _citations(row["evidence"])]
    lines += ["", "## 原评阅事实、失分与业务依据", ""]
    for fact in grade.get("facts", []):
        lines += ["- **" + _text(fact["id"]) + "**：" + _text(fact["statement"]), "  证据：" + _citations(fact["evidence"])]
    if not grade.get("facts"):
        lines.append("尚未取得已封存事实；本报告不代写失分归因。")
    validity = grade.get("validity") or {}
    reviewer = grade.get("reviewer", {})
    lines += ["", "## 有效性与研发结论", "", "- 原评阅 local_eligible：" + _text(validity.get("local_eligible")),
        "- 原评阅模型／档位／实际 SID：" + _text(reviewer.get("requested_model")) + " / " + _text(reviewer.get("effort")) + " / " + _text(reviewer.get("sid")),
        "- 评分与封存事实同 SID 的来源绑定：" + _text(reviewer.get("same_sid_as_sealed_facts_verified")),
        "- 原资格理由：" + _text(validity.get("reason")), "- 资格证据：" + _citations(validity.get("evidence", [])),
        "- 得分比较：" + _text(_label(report["thresholds"]["status"])) + "；" + _text(report["thresholds"]["reason"]),
        "- 稳定性：" + _text(_label(report["stability"]["status"])) + "；可比已评分原卷 " + _text(report["stability"]["sample_count"]) + " 份。",
        "- 教师访问／公开预填：未单独裁定；" + _text(report["validity_boundaries"]["reason"]),
        "- 专家／官方确认：未知；本地评阅结果不能代替确认。", "", "## 证据限制", ""]
    if report["thresholds"].get("rules"):
        lines += ["所选规则及来源：" + _text(report["thresholds"]["rules"])]
    if report["stability"].get("sample_count", 0) >= 2:
        lines += ["同配置原分范围／平均值／总体标准差：" + _text({key: report["stability"][key]
            for key in ("minimum", "maximum", "mean", "population_stddev")})]
    if report["stability"]["excluded"]:
        lines += ["未混算的原卷与原因：" + _text(report["stability"]["excluded"])]
    limitations = [*report["limitations"], *metrics.get("limitations", []), *grade.get("limitations", []), *grade.get("facts_limitations", [])]
    lines += ["- " + _text(value) for value in limitations] or ["没有额外限制记录；这不表示所有风险均已排除。"]
    lines += ["", "## 原件引用", ""]
    for ref in [*report["references"], *metrics.get("evidence", [])]:
        lines.append("- " + _text(ref.get("location", "原件")) + "：" + _text(ref["path"]) + "；SHA256 " + _text(ref["sha256"]))
    return "\n".join(lines) + "\n"


def write_report(directory, report):
    """Append a content-addressed derived snapshot; never overwrite originals."""
    value = copy.deepcopy(report)
    value.pop("generated_at", None)
    digest = hashlib.sha256(canonical(value)).hexdigest()
    root = Path(directory).absolute() / digest
    if root.resolve() != root or root.is_symlink():
        raise TaskflowError("report directory must not traverse symlinks")
    if any(root.is_relative_to(Path(path).resolve()) for path in report["protected_source_roots"]):
        raise TaskflowError("report output cannot enter original inputs, runtime, work or archive directories")
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".report.lock"
    if lock_path.is_symlink():
        raise TaskflowError("report lock cannot traverse a symlink")
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        return _write_locked_snapshot(root, report, value, digest)


def _write_locked_snapshot(root, report, value, digest):
    json_path, md_path = root / "report.json", root / "report.md"
    if json_path.is_symlink() or md_path.is_symlink():
        raise TaskflowError("report output cannot be a symlink")
    if json_path.exists():
        existing = json.loads(json_path.read_text(encoding="utf-8"))
        expected = copy.deepcopy(existing)
        expected.pop("generated_at", None)
        if canonical(expected) != canonical(value):
            raise TaskflowError("report snapshot bytes differ from their content identity")
        report = existing
    raw = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    for path, content in ((json_path, raw), (md_path, render_markdown(report))):
        if path.is_symlink():
            raise TaskflowError("report output must not replace a symlink")
        if path.exists():
            if path.read_text(encoding="utf-8") != content:
                raise TaskflowError("derived report snapshot cannot be replaced")
        else:
            with path.open("x", encoding="utf-8") as handle:
                handle.write(content)
    return {"json": str(json_path), "markdown": str(md_path), "content_digest": digest}

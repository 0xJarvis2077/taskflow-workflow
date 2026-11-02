"""Typed projections of new role deliveries, never business judgements.

The caller authenticates the completed independent attempt and its collection
before using these functions.  Here we verify its declared routing, citations
and actual candidate bytes; no prose or model score selects a branch.
"""

import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

from taskflow.contracts import PUBLIC_ROLES, TaskflowError, canonical, relative_path, sha256
from taskflow.execution_runner import RunnerError, _open_regular, _regular


_DIGESTS = ("content_digest", "input_digest", "evaluation_digest")
_KINDS = frozenset({"material", "reference", "rubric", "claim"})
_OUTCOMES = frozenset({"accepted", "revision_required", "inconclusive"})
_SCOPES = frozenset({"whole_package", "affected"})
_LEGACY_OUTCOMES = {
    "needs_author_revision": "revision_required",
    "failed": "revision_required",
    "passed": "accepted",
    "accepted": "accepted",
    "revision_required": "revision_required",
    "inconclusive": "inconclusive",
}


NATIVE_SIGNOFF_PROTOCOL_TEXT = json.dumps({
    "protocol": "taskflow-native-quality-signoff-v1",
    "schema_version": 1,
    "delivery": "Use the same intake.md, issues.json and adoption-readiness.json. Add native_signoff to adoption-readiness.json in this same independent role turn; no extra review/model/signature is requested.",
    "binding": "The existing execution_routing binds this exact intent, candidate and content/input/evaluation digests and its actual whole_package or affected review scope. native_signoff does not replace or broaden that signature.",
    "output_shape": {
        "native_signoff": {
            "schema_version": 1,
            "protocol_sha256": "Copy the exact SHA256 from this protocol's request marker.",
            "coverage": "whole_package | unchanged_scope | affected; choose one actual value",
            "affected_paths": ["Current candidate package-relative paths; their meaning is specified in coverage_rules."],
            "coverage_basis": "Concrete explanation of actual coverage and its exclusions or necessary associations.",
            "dimensions": {kind: {
                "affirmed": "boolean: true or false, never a forced pass or a string",
                "evidence": [{"path": "Current package path or an explicitly staged observation alias", "sha256": "Actual source SHA256", "location": "Exact sentence/cell/field/code location"}],
                "limitations": ["Actual limits; an explicit empty list is allowed only if there are no such limits."]
            } for kind in ("business", "artifact", "calculation")},
            "normative": {
                "status": "determined | needs_business_decision; choose one actual value",
                "files": [{"package_path": "Exact current private candidate path", "sha256": "Actual current SHA256",
                           "size": "Actual nonnegative integer byte count", "role": "Actual declared role",
                           "purpose": "answer | rubric | reference | legitimate_alternative; choose one",
                           "basis": "Exact frozen criterion/rule/alternative and why this source is necessary for scoring"}],
                "evidence": [{"path": "Current package path or an explicitly staged observation alias", "sha256": "Actual SHA256", "location": "Exact basis for the current selection or unresolved choice"}],
                "limitations": ["Actual normative selection limits; preserve uncertainty."]
            },
            "responsibility_analysis": [{
                "public_evidence": [{"path": "Actual current public prompt/input/template path", "sha256": "Actual SHA256", "location": "Original business sentence/field/program location"}],
                "upstream_work_already_done": "What supplied originals, policy, source code or prior legitimate role has already established",
                "remaining_judgment": "What the delegated role must still establish, choose, translate or execute; explicitly say when none is established",
                "consequence": "How that remaining judgment changes the complete business decision, effect, executable delivery or acceptance",
                "retained_necessary_conditions": ["Concrete necessary source conditions retained, with original locations; do not delete legitimate upstream work to manufacture difficulty"],
                "legitimate_branches": ["Concrete normal/equivalent/conditional/short/defer alternatives and their applicability; explain a genuinely inapplicable branch rather than inventing one"],
                "disposition": "retained | blocking_defect | unresolved; choose one actual value",
                "blocking_issue_ids": ["Actual current issues.json IDs when applicable"]
            }]
        }
    },
    "coverage_rules": [
        "whole_package affirms only the actually reviewed whole current package. A whole review finding defects may instead sign unchanged_scope; affected_paths then explicitly lists the areas excluded from those affirmative dimensions, not a claim that the whole package passed.",
        "affected rechecks sign coverage=affected and list the actual repaired responsibilities and necessary associations in affected_paths. Reading additional related files or certifying the complete normative list does not upgrade an affected recheck to a new whole-package review.",
        "Keep true and false dimension affirmations, concrete evidence and limits honest. A false affirmation or unresolved evidence is legal output; do not set true just to release the next worker. execution_routing.outcome and actual blocking issues remain the business routing authority.",
        "Each dimension concerns only its stated coverage. Preserve original whole/unchanged scope separately from actual affected closure. Never restamp earlier limited acceptance as a new whole signature."
    ],
    "dimension_rules": [
        "business: Review the complete current public responsibility, source guarantees/applicability, reference and fair rubric, meaningful remaining professional work and legitimate alternatives; a successful public solution, author self-check or generic accepted route is not a substitute for this explicit judgment.",
        "artifact: Read back the actually declared formats, code/data/dependencies and delivered originals within the real local scope. Preserve visual/native-installation/production limits rather than asserting checks that did not occur.",
        "calculation: Locate and actually check the applicable deterministic arithmetic, state transitions, invariants and normal/wrong controls. Distinguish author-reported checks from this independent role's observations and preserve unsupported scope."
    ],
    "normative_rules": [
        "When determined, files is the complete exact current private normative allowlist, including the actual answer, rubric and legitimately necessary reference/model/alternative sources. An affected recheck must sign the entire current list again, not only changed files; this selection is not a whole-quality scope upgrade.",
        "Every selected path/SHA256/size/role must match the current declared candidate bytes. Select by actual normative function and criterion basis, not filename keywords or all-private convenience. Explain reference code and legitimate alternative applicability explicitly.",
        "Exclude author research, historical grades, calibration runs/local scores, controller purposes/authorization/targets and engineering control/probe ledgers. A prospective frozen rule/model that actually defines correctness or allowed alternatives may be a reference; its historical execution result is not itself a scoring standard.",
        "If the exact normative scope needs a substantive choice or cannot be established, sign needs_business_decision with the exact missing choice and evidence. Do not guess the allowlist, silently select every private file or demand a routine extra whole-model review.",
        "Current task.json/scoring is transported by the existing deterministic scoring projection; do not treat the whole declaration/history metadata as a normative private file."
    ],
    "evidence_rules": [
        "Dimension/normative evidence paths are current candidate package paths or actual staged public-observation aliases whose original bytes are bound by the factory. execution/task.json is declaration-only evidence, not a public business original or an affected material path.",
        "responsibility_analysis.public_evidence must cite true current declared public prompt/input/template originals. A private answer, observation, old report or declaration alias cannot prove the public business premise.",
        "Keep the full original source and exact locator for every affirmative claim. Narrative text alone, a copied author conclusion or this protocol's own text is not source evidence."
    ],
    "responsibility_rules": [
        "Read every current public original before comparing teacher design. Trace original location -> actual upstream work already done -> remaining judgment -> its business consequence -> retained necessary conditions and legitimate branches -> disposition. Do not merely paraphrase the prompt or list a few local mistakes.",
        "Independently add material/reference/rubric/claim findings that the author omitted, with current original evidence and actual issues.json linkage; no author finding list limits this independent judgment.",
        "Do not manufacture a missing duty, remove useful sources, prohibit ordinary programs/ledgers or infer coverage solely because a solution is programmable. Conversely a renamed or parameter-expanded already-solved responsibility is not new work merely because its description is longer.",
        "Use concrete retained conditions and legitimate branches, not empty placeholders. If an extra condition/branch truly does not apply, state that fact with its original basis rather than inventing a hidden obligation.",
        "No model grade, promised failure, difficulty percentage, point threshold or controller investment conclusion is requested by this quality contract."
    ],
    "format_rule": "This JSON describes the contract. Emit actual booleans, integer sizes, concrete evidence/paths and one enum value; do not copy type descriptions or vertical-bar placeholders into the signed output."
}, ensure_ascii=False, sort_keys=True, indent=2)
NATIVE_SIGNOFF_PROTOCOL_SHA256 = hashlib.sha256(NATIVE_SIGNOFF_PROTOCOL_TEXT.encode("utf-8")).hexdigest()
_NATIVE_SIGNOFF_START = "<taskflow_native_signoff_protocol "
_NATIVE_SIGNOFF_END = "</taskflow_native_signoff_protocol>"


def render_native_signoff_protocol() -> str:
    """Return the complete fixed same-role contract, including its byte digest."""
    return (_NATIVE_SIGNOFF_START + 'sha256="' + NATIVE_SIGNOFF_PROTOCOL_SHA256 + '">\n'
            + NATIVE_SIGNOFF_PROTOCOL_TEXT + "\n" + _NATIVE_SIGNOFF_END)


def request_has_native_signoff_protocol(request: str | bytes) -> bool:
    """Check actual locked request bytes; an output's self-reported SHA is insufficient."""
    if isinstance(request, bytes):
        try:
            request = request.decode("utf-8")
        except UnicodeError:
            return False
    return (isinstance(request, str) and request.count(_NATIVE_SIGNOFF_START) == 1
            and request.count(_NATIVE_SIGNOFF_END) == 1
            and render_native_signoff_protocol() in request)


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TaskflowError(f"{name}: nonempty text required")
    return value


def _sha(value: Any, name: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise TaskflowError(f"{name}: canonical SHA256 required")
    return value


def _ids(value: Any, name: str, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list):
        raise TaskflowError(f"{name}: list required")
    result = [_text(item, name) for item in value]
    if len(result) != len(set(result)) or (nonempty and not result):
        raise TaskflowError(f"{name}: unique {'nonempty ' if nonempty else ''}list required")
    return result


def _object(value: Any, name: str) -> dict:
    if not isinstance(value, dict):
        raise TaskflowError(f"{name}: object required")
    return value


def _unique_object(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise TaskflowError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def _json(path: Path) -> dict:
    try:
        value = json.loads(_regular(Path(path)).decode("utf-8"),
                           object_pairs_hook=_unique_object,
                           parse_constant=lambda value: _invalid_constant(value))
    except (OSError, RunnerError, UnicodeError, ValueError) as error:
        raise TaskflowError(f"Invalid role JSON {path}: {error}") from error
    return _object(value, str(path))


def _invalid_constant(value: str) -> None:
    raise TaskflowError(f"Non-finite JSON constant: {value}")


def _file_sha(path: Path) -> str:
    try:
        with os.fdopen(_open_regular(path), "rb") as stream:
            digest = hashlib.sha256()
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()
    except (OSError, RunnerError) as error:
        raise TaskflowError(f"Cannot read regular candidate file {path}: {error}") from error


def _fingerprint(candidate: dict) -> dict:
    candidate = _object(candidate, "candidate")
    _text(candidate.get("task_id"), "task_id")
    return {"candidate_id": _text(candidate.get("candidate_id"), "candidate_id"),
            **{key: _sha(candidate.get(key), key) for key in _DIGESTS}}


def _binding(value: dict, candidate: dict, intent_id: str) -> None:
    expected = {"intent_id": _text(intent_id, "intent_id"), **_fingerprint(candidate)}
    for key, item in expected.items():
        if value.get(key) != item:
            raise TaskflowError(f"Role result {key} does not match current work")
    if "task_id" in value and value["task_id"] != candidate.get("task_id"):
        raise TaskflowError("Role result task_id does not match current work")


def _files(candidate: dict) -> dict[str, dict]:
    rows = candidate.get("files")
    if not isinstance(rows, list) or not rows:
        raise TaskflowError("candidate.files: nonempty captured file list required")
    result = {}
    for row in rows:
        row = _object(row, "candidate file")
        package_path = relative_path(row.get("package_path"))
        if package_path in result:
            raise TaskflowError(f"Duplicate candidate file: {package_path}")
        path = Path(_text(row.get("path"), "candidate file path"))
        if not path.is_absolute():
            raise TaskflowError("candidate file path must be absolute")
        expected = _sha(row.get("sha256"), "candidate file sha256")
        if _file_sha(path) != expected:
            raise TaskflowError(f"Current candidate bytes changed: {package_path}")
        result[package_path] = {**row, "path": str(path)}
    return result


def _scope(value: str) -> str:
    if not isinstance(value, str) or value not in _SCOPES:
        raise TaskflowError("scope: whole_package or affected required")
    return value


def _review_metadata(candidate: dict, files: dict[str, dict],
                     aliases: dict[str, dict] | None) -> dict[str, dict]:
    """Bind an outer-authenticated staged declaration to its captured source."""
    if aliases is None:
        return {}
    aliases = _object(aliases, "metadata_aliases")
    if not aliases:
        return {}
    if set(aliases) != {"execution/task.json"} or "execution/task.json" in files:
        raise TaskflowError("Only the explicit staged declaration metadata alias is supported")
    item = _object(aliases["execution/task.json"], "declaration metadata alias")
    path = Path(_text(item.get("path"), "metadata source path"))
    expected = _sha(item.get("sha256"), "metadata source sha256")
    if not path.is_absolute() or path.name != "task.json":
        raise TaskflowError("Metadata source must be the absolute candidate declaration")
    evidence = candidate.get("evidence")
    declarations = [row for row in evidence if isinstance(row, dict)
                    and row.get("location") == "explicit package declaration"] if isinstance(evidence, list) else []
    if (len(declarations) != 1 or declarations[0].get("path") != str(path)
            or declarations[0].get("sha256") != expected
            or any(Path(row["path"]) != path.parent / key for key, row in files.items())):
        raise TaskflowError("Metadata source is not this captured candidate declaration")
    if _file_sha(path) != expected:
        raise TaskflowError("Current candidate declaration metadata bytes changed")
    return {"execution/task.json": {"path": str(path), "sha256": expected,
                                    "role": "source_metadata"}}


def _review_observations(files: dict[str, dict],
                         aliases: dict[str, dict] | None) -> dict[str, dict]:
    """Validate explicit public-predecessor aliases authenticated by the caller."""
    if aliases is None:
        return {}
    result = {}
    for key, item in _object(aliases, "observed_aliases").items():
        key = relative_path(key)
        item = _object(item, "observed alias")
        kind = item.get("kind")
        permitted = ((kind == "submission"
                      and key.startswith(("actual-public-work/", "actual-public-scratch/")))
                     or (kind == "trace" and key == "actual-public-trace.jsonl"))
        if key in files or not permitted:
            raise TaskflowError("Unsupported public-predecessor observation alias")
        path = Path(_text(item.get("path"), "observed source path"))
        expected = _sha(item.get("sha256"), "observed source sha256")
        if not path.is_absolute() or _file_sha(path) != expected:
            raise TaskflowError("Observed source path or actual SHA does not match")
        result[key] = {"path": str(path), "sha256": expected, "role": kind}
    return result


def _public_first_parameters(context: dict) -> tuple[str, str, str]:
    configured = _object(context.get("public_first"), "public_first")
    index = relative_path(configured.get("index"))
    checkpoint = relative_path(configured.get("checkpoint"))
    digest = _sha(configured.get("input_digest"), "public_first.input_digest")
    if context.get("input_digest") is not None and context["input_digest"] != digest:
        raise TaskflowError("public_first input digest differs from the captured current package")
    return index, checkpoint, digest


def public_first_instructions(context: dict) -> str:
    """Place this operation before every other whole-review request section."""
    context = _object(context, "context")
    index, checkpoint, digest = _public_first_parameters(context)
    return (
        "阶段1：仅读公开原件。先读取中性公开索引 " + index + "，input_digest=" + digest + "。"
        "它只列PUBLIC_ROLES的path/role/SHA/size；按索引实际通读当前全部公开原件，"
        "先不读完整 execution/task.json、scoring、教师设计/答案/rubric、旧审查、监控或作者研究。"
        "不从全量声明、历史结论或参考答案反推公开责任。\n"
        "阶段2：保存独立责任检查点。仅凭已读公开依据，逐项定位公开原句/字段，分析来源适用性、"
        "上游已完成的工作、仍需独立建立的关系或选择、其业务后果、保留的必要条件和合法分支；"
        "包括普通短解、等价/条件/延后路径，不为制造难度删除正常成果。明确缺证和真实不确定。"
        "将上述判断与实际公开来源/SHA/位置保存为同一角色work中的普通文件 " + checkpoint + "。\n"
        "阶段3：真实工具打印检查点摘要。保存后，用实际工具重新读取该文件并计算SHA256，"
        "在可见工具响应打印 PUBLIC_RESPONSIBILITY_FIRST_SHA256=<实际SHA256>，"
        "保留该工具响应的trace/item/行号等可定位位置；不能只在最终文字里自报摘要。\n"
        "阶段4：随后读取教师。只有保存检查点并取得上述真实工具响应后，才读取完整声明、"
        "同版教师/评分及授权知情材料，逐项比较并完成原三文件报告。"
        "在 intake.md、adoption-readiness.json 和 issues.json 的报告元数据中引用检查点路径、"
        "实际SHA、工具响应可见位置和后续教师读取位置；没有问题时issues仍为空，不捏造问题。"
        "检查点由既有fullarchive保存，不是新模型、审批或全局passed门。\n"
        "摘要只锚定保存字节和可见顺序，不证明业务判断正确、材料无预填或此前上下文无教师信息。"
        "若已先见教师/自动上下文或缺检查点、工具响应、时间顺序证据，须如实记录实际范围，"
        "收窄声明或签inconclusive；不能事后补打印来追认未锚定的独立公开阶段。\n"
    )


def _affected_diff_instructions(context: dict) -> str:
    configured = context.get("repair_diff")
    if configured is None:
        return "本次未提供actual repair_diff索引；如实标明可核旧/新源与缺口，不得猜造旧SHA或宣称完成未发生的逐件对比。\n"
    configured = _object(configured, "repair_diff")
    if type(configured.get("available")) is not bool:
        raise TaskflowError("repair_diff.available must be an explicit boolean")
    index = relative_path(configured.get("index"))
    if not configured["available"]:
        reason = _text(configured.get("reason", "父版或旧源当前不可核验"), "repair_diff.reason")
        return ("实际差异索引 " + index + " 标明available=false：" + reason
                + "。如实说明父版/旧源缺口，不要求伪造old/new字段或旧文件；"
                "不能将未能核验的必要关联当作已闭合，必要证据不足时签inconclusive。\n")
    return (
        "先读取实际差异索引 " + index + "，核parent_binding/current_binding与当前候选。"
        "逐件读取changed中的历史父版before_staged_path（execution/parent-inputs/下、kind=history）"
        "及当前current_staged_path原件；父版是历史对比源，不是当前teacher规范。"
        "核path、old_sha256/new_sha256、old_role/new_role和实际字节，不只看作者摘要或记忆。\n"
        "defect_resolution.evidence必须覆盖每个实际changed：当前仍存在的文件用题包相对path、"
        "当前sha256和非空location定位实际内容，不能只把任意dimensions.evidence合并当成覆盖。"
        "超出原问题affected_paths的必要关联必须逐件进入defect_resolution.affected_paths，"
        "给出该已交责任为何需要此变动的具体依据，不能只列文件名或泛称相关。\n"
        "coverage.changed_files完整列出每个实际差异的path、old、new，按实际索引对应旧/新SHA；"
        "不能漏掉记录/设计/派生数据等实际payload变更。新增的old为null、删除的new为null；"
        "对删除项如实引用历史父源与删除事实，不捏造当前SHA/文件。缺旧源就明确不可核，"
        "不猜旧字节，也不扩大当前接口可接受的修订范围。"
        "同字节task.json声明物理化仅记provenance，不是candidate.files的payload差异，不能塞入changed。\n"
        "本次仍是知情affected复检，仅签真实已查责任及必要关联；保留旧整包/unchanged范围和真实局限，"
        "不把当前复检升级为whole_package，也不冒充无背景的公开盲评。\n"
    )


def review_metadata_aliases(candidate: dict, manifest: dict, runtime: Path) -> dict:
    """Resolve only the actual review's locked same-candidate declaration."""
    from taskflow.execution_runner import _info, _validate
    from taskflow.packages import check_task

    runtime = Path(runtime)
    if _validate(runtime) != manifest or manifest.get("candidate_binding") != {
            key: candidate[key] for key in ("candidate_id", "task_id", "input_digest", "content_digest", "evaluation_digest")}:
        raise TaskflowError("review metadata belongs to a different candidate or intent")
    matches = [item for item in manifest["inputs"] if item["target"] == "execution/task.json"]
    if not matches:
        return {}
    if len(matches) != 1 or matches[0]["kind"] != "source_metadata":
        raise TaskflowError("locked candidate declaration metadata missing or ambiguous")
    declarations = [ref for ref in candidate["evidence"] if ref.get("location") == "explicit package declaration"]
    if len(declarations) != 1 or matches[0]["source"] != declarations[0]["path"]:
        raise TaskflowError("review declaration alias has a foreign source")
    item, declaration = matches[0], declarations[0]
    source, staged = Path(declaration["path"]), runtime / "inputs/execution/task.json"
    raw = _regular(source)
    if (raw != _regular(staged) or sha256(raw) != declaration["sha256"]
            or _info(source) != {key: item[key] for key in ("sha256", "size")}):
        raise TaskflowError("review declaration metadata source/staged bytes changed")
    actual = check_task(source.parent, write_report=False)
    if actual["errors"] or any(actual[key] != candidate[key] for key in ("task_id", "content_digest", "input_digest", "evaluation_digest")):
        raise TaskflowError("review declaration metadata is not the current candidate version")
    return {"execution/task.json": {"path": str(source), "sha256": sha256(raw)}}


def review_instructions(context: dict, scope: str) -> str:
    """Render routing and native signoff in the same existing three-file delivery."""
    context = _object(context, "context")
    scope = _scope(scope)
    binding = {"intent_id": _text(context.get("intent_id"), "intent_id"),
               **_fingerprint(context)}
    requested = _ids(context.get("defect_ids", []), "defect_ids")
    routing = {"schema_version": 1, **binding,
               "signer_role": "independent_quality",
               "independent_of_current_author": True,
               "participated_in_production": False, "scope": scope,
               "outcome": "accepted|revision_required|inconclusive",
               "blocking_issue_ids": [], "covered_defect_ids": requested,
               "reason": "具体签发依据、实际覆盖及局限"}
    coverage = (
        "本次执行签发scope=affected；native_signoff.coverage必须为affected，"
        "只肯定实际修订责任及必要关联，不能升级为新整包签发。规范清单仍须重新列出完整当前选择。"
        if scope == "affected" else
        "本次执行签发scope=whole_package；如实签whole_package，或在发现阻断时仅签unchanged_scope，"
        "明确排除affected_paths。任何维度都不强迫affirmed=true。"
    )
    if scope == "affected":
        order = "本次是知情affected复检，不重新声称未见教师的公开盲评或新的整包判断。\n" + _affected_diff_instructions(context)
    elif context.get("public_first") is not None:
        index, checkpoint, digest = _public_first_parameters(context)
        order = ("本次整包报告须保留前置公开阶段的实际顺序证据：中性索引 " + index + "、input_digest=" + digest
                 + "、检查点 " + checkpoint + "、PUBLIC_RESPONSIBILITY_FIRST_SHA256真实工具响应及可见位置，"
                 "以及随后读取教师的定位。原三文件只按这些实际来源说明独立责任判断；"
                 "缺证或先见教师时如实收窄/inconclusive，不靠hash或事后补写追认。\n")
    else:
        order = "未提供中性公开索引/检查点元数据时，不凭空宣称已有公开先行顺序锚点；如实说明实际已见材料与独立性局限。\n"
    return (
        "沿用 intake.md、issues.json、adoption-readiness.json，不新增审批表，"
        "不要求每个业务判断另填 passed。你须未制作本修订；若独立性或证据不够，"
        "如实签 inconclusive，不自行修改后给自己的改稿签独立通过。\n"
        f"当前 task_id={_text(context.get('task_id'), 'task_id')}。"
        "实际通读授权范围、核实际文件与成果；程序只校类型/版本/字节，不替你裁定业务。\n"
        + order
        + "adoption-readiness.json 保留现有业务报告，并增加 execution_routing 对象：\n"
        + json.dumps(routing, ensure_ascii=False, indent=2)
        + "\noutcome 必须选一个精确值，不能照抄竖线占位。accepted 的阻断列表必须空；"
        "revision_required 必须列出实际存在的具体阻断。covered_defect_ids 只列本次"
        "真正复检覆盖的已交问题，未查的不可宣称关闭。旧 candidate_business_quality.status"
        "若保留，须与本次路由一致；公开试解成功、结构通过或作者自检不等于业务 accepted。\n"
        "issues.json 增加 execution_binding，其对象为：\n"
        + json.dumps(binding, ensure_ascii=False, indent=2)
        + "\nissues 必须为列表。每个问题含 id、kind（material/reference/rubric/claim）、"
        "responsibility、affected_paths（当前题包相对路径列表）、evidence（非空列表，"
        "每条为 path=当前题包相对路径、sha256=该实际原件摘要、location=原句/单元格/字段位置）。"
        "material 必须引用至少一条当前公开 prompt/input/template 依据。不要仅引自写报告"
        "或私有答案证明公开材料缺陷；非阻断说明可保留，但不得冒充阻断。"
        "没有问题时 issues=[]。保留真实批准成果、制度、公式、稳定键及必要条件，"
        "对新问题和正常反例据实际范围签发；不评分、不更改原卷、冻结或旧评价。\n"
        + "\n在同一次adoption-readiness.json原业务报告中增加native_signoff，"
        "不是另起模型或追加一轮整包审查。先逐项完成公开责任/上游已做/剩余工作/后果/必要条件和合法分支分析，"
        "独立新增作者遗漏，并把已实际核定的三维覆盖、证据、局限和完整当前规范清单同时写明。\n"
        + coverage + "\nnative_signoff.protocol_sha256必须为" + NATIVE_SIGNOFF_PROTOCOL_SHA256
        + "。以下完整协议正文及SHA由本次实际request摘要锁定；输出只自报该SHA不构成来源证明。\n"
        + render_native_signoff_protocol() + "\n"
    )


def _nonblocking_observation(row: object, blocking_ids: list[str]) -> bool:
    """Classify an original observation without changing its issue kind."""
    if (not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"].strip()
            or row["id"] in blocking_ids):
        return False
    if row.get("kind") == "claim":
        return row.get("blocking", False) is False
    reason = row.get("nonblocking_reason")
    return (row.get("kind") in {"reference", "rubric"} and row.get("blocking") is False
            and row.get("status") == "open_nonblocking" and isinstance(reason, str) and bool(reason.strip())
            and row.get("needs_business_decision", False) is False
            and row.get("disposition") in (None, "observed_unrepaired", "retained"))


def parse_review(report_path: Path, issues_path: Path, *, candidate: dict,
                 intent_id: str, scope: str,
                 metadata_aliases: dict[str, dict] | None = None,
                 observed_aliases: dict[str, dict] | None = None) -> dict:
    """Validate a role; optional metadata aliases require outer manifest validation.

    Declaration metadata is citation-only and never a public or affected file.
    Observation aliases require the caller to bind the same public predecessor's
    archive, staged bytes and locked manifest. Claims retain their legacy scope;
    explicitly open nonblocking reference/rubric observations may cite these
    supplemental bytes. Explicit blocking reference/rubric findings may also
    cite them when listed by a revision_required routing, with current candidate
    or bound metadata evidence; their affected paths stay current candidate files.
    Without an explicit alias, citations remain limited to candidate.files.
    """
    scope = _scope(scope)
    report, issues = _json(report_path), _json(issues_path)
    routing = _object(report.get("execution_routing"), "execution_routing")
    if type(routing.get("schema_version")) is not int or routing["schema_version"] != 1:
        raise TaskflowError("execution_routing.schema_version must be 1")
    _binding(routing, candidate, intent_id)
    _binding(_object(issues.get("execution_binding"), "execution_binding"),
             candidate, intent_id)
    if (routing.get("signer_role") != "independent_quality"
            or routing.get("independent_of_current_author") is not True
            or routing.get("participated_in_production") is not False):
        raise TaskflowError("Independent quality role declaration required")
    if routing.get("scope") != scope:
        raise TaskflowError("Review scope does not match authorized scope")
    outcome = routing.get("outcome")
    if not isinstance(outcome, str) or outcome not in _OUTCOMES:
        raise TaskflowError("Unknown business routing outcome")
    blocking = _ids(routing.get("blocking_issue_ids"), "blocking_issue_ids")
    covered = _ids(routing.get("covered_defect_ids"), "covered_defect_ids")
    reason = _text(routing.get("reason"), "reason")
    if outcome == "accepted" and blocking:
        raise TaskflowError("Accepted review cannot contain blocking issues")
    if outcome == "revision_required" and not blocking:
        raise TaskflowError("Revision required needs concrete blocking issues")
    legacy = report.get("candidate_business_quality")
    if legacy is not None:
        status = _object(legacy, "candidate_business_quality").get("status")
        if not isinstance(status, str) or _LEGACY_OUTCOMES.get(status) != outcome:
            raise TaskflowError("Legacy business status is unknown or contradicts routing")
        if "blocking_issue_ids" in legacy:
            if set(_ids(legacy["blocking_issue_ids"], "legacy blocking IDs")) != set(blocking):
                raise TaskflowError("Legacy blocking IDs contradict routing")
    files = _files(candidate)
    metadata = _review_metadata(candidate, files, metadata_aliases)
    observed = _review_observations(files, observed_aliases)
    rows = issues.get("issues")
    if not isinstance(rows, list):
        raise TaskflowError("issues: list required")
    defects = {}
    for row in rows:
        row = _object(row, "issue")
        identity = _text(row.get("id"), "issue.id")
        if identity in defects:
            raise TaskflowError("Duplicate issue ID")
        if not isinstance(row.get("kind"), str) or row["kind"] not in _KINDS:
            raise TaskflowError("Unknown issue kind")
        observation = _nonblocking_observation(row, blocking)
        blocking_observation_basis = (row["kind"] in {"reference", "rubric"}
                                      and row.get("blocking") is True
                                      and identity in blocking
                                      and outcome == "revision_required")
        responsibility = _text(row.get("responsibility"), "issue.responsibility")
        affected = _ids(row.get("affected_paths"), "affected_paths", nonempty=True)
        for value in affected:
            key = relative_path(value)
            if key in observed and (not observation or row["kind"] != "claim"):
                raise TaskflowError("Observed paths are only for nonblocking claims, not repair targets")
            if key not in files and key not in observed:
                raise TaskflowError(f"Unknown affected candidate path: {value}")
        citations = row.get("evidence")
        if not isinstance(citations, list) or not citations:
            raise TaskflowError("Issue requires actual candidate evidence")
        refs, has_public, has_observed, has_current_basis = [], False, False, False
        for citation in citations:
            citation = _object(citation, "issue evidence")
            key = relative_path(citation.get("path"))
            if key in observed and not (observation or blocking_observation_basis):
                raise TaskflowError("Observed evidence requires a nonblocking observation or an explicit routing-bound blocking reference/rubric finding")
            item = files.get(key) or metadata.get(key) or observed.get(key)
            if (item is None
                    or _sha(citation.get("sha256"), "evidence sha256") != item["sha256"]):
                raise TaskflowError("Issue evidence is absent from current candidate or has wrong SHA")
            refs.append({"path": item["path"], "sha256": item["sha256"],
                         "location": _text(citation.get("location"), "evidence location")})
            has_public = has_public or item.get("role") in PUBLIC_ROLES
            has_observed = has_observed or key in observed
            has_current_basis = has_current_basis or key in files or key in metadata
        if row["kind"] in {"reference", "rubric"} and has_observed and not has_current_basis:
            raise TaskflowError("Observed reference/rubric evidence is supplemental and needs current candidate or bound metadata basis")
        if row["kind"] == "material" and not has_public:
            raise TaskflowError("Material defect requires current public basis")
        defects[identity] = {"id": identity, "kind": row["kind"],
                             "responsibility": responsibility,
                             "affected_paths": affected, "evidence": refs}
    if not set(blocking) <= set(defects):
        raise TaskflowError("Blocking ID has no concrete issue record")
    return {"outcome": outcome, "whole_package": scope == "whole_package",
            "covered_defect_ids": covered, "defects": [defects[key] for key in blocking],
            "reason": reason}


def repair_instructions(context: dict) -> str:
    """Describe one actual repair and its byte manifest without self-approval."""
    context = _object(context, "context")
    parent_id = _text(context.get("candidate_id"), "candidate_id")
    defects = _ids(context.get("defect_ids"), "defect_ids", nonempty=True)
    record = {"role": "repair", "parent_id": parent_id,
              "candidate_id": _text(context.get("revised_candidate_id"),
                                    "revised_candidate_id"),
              "defect_ids": defects,
              "needs_business_decision": False,
              "changes": [{"path": "题包相对路径",
                           "before_sha256": "修改前实际SHA，新增文件为null",
                           "after_sha256": "修改后实际SHA，删除文件为null"}]}
    return (
        "仅落实已交的具体缺陷，保留必要制度/公式/字段/权限/期限和正常替代。"
        "交付真实完整修订及 repair-record.json，不为自己签独立质量通过。\n"
        + json.dumps(record, ensure_ascii=False, indent=2)
        + "\nchanges 只记录父/新 candidate.files 清单中真实 payload 的新增/修改/删除及角色归属变化，"
        "必须完整覆盖这些实际差异，未改变的文件不列。若只是将锁定的 execution/task.json"
        "按同字节物理化为完整候选的 task.json，且它不是 candidate.files 中声明的 payload，"
        "仅在单独 provenance 字段说明来源与摘要，不列入 changes，也不以 before_sha256=null"
        "把这次复制报成新增 payload。不要将仅改报告或能力声明"
        "说成材料已修或题目已增难。若缺依据或需研发决定，明确 needs_business_decision=true"
        "及原因；这会停止自动接续而非自动投测。旧原件/原卷/评价保持原样，交新独立复检。\n"
    )


def repair_defect_ids(record: dict, defect_ids: list) -> list[str]:
    """Normalize repair identity only; this is never an independent verdict.

    Historical repair authors used the review spelling for the same assigned
    identities. Each supplied field must independently cover the exact set.
    """
    expected = _ids(defect_ids, "defect_ids", nonempty=True)
    alias = "covered_defect_ids" in record
    if ((alias and "defect_ids" not in record and record.get("role") != "repair")
            or ("role" in record and record["role"] != "repair")):
        raise TaskflowError("Repair defect identity requires the repair role")
    fields = [key for key in ("defect_ids", "covered_defect_ids") if key in record]
    if not fields:
        raise TaskflowError("repair defect_ids: list required")
    for key in fields:
        if set(_ids(record[key], "repair " + key, nonempty=True)) != set(expected):
            raise TaskflowError("Repair defect IDs do not match assigned defects")
    return expected


def validate_repair(record_path: Path, *, parent: dict, candidate: dict,
                    defect_ids: list) -> dict:
    """Bind a repair manifest to both actually captured file sets."""
    record = _json(record_path)
    expected_ids = _ids(defect_ids, "defect_ids", nonempty=True)
    if record.get("parent_id") != _fingerprint(parent)["candidate_id"]:
        raise TaskflowError("Repair parent_id does not match actual parent")
    _fingerprint(candidate)
    if parent.get("task_id") != candidate.get("task_id"):
        raise TaskflowError("Repair candidates belong to different tasks")
    if candidate["candidate_id"] == parent["candidate_id"]:
        raise TaskflowError("Repair requires a new candidate identity")
    if "parent_id" in candidate and candidate["parent_id"] != parent["candidate_id"]:
        raise TaskflowError("Captured repair ancestry does not match parent")
    if record.get("candidate_id") != candidate["candidate_id"]:
        raise TaskflowError("Repair record candidate_id mismatch")
    repair_defect_ids(record, expected_ids)
    if record.get("needs_business_decision") is not False:
        raise TaskflowError("Repair requires business decision; cannot auto-advance")
    before, after = _files(parent), _files(candidate)
    changed = {key for key in set(before) | set(after)
               if key not in before or key not in after
               or (before[key]["sha256"], before[key].get("role"))
               != (after[key]["sha256"], after[key].get("role"))}
    if not changed:
        raise TaskflowError("Repair has no actual candidate changes")
    rows = record.get("changes")
    if not isinstance(rows, list):
        raise TaskflowError("repair changes: list required")
    validated = {}
    for row in rows:
        row = _object(row, "repair change")
        key = relative_path(row.get("path"))
        if key in validated or key not in changed:
            raise TaskflowError("Repair changes duplicate or do not reflect actual differences")
        for field, files in (("before_sha256", before), ("after_sha256", after)):
            if field not in row:
                raise TaskflowError(f"Repair change missing {field}")
            expected = files[key]["sha256"] if key in files else None
            if row[field] != expected:
                raise TaskflowError(f"Repair {field} differs from actual captured bytes")
        validated[key] = {"path": key, "before_sha256": row["before_sha256"],
                          "after_sha256": row["after_sha256"]}
    if set(validated) != changed:
        raise TaskflowError("Repair manifest omits actual candidate changes")
    return {"parent_id": parent["candidate_id"], "candidate_id": candidate["candidate_id"],
            "defect_ids": expected_ids, "needs_business_decision": False,
            "changes": [validated[key] for key in sorted(validated)]}


def validate_completed_repair_identity(record_path: Path, *, attempt: dict,
                                       parent: dict, candidate: dict,
                                       parent_root: Path, candidate_root: Path,
                                       archive: Path, defect_ids: list) -> dict:
    """Revalidate the original managed delivery and sources without writing.

    This proof authorizes consumption of a sealed repair, never quality closure.
    No projected record is created or accepted by this boundary.
    """
    from taskflow.execution_runner import _info, _validate, _validated_collection
    from taskflow.execution_store import evidence_ref, validate_evidence
    from taskflow.packages import check_task

    if (attempt.get("state") != "completed" or attempt.get("role") != "repair"
            or type(attempt.get("process_exit")) is not int or attempt["process_exit"] != 0
            or any(attempt.get(key) is not True for key in
                   ("response_complete", "artifacts_complete", "technical_accepted"))
            or not attempt.get("cli_sid")
            or candidate.get("author_attempt") != attempt.get("attempt_id")
            or candidate.get("parent_id") != parent.get("candidate_id")
            or attempt.get("candidate_id") != parent.get("candidate_id")
            or attempt.get("task_id") != parent.get("task_id")):
        raise TaskflowError("repair identity requires its exact completed managed author")
    runtime = Path(attempt["runtime_path"])
    try:
        manifest = _validate(runtime)
        collected = _validated_collection(runtime, manifest)
        if (manifest.get("role") != "repair" or manifest.get("intent_id") != attempt["intent_id"]
                or manifest.get("candidate_binding") != {"task_id": parent["task_id"], **_fingerprint(parent)}
                or manifest.get("request", {}).get("sha256") != attempt["request_digest"]
                or collected.get("sid") != attempt["cli_sid"]
                or any(collected.get(key) is not True for key in
                       ("technical_accepted", "delivery_complete", "process_ended"))):
            raise TaskflowError("repair identity original manifest/collection binding differs")
        name = str(record_path.relative_to(runtime / "artifacts"))
        if collected["artifacts"].get(name) != _info(record_path):
            raise TaskflowError("repair identity record is not the sealed collected original")
        record = _json(record_path)
        if ("covered_defect_ids" not in record
                or ("intent_id" in record and record["intent_id"] != attempt["intent_id"])):
            raise TaskflowError("repair identity needs the original repair field and intent")
        validation = validate_repair(record_path, parent=parent, candidate=candidate, defect_ids=defect_ids)
        for source, expected in ((parent_root, parent), (candidate_root, candidate)):
            actual = check_task(source, write_report=False)
            if actual["errors"] or any(actual[key] != expected[key] for key in ("task_id", *_DIGESTS)):
                raise TaskflowError("repair identity current candidate source drifted")
            declaration = source / "task.json"
            validate_evidence(expected.get("evidence"))
            if not any(ref["path"] == str(declaration) and ref["sha256"] == _info(declaration)["sha256"]
                       for ref in expected["evidence"]):
                raise TaskflowError("repair identity source declaration differs from capture")
            files = _files(expected)
            declared = {row["path"]: row["role"] for row in _json(declaration)["files"]}
            if declared != {key: row["role"] for key, row in files.items()}:
                raise TaskflowError("repair identity source members/roles differ from capture")
            for key, row in files.items():
                if row["path"] != str(source / key):
                    raise TaskflowError("repair identity source path differs from capture")
                if source == candidate_root:
                    delivered = str((source / key).relative_to(runtime / "artifacts"))
                    if collected["artifacts"].get(delivered) != _info(source / key):
                        raise TaskflowError("repair identity candidate member absent from original delivery")
        declaration_name = str((candidate_root / "task.json").relative_to(runtime / "artifacts"))
        if collected["artifacts"].get(declaration_name) != _info(candidate_root / "task.json"):
            raise TaskflowError("repair identity candidate declaration absent from original delivery")
        # Inspect the existing archive binding and every bound byte. Do not call
        # archive_full_attempt here: a resume preflight must never publish it.
        binding = _json(archive / "archive-binding.json")
        if (binding.get("intent_id") != attempt["intent_id"]
                or binding.get("collection") != _info(runtime / "collection.json")
                or not isinstance(binding.get("files"), dict)
                or not {"manifest.json", "collection.json", "artifacts/" + name} <= set(binding["files"])):
            raise TaskflowError("repair identity original archive binding differs")
        for member, expected in binding["files"].items():
            member = relative_path(member)
            if _info(archive / member) != expected or _info(runtime / member) != expected:
                raise TaskflowError("repair identity original archived evidence changed")
        return {"attempt_id": attempt["attempt_id"], "work_id": attempt["work_id"],
                "parent_id": parent["candidate_id"], "candidate_id": candidate["candidate_id"],
                "parent_root": str(parent_root), "candidate_root": str(candidate_root),
                "original": evidence_ref(record_path, "original sealed repair identity; no quality inference"),
                "archive": evidence_ref(archive / "archive-binding.json", "original completed repair archive"),
                "collection": evidence_ref(runtime / "collection.json", "original sealed repair collection"),
                "validation": validation}
    except (OSError, RunnerError, ValueError, KeyError, TypeError) as error:
        raise TaskflowError(f"Cannot verify completed repair identity: {error}") from error


def resolve_repair_record_projection(projection: dict, *, attempt: dict, parent: dict,
                                     candidate: dict, defect_ids: list) -> dict:
    """Verify an explicit derived record separating an unchanged declaration copy.

    The original collected author record stays strict and unchanged. Only one
    task.json materialization can be excluded from its payload change list.
    """
    from taskflow.execution_runner import _info, _validate, _validated_collection
    from taskflow.execution_store import evidence_ref, validate_evidence
    from taskflow.packages import check_task

    if not isinstance(projection, dict) or set(projection) != {"original", "projected"}:
        raise TaskflowError("repair projection requires exactly original and projected references")
    original_ref, projected_ref = validate_evidence([projection[key] for key in ("original", "projected")])
    if (attempt.get("state") != "completed" or attempt.get("role") != "repair"
            or type(attempt.get("process_exit")) is not int or attempt["process_exit"] != 0
            or any(attempt.get(key) is not True for key in
                   ("response_complete", "artifacts_complete", "technical_accepted"))
            or candidate.get("author_attempt") != attempt.get("attempt_id")
            or candidate.get("parent_id") != parent.get("candidate_id")
            or attempt.get("candidate_id") != parent.get("candidate_id")
            or attempt.get("task_id") != parent.get("task_id")
            or candidate.get("task_id") != parent.get("task_id")):
        raise TaskflowError("repair projection requires the exact completed managed repair author and candidates")
    runtime = Path(attempt["runtime_path"])
    try:
        manifest = _validate(runtime)
        collected = _validated_collection(runtime, manifest)
        if (manifest.get("intent_id") != attempt["intent_id"] or manifest.get("role") != "repair"
                or manifest.get("candidate_binding") != {"task_id": parent["task_id"], **_fingerprint(parent)}
                or manifest.get("request", {}).get("sha256") != attempt["request_digest"]
                or collected.get("technical_accepted") is not True
                or collected.get("delivery_complete") is not True or collected.get("process_ended") is not True):
            raise TaskflowError("repair projection original manifest or collection binding differs")
        original_path = runtime / "artifacts/repair-record.json"
        if (Path(original_ref["path"]) != original_path
                or Path(projected_ref["path"]) == original_path
                or collected["artifacts"].get("repair-record.json") != _info(original_path)
                or original_ref["sha256"] != collected["artifacts"]["repair-record.json"]["sha256"]):
            raise TaskflowError("repair projection original must be the exact sealed collected repair-record.json")
        before, after = _files(parent), _files(candidate)
        if "task.json" in before or "task.json" in after:
            raise TaskflowError("repair projection cannot exclude a declared task.json payload")
        inputs = [row for row in manifest["inputs"] if row["target"] == "execution/task.json"]
        if len(inputs) != 1 or inputs[0]["kind"] != "source_metadata":
            raise TaskflowError("repair projection requires one actual locked declaration input")
        item = inputs[0]
        staged_path = runtime / "inputs/execution/task.json"
        staged = _regular(staged_path)
        if (sha256(staged) != item["sha256"] or len(staged) != item["size"]
                or _regular(Path(item["source"])) != staged
                or not any(ref.get("path") == item["source"] and ref.get("sha256") == item["sha256"]
                           for ref in parent.get("evidence", []))):
            raise TaskflowError("repair projection declaration input does not bind the actual parent")
        declarations = [ref for ref in candidate.get("evidence", [])
                        if Path(ref["path"]).name == "task.json"
                        and Path(ref["path"]).is_relative_to(runtime / "artifacts")]
        if len(declarations) != 1:
            raise TaskflowError("repair projection requires the actual delivered candidate declaration")
        declaration_path = Path(declarations[0]["path"])
        name = str(declaration_path.relative_to(runtime / "artifacts"))
        if (_regular(declaration_path) != staged or declarations[0]["sha256"] != item["sha256"]
                or collected["artifacts"].get(name) != {"sha256": item["sha256"], "size": item["size"]}):
            raise TaskflowError("repair projection delivered declaration differs from the locked original input")
        for source, expected in ((Path(item["source"]).parent, parent), (declaration_path.parent, candidate)):
            actual = check_task(source, write_report=False)
            if actual["errors"] or any(actual[key] != expected[key] for key in ("task_id", *_DIGESTS)):
                raise TaskflowError("repair projection declaration does not describe the captured candidate")
        original = _json(original_path)
        rows = original.get("changes")
        if not isinstance(rows, list):
            raise TaskflowError("repair projection original changes must be a list")
        metadata = [index for index, row in enumerate(rows) if isinstance(row, dict) and row.get("path") == "task.json"]
        if len(metadata) != 1:
            raise TaskflowError("repair projection requires one unique task.json materialization")
        excluded = rows[metadata[0]]
        if ("before_sha256" not in excluded or excluded["before_sha256"] is not None
                or excluded.get("after_sha256") != item["sha256"]):
            raise TaskflowError("repair projection may exclude only the unchanged task.json materialization")
        expected = {**original, "changes": [row for index, row in enumerate(rows) if index != metadata[0]]}
        projected_path = Path(projected_ref["path"])
        if canonical(_json(projected_path)) != canonical(expected):
            raise TaskflowError("repair projection must preserve every original field except that unique metadata row")
        try:
            validate_repair(original_path, parent=parent, candidate=candidate, defect_ids=defect_ids)
        except TaskflowError as error:
            if str(error) != "Repair changes duplicate or do not reflect actual differences":
                raise
        else:
            raise TaskflowError("repair projection requires the original strict diff exception")
        validation = validate_repair(projected_path, parent=parent, candidate=candidate, defect_ids=defect_ids)
        validate_evidence([original_ref, projected_ref])
        declaration_input = evidence_ref(staged_path, "original manifest-locked execution/task.json")
        declaration_delivered = evidence_ref(declaration_path, "byte-identical delivered task.json materialization")
        if (declaration_input["sha256"] != item["sha256"]
                or declaration_delivered["sha256"] != item["sha256"]):
            raise TaskflowError("repair projection declaration changed during verification")
        return {"attempt_id": attempt["attempt_id"], "work_id": attempt["work_id"],
                "parent_id": parent["candidate_id"], "candidate_id": candidate["candidate_id"],
                "original": original_ref, "projected": projected_ref, "record_path": str(projected_path),
                "declaration_input": {**declaration_input, "size": len(staged)},
                "declaration_delivered": {**declaration_delivered, "size": len(staged)},
                "validation": validation}
    except (OSError, RunnerError, ValueError, KeyError, TypeError) as error:
        raise TaskflowError(f"Cannot verify repair record projection: {error}") from error

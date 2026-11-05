"""Version an approved legacy formal adapter for an explicit confirmed CLI profile.

This migration versions model/reasoning, confirmed host metadata and an optional
exact task-local permission-probe contract.
It never prepares a task, opens a database, starts a CLI or calls a model.
"""

import copy
import json
import re
from pathlib import Path
import shutil

from taskflow.contracts import TaskflowError, canonical, sha256
from taskflow.execution_batch import load_batch_snapshot, role_config
from taskflow.execution_runner import _regular
from taskflow.execution_store import evidence_ref, validate_evidence


MEMBERS = {"runtime_gate.py", "execute_diagnostic.py", "file_evidence.py", "baseline-plan.json"}


def _replace_once(source, before, after):
    if source.count(before) != 1:
        raise TaskflowError("formal adapter profile: unsupported source template boundary")
    return source.replace(before, after, 1)


def _provider_baseline(baseline: dict, setting: dict, transport_retry_mode: str = "legacy_disabled") -> dict | None:
    """Configure the existing versioned Codex command without executing it."""
    reference = setting.get("native_provider")
    if reference is None:
        return None
    from taskflow.execution_codex_provider import _checked, provider_settings
    from taskflow.execution_codex_retry import provider_settings_with_retry
    from tools.isolated_quality import inline_table

    selected = json.loads(_checked(reference).read_text(encoding="utf-8"))
    argv = baseline["argv"]
    retry_keys = ("model_providers.taskflow_external.request_max_retries=",
                  "model_providers.taskflow_external.stream_max_retries=")
    if transport_retry_mode == "codex_default_v1" and any(part.startswith(retry_keys) for part in argv):
        raise TaskflowError("formal adapter profile: source already overrides provider retries")
    profiles = [json.loads(part.split("=", 1)[1]) for part in argv
                if part.startswith("default_permissions=")]
    if len(profiles) != 1:
        raise TaskflowError("formal provider needs one explicit native permission profile")
    profile = profiles[0]
    prefix = "permissions." + profile + ".filesystem="
    indexes = [i for i, part in enumerate(argv) if part.startswith(prefix)]
    if len(indexes) != 1:
        raise TaskflowError("formal provider needs one declared filesystem map")
    table = argv[indexes[0]][len(prefix):]
    token = r'("(?:[^"\\]|\\.)*")\s*=\s*("(?:[^"\\]|\\.)*")'
    if not table.startswith("{") or not table.endswith("}"):
        raise TaskflowError("formal filesystem must be an explicit string table")
    body = table[1:-1]
    if re.sub(token, "", body).strip(", \t\r\n"):
        raise TaskflowError("unsupported formal filesystem declaration")
    pairs = [(json.loads(key), json.loads(value)) for key, value in re.findall(token, body)]
    filesystem = dict(pairs)
    if len(filesystem) != len(pairs):
        raise TaskflowError("duplicate formal filesystem declaration")
    filesystem[str(Path(selected["codex_home"]).resolve())] = "deny"
    config = {**setting, "native_permissions": {"profile": profile}}
    options, provider = (provider_settings_with_retry(config, filesystem) if transport_retry_mode == "codex_default_v1"
                         else provider_settings(config, filesystem))
    argv[indexes[0]] = prefix + inline_table(filesystem)
    argv[0] = selected["cli"]["path"]
    split = argv.index("exec")
    argv[split:split] = [part for option in options for part in ("-c", option)]
    code_sources = []
    if transport_retry_mode == "codex_default_v1":
        for name in ("execution_codex_provider.py", "execution_codex_retry.py"):
            path = Path(__file__).resolve().parents[1] / "taskflow" / name
            code_sources.append({**evidence_ref(path, "versioned provider preparation source"), "size":path.stat().st_size})
    baseline.update(native_provider_ref=reference, codex_provider=provider,
                    codex_provider_sources=[reference, selected["catalog"], *selected["documentation"], *code_sources],
                    observed_cli=selected["cli"])
    return provider



def _task_local_probe_sources(original):
    """Render the exact task-local public/private target contract, without probes."""
    sources = dict(original)
    prepare = sources['prepare_diagnostic.py'].decode()
    start = prepare.index("    targets = [('public',")
    end = prepare.index('    for name, path, kind, expected in targets:', start)
    prepare = prepare[:start] + '    targets = gate.task_local_probe_targets(plan)\n' + prepare[end:]
    prepare = _replace_once(prepare, "'nature': 'no_inference_existing_target_boundary_probes'",
                           "'nature': 'no_inference_task_local_boundary_probes_v1'")
    sources['prepare_diagnostic.py'] = prepare.encode()
    
    gate = sources['runtime_gate.py'].decode()
    helper = (
        'def task_local_probe_targets(plan: dict) -> list[tuple]:\n'
        '    """Only this frozen task\'s public/private files and own neutral marker.\n'
        '\n'
        '    This gate does not establish credential, global-session or parent isolation.\n'
        '    """\n'
        "    if plan.get('additional_deny_read_probes', []):\n"
        "        raise ValueError('Task-local adapter does not accept extra read-probe paths')\n"
        "    work = Path(plan['cwd'])\n"
        "    private = Path(plan['private_directory'])\n"
        "    targets = [('public', work / 'draft/input/prompt.md', 'file', True),\n"
        "               ('reviewer_private', private / 'answer.md', 'file', False),\n"
        "               ('frozen_teacher', private / 'answer.md', 'file', False),\n"
        "               ('frozen_rubric', private / 'rubric.md', 'file', False),\n"
        "               ('neutral_control', Path(plan['root']) / 'control/marker.txt', 'file', False)]\n"
        "    for index, name in enumerate(plan['public_tree']):\n"
        "        if Path(name).is_absolute() or '..' in Path(name).parts:\n"
        "            raise ValueError('Public probe member must be a relative frozen task path')\n"
        "        targets.append((f'public_member_{index:02d}', work / name, 'file', True))\n"
        '    return targets\n'
        '\n'
        '\n'
    )
    gate = _replace_once(gate, 'def shell_probe_passed(report: dict, plan: dict) -> bool:\n',
                        helper + 'def shell_probe_passed(report: dict, plan: dict) -> bool:\n')
    start = gate.index('        required_denied = ')
    end = gate.index('        base = shell_probe_base(plan)', start)
    gate = gate[:start] + (
        '        targets = task_local_probe_targets(plan)\n'
        "        targets.append(('symlink_teacher', Path(plan['cwd']) / 'audit-teacher-link', 'file', False))\n"
        "        expected_specs = [{'name': name, 'path': str(path), 'kind': kind, 'expected_readable': expected,\n"
        "                           'expected_sha256': plan['public_tree'].get(str(path.relative_to(Path(plan['cwd']))), {}).get('sha256')\n"
        "                           if expected and path.is_relative_to(Path(plan['cwd'])) else None}\n"
        '                          for name, path, kind, expected in targets]\n'
    ) + gate[end:]
    gate = _replace_once(gate, "report.get('nature') == 'no_inference_existing_target_boundary_probes'",
                        "report.get('nature') == 'no_inference_task_local_boundary_probes_v1'")
    gate = _replace_once(gate, 'and observed_public == public and required_denied <= denied',
                        'and observed_public == public and specs == expected_specs')
    gate = _replace_once(gate, "        denied = {entry['path'] for entry in specs if entry.get('expected_readable') is False}\n", '')
    sources['runtime_gate.py'] = gate.encode()
    return sources

def rendered_profile_sources(*, adapter_ref, snapshot_ref, runtime_support_refresh=None, probe_scope=None,
                             transport_retry_mode="legacy_disabled"):
    """Read exact sources and return the deterministic versioned profile bytes."""
    if transport_retry_mode not in ("legacy_disabled", "codex_default_v1"):
        raise TaskflowError("formal adapter profile: unsupported transport retry mode")
    if probe_scope not in (None, "task_local_v1"):
        raise TaskflowError("formal adapter profile: unsupported probe scope")
    source = validate_evidence([adapter_ref])[0]
    if set(adapter_ref.get("files", {})) != MEMBERS:
        raise TaskflowError("formal adapter profile: complete original source set required")
    root = Path(source["path"]).parent
    sources = {"prepare_diagnostic.py": _regular(Path(source["path"]))}
    for name, digest in adapter_ref["files"].items():
        raw = _regular(root / name)
        if sha256(raw) != digest:
            raise TaskflowError("formal adapter profile: original source changed: " + name)
        sources[name] = raw
    snapshot = load_batch_snapshot(snapshot_ref)
    setting = role_config(snapshot, "solver")
    if setting["reasoning_selection"] != "explicit" or setting["reasoning_context"] is not None:
        raise TaskflowError("formal adapter profile: explicit effort with no context override required")
    baseline = json.loads(sources["baseline-plan.json"])
    argv = baseline["argv"]
    if argv.count("-m") != 1 or argv.index("-m") + 1 >= len(argv):
        raise TaskflowError("formal adapter profile: one original requested model required")
    if any(part.startswith(("model_reasoning_effort=", "model_reasoning_context=")) for part in argv):
        raise TaskflowError("formal adapter profile: source already has a reasoning override")
    baseline["argv"] = copy.deepcopy(argv)
    baseline["argv"][argv.index("-m") + 1] = setting["model"]
    split = baseline["argv"].index("exec")
    baseline["argv"][split:split] = ["-c", "model_reasoning_effort=" + json.dumps(setting["reasoning_effort"])]
    baseline.update(requested_model=setting["model"], reasoning_setting="explicit",
                    request_reasoning_override=setting["reasoning_effort"])
    if runtime_support_refresh is not None:
        from taskflow.execution_cli_baseline import refreshed_cli_baseline

        baseline = refreshed_cli_baseline(baseline, refresh=runtime_support_refresh, snapshot=snapshot_ref)
    provider = _provider_baseline(baseline, setting, transport_retry_mode)
    if transport_retry_mode == "codex_default_v1" and provider is None:
        raise TaskflowError("formal adapter profile: transport retry mode requires an explicit provider")
    sources["baseline-plan.json"] = canonical(baseline) + b"\n"
    gate = sources["runtime_gate.py"].decode("utf-8")
    old_model_lines = [line for line in gate.splitlines() if line.startswith("REQUESTED_MODEL = ")]
    if len(old_model_lines) != 1:
        raise TaskflowError("formal adapter profile: one original model declaration required")
    gate = _replace_once(gate, old_model_lines[0],
        "PROFILE = json.loads((HERE / 'baseline-plan.json').read_text(encoding='utf-8'))\n"
        "REQUESTED_MODEL = PROFILE['requested_model']\n"
        "REQUESTED_EFFORT = PROFILE['request_reasoning_override']")
    gate = _replace_once(gate, "    argv = plan['argv']\n", "    argv = plan['argv']\n"
        "    efforts = [part.split('=', 1)[1] for part in argv if part.startswith('model_reasoning_effort=')]\n"
        "    if len(efforts) != 1 or json.loads(efforts[0]) != REQUESTED_EFFORT:\n"
        "        raise ValueError('Explicit reasoning differs from confirmed profile')\n"
        "    if plan.get('reasoning_setting') != 'explicit' or plan.get('request_reasoning_override') != REQUESTED_EFFORT:\n"
        "        raise ValueError('Plan reasoning declaration differs from confirmed profile')\n")
    gate = _replace_once(gate, "            and observation.get('model') == REQUESTED_MODEL\n",
        "            and observation.get('model') == REQUESTED_MODEL\n"
        "            and isinstance(observation.get('reasoning'), dict)\n"
        "            and observation['reasoning'].get('effort') == REQUESTED_EFFORT\n")
    sources["runtime_gate.py"] = gate.encode("utf-8")
    prepare = sources["prepare_diagnostic.py"].decode("utf-8")
    prepare = _replace_once(prepare, "'reasoning_setting': 'not_explicitly_specified',",
        "'reasoning_setting': 'explicit', 'request_reasoning_override': gate.REQUESTED_EFFORT,")
    prepare = _replace_once(prepare, "    if observations[0]['model'] != gate.REQUESTED_MODEL:\n",
        "    if (not isinstance(observations[0].get('reasoning'), dict)\n"
        "            or observations[0]['reasoning'].get('effort') != gate.REQUESTED_EFFORT):\n"
        "        raise ValueError('Fixture did not preserve explicit reasoning effort')\n"
        "    if observations[0]['model'] != gate.REQUESTED_MODEL:\n")
    if provider:
        from taskflow import execution_codex_environment

        execute = sources["execute_diagnostic.py"].decode("utf-8")
        environment = Path(execution_codex_environment.__file__).read_text(encoding="utf-8")
        execute = _replace_once(execute, "def execute(", environment + "\n\ndef execute(")
        execute = _replace_once(execute, "stdout=stdout, stderr=stderr, start_new_session=True)",
            "stdout=stdout, stderr=stderr, env=process_environment("
            "gate.read_json(gate.HERE / 'baseline-plan.json').get('codex_provider')), start_new_session=True)")
        sources["execute_diagnostic.py"] = execute.encode("utf-8")
        gate = sources["runtime_gate.py"].decode("utf-8")
        gate = _replace_once(gate, "    for module in list(sys.modules.values()):\n",
            "    for ref in PROFILE.get('codex_provider_sources', []):\n"
            "        path = Path(ref['path'])\n"
            "        if digest(path.read_bytes()) != ref['sha256'] or path.stat().st_size != ref['size']:\n"
            "            raise ValueError('Codex provider original changed')\n"
            "        paths.add(path.resolve())\n"
            "    for module in list(sys.modules.values()):\n")
        sources["runtime_gate.py"] = gate.encode("utf-8")
        prepare = _replace_once(prepare, "            'served_model': None, 'served_revision': None,",
            "            'native_provider_ref': baseline['native_provider_ref'],\n"
            "            'served_model': None, 'served_revision': None,")
    sources["prepare_diagnostic.py"] = prepare.encode("utf-8")
    if probe_scope == "task_local_v1":
        sources = _task_local_probe_sources(sources)
    return sources, setting


def build_formal_profile_adapter(*, adapter_ref, snapshot_ref, destination, runtime_support_refresh=None, probe_scope=None,
                                 transport_retry_mode="legacy_disabled"):
    """Produce a new immutable adapter without preparing or executing a task."""
    sources, setting = rendered_profile_sources(adapter_ref=adapter_ref, snapshot_ref=snapshot_ref,
                                                runtime_support_refresh=runtime_support_refresh, probe_scope=probe_scope,
                                                transport_retry_mode=transport_retry_mode)
    destination = Path(destination).absolute()
    if destination.exists() or destination.is_symlink() or any(p.is_symlink() for p in destination.parents):
        raise TaskflowError("formal adapter profile: new plain destination required")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.mkdir()
    try:
        for name, raw in sources.items():
            if name.endswith(".py"):
                compile(raw, name, "exec")
            (destination / name).write_bytes(raw)
        result = {**evidence_ref(destination / "prepare_diagnostic.py", "versioned explicit confirmed solver adapter"),
                  "files": {name: sha256(sources[name]) for name in MEMBERS}}
        provenance = {"schema_version": 1, "source_adapter": adapter_ref, "snapshot": snapshot_ref,
                      "solver": setting, "adapter": result, "model_calls": 0,
                      "runtime_prepared": False, "isolation_proven": False}
        if transport_retry_mode != "legacy_disabled":
            provenance["transport_retry_mode"] = transport_retry_mode
        if probe_scope is not None:
            provenance["probe_scope"] = probe_scope
        if runtime_support_refresh is not None:
            provenance["runtime_support_refresh"] = runtime_support_refresh
        (destination / "profile-migration.json").write_bytes(canonical(provenance) + b"\n")
        return result
    except BaseException:
        # This directory was created here and contains generated source only.
        shutil.rmtree(destination)
        raise

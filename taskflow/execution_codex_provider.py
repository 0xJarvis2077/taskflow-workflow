"""Explicit role-local Codex provider and credential-source binding.

This configures the existing Codex CLI. It does not implement a model protocol,
copy a credential value, mutate global settings or grant model tools host data.
"""
import copy
import hashlib
import json
from pathlib import Path

from taskflow.contracts import TaskflowError, canonical, read_json


def _checked(ref: dict) -> Path:
    path=Path(ref["path"])
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise TaskflowError("Codex provider original must be an absolute ordinary file")
    raw=path.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=ref["sha256"] or len(raw)!=ref["size"]:
        raise TaskflowError("Codex provider source changed")
    return path


def provider_references_equal(original: dict | None, selected: dict | None) -> bool:
    """Verify both source files and compare every declaration except location.

    Absent references retain the built-in provider path. Missing optional size
    metadata is verified against current bytes, without adding it to either
    original declaration or weakening comparisons of their existing fields.
    """
    if original is None and selected is None:
        return True
    if not isinstance(original, dict) or not isinstance(selected, dict):
        return False
    for reference in (original, selected):
        if (not isinstance(reference.get("path"), str) or not reference["path"]
                or not isinstance(reference.get("sha256"), str) or not reference["sha256"]):
            return False
        if "size" in reference and type(reference["size"]) is not int:
            return False
        try:
            size = reference.get("size", Path(reference["path"]).stat().st_size)
            _checked({**reference, "size": size})
        except (TaskflowError, OSError, ValueError, TypeError, KeyError):
            return False
    return canonical({key: value for key, value in original.items() if key != "location"}) == canonical(
        {key: value for key, value in selected.items() if key != "location"})


def provider_settings(config: dict, filesystem: dict) -> tuple[list[str], dict | None]:
    ref=config.get("native_provider")
    if ref is None:return [],None
    selected=read_json(_checked(ref))
    fields={"kind","provider","base_url","wire_api","credential_env","model","reasoning_effort",
            "cli","catalog","codex_home","documentation"}
    allowed={"openai":("https://api.openai.com/v1","OPENAI_API_KEY"),
             "deepseek":("https://api.deepseek.com","DEEPSEEK_API_KEY")}
    expected=allowed.get(selected.get("provider"))
    if (set(selected)!=fields or selected["kind"]!="codex_role_provider_v1" or expected is None
            or (selected["base_url"].rstrip('/'),selected["credential_env"])!=expected
            or selected["wire_api"]!="responses" or selected["model"]!=config["model"]
            or selected["reasoning_effort"]!=config["reasoning_effort"]
            or _checked(selected["cli"]).resolve()!=Path(config["cli"]).resolve()):
        raise TaskflowError("Codex provider/model/effort/CLI binding differs")
    catalog=_checked(selected["catalog"]);models=read_json(catalog)["models"]
    matches=[row for row in models if row.get("slug")==selected["model"]]
    if (len(matches)!=1 or matches[0].get("tool_mode")!="direct"
            or selected["reasoning_effort"] not in [x["effort"] for x in matches[0]["supported_reasoning_levels"]]):
        raise TaskflowError("Codex role model metadata must explicitly support requested effort and actual direct tools")
    if any(filesystem.get(k)!=v for k,v in {":root":"deny",":minimal":"read",":tmpdir":"deny",":slash_tmp":"deny","/private/tmp":"deny"}.items()):
        raise TaskflowError("explicit role-local Codex provider requires full restrictive native filesystem profile")
    home=Path(selected["codex_home"]).resolve()
    if not home.is_dir() or filesystem.get(str(home))!="deny":
        raise TaskflowError("role-local Codex home must be inaccessible to model tools")
    if not selected["documentation"]:raise TaskflowError("provider capability source missing")
    for source in selected["documentation"]:_checked(source)
    key="model_providers.taskflow_external."
    settings=['model_provider="taskflow_external"','model_catalog_json='+json.dumps(str(catalog)),
        key+'name='+json.dumps(selected["provider"]),key+'base_url='+json.dumps(selected["base_url"]),
        key+'wire_api="responses"',key+'env_key='+json.dumps(selected["credential_env"]),
        key+'requires_openai_auth=false',key+'request_max_retries=0',key+'stream_max_retries=0',
        'features.code_mode=false','features.code_mode_only=false','features.code_mode_host=false',
        'shell_environment_policy.inherit="none"','web_search="disabled"',
        'features.network_proxy=false','permissions.'+config['native_permissions']['profile']+'.network.enabled=false']
    return settings,{"provider":selected["provider"],"wire_api":"responses","base_url":selected["base_url"],
                     "credential_env":selected["credential_env"],"codex_home":str(home),"source":ref,
                     "catalog":selected["catalog"],"cli":selected["cli"],"automatic_retry":False}


def selection_metadata(config: dict, evidence: dict, provider: dict) -> tuple[dict, str]:
    """Record configured custom-model selection without claiming served identity.

    Built-in catalogs describe bundled models, not custom provider availability.
    The checked role catalog and provider documentation own that declaration.
    """
    cli = str(Path(config["cli"]).resolve())
    rows = evidence.get("commands", [])
    expected = [[cli, "--version"], [cli, "--no-daemon", "debug", "models", "--bundled"]]
    if (evidence.get("model_calls") != 0 or len(rows) != 2
            or any(row.get("argv") != argv or row.get("returncode") != 0
                   for row, argv in zip(rows, expected))
            or not isinstance(rows[0].get("stdout"), str)
            or not rows[0]["stdout"].strip()):
        raise TaskflowError("Original Codex version and bundled catalog observation must succeed")
    return {
        "requested": config["model"],
        "reasoning_effort": config["reasoning_effort"],
        "selection": "Explicit custom provider, -m and model_reasoning_effort; checked role-local catalog.",
        "catalog_source": provider["catalog"],
        "client_resolved": None,
        "client_resolution_scope": "Configuration and catalog checked only; outbound request and server execution unobserved.",
        "served": None,
        "revision": None,
    }, rows[0]["stdout"].strip()

def copy_provider_setting(target: dict, setting: dict) -> None:
    """Propagate only the optional provider reference from the selected policy.

    Absence removes an obsolete template choice. Full source and runtime
    validation stays at the existing batch/preparation boundaries.
    """
    if setting.get("native_provider") is not None:
        target["native_provider"] = copy.deepcopy(setting["native_provider"])
    else:
        target.pop("native_provider", None)


TEXT_ONLY_DISABLED_FEATURES = (
    "shell_tool", "shell_snapshot", "view_image", "code_mode", "code_mode_host",
    "code_mode_only", "apps", "plugins", "hooks", "memories", "multi_agent",
    "skill_search", "image_generation", "browser_use", "computer_use",
    "deferred_executor", "request_permissions_tool", "send_message_to_user_async",
    "token_budget", "current_time_reminder", "sleep_tool", "tool_suggest",
)


def text_only_catalog(catalog: dict, model: str, *, instructions: str | None = None) -> dict:
    """Configure Codex without model-accessible file/command capabilities."""
    matches = [item for item in catalog["models"] if item.get("slug") == model]
    if len(matches) != 1:
        raise TaskflowError("exact text-only model metadata required")
    selected = copy.deepcopy(matches[0])
    selected.update(tool_mode="direct", apply_patch_tool_type=None, shell_type="disabled",
                    use_responses_lite=False, experimental_supported_tools=[],
                    supports_parallel_tool_calls=False)
    if instructions is not None:
        selected["base_instructions"] = instructions
        if isinstance(selected.get("model_messages"), dict):
            selected["model_messages"]["instructions_template"] = instructions
    return {"models": [selected]}

"""One explicitly authorized tool-free Codex connectivity attempt per provider.

Python manages the process and originals; Codex owns the model protocol.
This is neither a business-task run nor a model-tool isolation qualification.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
from datetime import datetime, timezone

from taskflow.contracts import write_json
from taskflow.execution_codex_provider import _checked, text_only_catalog, TEXT_ONLY_DISABLED_FEATURES
from taskflow.execution_codex_environment import process_environment

MARKER = "CONNECTION_OK"
DISABLED = TEXT_ONLY_DISABLED_FEATURES


def observed(stdout: str, returncode: int | None) -> dict:
    events = []
    for line in stdout.splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict):
            events.append(item)
    messages = [event['item'].get('text', '') for event in events
                if event.get('type') == 'item.completed'
                and event.get('item', {}).get('type') == 'agent_message']
    tool_items, warnings, errors = [], [], []
    turn_started = False
    for event in events:
        if event.get('type') == 'turn.started':
            turn_started = True
        if event.get('type') not in {'item.started', 'item.completed'}:
            continue
        item = event.get('item', {})
        kind = item.get('type')
        if kind == 'error':
            message = item.get('message', '')
            if not turn_started and message.startswith('Under-development features enabled:'):
                warnings.append(message)
            else:
                errors.append(message)
        elif kind not in {'agent_message', 'reasoning'}:
            tool_items.append(kind)
    completed = [event for event in events if event.get('type') == 'turn.completed']
    return {'success': returncode == 0 and bool(completed) and bool(messages)
                       and messages[-1].strip() == MARKER and not tool_items and not errors,
            'reply': messages[-1] if messages else None,
            'codex_sid': next((event.get('thread_id') for event in events if event.get('type') == 'thread.started'), None),
            'usage': completed[-1].get('usage') if completed else None,
            'tool_events': tool_items, 'startup_warnings': warnings, 'errors': errors,
            'response_id': None, 'served_model': None,
            'identity_limit': 'Codex JSON exposes its SID and usage, not necessarily provider response ID or served model.'}


def run(profile_path: Path, root: Path, *, execute: bool = False) -> dict:
    profile = json.loads(profile_path.read_text(encoding='utf-8'))
    provider = profile['provider']
    official = {'openai': ('https://api.openai.com/v1', 'OPENAI_API_KEY'),
                'deepseek': ('https://api.deepseek.com', 'DEEPSEEK_API_KEY')}
    if (provider not in official or (profile['base_url'].rstrip('/'), profile['credential_env']) != official[provider]
            or profile['wire_api'] != 'responses'):
        raise ValueError('Explicit official Responses endpoint and credential source required')
    cli = _checked(profile['cli'])
    catalog = json.loads(_checked(profile['catalog']).read_text(encoding='utf-8'))
    models = [row for row in catalog['models'] if row['slug'] == profile['model']]
    if len(models) != 1 or profile['reasoning_effort'] not in [x['effort'] for x in models[0]['supported_reasoning_levels']]:
        raise ValueError('Explicit requested model/effort missing from selected catalog')
    if not os.environ.get(profile['credential_env']):
        raise ValueError('Selected provider credential source is not available')
    if not execute:
        return {'ready': True, 'provider': provider, 'model': profile['model'], 'effort': profile['reasoning_effort'], 'model_calls': 0}
    root.mkdir(parents=True, exist_ok=False)
    home, work = root/'codex-home', root/'work'
    home.mkdir(); work.mkdir()
    selected = {**profile, 'codex_home': str(home)}
    instruction = 'This is a minimal provider connectivity check. Reply only CONNECTION_OK. Do not use tools.'
    write_json(root/'catalog.json', text_only_catalog(catalog, profile['model'], instructions=instruction))
    request = 'Reply with exactly CONNECTION_OK.'
    (root/'request.txt').write_text(request+'\n', encoding='utf-8')
    key = 'model_providers.connectivity.'
    settings = ['model_provider="connectivity"', 'model_catalog_json='+json.dumps(str(root/'catalog.json')),
        key+'name='+json.dumps(provider), key+'base_url='+json.dumps(profile['base_url']),
        key+'wire_api="responses"', key+'env_key='+json.dumps(profile['credential_env']),
        key+'requires_openai_auth=false', key+'supports_websockets=false',
        key+'request_max_retries=0', key+'stream_max_retries=0',
        'model_reasoning_effort='+json.dumps(profile['reasoning_effort']),
        'web_search="disabled"', 'shell_environment_policy.inherit="none"',
        'features.network_proxy=false', 'apps._default.enabled=false', 'agents.enabled=false',
        'history.persistence="none"', 'tools.update_plan.enabled=false',
        'tools.experimental_request_user_input.enabled=false']
    argv = [str(cli), '--no-daemon', '-a', 'never']
    argv += [part for setting in settings for part in ('-c', setting)]
    argv += [part for feature in DISABLED for part in ('--disable', feature)]
    argv += ['--enable', 'skip_host_skill_discovery', '-C', str(work), 'exec', '--ignore-user-config',
             '--ignore-rules', '--skip-git-repo-check', '--ephemeral', '--json', '-m', profile['model'], '-']
    claim = {'kind': 'real_codex_connectivity_attempt', 'provider': provider, 'model_requested': profile['model'],
        'effort_requested': profile['reasoning_effort'], 'endpoint': profile['base_url'].rstrip('/')+'/responses',
        'credential_env': profile['credential_env'], 'purpose': 'New explicitly authorized connectivity-only call; no business attempt retry',
        'started_at': datetime.now(timezone.utc).isoformat(), 'argv': argv, 'automatic_retry': False,
        'task_materials': [], 'simulation': False, 'tools_disabled': list(DISABLED)}
    write_json(root/'claim.json', claim)
    process = None
    timed_out = False
    launch_error = None
    try:
        process = subprocess.Popen(argv, cwd=work, env=process_environment(selected), stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        write_json(root/'process.json', {'pid': process.pid, 'purpose': 'connectivity only'})
        try:
            stdout, stderr = process.communicate(request.encode(), timeout=180)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
    except OSError as error:
        launch_error = type(error).__name__
        stdout, stderr = b'', ('Codex process error: '+launch_error).encode()
    returncode = process.returncode if process is not None else None
    stdout, stderr = stdout.decode(errors='replace'), stderr.decode(errors='replace')
    secret = os.environ[profile['credential_env']]
    redacted = secret in stdout or secret in stderr
    stdout, stderr = stdout.replace(secret, '[REDACTED]'), stderr.replace(secret, '[REDACTED]')
    (root/'stdout.jsonl').write_text(stdout, encoding='utf-8')
    (root/'stderr.txt').write_text(stderr, encoding='utf-8')
    result = {**observed(stdout, returncode), 'provider': provider, 'model_requested': profile['model'],
        'effort_requested': profile['reasoning_effort'], 'endpoint': claim['endpoint'], 'returncode': returncode, 'launch_error': launch_error,
        'timed_out': timed_out, 'ended_at': datetime.now(timezone.utc).isoformat(), 'credential_redacted': redacted,
        'request_sha256': hashlib.sha256((root/'request.txt').read_bytes()).hexdigest(),
        'stdout_sha256': hashlib.sha256(stdout.encode()).hexdigest(), 'automatic_retry': False,
        'business_task_run': False, 'model_tool_isolation_qualified': False}
    write_json(root/'result.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    result = run(args.profile, args.output, execute=args.execute)
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(0 if result.get('success', result.get('ready', False)) else 1)

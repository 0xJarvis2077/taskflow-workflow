"""Select new Codex transport defaults without rewriting locked provider code."""

from taskflow.contracts import TaskflowError
from taskflow.execution_codex_provider import provider_settings


def provider_settings_with_retry(config: dict, filesystem: dict) -> tuple[list[str], dict | None]:
    """Keep original provider validation; omit only its two historical retry overrides."""
    settings, provider = provider_settings(config, filesystem)
    if provider is None:
        return settings, None
    omitted = {"model_providers.taskflow_external.request_max_retries=0",
               "model_providers.taskflow_external.stream_max_retries=0"}
    if not all(settings.count(item) == 1 for item in omitted):
        raise TaskflowError("expected original provider retry overrides unavailable")
    settings = [item for item in settings if item not in omitted]
    return settings, {**provider, "automatic_retry_scope":"business_work_redispatch",
                      "transport_retry":"codex_configured_default",
                      "transport_retry_observation":"not explicitly overridden; effective values unobserved"}

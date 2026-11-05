"""Standalone process environment selection for the existing Codex launchers."""
import os


def process_environment(selected: dict | None, *, authentication: str | None = None,
                        role_home: str | None = None) -> dict | None:
    """Select a declared credential source; never persist its value.

    Environment selection alone does not establish OS credential isolation.
    """
    if authentication is not None:
        if authentication != "chatgpt" or selected is not None:
            raise ValueError("explicit ChatGPT launch environment cannot mix provider credentials")
        names = {"PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE", "TZ",
                 "CODEX_HOME", "TMPDIR", "TMPPREFIX", "SYSTEMROOT", "WINDIR",
                 "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
                 "http_proxy", "https_proxy", "all_proxy", "no_proxy",
                 "SSL_CERT_FILE", "SSL_CERT_DIR", "REQUESTS_CA_BUNDLE", "NODE_EXTRA_CA_CERTS"}
        return {key: value for key, value in os.environ.items() if key in names}
    if role_home is not None:
        if selected and selected["codex_home"] != role_home:
            raise ValueError("provider Codex home differs from the declared role home")
        if not selected:
            result = process_environment(None, authentication="chatgpt")
            result.update(CODEX_HOME=role_home)
            return result
    if not selected:
        return None
    core = {"PATH", "LANG", "LC_ALL", "USER", "LOGNAME", "SYSTEMROOT", "WINDIR"}
    result = {key: value for key, value in os.environ.items() if key in core}
    name = selected["credential_env"]
    if os.environ.get(name):
        result[name] = os.environ[name]
    result.update(CODEX_HOME=selected["codex_home"], HOME=selected["codex_home"])
    return result

"""WorkspaceClient factory using Databricks unified authentication."""

from urllib.parse import urlsplit

from databricks.sdk import WorkspaceClient

from dr_sync.exceptions import ConfigurationError


def _validate_host(host):
    if host is None:
        return None
    parsed = urlsplit(host)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ConfigurationError(
            "workspace host must be an HTTPS origin without credentials or a path"
        )
    return host.rstrip("/")


def create_client(host=None, token=None, profile=None):
    """Create a client through the SDK's unified authentication chain.

    Prefer a named CLI profile for interactive use and workload identity
    federation/OAuth for automation. ``token`` exists only for legacy PAT
    migration and must not be combined with ``profile``.
    """
    if token and profile:
        raise ConfigurationError("token and profile are mutually exclusive")

    kwargs = {}
    validated_host = _validate_host(host)
    if validated_host:
        kwargs["host"] = validated_host
    if profile:
        kwargs["profile"] = profile
    elif token:
        kwargs["token"] = token

    return WorkspaceClient(**kwargs)

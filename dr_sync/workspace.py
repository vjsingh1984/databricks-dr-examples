"""WorkspaceClient factory with flexible authentication."""

import os

from databricks.sdk import WorkspaceClient


def create_client(host=None, token=None, profile=None):
    """Create a WorkspaceClient with auth resolution.

    Resolution order: explicit args -> environment variables -> SDK default chain.

    Args:
        host: Workspace URL (e.g. https://adb-xxx.azuredatabricks.net).
        token: Personal access token.
        profile: Databricks CLI profile name.

    Returns:
        Configured WorkspaceClient instance.
    """
    resolved_host = host or os.environ.get("DATABRICKS_HOST")
    resolved_token = token or os.environ.get("DATABRICKS_TOKEN")

    kwargs = {}
    if resolved_host:
        kwargs["host"] = resolved_host
    if resolved_token:
        kwargs["token"] = resolved_token
    if profile:
        kwargs["profile"] = profile

    return WorkspaceClient(**kwargs)

"""Tests for dr_sync.workspace module."""

from unittest.mock import patch


from dr_sync.workspace import create_client


class TestCreateClient:
    """Tests for create_client function."""

    @patch("dr_sync.workspace.WorkspaceClient")
    def test_create_client_with_explicit_args(self, mock_workspace_client):
        """Test client creation with explicit arguments."""
        create_client(host="https://example.com", token="my-token")

        mock_workspace_client.assert_called_once_with(
            host="https://example.com",
            token="my-token",
        )

    @patch("dr_sync.workspace.WorkspaceClient")
    def test_create_client_from_env(self, mock_workspace_client, monkeypatch):
        """Test client creation from environment variables."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://env.example.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

        create_client()

        mock_workspace_client.assert_called_once()

    @patch("dr_sync.workspace.WorkspaceClient")
    def test_create_client_with_profile(self, mock_workspace_client):
        """Test client creation with profile argument."""
        create_client(profile="my-profile")

        mock_workspace_client.assert_called_once_with(profile="my-profile")

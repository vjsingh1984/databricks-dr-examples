import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
SCRIPTS = sorted(ROOT.glob("sync_*.py"))


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda path: path.name)
def test_operational_scripts_use_the_guarded_client_factory(script):
    source = script.read_text(encoding="utf-8")
    assert "WorkspaceClient(" not in source
    assert "create_client(" in source
    assert "profile=config." in source


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda path: path.name)
def test_cli_safety_flags_are_applied_before_workspace_clients(script):
    source = script.read_text(encoding="utf-8")
    assert source.index("configure_runtime(") < source.index("create_client(")
    assert "config.dry_run = args.dry_run" not in source


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda path: path.name)
def test_operational_scripts_are_syntax_valid(script):
    ast.parse(script.read_text(encoding="utf-8"), filename=str(script))


def test_repository_never_places_azure_secret_values_in_mapping_csv():
    header = (ROOT / "data" / "azure_cred_mapping.csv").read_text(encoding="utf-8-sig")
    assert "target_sp_secret_env" in header
    assert "target_sp_secret," not in header

import pandas as pd
import pytest

from dr_sync.csv_mapping import (
    load_mapping,
    lookup_value,
    validate_catalog_mapping,
    validate_cred_mapping,
    validate_ext_location_mapping,
)
from dr_sync.exceptions import MappingError


def test_load_and_lookup_mapping(tmp_path):
    mapping = tmp_path / "mapping.csv"
    mapping.write_text("source_catalog,target_storage_root\none,s3://bucket\n", encoding="utf-8")
    frame = load_mapping(mapping, ["source_catalog", "target_storage_root"])
    assert lookup_value(frame, "source_catalog", "one", "target_storage_root") == "s3://bucket"
    assert lookup_value(frame, "source_catalog", "missing", "target_storage_root") is None


def test_load_mapping_rejects_missing_file_and_columns(tmp_path):
    with pytest.raises(MappingError):
        load_mapping(tmp_path / "missing.csv")
    mapping = tmp_path / "mapping.csv"
    mapping.write_text("present\nvalue\n", encoding="utf-8")
    with pytest.raises(MappingError):
        load_mapping(mapping, ["missing"])


def test_mapping_validators_report_duplicate_and_empty_values(tmp_path):
    catalogs = tmp_path / "catalogs.csv"
    catalogs.write_text(
        "source_catalog,target_storage_root\none,s3://a\none,s3://b\n", encoding="utf-8"
    )
    assert "Duplicate source_catalog" in validate_catalog_mapping(catalogs)[0]

    aws = tmp_path / "aws.csv"
    aws.write_text("source_cred_name,target_iam_role\none,\n", encoding="utf-8")
    assert "Empty target_iam_role" in validate_cred_mapping(aws, "aws")[0]

    locations = tmp_path / "locations.csv"
    locations.write_text("source_loc_name,target_url\none,\n", encoding="utf-8")
    assert "Empty target_url" in validate_ext_location_mapping(locations)[0]


def test_azure_validator_accepts_managed_identity_mapping(tmp_path):
    mapping = tmp_path / "azure.csv"
    mapping.write_text(
        "source_cred_name,target_mgd_id_connector,target_mgd_id_identity,"
        "target_sp_directory,target_sp_appid,target_sp_secret_env\n"
        "one,/subscriptions/connector,,,,\n",
        encoding="utf-8",
    )
    assert validate_cred_mapping(mapping, "azure") == []
    assert isinstance(load_mapping(mapping), pd.DataFrame)


def test_azure_validator_rejects_secret_values_and_ambiguous_methods(tmp_path):
    mapping = tmp_path / "azure.csv"
    mapping.write_text(
        "source_cred_name,target_mgd_id_connector,target_mgd_id_identity,"
        "target_sp_directory,target_sp_appid,target_sp_secret_env\n"
        "one,/subscriptions/connector,,directory,app,SECRET_ENV\n"
        "two,,identity,,,\n",
        encoding="utf-8",
    )
    errors = validate_cred_mapping(mapping, "azure")
    assert any("exactly one" in error for error in errors)
    assert any("requires target_mgd_id_connector" in error for error in errors)

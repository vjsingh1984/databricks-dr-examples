"""Tests for dr_sync.csv_mapping module."""

import pytest

from dr_sync.csv_mapping import (
    load_mapping,
    lookup_value,
    validate_catalog_mapping,
    validate_cred_mapping,
    validate_ext_location_mapping,
)
from dr_sync.exceptions import MappingError


class TestLoadMapping:
    """Tests for load_mapping function."""

    def test_load_mapping_success(self, temp_csv_file):
        """Test successful CSV loading."""
        df = load_mapping(temp_csv_file)

        assert len(df) == 2
        assert list(df.columns) == ["source", "target"]
        assert df["source"].tolist() == ["src1", "src2"]
        assert df["target"].tolist() == ["tgt1", "tgt2"]

    def test_load_mapping_file_not_found(self):
        """Test error when file doesn't exist."""
        with pytest.raises(MappingError, match="not found"):
            load_mapping("nonexistent.csv")

    def test_load_mapping_missing_columns(self, temp_csv_file):
        """Test error when required columns are missing."""
        with pytest.raises(MappingError, match="(?i)missing required columns"):
            load_mapping(temp_csv_file, required_columns=["source", "target", "extra"])


class TestLookupValue:
    """Tests for lookup_value function."""

    def test_lookup_value_found(self, temp_csv_file):
        """Test successful lookup."""
        df = load_mapping(temp_csv_file)
        result = lookup_value(df, "source", "src1", "target")

        assert result == "tgt1"

    def test_lookup_value_not_found(self, temp_csv_file):
        """Test lookup when key doesn't exist."""
        df = load_mapping(temp_csv_file)
        result = lookup_value(df, "source", "nonexistent", "target")

        assert result is None


class TestValidateCatalogMapping:
    """Tests for validate_catalog_mapping function."""

    def test_validate_catalog_mapping_success(self, tmp_path):
        """Test validation with valid catalog mapping."""
        mapping_file = tmp_path / "catalog_mapping.csv"
        mapping_file.write_text(
            "source_catalog,target_storage_root\ncat1,s3://bucket/cat1\ncat2,s3://bucket/cat2\n"
        )

        errors = validate_catalog_mapping(str(mapping_file))
        assert errors == []

    def test_validate_catalog_mapping_missing_columns(self, tmp_path):
        """Test error when required columns are missing."""
        mapping_file = tmp_path / "catalog_mapping.csv"
        mapping_file.write_text("source_catalog\ncat1\n")

        with pytest.raises(MappingError, match="(?i)missing required columns"):
            validate_catalog_mapping(str(mapping_file))

    def test_validate_catalog_mapping_duplicates(self, tmp_path):
        """Test error when duplicate source_catalog entries exist."""
        mapping_file = tmp_path / "catalog_mapping.csv"
        mapping_file.write_text(
            "source_catalog,target_storage_root\ncat1,s3://bucket/cat1\ncat1,s3://bucket/cat1-duplicate\n"
        )

        errors = validate_catalog_mapping(str(mapping_file))
        assert len(errors) == 1
        assert "duplicate" in errors[0].lower()


class TestValidateCredMapping:
    """Tests for validate_cred_mapping function."""

    def test_validate_aws_cred_mapping_success(self, tmp_path):
        """Test validation with valid AWS credential mapping."""
        mapping_file = tmp_path / "cred_mapping.csv"
        mapping_file.write_text(
            "source_cred_name,target_iam_role\ncred1,arn:aws:iam::123456789012:role/MyRole\n"
        )

        errors = validate_cred_mapping(str(mapping_file), "aws")
        assert errors == []

    def test_validate_aws_cred_mapping_empty_iam_role(self, tmp_path):
        """Test error when target_iam_role is empty."""
        mapping_file = tmp_path / "cred_mapping.csv"
        mapping_file.write_text("source_cred_name,target_iam_role\ncred1,\n")

        errors = validate_cred_mapping(str(mapping_file), "aws")
        assert len(errors) == 1
        assert "iam_role" in errors[0].lower()

    def test_validate_azure_cred_mapping_success(self, tmp_path):
        """Test validation with valid Azure credential mapping."""
        mapping_file = tmp_path / "cred_mapping.csv"
        mapping_file.write_text(
            "source_cred_name,target_mgd_id_connector\ncred1,/subscriptions/xxx/resourceGroups/yyy/providers/xxx\n"
        )

        errors = validate_cred_mapping(str(mapping_file), "azure")
        assert errors == []


class TestValidateExtLocationMapping:
    """Tests for validate_ext_location_mapping function."""

    def test_validate_ext_location_mapping_success(self, tmp_path):
        """Test validation with valid external location mapping."""
        mapping_file = tmp_path / "ext_location_mapping.csv"
        mapping_file.write_text("source_loc_name,target_url\nloc1,s3://bucket/path\n")

        errors = validate_ext_location_mapping(str(mapping_file))
        assert errors == []

    def test_validate_ext_location_mapping_missing_columns(self, tmp_path):
        """Test error when required columns are missing."""
        mapping_file = tmp_path / "ext_location_mapping.csv"
        mapping_file.write_text("source_loc_name\nloc1\n")

        with pytest.raises(MappingError, match="(?i)missing required columns"):
            validate_ext_location_mapping(str(mapping_file))

    def test_validate_ext_location_mapping_empty_url(self, tmp_path):
        """Test error when target_url is empty."""
        mapping_file = tmp_path / "ext_location_mapping.csv"
        mapping_file.write_text("source_loc_name,target_url\nloc1,\n")

        errors = validate_ext_location_mapping(str(mapping_file))
        assert len(errors) == 1
        assert "target_url" in errors[0].lower()

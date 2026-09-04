"""CSV mapping file loading and lookup utilities."""

import logging
import os

import pandas as pd

from dr_sync.exceptions import MappingError

logger = logging.getLogger("dr_sync")


def load_mapping(filepath, required_columns=None):
    """Load a CSV mapping file with validation.

    Args:
        filepath: Path to the CSV file.
        required_columns: Optional list of column names that must be present.

    Returns:
        pandas DataFrame with the mapping data.

    Raises:
        MappingError: If the file doesn't exist or required columns are missing.
    """
    if not os.path.exists(filepath):
        raise MappingError(filepath, "", "", f"Mapping file not found: {filepath}")

    df = pd.read_csv(filepath, keep_default_na=False)

    if required_columns:
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise MappingError(
                filepath, "", "", f"Missing required columns in {filepath}: {missing}"
            )

    return df


def lookup_value(df, key_col, key_val, value_col):
    """Safe lookup of a single value from a mapping DataFrame.

    Args:
        df: pandas DataFrame to search.
        key_col: Column name to match against.
        key_val: Value to search for.
        value_col: Column name to return.

    Returns:
        The matched value, or None if no match found.
    """
    matches = df[value_col].loc[df[key_col] == key_val]
    if matches.empty:
        return None
    return matches.iloc[0]


def validate_catalog_mapping(filepath):
    """Validate catalog mapping CSV for duplicates and valid storage roots.

    Returns:
        List of error strings (empty = valid).
    """
    errors = []
    df = load_mapping(filepath, required_columns=["source_catalog", "target_storage_root"])
    dupes = df[df.duplicated(subset=["source_catalog"], keep=False)]
    if not dupes.empty:
        dupe_names = dupes["source_catalog"].unique().tolist()
        errors.append(f"Duplicate source_catalog entries: {dupe_names}")
    return errors


def validate_cred_mapping(filepath, cloud_type):
    """Validate credential mapping CSV has cloud-specific required columns.

    Returns:
        List of error strings (empty = valid).
    """
    errors = []
    if cloud_type == "aws":
        df = load_mapping(filepath, required_columns=["source_cred_name", "target_iam_role"])
        empty_roles = df[df["target_iam_role"] == ""]
        if not empty_roles.empty:
            names = empty_roles["source_cred_name"].tolist()
            errors.append(f"Empty target_iam_role for credentials: {names}")
    elif cloud_type == "azure":
        df = load_mapping(filepath, required_columns=["source_cred_name"])
        auth_columns = {
            "target_mgd_id_connector",
            "target_mgd_id_identity",
            "target_sp_directory",
            "target_sp_appid",
            "target_sp_secret_env",
        }
        if not auth_columns.issubset(df.columns):
            errors.append(
                "Azure mappings must use the managed-identity or secret-environment columns"
            )
        else:
            for row in df.to_dict(orient="records"):
                connector = row["target_mgd_id_connector"]
                identity = row["target_mgd_id_identity"]
                sp_values = [
                    row["target_sp_directory"],
                    row["target_sp_appid"],
                    row["target_sp_secret_env"],
                ]
                uses_managed_identity = bool(connector)
                uses_service_principal = any(sp_values)
                if identity and not connector:
                    errors.append(
                        f"{row['source_cred_name']}: target_mgd_id_identity requires "
                        "target_mgd_id_connector"
                    )
                if uses_managed_identity == uses_service_principal:
                    errors.append(
                        f"{row['source_cred_name']}: configure exactly one Azure credential method"
                    )
                elif uses_service_principal and not all(sp_values):
                    errors.append(
                        f"{row['source_cred_name']}: service-principal fields must all be set"
                    )
    return errors


def validate_ext_location_mapping(filepath):
    """Validate external location mapping CSV for non-empty URLs.

    Returns:
        List of error strings (empty = valid).
    """
    errors = []
    df = load_mapping(filepath, required_columns=["source_loc_name", "target_url"])
    empty_urls = df[df["target_url"] == ""]
    if not empty_urls.empty:
        names = empty_urls["source_loc_name"].tolist()
        errors.append(f"Empty target_url for locations: {names}")
    return errors

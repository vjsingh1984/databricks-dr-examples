"""CSV mapping file loading and lookup utilities."""

import os

import pandas as pd

from dr_sync.exceptions import MappingError


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
                filepath, "", "",
                f"Missing required columns in {filepath}: {missing}"
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

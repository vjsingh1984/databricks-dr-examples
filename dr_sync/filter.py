"""Filtering utilities for selective sync operations."""

import fnmatch
from typing import List, Optional


class ResourceFilter:
    """Filter resources based on include/exclude glob patterns.

    Patterns use Unix shell-style wildcards:
        * matches everything
        ? matches any single character
        [seq] matches any character in seq
        [!seq] matches any character not in seq

    For three-part names (catalog.schema.table):
        - "cat.*.tbl_*" matches tbl_* tables in any schema under cat catalog
        - "*.prod.*" matches prod schemas in any catalog
    """

    def __init__(
        self,
        include_patterns: Optional[List[str]] = None,
        exclude_patterns: Optional[List[str]] = None,
    ):
        """Initialize resource filter.

        Args:
            include_patterns: List of glob patterns to include (None = include all).
            exclude_patterns: List of glob patterns to exclude (None = exclude none).
        """
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []

    def matches(self, resource_name: str, parts: int = 3) -> bool:
        """Check if resource name matches filter criteria.

        Args:
            resource_name: Full resource name (e.g., "catalog.schema.table").
            parts: Expected number of dot-separated parts (default: 3).

        Returns:
            True if resource should be included, False otherwise.
        """
        # Check exclude patterns first (take precedence)
        for pattern in self.exclude_patterns:
            if self._pattern_matches(pattern, resource_name, parts):
                return False

        # If no include patterns, include everything not excluded
        if not self.include_patterns:
            return True

        # Check include patterns
        for pattern in self.include_patterns:
            if self._pattern_matches(pattern, resource_name, parts):
                return True

        return False

    def _pattern_matches(self, pattern: str, name: str, expected_parts: int) -> bool:
        """Check if a glob pattern matches a resource name.

        Args:
            pattern: Glob pattern (may contain wildcards).
            name: Resource name to match against.
            expected_parts: Expected number of dot-separated parts.

        Returns:
            True if pattern matches name.
        """
        name_parts = name.split(".")

        # Handle multi-part patterns (e.g., "cat.*.tbl_*")
        if "." in pattern:
            pattern_parts = pattern.split(".")

            # If pattern has different number of parts, it can't match
            if len(pattern_parts) != expected_parts or len(name_parts) != expected_parts:
                return False

            # Match each part individually
            for pattern_part, name_part in zip(pattern_parts, name_parts):
                if not fnmatch.fnmatch(name_part, pattern_part):
                    return False
            return True
        else:
            # Single-part pattern matches against full name
            return fnmatch.fnmatch(name, pattern)

    def filter_names(self, names: List[str], parts: int = 3) -> List[str]:
        """Filter a list of resource names.

        Args:
            names: List of resource names.
            parts: Expected number of dot-separated parts.

        Returns:
            Filtered list of names.
        """
        return [name for name in names if self.matches(name, parts)]

    def filter_dicts(self, items: List[dict], name_key: str = "name", parts: int = 3) -> List[dict]:
        """Filter a list of dictionaries by name field.

        Args:
            items: List of dictionaries with name field.
            name_key: Key containing the resource name.
            parts: Expected number of dot-separated parts.

        Returns:
            Filtered list of dictionaries.
        """
        return [item for item in items if self.matches(item[name_key], parts)]


def parse_filter_args(
    include: Optional[str] = None,
    exclude: Optional[str] = None,
) -> ResourceFilter:
    """Parse comma-separated filter arguments into ResourceFilter.

    Args:
        include: Comma-separated include patterns.
        exclude: Comma-separated exclude patterns.

    Returns:
        ResourceFilter instance.
    """
    include_patterns = include.split(",") if include else None
    exclude_patterns = exclude.split(",") if exclude else None

    return ResourceFilter(include_patterns, exclude_patterns)

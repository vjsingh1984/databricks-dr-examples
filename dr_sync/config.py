"""Configuration management for DR sync scripts."""

import os
from dataclasses import dataclass, field
from typing import List

from dr_sync.exceptions import ConfigurationError


@dataclass
class DRSyncConfig:
    """Central configuration for all DR sync scripts.

    Can be populated from common.py (backward compat) or environment variables.
    """

    # Cloud and workspace settings
    cloud_type: str = "azure"
    source_host: str = ""
    source_token: str = ""
    target_host: str = ""
    target_token: str = ""

    # Catalogs
    catalogs_to_copy: List[str] = field(default_factory=list)

    # Mapping file paths
    cred_mapping_file: str = "data/azure_cred_mapping.csv"
    loc_mapping_file: str = "data/ext_location_mapping.csv"
    catalog_mapping_file: str = "data/catalog_mapping.csv"
    schema_mapping_file: str = "data/schema_mapping.csv"

    # Execution settings
    landing_zone_url: str = ""
    num_exec: int = 4
    warehouse_size: str = "Small"
    response_backoff: float = 0.5
    metastore_id: str = ""
    manifest_name: str = "manifest"

    # Runtime flags
    dry_run: bool = False

    @classmethod
    def from_common_module(cls):
        """Create config by importing from common.py (backward compatible)."""
        try:
            import common
        except ImportError:
            raise ConfigurationError(
                "common.py not found. Please create it or use environment variables."
            )

        kwargs = {}
        field_map = {
            "cloud_type": "cloud_type",
            "source_host": "source_host",
            "source_token": "source_pat",
            "target_host": "target_host",
            "target_token": "target_pat",
            "catalogs_to_copy": "catalogs_to_copy",
            "cred_mapping_file": "cred_mapping_file",
            "loc_mapping_file": "loc_mapping_file",
            "catalog_mapping_file": "catalog_mapping_file",
            "schema_mapping_file": "schema_mapping_file",
            "landing_zone_url": "landing_zone_url",
            "num_exec": "num_exec",
            "warehouse_size": "warehouse_size",
            "response_backoff": "response_backoff",
            "metastore_id": "metastore_id",
            "manifest_name": "manifest_name",
        }

        for config_key, common_key in field_map.items():
            if hasattr(common, common_key):
                kwargs[config_key] = getattr(common, common_key)

        return cls(**kwargs)

    @classmethod
    def from_env(cls):
        """Create config from DR_SYNC_* environment variables."""

        def get(name, default=""):
            return os.environ.get(f"DR_SYNC_{name}", default)

        catalogs = get("CATALOGS_TO_COPY", "")
        catalog_list = (
            [c.strip() for c in catalogs.split(",") if c.strip()] if catalogs else []
        )

        return cls(
            cloud_type=get("CLOUD_TYPE", "azure"),
            source_host=get("SOURCE_HOST"),
            source_token=get("SOURCE_TOKEN"),
            target_host=get("TARGET_HOST"),
            target_token=get("TARGET_TOKEN"),
            catalogs_to_copy=catalog_list,
            cred_mapping_file=get("CRED_MAPPING_FILE", "data/azure_cred_mapping.csv"),
            loc_mapping_file=get("LOC_MAPPING_FILE", "data/ext_location_mapping.csv"),
            catalog_mapping_file=get(
                "CATALOG_MAPPING_FILE", "data/catalog_mapping.csv"
            ),
            schema_mapping_file=get("SCHEMA_MAPPING_FILE", "data/schema_mapping.csv"),
            landing_zone_url=get("LANDING_ZONE_URL"),
            num_exec=int(get("NUM_EXEC", "4")),
            warehouse_size=get("WAREHOUSE_SIZE", "Small"),
            response_backoff=float(get("RESPONSE_BACKOFF", "0.5")),
            metastore_id=get("METASTORE_ID"),
            manifest_name=get("MANIFEST_NAME", "manifest"),
            dry_run=get("DRY_RUN", "false").lower() in ("true", "1", "yes"),
        )

    def validate(self) -> List[str]:
        """Validate configuration and return list of errors (empty = valid)."""
        errors = []

        if not self.target_host:
            errors.append("target_host is required")
        if not self.target_token:
            errors.append("target_token is required")
        if not self.catalogs_to_copy:
            errors.append("catalogs_to_copy must not be empty")
        if self.cloud_type not in ("aws", "azure", "gcp"):
            errors.append(
                f"cloud_type must be one of aws, azure, gcp (got {self.cloud_type!r})"
            )
        if self.num_exec < 1:
            errors.append(f"num_exec must be >= 1 (got {self.num_exec})")

        return errors

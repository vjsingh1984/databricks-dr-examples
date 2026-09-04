# Azure Usage Guide

## Overview
This guide explains the Azure-specific configuration for the disaster-recovery examples.
Use a Databricks access connector with managed identity when possible; the service-principal
fields are retained only for legacy environments.

The tool reads `DR_SYNC_*` environment variables when any are present and otherwise falls back
to `common.py`. The scripts do not load `.env` automatically; `.env.example` is a template for
your shell, CI platform, or secret manager.

### Core Settings
- Cloud Platform: Azure
- Source Workspace: e.g. `https://adb-3960987466318833.13.azuredatabricks.net`
- Target Workspace: e.g. `https://adb-1586766716853906.6.azuredatabricks.net`
- Metastore ID: e.g. 7215a9fc-0933-4efd-b718-c2fd8ac512b9

### Mapping Files
- Credential Mapping: `data/azure_cred_mapping.csv`
- External Location Mapping: `data/ext_location_mapping.csv` 
- Catalog Mapping: `data/catalog_mapping.csv`
- Schema Mapping: `data/schema_mapping.csv`


## Populating azure_cred_mapping.csv

This file maps Unity Catalog Credentials

- source_cred_name: name of UC credential you are copying
- `target_mgd_id_connector`: Azure Databricks access-connector resource ID (preferred)
- `target_mgd_id_identity`: optional user-assigned managed-identity resource ID paired with
  the access connector
- `target_sp_directory`: legacy service-principal directory/tenant ID
- `target_sp_appid`: legacy service-principal application ID
- `target_sp_secret_env`: name of an environment variable containing the legacy client secret

Never place the client-secret value in this CSV. Export the variable named by
`target_sp_secret_env` from a protected secret store immediately before execution.

![Ext Location Mapping](images/screenshot_azure_credential_mapping.png)
*Example mapping file for UC Credentials*


![Connector ID for Managed Identity](images/underlined-connector-id-azure.png)
*Example of where to find the access connector resource ID (`target_mgd_id_connector`).*


## Populating ext_location_mapping.csv

- `source_loc_name`: Unity Catalog external-location name to copy
- `target_url`: Azure storage URL for the target location
- `target_access_pt`: unused for Azure

![Ext Location Mapping](images/screenshot_azure_ext_loc_mapping.png)

## Populating catalog_mapping.csv

![Catalog Mapping](images/screenshot_catalog_mapping.png)


## Populating schema_mapping.csv

![Catalog Mapping](images/screenshot_schema_mapping.png)




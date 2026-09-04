# common.py
#
# contains all variables/settings to be used in other scripts

cloud_type = "azure"  # cloud where primary/secondary metastores exist
cred_mapping_file = "data/azure_cred_mapping.csv"  # path to credential mapping file
loc_mapping_file = "data/ext_location_mapping.csv"  # path to locations mapping file
catalog_mapping_file = "data/catalog_mapping.csv"  # path to catalog mapping file
schema_mapping_file = "data/schema_mapping.csv"  # path to schema mapping file
source_host = ""  # optional when the SOURCE profile supplies the host
source_profile = "SOURCE"  # Databricks CLI profile using unified authentication
source_pat = ""  # legacy-only; prefer OAuth/WIF or a CLI profile
target_host = ""  # optional when the TARGET profile supplies the host
target_profile = "TARGET"  # Databricks CLI profile using unified authentication
target_pat = ""  # legacy-only; prefer OAuth/WIF or a CLI profile
catalogs_to_copy = ["my-catalog1", "my-catalog2"]  # list of catalogs to replicate
landing_zone_url = "path/to/storage/"  # if using sync_tables, intermediate storage location
num_exec = 4  # number of parallel threads to execute
warehouse_size = "Small"  # serverless warehouse size in target WS
response_backoff = 0.5  # polling backoff for checking query status
metastore_id = "<secondary-metastore-id>"  # global metastore ID for secondary metastore
manifest_name = "manifest"  # name of the manifest file, if written

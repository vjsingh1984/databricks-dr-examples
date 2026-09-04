# sync_creds_and_locs.py
#
# This is a minimal script to sync storage credentials and external locations between two workspaces.
# To run it, you must first fill out the appropriate mapping files:
#   - <cloud>_cred_mapping.csv: contains the required mapping parameters to translate a credential between two regions.
#     Note that this varies by cloud. For AWS, this is simply the IAM role ARN. For Azure, you will need the SP OR
#     Managed Identity info. Note that for Azure, only ONE of the following should be provided:
#       + target_mgd_id_connector (if using a standard access connector)
#       + target_mgd_id_identity (if using a user-assigned identity)
#       + target_sp fields (if using a Service Principal)
#   - ext_location_mapping.csv: maps the target URL between source and target. For AWS, also has an option to provide
#     an S3 access point for the target region.
#
# Also make sure to set cloud_type to the appropriate choice between aws, azure and gcp.
#
# The script will compare the source and target metastores to first get any storage creds that exist in the source but
# not the target, and then will attempt to create those credentials in the target. It will then do the same for the
# external locations in the source/target. Note that all comparisons here are done on the *name* of the objects; this is
# necessary since all other parameters will change when switching between metastores.
#
# Workspace clients use Databricks unified authentication. Prefer workload
# identity federation in automation and named CLI profiles for local use.

import os

from databricks.sdk.service import catalog

from dr_sync.cli import configure_runtime
from dr_sync.config import DRSyncConfig
from dr_sync.csv_mapping import load_mapping
from dr_sync.log import setup_logging
from dr_sync.workspace import create_client

config = DRSyncConfig.load()
logger = (
    configure_runtime(config, "Sync storage credentials and external locations between workspaces")
    if __name__ == "__main__"
    else setup_logging()
)
target_host = config.target_host
target_pat = config.target_token
source_host = config.source_host
source_pat = config.source_token
cred_mapping_file = config.cred_mapping_file
loc_mapping_file = config.loc_mapping_file
cloud_type = config.cloud_type

# create WorkspaceClient objects
w_source = create_client(host=source_host, token=source_pat, profile=config.source_profile)
w_target = create_client(host=target_host, token=target_pat, profile=config.target_profile)

# get source and target credentials and external locations
source_creds = list(w_source.storage_credentials.list())
target_creds = list(w_target.storage_credentials.list())
source_extloc = list(w_source.external_locations.list())
target_extloc = list(w_target.external_locations.list())

# compare source and target storage credentials
# we can only do this by name since the URL and IDs will change between workspaces
source_cred_names = [x.name for x in source_creds]
target_cred_names = [x.name for x in target_creds]
cred_diff = list(set(source_cred_names) - set(target_cred_names))
creds_to_create = [x for x in source_creds if x.name in cred_diff]
cred_df = load_mapping(cred_mapping_file)
cred_lookup = cred_df.set_index("source_cred_name").to_dict("index")

if not creds_to_create:
    logger.info("All source credentials exist in target metastore.")

for cred in creds_to_create:
    # get parameters that map directly between creds
    cred_name = cred.name
    cred_read_only = cred.read_only
    cred_comment = cred.comment
    logger.info("Creating storage credential %s...", cred_name)

    if cloud_type == "aws":
        # get cred IAM role based off of name
        row = cred_lookup.get(cred_name)
        if row is None:
            logger.error("Could not create credential %s. Please check mapping file.", cred_name)
            continue
        iam_role_arn = row["target_iam_role"]

        # create storage credential in target WS
        cred_iam_role = catalog.AwsIamRole(role_arn=iam_role_arn)
        if config.dry_run:
            logger.info("[DRY RUN] Would create credential %s", cred_name)
            continue
        w_target.storage_credentials.create(
            name=cred_name,
            read_only=cred_read_only,
            comment=cred_comment,
            aws_iam_role=cred_iam_role,
        )
    elif cloud_type == "azure":
        # get SP and Mgd ID info based off of name
        row = cred_lookup.get(cred_name)
        if row is None:
            logger.error("Could not create credential %s. Please check mapping file.", cred_name)
            continue
        managed_id_connector = row.get("target_mgd_id_connector", "")
        managed_id_identity = row.get("target_mgd_id_identity", "")
        sp_directory = row.get("target_sp_directory", "")
        sp_appid = row.get("target_sp_appid", "")
        sp_secret_env = row.get("target_sp_secret_env", "")

        if not managed_id_connector and not sp_directory:
            logger.error("Could not create credential %s. Please check mapping file.", cred_name)
            continue

        # create storage credential in target WS
        if config.dry_run:
            logger.info("[DRY RUN] Would create credential %s", cred_name)
            continue
        if managed_id_connector:
            cred_mgd_id = catalog.AzureManagedIdentityRequest(
                access_connector_id=managed_id_connector,
                managed_identity_id=managed_id_identity or None,
            )
            w_target.storage_credentials.create(
                name=cred_name,
                read_only=cred_read_only,
                comment=cred_comment,
                azure_managed_identity=cred_mgd_id,
            )
        else:
            try:
                sp_secret = os.environ[sp_secret_env]
                cred_sp = catalog.AzureServicePrincipal(
                    directory_id=sp_directory,
                    application_id=sp_appid,
                    client_secret=sp_secret,
                )
                w_target.storage_credentials.create(
                    name=cred_name,
                    read_only=cred_read_only,
                    comment=cred_comment,
                    azure_service_principal=cred_sp,
                )
            except (KeyError, ValueError) as exc:
                logger.error(
                    "Could not create credential %s. Please make sure that only one of "
                    "managed identity or service-principal configuration is provided and "
                    "that the named secret environment variable exists: %s",
                    cred_name,
                    exc,
                )

    elif cloud_type == "gcp":
        logger.warning("GCP not yet implemented.")
        continue
    else:
        logger.error("Cloud type must be one of AWS, GCP, or Azure.")
        continue

    logger.info("Created storage credential %s.", cred_name)

# compare source and target external locations
# we can only do this by name since the URL and IDs will change between workspaces
source_extloc_names = [x.name for x in source_extloc]
target_extloc_names = [x.name for x in target_extloc]
loc_diff = list(set(source_extloc_names) - set(target_extloc_names))
locs_to_create = [x for x in source_extloc if x.name in loc_diff]
loc_df = load_mapping(loc_mapping_file)
loc_lookup = loc_df.set_index("source_loc_name").to_dict("index")

if not locs_to_create:
    logger.info("All source external locations exist in target metastore.")

for loc in locs_to_create:
    # get parameters that map directly between creds
    loc_name = loc.name
    loc_cred_name = loc.credential_name
    loc_comment = loc.comment
    loc_fallback = loc.fallback
    loc_read_only = loc.read_only
    logger.info("Creating external location %s...", loc_name)

    if cloud_type == "aws":
        row = loc_lookup.get(loc_name)
        if row is None:
            logger.error("Could not create location %s. Please check mapping file.", loc_name)
            continue
        url = row["target_url"]
        access_pt = row.get("target_access_pt", "")

        if url is None:
            logger.error("Could not create location %s. Please check mapping file.", loc_name)
            continue

        if config.dry_run:
            logger.info("[DRY RUN] Would create external location %s", loc_name)
            continue

        if access_pt:
            w_target.external_locations.create(
                name=loc_name,
                credential_name=loc_cred_name,
                comment=loc_comment,
                fallback=loc_fallback,
                read_only=loc_read_only,
                url=url,
                access_point=access_pt,
            )
        else:
            w_target.external_locations.create(
                name=loc_name,
                credential_name=loc_cred_name,
                comment=loc_comment,
                fallback=loc_fallback,
                read_only=loc_read_only,
                url=url,
            )
    elif cloud_type == "azure":
        row = loc_lookup.get(loc_name)
        if row is None:
            logger.error("Could not create location %s. Please check mapping file.", loc_name)
            continue
        url = row["target_url"]

        if url is None:
            logger.error("Could not create location %s. Please check mapping file.", loc_name)
            continue

        if config.dry_run:
            logger.info("[DRY RUN] Would create external location %s", loc_name)
            continue

        w_target.external_locations.create(
            name=loc_name,
            credential_name=loc_cred_name,
            comment=loc_comment,
            fallback=loc_fallback,
            read_only=loc_read_only,
            url=url,
        )
    elif cloud_type == "gcp":
        logger.warning("GCP not yet implemented.")
        continue
    else:
        logger.error("Cloud type must be one of AWS, GCP, or Azure.")
        continue

    logger.info("External location %s created.", loc_name)

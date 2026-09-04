# Databricks disaster-recovery examples

Tested Python examples for copying selected Unity Catalog metadata, grants, and data from a
primary Databricks workspace to a secondary workspace. This fork is based on
`gregwood-db/databricks-dr-examples` and adds unified authentication, validation, dry-run
controls, packaging, and CI coverage.

These are baseline examples, not a complete recovery product. Validate generated resources,
data freshness, grants, networking, regional service availability, and an actual failover and
failback procedure before relying on them for a recovery objective.

## Supported runtime and authentication

- Python 3.10, 3.11, 3.12, or 3.13
- `databricks-sdk==0.123.0`
- HTTPS workspace origins only
- Databricks SDK unified authentication

For local use, configure distinct `SOURCE` and `TARGET` Databricks CLI profiles. For CI,
prefer [workload identity federation](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-provider),
including [GitHub OIDC](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github), instead of
long-lived personal access tokens. A workflow-identity service principal used by a script must
be assigned to both workspaces with only the Unity Catalog and workspace privileges required by
that operation. If the workspaces require different identities, use distinct CLI profiles or
extend the client factory; the current environment-based unified-auth path supplies one identity
to both clients.

`DR_SYNC_SOURCE_TOKEN` and `DR_SYNC_TARGET_TOKEN` remain migration-only PAT fallbacks. Inject
them at runtime, never commit them, and never place secrets in mapping CSV files.

## Execution boundaries

The following scripts use workspace REST APIs and can run from a local machine or CI runner with
network access to both workspace front ends:

- `sync_creds_and_locs.py`
- `sync_catalogs_and_schemas.py`
- `sync_uc_models.py`

The following scripts also reference the active `spark` session and are intended for a
Databricks notebook/job, or for a correctly configured Databricks Connect environment:

- `sync_tables.py`
- `sync_grs_ext.py`
- `sync_ext_volumes.py`
- `sync_perms.py`
- `sync_shared_tables.py`
- `sync_views.py`
- the scripts under `examples/`

See the [Databricks Connect guide](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html)
if you need to supply that Spark session remotely.

## Install

```bash
git clone https://github.com/vjsingh1984/databricks-dr-examples.git
cd databricks-dr-examples
python -m venv .venv
source .venv/bin/activate
python -m pip install .
```

`.env.example` is a documented template; the scripts do not parse `.env` files automatically.
Export the selected `DR_SYNC_*` variables with your shell, CI platform, or secret manager, or
configure the backward-compatible non-secret defaults in `common.py`.

Setting any `DR_SYNC_*` variable selects environment configuration instead of `common.py`.
Important variables include:

| Variable | Meaning |
| --- | --- |
| `DR_SYNC_CLOUD_TYPE` | `aws`, `azure`, or `gcp` |
| `DR_SYNC_SOURCE_PROFILE`, `DR_SYNC_TARGET_PROFILE` | Distinct local unified-auth profiles |
| `DR_SYNC_SOURCE_HOST`, `DR_SYNC_TARGET_HOST` | HTTPS workspace origins; required with PAT fallback |
| `DR_SYNC_CATALOGS_TO_COPY` | Comma-separated catalog names; must not be empty |
| `DR_SYNC_CRED_MAPPING_FILE` | Credential mapping CSV |
| `DR_SYNC_LOC_MAPPING_FILE` | External-location mapping CSV |
| `DR_SYNC_CATALOG_MAPPING_FILE` | Catalog mapping CSV |
| `DR_SYNC_SCHEMA_MAPPING_FILE` | Schema mapping CSV |
| `DR_SYNC_LANDING_ZONE_URL` | Intermediate storage for deep-clone workflows |
| `DR_SYNC_METASTORE_ID` | Target metastore ID |
| `DR_SYNC_NUM_EXEC` | Worker count, at least 1 |
| `DR_SYNC_DRY_RUN` | `true`, `1`, or `yes` to suppress mutations |

Run every operation in dry-run mode first:

```bash
export DR_SYNC_SOURCE_PROFILE=SOURCE
export DR_SYNC_TARGET_PROFILE=TARGET
export DR_SYNC_CATALOGS_TO_COPY=my-catalog1,my-catalog2
python sync_catalogs_and_schemas.py --dry-run
```

All top-level sync scripts parse `--dry-run` and `--log-level` before workspace operations.
Review the plan, mappings, target identities, and storage paths before removing `--dry-run`.

## Mapping files

### Storage credentials and external locations

`data/<cloud>_cred_mapping.csv` maps a source credential to a target cloud identity:

- AWS: `target_iam_role` is the target IAM role ARN.
- Azure managed identity: `target_mgd_id_connector` is the Databricks access connector resource
  ID; `target_mgd_id_identity` is the optional user-assigned managed-identity resource ID.
- Azure legacy service principal: `target_sp_directory`, `target_sp_appid`, and
  `target_sp_secret_env`; the final field is the name of an environment variable containing the
  secret, not the secret itself.
- GCP: use the target service-account fields shown in `data/gcp_cred_mapping.csv`.

`data/ext_location_mapping.csv` maps `source_loc_name` to `target_url`; `target_access_pt` is an
optional AWS access point.

### Catalogs and schemas

- `data/catalog_mapping.csv` maps `source_catalog` to `target_storage_root`.
- `data/schema_mapping.csv` maps `source_catalog` and `source_schema` to
  `target_storage_root`.

Treat mapping files as non-secret configuration. Validate identifiers and storage ownership in
the target region before execution.

## Operation guide

| Goal | Script | Important prerequisite |
| --- | --- | --- |
| Credentials and external locations | `sync_creds_and_locs.py` | Target cloud identities exist |
| Catalogs and schemas | `sync_catalogs_and_schemas.py` | Credentials/locations are ready |
| Managed tables via landing storage | `sync_tables.py` | Spark plus target landing zone |
| External table metadata | `sync_grs_ext.py` | Cloud data replication is complete |
| External volume metadata | `sync_ext_volumes.py` | Cloud data replication is complete |
| Views | `sync_views.py` | Referenced objects exist in target |
| Grants | `sync_perms.py` | Principals and securables exist in target |
| Managed tables via Delta Sharing | `sync_shared_tables.py` | Sharing and Spark are configured |
| Unity Catalog models | `sync_uc_models.py` | Model dependencies exist in target |

The account or service principal needs only the privileges required by the selected operation.
Common examples include `CREATE CATALOG`, `CREATE EXTERNAL LOCATION`, and
`CREATE STORAGE CREDENTIAL`; do not grant broad metastore administration by default.

## Development and verification

The lock file is hash-pinned for repeatable CI installs. To reproduce the gates:

```bash
python -m pip install --require-hashes -r requirements-ci.txt
ruff check .
ruff format --check .
bandit -q -r dr_sync *.py examples/*.py
pytest --cov=dr_sync --cov-branch --cov-fail-under=80
python -m build
```

CI tests Python 3.10, 3.12, and 3.13, audits dependencies, builds the wheel, checks documentation
links, and enforces at least 80% combined line/branch coverage.

Additional walkthroughs:

- [Azure managed-identity configuration](docs/azure_usage_guide.md)
- [Databricks Workflow example](docs/Databricks%20Workflow%20Example.md)

## License

Apache License 2.0; see [LICENSE](LICENSE).

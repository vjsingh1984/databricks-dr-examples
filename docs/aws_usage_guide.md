# AWS Usage Guide for Databricks DR Sync

This guide covers AWS-specific setup for using the Databricks DR sync tools in AWS environments.

## Prerequisites

- Databricks workspaces deployed in AWS
- IAM roles with appropriate Databricks permissions
- S3 buckets for data storage and landing zones
- (Optional) AWS Secrets Manager for secret storage

## IAM Role Setup

### Cross-Account Access

If your primary and secondary workspaces are in different AWS accounts:

1. Create an IAM role in the target account with Databricks workspace trust relationship
2. Attach policy with permissions to create S3 buckets, access Databricks APIs
3. Update `data/aws_cred_mapping.csv` with target IAM role ARN

Example `aws_cred_mapping.csv`:
```csv
source_cred_name,target_iam_role
primary_workspace_role,arn:aws:iam::TARGET_ACCOUNT_ID:role/DatabricksTargetRole
```

### IAM Policy for Databricks Access

Minimum required IAM policy for DR sync operations:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "databricks:*",
        "s3:*"
      ],
      "Resource": "*"
    }
  ]
}
```

For production, scope down to specific workspace ARNs and S3 buckets.

## S3 Cross-Region Replication (CRR)

For disaster recovery across AWS regions, configure S3 CRR:

### Setup

1. Create S3 buckets in primary and secondary regions
2. Enable versioning on both buckets
3. Configure cross-region replication:

```bash
# On primary bucket
aws s3 put bucket-replication \
  --bucket primary-bucket \
  --replicationconfiguration \
  file://replication.json
```

`replication.json`:
```json
{
  "Role": "arn:aws:iam::ACCOUNT_ID:role/s3-crr-role",
  "Rules": [
    {
      "Destination": "arn:aws:s3:::secondary-bucket",
      "Prefix": "databricks/",
      "Status": "Enabled"
    }
  ]
}
```

### Considerations

- **Replication lag**: S3 CRR is eventually consistent. Add delay between Phase 1 and Phase 2 of `sync_tables.py`
- **Storage class**: Use INTENTIONAL storage class for landing zone to reduce costs
- **Metrics**: Enable S3 CloudWatch metrics for replication monitoring

## S3 Landing Zone Configuration

### Bucket Structure

Recommended landing zone structure:

```
s3://landing-zone-{region}/{workspace}/
├── manifests/           # Table manifests from sync_tables.py
├── sync_status/          # Sync status Delta tables
└── data/                 # Deep cloned table data (Phase 1)
    ├── {catalog}/
    │   ├── {schema}/
    │   │   ├── {table}/
    │   │   └── _delta_log/
```

### Lifecycle Policies

Configure lifecycle rules to manage landing zone costs:

```bash
aws s3 put-bucket-lifecycle-configuration \
  --bucket landing-zone-us-west-2 \
  --lifecycle-configuration file://lifecycle.json
```

`lifecycle.json`:
```json
{
  "Rules": [
    {
      "ID": "DeleteOldManifests",
      "Filter": {"Prefix": "manifests/"},
      "Status": "Enabled",
      "Expiration": {"Days": 30},
      "NoncurrentVersionExpiration": {"NoncurrentDays": 7}
    },
    {
      "ID": "DeleteOldStatus",
      "Filter": {"Prefix": "sync_status/"},
      "Status": "Enabled",
      "Expiration": {"Days": 90}
    }
  ]
}
```

## AWS Secrets Manager Integration

### Secret Scope Configuration

Create AWS Secrets Manager-backed secret scopes:

```python
# In your Databricks workspace
scope_name = "aws_secrets"
scope_backend = "secrets-manager"
arn_prefix = "arn:aws:secretsmanager:us-west-2:123456789012:secret:"
```

### Syncing Secret Scopes

When running `sync_secret_scopes.py`:

- Secret scope definitions are synced
- ACLs (permissions) are synced
- **Secret values are NOT synced** (by design for security)

For AWS Secrets Manager-backed scopes:
- The secret scope references the same AWS secret ARN in both workspaces
- Ensure the AWS secret exists in the target AWS account
- No manual recreation needed for secret values

### Example

```bash
# List AWS Secrets Manager secrets in source workspace
aws secretsmanager list-secrets --region us-east-1

# Sync scopes (metadata + ACLs only)
python sync_secret_scopes.py --dry-run
python sync_secret_scopes.py
```

## EC2 Instance Types for Instance Pools

When syncing instance pools between regions with `sync_instance_pools.py`:

1. Instance types are automatically remapped if available in target region
2. For production, extend `AWS_INSTANCE_TYPE_MAPPINGS` in the script:

```python
AWS_INSTANCE_TYPE_MAPPINGS = {
    "us-east-1": {
        "i3.xlarge": "i3.xlarge",  # Same instance type
        "i3.2xlarge": "i3.2xlarge",
    },
    "us-west-2": {
        "i3en.xlarge": "i3en.xlarge",  # EN instances only in specific regions
    }
}
```

## CloudWatch Monitoring

### Sync Status Monitoring

Send DR sync status to CloudWatch Logs:

1. Configure Databricks to send logs to CloudWatch via Log Delivery
2. Create CloudWatch metric filters for DR sync events
3. Set up CloudWatch Alarms for sync failures

### Example CloudWatch Insights Query

```sql
-- Find sync failures in last hour
fields @timestamp, @message
| filter @message like /ERROR/
| sort @timestamp desc
| limit 100
```

## VPC Networking

### PrivateLink for Databricks Workspaces

For VPC-isolated workspaces:

1. Enable AWS PrivateLink for Databricks in target region
2. Configure VPC endpoints for:
   - `databricks.workspace` (workspace APIs)
   - `s3.{region}.amazonaws.com` (S3 access)
3. Update `common.py` with workspace URLs using VPC interface

### Firewall Rules

Ensure security groups allow:
- Outbound HTTPS (443) to Databricks APIs
- S3 endpoint access (HTTPS 443 or VPC endpoint)
- Databricks Relay access (if using Delta Sharing)

## Cost Optimization

### Serverless SQL Warehouses

The sync scripts create serverless warehouses temporarily. To minimize costs:

- Use `warehouse_size="Small"` for lightweight sync operations
- Warehouses auto-stop after 10 minutes (configured in `managed_warehouse`)
- Monitor warehouse usage with CloudWatch

### S3 Transfer Acceleration

For cross-region data transfer:

```bash
# Enable S3 Transfer Acceleration on landing zone bucket
aws s3 put-bucket-accelerate-configuration \
  --bucket landing-zone-us-west-2 \
  --accelerate-configuration Status=Enabled
```

## Example: Full DR Run Sequence

### 1. Initial Setup (One-time)

```bash
# Install tools
git clone https://github.com/gregwood-db/databricks-dr-examples.git
cd databricks-dr-examples
pip install -e .
```

### 2. Configure Environment

```bash
export DR_SYNC_SOURCE_HOST=https://primary-us-east-1.cloud.databricks.com
export DR_SYNC_SOURCE_TOKEN=dapi...
export DR_SYNC_TARGET_HOST=https://secondary-us-west-2.cloud.databricks.com
export DR_SYNC_TARGET_TOKEN=dapi...
export DR_SYNC_CATALOGS_TO_COPY="prod_catalog,analytics_catalog"
export DR_SYNC_LANDING_ZONE_URL=s3://landing-zone-us-west-2/dr-sync/
export DR_SYNC_CLOUD_TYPE=aws
```

### 3. Run DR Sync

```bash
# Preview what will be synced
dr-sync run --all --dry-run

# Run full sync with checkpointing
dr-sync run --all

# Resume if interrupted
dr-sync run --all --resume
```

### 4. Verify Sync

```bash
# Check sync status
dr-sync checkpoint list

# Verify in target workspace
# (connect to target workspace and list resources)
```

## Troubleshooting

### IAM Role Not Accessible

**Error**: `Failed to create credential. Please check mapping file.`

**Solution**:
1. Verify IAM role ARN in `data/aws_cred_mapping.csv`
2. Check trust relationship on target IAM role
3. Ensure Databricks workspace has permission to assume the role

### S3 Access Denied

**Error**: `Permission denied when accessing S3 bucket`

**Solution**:
1. Verify instance profile has S3 permissions
2. Check S3 bucket policy allows workspace access
3. For VPC endpoints, verify DNS resolution

### Secret Scope Sync Issues

**Error**: `Failed to sync secret scope`

**Solution**:
1. For Databricks-backed scopes: recreate secrets in target workspace
2. For AWS Secrets Manager: verify secret exists in target AWS account
3. Check ACLs on source scope (may prevent reading scope definition)

# Installation Guide

Complete setup guide for the CoreTelecoms Customer Complaint Analytics Platform.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment Variables Configuration](#environment-variables-configuration)
- [Airflow Connections Setup](#airflow-connections-setup)
- [Database Schema Initialization](#database-schema-initialization)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before installing the pipeline, ensure you have the following tools and access configured.

### Required Software

- [ ] **Docker** (v20.10+) and **Docker Compose** (v2.0+)
  - Install: [Docker Desktop](https://www.docker.com/products/docker-desktop/)
  - Verify: `docker --version` and `docker compose version`

- [ ] **Terraform** (v1.0+)
  - Install: [Terraform Downloads](https://www.terraform.io/downloads)
  - Verify: `terraform --version`

- [ ] **AWS CLI** (v2.0+)
  - Install: [AWS CLI Installation](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
  - Verify: `aws --version`

- [ ] **Git**
  - Verify: `git --version`

### AWS Account Requirements

- [ ] **Active AWS Account** with appropriate permissions
- [ ] **IAM User or Role** with the following permissions:
  - `AmazonS3FullAccess` (for Bronze data lake)
  - `AmazonRDSFullAccess` (for Silver conformance layer)
  - `IAMFullAccess` (for creating service roles)
  - `AmazonVPCFullAccess` (for network configuration)
  - `AmazonEC2FullAccess` (for security groups)

- [ ] **AWS CLI Configured** with credentials
  ```bash
  aws configure
  # OR
  aws sso login --profile your-profile
  ```

- [ ] **Verify AWS Access**
  ```bash
  aws sts get-caller-identity
  ```
  Should return your account ID and user/role ARN.

### Service Account Requirements

- [ ] **Google Cloud Service Account** (for Google Sheets integration)
  - Create service account in Google Cloud Console
  - Enable Google Sheets API
  - Download JSON key file
  - Share target Google Sheet with service account email

- [ ] **Slack Workspace** (optional, for notifications)
  - Create Slack App: [Slack API](https://api.slack.com/apps)
  - Add Incoming Webhook
  - Note webhook URL

- [ ] **dbt Cloud Account** (for dimensional modeling)
  - Free tier available: [dbt Cloud](https://cloud.getdbt.com/)
  - Create API token
  - Note Job ID for your dbt project

### Source Data Access

- [ ] **Source S3 Bucket** with read access
  - Contains customers CSV, call logs CSV, social media JSON

- [ ] **Source PostgreSQL Database** (for web complaints)
  - Host, database name, credentials
  - Network access from Airflow (security group/VPC)

- [ ] **Google Sheet ID** (for agents data)
  - Share with service account email
  - Note Sheet ID from URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`

### Cost Estimation

Estimated monthly AWS costs for development environment:
- **RDS PostgreSQL (db.t3.micro)**: ~$15/month
- **S3 Storage (100GB)**: ~$2/month
- **Data Transfer**: ~$5/month
- **Total**: ~$22/month

**Note**: Stop RDS instance when not in use to reduce costs.

---

## Installation

### Step 1: Clone Repository

```bash
git clone https://github.com/DesayoEm/CoreTelecoms_customer_complaint_pipeline.git
cd CoreTelecoms_customer_complaint_pipeline
```

### Step 2: Infrastructure Deployment with Terraform

Navigate to infrastructure directory:

```bash
cd infra
```

#### Authenticate with AWS

```bash
# Option 1: SSO
aws sso login --profile your-profile

# Option 2: Configure credentials
aws configure
```

#### Initialize Terraform

```bash
terraform init
```

This downloads required providers (AWS, etc.) and prepares Terraform state.

#### Review Planned Changes

```bash
terraform plan
```

Review the resources Terraform will create:
- S3 bucket (Bronze layer)
- RDS PostgreSQL instance (Silver layer)
- IAM roles and policies
- VPC security groups
- Secrets Manager entries (optional)

#### Apply Infrastructure

```bash
terraform apply
```

Type `yes` when prompted.

**Expected duration**: 5-10 minutes (RDS takes longest to provision).

#### Save Terraform Outputs

```bash
terraform output > ../terraform-outputs.txt
```

**Important outputs to note**:
- `bronze_bucket_name`: S3 bucket for data lake
- `rds_endpoint`: RDS connection endpoint
- `rds_database_name`: Database name
- `iam_role_arn`: Service role ARN for Airflow

**Save these values** - you'll need them for environment configuration.

### Step 3: Configure Environment Variables

Return to project root:

```bash
cd ..
```

Create `.env` file from template:

```bash
cp .env.example .env
```

Edit `.env` with your credentials (see [Environment Variables Configuration](#environment-variables-configuration) below).

### Step 4: Configure Google Service Account

Place your Google Cloud service account JSON in the config directory:

```bash
mkdir -p config
cp /path/to/your/service-account-key.json config/service-account.json
```

Ensure proper permissions:

```bash
chmod 600 config/service-account.json
```

### Step 5: Initialize Airflow Database

**First time only** - initialize Airflow's metadata database:

```bash
docker compose up airflow-init
```

Wait for message: `Upgrades done` and container exits.

### Step 6: Start Airflow Services

```bash
docker compose up -d
```

This starts:
- **Airflow Webserver** (port 8080)
- **Airflow Scheduler**
- **Postgres** (Airflow metadata)
- **Redis** (Celery backend)
- **Airflow Workers** (if using CeleryExecutor)

Verify services are running:

```bash
docker compose ps
```

All services should show status `Up` or `healthy`.

### Step 7: Access Airflow UI

Open browser: http://localhost:8080

**Default credentials**:
- Username: `admin`
- Password: `admin`

**⚠️ Security Warning**: Change default password in production!

---

## Environment Variables Configuration

Edit `.env` file with your configuration:

### Google Sheets Configuration

```bash
# Google Sheet ID from URL
GOOGLE_SHEET_ID=1abc123def456ghi789jkl

# Path to service account JSON (inside Docker container)
GOOGLE_SERVICE_ACCOUNT_PATH=/opt/airflow/config/service-account.json
```

### AWS Credentials (Destination)

These credentials are for **Airflow** to write to your data lake:

```bash
# AWS Access Keys for destination (Bronze bucket)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
```

**Best Practice**: Use IAM role with instance profile in production instead of access keys.

### AWS Source Credentials

If your **source data** is in a different AWS account:

```bash
# AWS credentials for source S3 bucket (if different from destination)
SRC_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
SRC_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
SRC_REGION=us-east-1
```

**Note**: If source and destination are in same account, you can use the same credentials.

### Source PostgreSQL Database

Connection string for web complaints source:

```bash
# Source database for web complaints
SRC_DB_CONN_STRING=postgresql://username:password@source-host.region.rds.amazonaws.com:5432/source_db
SRC_DB_SCHEMA=public
```

### RDS PostgreSQL (Silver Layer)

Connection string from Terraform output:

```bash
# Silver conformance layer (from Terraform output)
SILVER_DB_CONN_STRING=postgresql://postgres:YourPassword@your-rds-endpoint.region.rds.amazonaws.com:5432/silver_db
```

### S3 Configuration

#### Bronze Bucket

```bash
# Bronze layer S3 bucket (from Terraform output)
BRONZE_BUCKET=coretelecoms-bronze-12345678
```

#### Staging Destinations

Paths within Bronze bucket for each entity:

```bash
# S3 staging paths (within Bronze bucket)
CUSTOMER_DATA_STAGING_DEST=bronze/customers
AGENT_DATA_STAGING_DEST=bronze/agents
CALL_LOGS_STAGING_DEST=bronze/call_logs
SM_COMPLAINTS_STAGING_DEST=bronze/social_media_complaints
WEB_COMPLAINTS_STAGING_DEST=bronze/web_complaints
```

#### Object Prefixes

File naming convention for each entity:

```bash
# Object key prefixes (for file naming)
CUSTOMER_DATA_OBJ_PREFIX=customers-
AGENT_DATA_OBJ_PREFIX=agents-
CALL_LOGS_OBJ_PREFIX=call-logs-
SM_COMPLAINTS_OBJ_PREFIX=sm-complaints-
WEB_COMPLAINTS_OBJ_PREFIX=web-complaints-
```

**Example**: With prefix `customers-` and date `2024-01-15`, files are named `customers-2024-01-15.parquet`.

### Source Data Locations

#### Source S3 Keys

```bash
# Source S3 bucket and object keys
SRC_BUCKET_NAME=your-source-data-bucket
SRC_CUSTOMERS_OBJ_KEY=raw/customers.csv
SRC_CALL_LOGS_OBJ_KEY=raw/call_logs.csv
SRC_SM_COMPLAINTS_OBJ_KEY=raw/social_media_complaints.json
```

### Optional: dbt Configuration

```bash
# dbt Cloud API token and job ID
DBT_CLOUD_API_TOKEN=your_dbt_cloud_api_token
DBT_JOB_ID=123456
```

### Complete .env Example

```bash
# Google Sheets
GOOGLE_SHEET_ID=1abc123def456ghi789jkl
GOOGLE_SERVICE_ACCOUNT_PATH=/opt/airflow/config/service-account.json

# AWS Destination (Airflow writes here)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1

# AWS Source (if different account)
SRC_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
SRC_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
SRC_REGION=us-east-1

# Source Database
SRC_DB_CONN_STRING=postgresql://user:pass@source.rds.amazonaws.com:5432/db
SRC_DB_SCHEMA=public

# Silver Layer (Conformance)
SILVER_DB_CONN_STRING=postgresql://postgres:password@rds-endpoint.us-east-1.rds.amazonaws.com:5432/silver_db

# S3 Bronze Bucket
BRONZE_BUCKET=coretelecoms-bronze-12345678

# S3 Staging Destinations
CUSTOMER_DATA_STAGING_DEST=bronze/customers
AGENT_DATA_STAGING_DEST=bronze/agents
CALL_LOGS_STAGING_DEST=bronze/call_logs
SM_COMPLAINTS_STAGING_DEST=bronze/social_media_complaints
WEB_COMPLAINTS_STAGING_DEST=bronze/web_complaints

# Object Prefixes
CUSTOMER_DATA_OBJ_PREFIX=customers-
AGENT_DATA_OBJ_PREFIX=agents-
CALL_LOGS_OBJ_PREFIX=call-logs-
SM_COMPLAINTS_OBJ_PREFIX=sm-complaints-
WEB_COMPLAINTS_OBJ_PREFIX=web-complaints-

# Source S3
SRC_BUCKET_NAME=your-source-bucket
SRC_CUSTOMERS_OBJ_KEY=raw/customers.csv
SRC_CALL_LOGS_OBJ_KEY=raw/call_logs.csv
SRC_SM_COMPLAINTS_OBJ_KEY=raw/social_media_complaints.json

# dbt Cloud (optional)
DBT_CLOUD_API_TOKEN=your_token
DBT_JOB_ID=123456
```

**⚠️ Security**: Never commit `.env` to version control. It's included in `.gitignore`.

---

## Airflow Connections Setup

Configure Airflow connections through the Web UI.

### Access Airflow Admin Panel

1. Navigate to http://localhost:8080
2. Login with `admin` / `admin`
3. Go to **Admin → Connections**

### 1. AWS Destination Connection

Connection for Airflow to write to Bronze S3 bucket.

**Click "+" to add new connection:**

- **Connection ID**: `aws_airflow_dest_user`
- **Connection Type**: `Amazon Web Services`
- **AWS Access Key ID**: (from `.env` AWS_ACCESS_KEY_ID)
- **AWS Secret Access Key**: (from `.env` AWS_SECRET_ACCESS_KEY)
- **Extra**: 
  ```json
  {"region_name": "us-east-1"}
  ```

**Test Connection** → Should succeed.

### 2. AWS Source Connection

Connection for Airflow to read from source S3 bucket.

**Add new connection:**

- **Connection ID**: `aws_airflow_src_user`
- **Connection Type**: `Amazon Web Services`
- **AWS Access Key ID**: (from `.env` SRC_ACCESS_KEY_ID)
- **AWS Secret Access Key**: (from `.env` SRC_SECRET_ACCESS_KEY)
- **Extra**:
  ```json
  {"region_name": "us-east-1"}
  ```

**Note**: If source and destination are same account, you can skip this and use `aws_airflow_dest_user` for both.

### 3. RDS PostgreSQL Connection (Silver Layer)

Connection to Silver conformance layer.

**Add new connection:**

- **Connection ID**: `rds_postgres`
- **Connection Type**: `Postgres`
- **Host**: (RDS endpoint from Terraform output, e.g., `coretelecoms.c9akl.us-east-1.rds.amazonaws.com`)
- **Database**: `silver_db` (or your database name)
- **Login**: `postgres` (or your username)
- **Password**: (your RDS password)
- **Port**: `5432`

**Test Connection** → Should succeed if RDS is accessible.

**Troubleshooting**: If connection fails:
- Check RDS security group allows inbound on port 5432 from Airflow
- Verify RDS is publicly accessible (or Airflow is in same VPC)
- Check database name and credentials

### 4. Slack Connection (Optional)

For pipeline notifications.

**Add new connection:**

- **Connection ID**: `slack`
- **Connection Type**: `Slack Incoming Webhook`
- **Webhook URL**: `https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXX`

**To get webhook URL**:
1. Go to [Slack API Apps](https://api.slack.com/apps)
2. Create new app or select existing
3. Add "Incoming Webhooks" feature
4. Create webhook for target channel (e.g., `#dag_alerts`)
5. Copy webhook URL

**Test**: Run any Airflow task and check Slack channel for notification.

### 5. Snowflake Connection (Gold Layer)

Connection to Snowflake data warehouse.

**Add new connection:**

- **Connection ID**: `snowflake`
- **Connection Type**: `Snowflake`
- **Account**: (Snowflake account identifier, e.g., `abc12345.us-east-1`)
- **Warehouse**: (your warehouse name, e.g., `COMPUTE_WH`)
- **Database**: (your database, e.g., `CORETELECOMS_DB`)
- **Schema**: `STG` (staging schema for initial loads)
- **Login**: (your Snowflake username)
- **Password**: (your Snowflake password)
- **Role**: (your role, e.g., `ACCOUNTADMIN` or `SYSADMIN`)

**Extra** (optional):
```json
{
  "region": "us-east-1"
}
```

**Test Connection** → Should succeed.

### 6. dbt Cloud Connection (Optional)

For automated dbt job execution.

**Add new connection:**

- **Connection ID**: `dbt_cloud_default`
- **Connection Type**: `dbt Cloud`
- **Account ID**: (your dbt Cloud account ID)
- **API Token**: (from `.env` DBT_CLOUD_API_TOKEN)

**To get dbt Cloud credentials**:
1. Login to [dbt Cloud](https://cloud.getdbt.com/)
2. Go to **Account Settings → API Access**
3. Create service token
4. Note Account ID from URL: `https://cloud.getdbt.com/deploy/{ACCOUNT_ID}/projects/...`

**Test**: Trigger DAG and verify dbt job runs successfully.

---

## Database Schema Initialization

Initialize the Silver conformance layer schema in RDS PostgreSQL.

### Step 1: Connect to RDS

```bash
# Using psql (install if needed)
psql -h your-rds-endpoint.us-east-1.rds.amazonaws.com -U postgres -d silver_db

# Or from Docker container
docker compose exec airflow-webserver psql -h your-rds-endpoint -U postgres -d silver_db
```

### Step 2: Run Initialization Script

```bash
# From project root
psql -h your-rds-endpoint -U postgres -d silver_db -f scripts/init_silver_schema.sql
```

**Alternative**: Execute SQL directly in psql session:

```sql
\i scripts/init_silver_schema.sql
```

### What Gets Created

The initialization script creates:

**Staging Schema**:
- `staging_customers`
- `staging_agents`
- `staging_call_logs`
- `staging_sm_complaints`
- `staging_web_complaints`

**Conformed Schema** (with constraints):
- `conformed_customers` (with surrogate keys, SCD Type 1)
- `conformed_agents` (with surrogate keys, SCD Type 1)
- `conformed_call_logs` (with FK constraints to customers/agents)
- `conformed_sm_complaints` (with FK constraints)
- `conformed_web_complaints` (with FK constraints)

**Data Quality**:
- `data_quality_quarantine` (JSONB storage for orphaned records)

**Indexes**:
- Primary keys on all conformed tables
- Foreign key indexes for join performance
- GIN index on `data_quality_quarantine.record_data` for JSON queries

### Verify Schema Creation

```sql
-- List all tables
\dt

-- Check table structure
\d conformed_customers
\d data_quality_quarantine

-- Verify indexes
\di
```

---

## Verification

Verify the installation is working correctly.

### 1. Check Airflow Services

```bash
docker compose ps
```

All services should be `Up` or `healthy`:
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-worker` (if using CeleryExecutor)
- `postgres` (Airflow metadata DB)
- `redis` (if using CeleryExecutor)

### 2. Access Airflow UI

Navigate to http://localhost:8080

Should see:
- Login page (or DAGs page if already logged in)
- No error messages in browser console

### 3. Verify DAG Loading

In Airflow UI:
1. Go to **DAGs** page
2. Locate `coretelecoms_dag`
3. Status should be **green** (DAG parsed successfully)
4. If **red**: Check Airflow logs for import errors

```bash
# View scheduler logs
docker compose logs airflow-scheduler

# View webserver logs
docker compose logs airflow-webserver
```

### 4. Test Connections

In Airflow UI:
1. Go to **Admin → Connections**
2. Find each connection
3. Click **Test** button
4. Should see green success message

Common issues:
- **AWS**: Check access keys, region configuration
- **RDS**: Check security group, host reachability
- **Snowflake**: Verify account identifier format

### 5. Manual DAG Trigger (Smoke Test)

**⚠️ Important**: Ensure source data exists before triggering.

1. In Airflow UI, find `coretelecoms_dag`
2. Toggle **ON** (unpause DAG)
3. Click **▶️ Play** → **Trigger DAG**
4. Provide execution date or use default
5. Click **Trigger**

**Monitor Execution**:
1. Click DAG name → Opens Graph view
2. Watch tasks turn green (success) or red (failure)
3. Click task → **Log** to view execution details

**Expected First Run**:
- `create_all_tables_task`: Creates RDS tables
- `truncate_conformance_staging_tables_task`: Clears staging
- `determine_load_type`: Returns first run path
- `ingest_customer_data_task`: Extracts customers from S3
- `ingest_agents_data_task`: Extracts agents from Google Sheets
- `transform_customers_task`: Cleans and validates
- `load_customers_task`: Loads to RDS
- ... (similar for agents and complaints)
- `static_data_gate`: Opens after static loads complete
- `run_dbt`: Triggers dbt Cloud job
- `cleanup_checkpoints_task`: Clears Airflow Variables

**Total Duration**: ~10-15 minutes for first run.

### 6. Verify Data in RDS

```sql
-- Connect to RDS
psql -h your-rds-endpoint -U postgres -d silver_db

-- Check row counts
SELECT 'customers' as table_name, COUNT(*) as row_count FROM conformed_customers
UNION ALL
SELECT 'agents', COUNT(*) FROM conformed_agents
UNION ALL
SELECT 'call_logs', COUNT(*) FROM conformed_call_logs
UNION ALL
SELECT 'sm_complaints', COUNT(*) FROM conformed_sm_complaints
UNION ALL
SELECT 'web_complaints', COUNT(*) FROM conformed_web_complaints;

-- Check for quarantined records
SELECT COUNT(*) as quarantined_count FROM data_quality_quarantine;
```

**Expected**:
- Customers: >0 rows
- Agents: >0 rows
- Complaints: >0 rows
- Quarantine: 0 (or small number if source data has issues)

### 7. Verify Data in Snowflake

```sql
-- In Snowflake worksheet
USE DATABASE CORETELECOMS_DB;
USE SCHEMA STG;

-- Check staging tables
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM agents;
SELECT COUNT(*) FROM call_logs;

-- Check analytics schema (after dbt runs)
USE SCHEMA ANALYTICS;

SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_agents;
SELECT COUNT(*) FROM fact_unified_complaint;
SELECT COUNT(*) FROM fact_accumulating_snapshot;
```

### 8. Verify Slack Notifications

Check your Slack channel (e.g., `#dag_alerts`):
- Should see success messages for each completed task
- Format: `✅ SUCCESS: {task_id} for CoreTelecoms ({date})`
- Includes metrics (rows processed, data quality %)

---

## Troubleshooting

Common issues and solutions.

### Docker Issues

**Issue**: `docker compose` command not found

**Solution**:
```bash
# Use docker-compose (older version)
docker-compose up -d

# Or upgrade Docker Desktop to v2.0+
```

**Issue**: Containers fail to start

**Solution**:
```bash
# Check logs
docker compose logs

# Restart services
docker compose down
docker compose up -d
```

### Airflow Issues

**Issue**: DAG not showing in UI

**Solution**:
1. Check DAG file has no Python syntax errors
2. View scheduler logs: `docker compose logs airflow-scheduler`
3. Verify DAG file is in `/dags` directory
4. Check Airflow `LOAD_EXAMPLES` is `false` in `docker-compose.yml`

**Issue**: Tasks fail with import errors

**Solution**:
```bash
# Install Python dependencies
docker compose exec airflow-webserver pip install -r requirements.txt

# Or rebuild image
docker compose down
docker compose build
docker compose up -d
```

### AWS Connection Issues

**Issue**: `boto3.exceptions.NoCredentialsError`

**Solution**:
1. Verify AWS connection in Airflow UI
2. Test connection → Should be green
3. Check access keys in `.env` match Airflow connection
4. Ensure region is correct

**Issue**: S3 access denied

**Solution**:
1. Check IAM permissions for user/role
2. Verify bucket name is correct
3. Check bucket policy allows access from IAM user

### RDS Connection Issues

**Issue**: `psycopg2.OperationalError: could not connect to server`

**Solution**:
1. Check RDS security group allows inbound on port 5432
2. Verify RDS is publicly accessible (or Airflow in same VPC)
3. Test connection from command line:
   ```bash
   psql -h rds-endpoint -U postgres -d silver_db
   ```
4. Check RDS endpoint is correct (from Terraform output)

**Issue**: Authentication failed

**Solution**:
1. Verify username/password in Airflow connection
2. Reset RDS password if needed:
   ```bash
   aws rds modify-db-instance \
     --db-instance-identifier your-instance \
     --master-user-password NewPassword123
   ```

### Google Sheets Issues

**Issue**: `gspread.exceptions.APIError: Service account not authorized`

**Solution**:
1. Verify Google Sheet is shared with service account email
2. Check service account has Sheets API enabled
3. Verify JSON key file path is correct in `.env`
4. Check file permissions: `chmod 600 config/service-account.json`

**Issue**: Sheet not found

**Solution**:
1. Verify GOOGLE_SHEET_ID in `.env` matches your sheet
2. Extract ID from URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`
3. Ensure sheet name in code matches actual sheet name

### Snowflake Connection Issues

**Issue**: `snowflake.connector.errors.DatabaseError: Authentication failed`

**Solution**:
1. Verify account identifier format: `abc12345.us-east-1` (not full URL)
2. Check username/password in Airflow connection
3. Verify role has necessary permissions
4. Test connection from command line:
   ```bash
   snowsql -a abc12345.us-east-1 -u username -d database
   ```

**Issue**: Object does not exist

**Solution**:
1. Verify database and schema names are correct
2. Check role has access to database/schema:
   ```sql
   USE ROLE ACCOUNTADMIN;
   GRANT USAGE ON DATABASE CORETELECOMS_DB TO ROLE your_role;
   GRANT USAGE ON SCHEMA STG TO ROLE your_role;
   ```

### dbt Cloud Issues

**Issue**: dbt job fails to trigger

**Solution**:
1. Verify dbt connection in Airflow
2. Check account ID and API token are correct
3. Verify job ID exists in dbt Cloud
4. Check API token has permissions to run jobs

**Issue**: dbt job runs but fails

**Solution**:
1. View dbt Cloud logs for specific error
2. Common issues:
   - Source tables not populated (check RDS conformance layer)
   - Schema changes (rebuild dbt models)
   - Snowflake permissions (grant access to STG schema)

### Data Quality Issues

**Issue**: High quarantine rate (>5%)

**Solution**:
1. Query quarantine table:
   ```sql
   SELECT table_name, COUNT(*) as count
   FROM data_quality_quarantine
   GROUP BY table_name;
   ```
2. Investigate problematic fields:
   ```sql
   SELECT record_data->>'customer_id' as customer_id,
          COUNT(*) as occurrences
   FROM data_quality_quarantine
   WHERE table_name = 'conformed_call_logs'
   GROUP BY 1
   ORDER BY 2 DESC;
   ```
3. Check source data for issues
4. Update cleaning logic if needed

**Issue**: FK violations blocking pipeline

**Solution**:
1. Check quarantine pattern is working (should isolate violations)
2. Verify customers/agents loaded before complaints
3. Check gate pattern is functioning:
   ```python
   # In Airflow UI, view task logs for determine_load_type
   # Should see: "First run - agents and customers must load before gate opens"
   ```

### Memory Issues

**Issue**: Tasks killed with `OOM` error

**Solution**:
1. Reduce batch size in `transformation.py`:
   ```python
   BATCH_SIZE = 50000  # Reduce from 100000
   ```
2. Increase Docker memory allocation (Docker Desktop → Settings → Resources)
3. Use ThreadPool parallelization for large datasets

---

## Next Steps

After successful installation:

1. **Configure Scheduling**: Set appropriate schedule for DAG (`@daily`, `@hourly`, etc.)
2. **Set up Monitoring**: Configure CloudWatch alarms, Datadog integration
3. **Security Hardening**: Rotate credentials, enable encryption, restrict access
4. **Backup Strategy**: Configure RDS automated backups, S3 versioning
5. **Data Quality Monitoring**: Set up alerts for quarantine rate spikes
6. **Documentation**: Update with any custom configurations

---

## Support

For issues not covered in troubleshooting:

1. **Check Airflow Logs**: Most errors have detailed stack traces in logs
2. **GitHub Issues**: Open issue with error logs and steps to reproduce
3. **Airflow Documentation**: [Apache Airflow Docs](https://airflow.apache.org/docs/)
4. **AWS Documentation**: [AWS RDS](https://docs.aws.amazon.com/rds/), [AWS S3](https://docs.aws.amazon.com/s3/)

---

## Appendix: Useful Commands

### Docker Commands

```bash
# Start services
docker compose up -d

# Stop services
docker compose down

# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# Restart specific service
docker compose restart airflow-scheduler

# Execute command in container
docker compose exec airflow-webserver bash

# View resource usage
docker stats
```

### Airflow Commands

```bash
# Test task execution
docker compose exec airflow-worker airflow tasks test coretelecoms_dag ingest_customer_data_task 2024-01-15

# Clear task state
docker compose exec airflow-worker airflow tasks clear coretelecoms_dag

# List DAGs
docker compose exec airflow-worker airflow dags list

# Trigger DAG
docker compose exec airflow-worker airflow dags trigger coretelecoms_dag
```

### Terraform Commands

```bash
# Navigate to infra directory
cd infra

# View current state
terraform show

# Destroy infrastructure (careful!)
terraform destroy

# Update single resource
terraform apply -target=aws_s3_bucket.bronze_bucket
```

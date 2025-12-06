# Installation Guide

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment Variables Configuration](#environment-variables-configuration)
- [Airflow Connections Setup](#airflow-connections-setup)
- [Database Schema Initialization](#database-schema-initialization)
- [Verification](#verification)


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
  - Generate a Slack API token

- [ ] **dbt Cloud Account** (for dimensional modeling)
  - Create a project connected to your GitHub repo
  - Create API token with "Job Admin" permissions
  - Save the Job ID from your dbt job

- [ ] **Snowflake Account** (for data warehouse)
  - Save your account identifier 
  - You'll need to create database, schema, and tables (see Snowflake Setup section)

### Source Data Access

- [ ] **Source S3 Bucket** with read access
  - Contains customers CSV, call logs CSV, social media JSON

- [ ] **Source PostgreSQL Database** (for web complaints)
  - Host, database name, credentials
  - Network access from Airflow (security group/VPC)

- [ ] **Google Sheet ID** (for agents data)
  - Share with service account email
  - Note Sheet ID from URL: `https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit`


## Installation

### Step 1: Clone Repository

```bash
git clone https://github.com/DesayoEm/CoreTelecoms_customer_complaint_pipeline.git
cd CoreTelecoms_customer_complaint_pipeline
```

### Step 2: Infrastructure Deployment with Terraform

```bash
cd infra
```

#### Authenticate with AWS

```bash
# Configure credentials
aws configure
```

#### Initialize Terraform

```bash
terraform init
```

This downloads required provider (AWS) and prepares Terraform state.

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

#### Save Terraform Outputs

```bash
terraform output > ../terraform-outputs.txt
```

### Step 3: Configure Environment Variables

```bash
cd ..
```

Create `.env` file from template:

Edit `.env` with your credentials (see [Environment Variables Configuration](#environment-variables-configuration) below).

### Step 4: Configure Google Service Account

Place your Google Cloud service account JSON in the infra directory . DO NOT FORGET TO gitignore THIS FILE.


### Step 5: Initialize Airflow Database

**First time only** - initialize Airflow's metadata database:

```bash
docker compose up airflow-init
```

### Step 6: Start Airflow Services

```bash
docker compose up -d
```
Verify services are running:

```bash
docker compose ps
```

### Step 7: Access Airflow UI

Open browser: http://localhost:8080

## Environment Variables Configuration

Edit `.env` file with your configuration:

### Google Sheets Configuration

```bash
# Google Sheet ID from URL
GOOGLE_SHEET_ID=1abc123def456ghi789jkl

```

### AWS Credentials (Destination)

These credentials are for **Airflow** to write to your data lake:

```bash
# AWS Access Keys for destination (Bronze bucket)
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1
```

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
BRONZE_BUCKET=coretelecoms-bronze
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

### dbt Cloud Configuration

```bash
# dbt Cloud API token and job ID (required for dimensional modeling)
DBT_CLOUD_API_TOKEN=your_dbt_cloud_api_token
DBT_JOB_ID=123456
```

**To get dbt Cloud credentials**:
1. Sign up for free account at [dbt Cloud](https://cloud.getdbt.com/)
2. Create new project connected to your GitHub repository
3. Go to **Account Settings ->> API Access**
4. Create **Service Token** with "Job Admin" permissions
5. Copy token to DBT_CLOUD_API_TOKEN
6. Go to **Deploy ->> Jobs** ->> Select your job ->> Note Job ID from URL
7. Copy Job ID to DBT_JOB_ID

### Snowflake Configuration

```bash
# Snowflake connection details (required for Gold layer)
SNOWFLAKE_ACCOUNT=abc123-dmd234
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password

SNOWFLAKE_WAREHOUSE=CORETELECOMS_WH
SNOWFLAKE_DATABASE=CORETELECOMS_DB
SNOWFLAKE_SCHEMA=STG
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

**To get Snowflake credentials**:
1. Sign up for free trial at [Snowflake](https://signup.snowflake.com/)
2. Note your **Account Identifier** (format: `abc12345.us-east-1`, NOT full URL)
3. Create database and schema (see Snowflake Setup section below)
4. Create user with appropriate permissions

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

# dbt Cloud (required)
DBT_CLOUD_API_TOKEN=dbtc_Ab12CdEfGhIjKlMnOpQrStUvWxYz
DBT_JOB_ID=123456

# Snowflake (required for Gold layer)
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
SNOWFLAKE_USER=AIRFLOW_USER
SNOWFLAKE_PASSWORD=YourStrongPassword123
SNOWFLAKE_WAREHOUSE=CORETELECOMS_WH
SNOWFLAKE_DATABASE=CORETELECOMS_DB
SNOWFLAKE_SCHEMA=STG
SNOWFLAKE_ROLE=TRANSFORMER
```
---

## Airflow Connections Setup

Configure Airflow connections through the Web UI.

### Access Airflow Admin Panel

1. Navigate to http://localhost:8080
2. Login with `admin` / `admin`
3. Go to **Admin ->> Connections**

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

**Test Connection** ->> Should succeed.

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

**Test Connection** ->> Should succeed if RDS is accessible.

**Troubleshooting**: If connection fails:
- Check RDS security group allows inbound on port 5432 from Airflow
- Verify RDS is publicly accessible (or Airflow is in same VPC)
- Check database name and credentials

### 4. Slack Connection (Optional)

For pipeline notifications.

**Add new connection:**

- **Connection ID**: `slack`
- **Connection Type**: `Slack API`

**To get webhook URL**:
1. Go to [Slack API Apps]
2. Create new app or select existing
3. Create a token for target channel (e.g., `#dag_alerts`)


### 5. Snowflake Connection (Gold Layer)

Connection to Snowflake data warehouse.

**Add new connection:**

- **Connection ID**: `snowflake`
- **Connection Type**: `Snowflake`
- **Account**: (Snowflake account identifier, e.g., `abc12345-tyuihg`)
- **Warehouse**: (your warehouse name, e.g., `CORETELECOMS_WH`)
- **Database**: (your database, e.g., `CORETELECOMS_DB`)
- **Schema**: `STG` (staging schema for initial loads)
- **Login**: (your Snowflake username)
- **Password**: (your Snowflake password)
- **Role**: (your role, e.g., `ACCOUNTADMIN` or `SYSADMIN


### 6. dbt Cloud Connection

For automated dbt job execution.

**Prerequisites**: Complete [Snowflake Setup](#snowflake-setup-gold-layer) section first.

**Add new connection:**

- **Connection ID**: `dbt_cloud_default`
- **Account ID**: (your dbt Cloud account ID from URL)
- **API Token**: (from `.env` DBT_CLOUD_API_TOKEN)

**To get dbt Cloud credentials**:
1. Login to [dbt Cloud](https://cloud.getdbt.com/)
2. Go to **Account Settings ->> API Access**
3. Create **Service Token** with "Job Admin" permissions
4. Copy token
5. Note Account ID from URL: `https://cloud.getdbt.com/deploy/{ACCOUNT_ID}/projects/...`

**Configure Job ID**:
- In dbt Cloud: **Deploy ->> Jobs** ->> Select your production job
- Note Job ID from URL or job details page
- Add to `.env` as `DBT_JOB_ID`

**Test**: Trigger DAG and verify:
1. DAG completes successfully
2. In dbt Cloud: **Deploy ->> Run History** shows new run
3. In Snowflake: Check `ANALYTICS` schema for dimensional tables:
   ```sql
   USE SCHEMA CORETELECOMS_DB.ANALYTICS;
   SHOW TABLES;
   -- Should see: dim_customers, dim_agents, dim_date, 
   --              fact_unified_complaint, fact_accumulating_snapshot
   ```

---

## Snowflake Setup (Gold Layer)

Configure Snowflake data warehouse for dimensional models.

### Step 1: Create Snowflake Account

1. Go to [Snowflake Trial Signup](https://signup.snowflake.com/)
2. Select edition: **Standard** (sufficient for this project)
3. Choose cloud provider: **AWS** (to match your data lake)
4. Select region: **US East (N. Virginia)** (or match your AWS region)
5. Complete registration
6. Check email for activation link
7. Set password and login

### Step 2: Note Account Identifier

After logging in:
1. Look at URL: `https://app.snowflake.com/{account_locator}/{...}`
2. Or go to **Admin ->> Accounts** ->> Copy **Account Locator**
3. Format is: `abc12345.us-east-1`
4. **Important**: Do NOT use full URL, just the account identifier

### Step 3: Create Database and Schema

In Snowflake worksheet, run:

```sql
-- Create database
CREATE DATABASE CORETELECOMS_DB;

-- Use the database
USE DATABASE CORETELECOMS_DB;

-- Create staging schema (for data loaded from RDS)
CREATE SCHEMA STG;

-- Create analytics schema (for dbt dimensional models)
CREATE SCHEMA ANALYTICS;

-- Verify
SHOW SCHEMAS;
```

### Step 3: Create Warehouse

```sql
-- Create compute warehouse for queries and dbt runs
CREATE WAREHOUSE CORETELECOMS_WH WITH
    WAREHOUSE_SIZE = 'X-SMALL'  
    AUTO_SUSPEND = 60           
    AUTO_RESUME = TRUE          
    INITIALLY_SUSPENDED = TRUE; 

-- Set as default warehouse
USE WAREHOUSE CORETELECOMS_WH;
```


### Step 4: Create Service Account for Airflow

Create dedicated user for pipeline:

```sql
-- Create role for pipeline
CREATE ROLE TRANSFORMER;

-- Grant database access
GRANT USAGE ON DATABASE CORETELECOMS_DB TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA CORETELECOMS_DB.STG TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA CORETELECOMS_DB.ANALYTICS TO ROLE TRANSFORMER;

-- Grant table permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA CORETELECOMS_DB.STG TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA CORETELECOMS_DB.ANALYTICS TO ROLE TRANSFORMER;

-- Grant future permissions (for new tables)
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CORETELECOMS_DB.STG TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CORETELECOMS_DB.ANALYTICS TO ROLE TRANSFORMER;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE CORETELECOMS_WH TO ROLE TRANSFORMER;

-- Create user
CREATE USER AIRFLOW_USER
    PASSWORD = 'YourStrongPassword123'
    DEFAULT_ROLE = TRANSFORMER
    DEFAULT_WAREHOUSE = CORETELECOMS_WH
    DEFAULT_NAMESPACE = CORETELECOMS_DB.STG;

-- Assign role to user
GRANT ROLE TRANSFORMER TO USER AIRFLOW_USER;
```

### Step 5: Create Staging Tables

These tables receive data from the RDS conformance layer.

```sql
use database coretelecoms_db;

CREATE SCHEMA stg;
CREATE SCHEMA analysis;
CREATE SCHEMA ml_features;

CREATE OR REPLACE TABLE customers (
    customer_key VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    gender CHAR(1) NOT NULL,
    date_of_birth DATE,
    signup_date DATE,
    email VARCHAR(255),
    address VARCHAR(500),
    zip_code VARCHAR(5),
    state_code VARCHAR(2),
    state VARCHAR(50),
    last_updated_at TIMESTAMP,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
)
CLUSTER BY (created_at, last_updated_at);

CREATE OR REPLACE TABLE agents (
    agent_key VARCHAR(100) PRIMARY KEY,
    agent_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(50) NOT NULL,
    experience VARCHAR(50),
    state VARCHAR(50),
    last_updated_at TIMESTAMP,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
)
CLUSTER BY (created_at, last_updated_at);


CREATE OR REPLACE TABLE sm_complaints (
    sm_complaint_key VARCHAR(100) PRIMARY KEY,
    complaint_id VARCHAR(100) UNIQUE NOT NULL,
    customer_id VARCHAR(100),
    agent_id VARCHAR(100),
    complaint_category VARCHAR(50),
    media_channel VARCHAR(50),
    request_date DATE,
    resolution_date DATE,
    resolution_status VARCHAR(50),
    media_complaint_generation_date DATE,
    last_updated_at TIMESTAMP,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
)
CLUSTER BY (created_at, last_updated_at);

CREATE OR REPLACE TABLE web_complaints (
    web_complaint_key VARCHAR(100) PRIMARY KEY,
    request_id VARCHAR(100) UNIQUE NOT NULL,
    customer_id VARCHAR(100),
    agent_id VARCHAR(100),
    complaint_category VARCHAR(100),
    request_date DATE,
    resolution_date DATE,
    resolution_status VARCHAR(50),
    web_form_generation_date DATE,
    last_updated_at TIMESTAMP,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
)
CLUSTER BY (created_at, last_updated_at);


CREATE OR REPLACE TABLE call_logs (
    call_log_key VARCHAR(100) PRIMARY KEY,
    call_id VARCHAR(100) UNIQUE NOT NULL,
    customer_id VARCHAR(100),
    agent_id VARCHAR(100),
    complaint_category VARCHAR(100),
    call_start_time TIMESTAMP,
    call_end_time TIMESTAMP,
    request_date DATE,
    resolution_status VARCHAR(50),
    call_logs_generation_date DATE,
    last_updated_at TIMESTAMP,
    created_at TIMESTAMP,
    loaded_at TIMESTAMP
)
CLUSTER BY (created_at, last_updated_at);



-- Verify tables created
SHOW TABLES IN SCHEMA STG;
```


### Step 6: Connect dbt Cloud to Snowflake

Configure dbt to read from Snowflake and write dimensional models.

#### In dbt Cloud:

1. **Create New Project**:
   - Go to [dbt Cloud](https://cloud.getdbt.com/)
   - Click **Create Project**
   - Connect to your **GitHub repository** (where dbt models are stored)

2. **Configure Snowflake Connection**:
   - Click **Configure Connection**
   - Select **Snowflake**
   - Enter connection details:
     - **Account**: `abc12345.us-east-1` (your account identifier)
     - **Database**: `CORETELECOMS_DB`
     - **Warehouse**: `CORETELECOMS_WH`
     - **Role**: `TRANSFORMER`
     - **Username**: `DBT_USER` (create separate user for dbt)
     - **Password**: (dbt user password)
   - Click **Test Connection** ->> Should succeed

3. **Create dbt Service User** (in Snowflake):
   ```sql
   -- Create user specifically for dbt
   CREATE USER DBT_USER
       PASSWORD = 'DbtStrongPassword456'
       DEFAULT_ROLE = TRANSFORMER
       DEFAULT_WAREHOUSE = CORETELECOMS_WH
       DEFAULT_NAMESPACE = CORETELECOMS_DB.ANALYTICS;
   
   -- Grant role
   GRANT ROLE TRANSFORMER TO USER DBT_USER;
   
   -- Grant additional permissions for analytics schema
   GRANT CREATE TABLE ON SCHEMA CORETELECOMS_DB.ANALYTICS TO ROLE TRANSFORMER;
   GRANT CREATE VIEW ON SCHEMA CORETELECOMS_DB.ANALYTICS TO ROLE TRANSFORMER;
   ```

4. **Configure Development Environment**:
   - **Schema**: `DBT_DEV` (dbt creates this for development)
   - Grant permissions:
     ```sql
     -- In Snowflake
     CREATE SCHEMA CORETELECOMS_DB.DBT_DEV;
     GRANT ALL ON SCHEMA CORETELECOMS_DB.DBT_DEV TO ROLE TRANSFORMER;
     ```

5. **Set Up Repository**:
   - Connect dbt to GitHub repo containing models (in `/models` directory)
   - Ensure repository structure:
   
6. **Create Production Job**:
   - Go to **Deploy ->> Environments**
   - Click **Create Environment** ->> Select **Production**
   - Set schema to `ANALYTICS`
   - Go to **Deploy ->> Jobs**
   - Click **Create Job**
   - Configure:
     - **Job Name**: `CoreTelecoms Production`
     - **Commands**:
       ```
       dbt deps
       dbt run
       dbt test
       ```
     - **Schedule**: Leave unscheduled (Airflow will trigger)
     - **Generate Docs**: Enable
   - Save and note **Job ID** (from URL)

7. **Test dbt Connection**:
   - In dbt Cloud IDE, run: `dbt debug`
   - Should see: `Connection test: OK connection ok`
   - Run models: `dbt run`
   - Verify tables created in Snowflake:
     ```sql
     USE SCHEMA CORETELECOMS_DB.ANALYTICS;
     SHOW TABLES;
     ```

### Step 8: Verify Snowflake Setup

```sql
-- Check all schemas exist
USE DATABASE CORETELECOMS_DB;
SHOW SCHEMAS;

-- Check analytics schema (after dbt runs)
USE SCHEMA ANALYTICS;
SHOW TABLES;

-- Verify warehouse
SHOW WAREHOUSES;

-- Check user permissions
SHOW GRANTS TO ROLE TRANSFORMER;
```

**Expected Results**:
- 3 schemas: `STG`, `ANALYTICS`, `DBT_DEV`
- 5 staging tables in `STG` schema
- Warehouse `CORETELECOMS_WH` exists and is suspended
- Role `TRANSFORMER` has appropriate grants
- After dbt runs: dimensional tables in `ANALYTICS` schema


---

## Database Schema Initialization

Initialize the Silver conformance layer schema in RDS PostgreSQL.

### Step 1: Connect to RDS

```bash
# Using psql
psql -h your-rds-endpoint.us-east-1.rds.amazonaws.com -U postgres -d silver_db

# Or from Docker container
docker compose exec airflow-webserver psql -h your-rds-endpoint -U postgres -d silver_db
```

## Verification


### . Verify Data in Snowflake

```sql
-- In Snowflake worksheet
USE WAREHOUSE CORETELECOMS_WH;
USE DATABASE CORETELECOMS_DB;
USE SCHEMA STG;

-- Check staging tables
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM agents;
SELECT COUNT(*) FROM call_logs;
SELECT COUNT(*) FROM sm_complaints;
SELECT COUNT(*) FROM web_complaints;

-- Check analytics schema (after dbt runs)
USE SCHEMA ANALYTICS;

SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM dim_agents;
SELECT COUNT(*) FROM dim_date;
SELECT COUNT(*) FROM fact_unified_complaint;
SELECT COUNT(*) FROM fact_accumulating_snapshot;
```



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




RDS Conformance Layer
The source data comes from five independent sources don't communicate with each other and have no mechanism to enforce referential integrity.
A complaint could potentially reference a customer ID that doesn't exist, or an agent ID that was never loaded into the system. Without a conformance layer, these orphaned records would silently corrupt the dimensional model, making analytics unreliable.

I namtain an AWS provisioned postgres database to serve as the enforcement point for data integrity through foreign key constraints. 

More importantly, when complaints are loaded daily, PostgreSQL's foreign key constraints automatically validate that every complaint references valid customer and agent records that exist in the dimension tables.

PostgreSQL provides ACID guarantees that ensure either a batch loads completely with all foreign keys satisfied, or it fails atomically without partial corruption. 

While PostgreSQL's foreign key constraints provide automatic enforcement, allowing the database to reject entire batches would be disruptive to the pipeline. 
To avoid this, I implement a quarantine pattern that detects orphaned records before attempting the database load. When complaint data arrives, i first compare the incoming customer and agent keys against the existing dimension tables in the conformance layer. Records that reference non-existent foreign keys are identified and quarantined into a separate orphaned_complaints table before the main load begins. 

This ensures that only referentially valid complaints reach the conformance layer, preventing foreign key violations from ever occurring.





he RDS conformance layer plays a crucial role in tracking how complaints evolve through their lifecycle. Complaints don't arrive once and remain static. They change status as they move from "backlog" to "in-progress" to "resolved", they get assigned to different agents, and their details may be updated as more information becomes available. Each day, when new complaint data arrives, it may contain updates to complaints that had been loaded before rather than entirely new complaints.

The conformance captures changes by maintaining the current state of all complaints in PostgreSQL. When a complaint comes through with an updated resolution status, the RDS layer records this change. The dimensional model in Snowflake can then detect these changes through incremental loading patterns implemented in dbt. This architecture enabled me to build accumulating snapshots for complaint facts, allowing me to ansIr questions like "how long did this complaint spend in each status?" 





### Data Quality
- **Three-Class Architecture**: Cleaner (value-level), DataQualityChecker (DataFrame-level), Transformer (orchestration)
- **Double-Pass Validation**: Preserve original invalid values for investigation while transforming clean data
- **Comprehensive Tracking**: Field-level granularity showing which fields failed and their original values
- **Problematic Data Persistence**: Upload invalid records to S3 with lineage manifests
- **148+ Unit Tests**: Comprehensive test coverage for all transformation logic

### Observability
- **Lineage Tracking**: Every file includes manifest with source, transformation, and destination metadata
- **XCom Integration**: Metadata flows through Airflow for monitoring and downstream task decisions
- **Change Detection**: RDS conformance layer tracks complaint state changes over time
- **Audit Trail**: Complete history of data movement through every pipeline layer

---

## Prerequisites

- **Docker** and **Docker Compose** (v2.0+)
- **AWS Account** with permissions for S3, RDS, IAM
- **Terraform** (v1.0+)
- **AWS CLI** configured with appropriate credentials
- **Google Cloud Service Account** with Sheets API enabled
- **PostgreSQL RDS Instance** (for Silver conformance layer)
- **Source Data Access** (S3 buckets, PostgreSQL database, Google Sheets)

---

## Installation

### 1. Clone the Repository
```bash
## Installation

### 1. Clone the Repository
```bashgit clone https://github.com/DesayoEm/CoreTelecoms_customer_complaint_pipeline.git
cd CoreTelecoms_customer_complaint_pipeline
```

### 2. Infrastructure Setup with Terraform

Navigate to the infrastructure directory and deploy AWS resources:
```bash
cd infraAuthenticate with AWS (use your organization's SSO or CLI method)
aws sso login --profile your-profile
OR
aws configureInitialize Terraform
terraform initReview the execution plan
terraform planApply infrastructure (creates S3 buckets, IAM roles, RDS instance)
terraform applyNote the output values - you'll need these for configuration
````
**Terraform will create:**
- Bronze S3 bucket for raw data
- IAM roles and policies for Airflow
- Security groups for RDS access
- RDS PostgreSQL instance (Silver layer)
- VPC configuration (if not using default)

**Save the Terraform outputs** - they contain connection strings and resource names needed for configuration.

### 3. Configure Environment Variables

Create a `.env` file in the project root:
```bashGoogle Sheets Configuration
GOOGLE_SHEET_ID=your_google_sheet_id_here
GOOGLE_SERVICE_ACCOUNT_PATH=/opt/airflow/config/service-account.jsonAWS Credentials (Destination/Airflow)
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=us-east-1AWS Source Credentials (if different from destination)
SRC_ACCESS_KEY_ID=your_src_access_key
SRC_SECRET_ACCESS_KEY=your_src_secret_key
SRC_REGION=us-east-1Database Connection Strings
SRC_DB_CONN_STRING=postgresql://user:password@source-host:5432/source_db
SRC_DB_SCHEMA=public
SILVER_DB_CONN_STRING=postgresql://user:password@rds-endpoint:5432/silver_dbS3 Bronze Layer Configuration (from Terraform outputs)
BRONZE_BUCKET=your-bronze-bucket-nameS3 Staging Destinations (paths within Bronze bucket)
CUSTOMER_DATA_STAGING_DEST=bronze/customers
AGENT_DATA_STAGING_DEST=bronze/agents
CALL_LOGS_STAGING_DEST=bronze/call_logs
SM_COMPLAINTS_STAGING_DEST=bronze/social_media_complaints
WEB_COMPLAINTS_STAGING_DEST=bronze/web_complaintsObject Prefixes (file naming convention)
CUSTOMER_DATA_OBJ_PREFIX=customers-
AGENT_DATA_OBJ_PREFIX=agents-
CALL_LOGS_OBJ_PREFIX=call-logs-
SM_COMPLAINTS_OBJ_PREFIX=sm-complaints-
WEB_COMPLAINTS_OBJ_PREFIX=web-complaints-Source S3 Configuration
SRC_BUCKET_NAME=your-source-bucket-name
SRC_CUSTOMERS_OBJ_KEY=raw/customers.csv
SRC_CALL_LOGS_OBJ_KEY=raw/call_logs.csv
SRC_SM_COMPLAINTS_OBJ_KEY=raw/social_media_complaints.json

**⚠️ Security Note:** Never commit the `.env` file to version control. It's already included in `.gitignore`.

### 4. Initialize Database Schema

Create the Silver layer schema in your RDS PostgreSQL instance:
```bashConnect to your RDS instance
psql -h your-rds-endpoint.region.rds.amazonaws.com -U postgres -d silver_dbRun the schema initialization script
\i scripts/init_silver_schema.sql

The schema script creates:
- **Dimension tables**: `customers`, `agents` (with SCD Type 2 columns)
- **Fact tables**: `web_complaints`, `sm_complaints`, `call_logs`
- **Quarantine table**: `orphaned_complaints` (for foreign key violations)
- **Staging schema**: `staging.customers`, `staging.agents`, etc.

### 5. Start Airflow
```bashReturn to project root
cd ..Initialize Airflow database (first time only)
docker-compose up airflow-initStart all Airflow services
docker-compose up -d

**Services started:**
- Airflow Webserver (http://localhost:8080)
- Airflow Scheduler
- Airflow Worker(s)
- PostgreSQL (Airflow metadata)
- Redis (Celery executor)

---

## Configuration

### Airflow Connections

Access the Airflow UI at `http://localhost:8080` (default credentials: `admin` / `admin`).

Navigate to **Admin → Connections** and create the following:

#### 1. AWS Destination Connection

- **Connection ID**: `aws_airflow_dest_user`
- **Connection Type**: `AWS`
- **AWS Access Key ID**: Your destination AWS access key
- **AWS Secret Access Key**: Your destination AWS secret key
- **Extra**: `{"region_name": "us-east-1"}`

#### 2. AWS Source Connection

- **Connection ID**: `aws_airflow_src_user`
- **Connection Type**: `AWS`
- **AWS Access Key ID**: Your source AWS access key
- **AWS Secret Access Key**: Your source AWS secret key
- **Extra**: `{"region_name": "us-east-1"}`

#### 3. RDS PostgreSQL Connection

- **Connection ID**: `rds_postgres`
- **Connection Type**: `Postgres`
- **Host**: Your RDS endpoint (from Terraform output)
- **Database**: `silver_db`
- **Login**: Database username
- **Password**: Database password
- **Port**: `5432`

#### 4. Slack (Optional - for notifications)

- **Connection ID**: `slack`
- **Connection Type**: `Slack`
- **Webhook URL**: Your Slack webhook URL

#### 5. Snowflake (Optional - for Gold layer)

- **Connection ID**: `snowflake`
- **Connection Type**: `Snowflake`
- **Account**: Your Snowflake account
- **Warehouse**: Your warehouse name
- **Database**: Your database name
- **Schema**: Your schema name
- **Login**: Username
- **Password**: Password

### Google Service Account

Place your Google service account JSON file in the project:
```bashmkdir -p config
cp /path/to/your/service-account.json config/service-account.json

Ensure the service account has:
- Google Sheets API enabled
- Read access to the target Google Sheet

---

## Running the Pipeline

### Manual Trigger

1. Navigate to **DAGs** page in Airflow UI
2. Locate `etl_pipeline`
3. Toggle to **ON** if paused
4. Click **▶️ Play** button → **Trigger DAG**
5. Optionally provide execution date or use default (today)

### Scheduled Execution

## Pre-Deployment Checklist

Before deploying to production:

#### Security

- [ ] Change default Airflow credentials in `docker-compose.yml`
- [ ] Migrate credentials from `.env` to AWS Secrets Manager
- [ ] Enable Airflow authentication (LDAP, OAuth, or RBAC)
- [ ] Rotate all service account keys and API tokens
- [ ] Enable encryption at rest for S3 buckets
- [ ] Enable encryption in transit for RDS (SSL/TLS)
- [ ] Implement least-privilege IAM policies
- [ ] Enable MFA for AWS console access

#### Monitoring

- [ ] Configure Slack notifications for pipeline failures
- [ ] Set up CloudWatch alarms for:
  - Pipeline duration exceeds threshold
  - Task failure rate exceeds threshold
  - RDS CPU/memory/storage usage
  - S3 bucket size growth rate
- [ ] Enable Airflow logs to S3 for long-term retention
- [ ] Configure Datadog/New Relic for application monitoring (optional)
- [ ] Set up alerting for quarantined record volume spikes

#### Data Management

- [ ] Configure S3 lifecycle policies:
  - Transition Bronze data to Glacier after 90 days
  - Delete problematic data files after 1 year
  - Retain manifests indefinitely
- [ ] Set up RDS automated backups (daily snapshots, 7-day retention)
- [ ] Test disaster recovery:
  - Simulate RDS failure and restore from backup
  - Simulate S3 data loss and recovery from replication
- [ ] Document data retention policies
- [ ] Implement data deletion procedures for compliance (GDPR, CCPA)

#### Performance

- [ ] Load test with production data volumes
- [ ] Optimize batch sizes based on actual performance
- [ ] Configure auto-scaling for Airflow workers (if using ECS/EKS)
- [ ] Set up RDS read replicas for analytics queries
- [ ] Enable query performance insights on RDS
- [ ] Implement connection pooling for database connections

#### Operations

- [ ] Document runbooks for common failure scenarios
- [ ] Create on-call rotation and escalation procedures
- [ ] Set up regular data quality audits
- [ ] Schedule quarterly disaster recovery drills
- [ ] Implement change management process for pipeline updates
- [ ] Create rollback procedures for failed deployments

### Infrastructure as Code

All infrastructure is defined in Terraform under the `infra/` directory:
```
infra/
├── main.tf              # Main configuration
├── variables.tf         # Input variables
├── outputs.tf           # Output values
├── s3.tf               # S3 bucket definitions
├── rds.tf              # RDS PostgreSQL instance
├── iam.tf              # IAM roles and policies
├── vpc.tf              # VPC and networking (optional)
└── backend.tf          # Terraform state backend
```

# CoreTelecoms Customer Complaint Pipeline

## Overview
Core Telecoms, a leading telecommunications provider, needed to be proactive with customer support. 
But with complaint data scattered across Postgres databases, CSV dumps, and JSON streams, the company couldn't identify at-risk customers or predict escalations before they happened.

This solution delivers a unified data platform that:
- Ingests data from 5 separate sources into a single source of truth for customer interactions
- Ensures data integrity through referential integrity checks within a conformance layer
- Delivers a dimensional model optimized for analytics and ML feature sets


## Table of Contents

- [Architecture](#architecture)
- [Architectural Decisions](#architectural-decisions)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the Pipeline](#running-the-pipeline)

---

## Architecture

### High-Level Data Flow
**Three-Layer Medallion Architecture:**
- **Bronze Layer (S3)**: Raw data extracted from sources, converted to Parquet with lineage manifests
- **Silver Layer (RDS PostgreSQL)**: Validated, conformed data with enforced referential integrity and change tracking
- **Gold Layer (Snowflake)**: Dimensional model optimized for analytics with SCD Type 2 for dimensions

### Data Sources

1. **Customers** - CSV files from S3
2. **Agents** - Google Sheets via API
3. **Ib Complaints** - PostgreSQL database tables
4. **Social Media Complaints** - JSON files from S3
5. **Call Logs** - CSV files from S3

---

## Architectural Decisions

### Orchestration: Conditional Execution and Shortest Path Execution

**Conditional execution**:The DAG uses Airflow's branching operator with state management via Airflow Variables to implement conditional execution:
- Static reference data (customers, agents) loads once on initial run
- Dynamic complaint data loads daily

This significantly reduces processing time and resource consumption on subsequent runs by skipping unnecessary reprocessing of unchanging data. 

**Shortest path execution**: Tasks have dependencies only when necessary and run in parallel wherever possible


### Extraction: Multi-Class Implementation

I Implemented separate extractor classes rather than a single generic extractor because each source has fundamentally different connection patterns:

- **S3Extractor**: Handles file-based sources (CSV, JSON, Excel)
- **SQLExtractor**: Handles PostgreSQL database tables
- **GoogleSheetsExtractor**: Handles Google Sheets API integration

**Shared Behavior**: All extractors convert data to Parquet and generate manifest files for lineage tracking.
This isolates authentication and connection logic  isolated while sharing common behaviors.

---
### Transformation: 
#### Three-Class Architecture

The transformation layer also implements a three-class architecture that separates value cleaning, DataFrame-level validation, and pipeline orchestration into distinct classes with single responsibilities.

**DataCleaner:** Handles individual field cleaning (email validation, phone formatting, timestamp parsing). Contains no orchestration logic—pure transformation functions.

**DataQualityChecker:** Examines raw data and identifies records with validation failures.
It then creates a separate DataFrame of problematic records with original invalid values intact.

**Transformer:**: The Transformer class doesn't do any actual cleaning or validation. Instead, it focuses on workflow: deciding whether to process data in batches or all at once, managing checkpoint state, applying transformations in the correct order, uploading problematic records to S3, and generating  metadata about the transformation run.

#### Double-Pass Strategy
I run validation logic twice, once to identify and preserve problematic data, and again to actually clean the data. This causes a some latency, but the performance cost of this double pass is justified by the observability it provides.

In the first pass, the DataQualityChecker examines the raw data and identifies records with validation failures, and  creates a separate DataFrame of problematic records that still contains the original invalid values. This DataFrame gets uploaded to S3 with complete lineage metadata, creating a permanent record of what data quality issues existed in the source.

In the second pass, the Transformer applies the Cleaner methods to fix or null out invalid values. These cleaned records proceed to the conformance layer for loading. Problematic original data is preserved for investigation, and clean data is loaded without losing information about what Int wrong.

**Trade-off**: Performance cost justified by observability. I maintain a complete audit trail of data quality issues while ensuring clean data reaches the conformance layer.

#### Parallel Processing with ThreadPool

To optimize performance for datasets exceeding 100,000 rows, the transformer switches to parallel processing using Python's ThreadPool. 
I chose ThreadPool over ProcessPoolExecutor due to a specific Airflow constraint: Airflow workers are daemonic processes, and can’t spawn child processes. 

While ThreadPool is limited by Python's Global Interpreter Lock (GIL) for pure Python operations, it still provides  performance improvements for some of the transformation workload as some of my cleaning operations like regex pattern matching in email cleaning release the GIL. 

**Trade-off**: ThreadPool is limited by Python's GIL for pure Python operations, but still provides performance gains for I/O-bound operations and regex pattern matching (which releases the GIL).

---
## Testing
- **140+ Unit Tests**: Comprehensive test coverage for all transformation logic


### Loading: Batched Transform-Load with Checkpointing

#### Batched Transform-Load with Checkpointing
The loading layer implements checkpoint-based recovery enabling resumption from the last successfully loaded batch after failure. This makes the flow of data more resistant to failure and
also reduces resource consumption. in the event of a failure, the maximum amount of rows that will require reprocessing is 100,000.

**Components**:
- **Loader**: Manages data loading to RDS, maintains state about completed batches and rows loaded
- **StateLoader**: Stateless utility retrieving checkpoint information from Airflow Variable on retry
- **LoadState**: Type-safe dataclass representing checkpoint state

After each batch successfully loads, `save_checkpoint()` pushes state to Airflow Variables. On failure, this checkpoint persists and is retrieved on the next retry.

**Batch Size**: 100,000 rows per batch enables handling large datasets without memory constraints, allowing the pipeline to scale without requiring larger EC2 instances.

---

### Conformance Layer: RDS PostgreSQL for FK Enforcement

The five independent data sources have no mechanism to enforce referential integrity. Without a conformance layer, orphaned records would silently corrupt the dimensional model.

**Solution**: I created and maintain an AWS RDS PostgreSQL database that serves as the enforcement point through foreign key constraints. When complaints load daily, PostgreSQL automatically validates that every complaint references valid customer and agent records.

**ACID Guarantees**: PostgreSQL ensures batches either load completely with all FKs satisfied or fail atomically without partial corruption.

**Quarantine Pattern**: Rather than allowing FK violations to reject entire batches, I detect orphaned records *before* attempting the database load. Records referencing non-existent foreign keys are quarantined to `data_quality_quarantine`, ensuring only referentially valid data reaches the conformance layer.

**Change Tracking**: The conformance layer tracks complaint lifecycle evolution (status changes, agent reassignments, detail updates). When updated complaints arrive, Postgres records the changes, enabling the dimensional model in Snowflake to build accumulating snapshots through dbt incremental loading patterns.

**Result**: Analysts can answer questions like "how long did this complaint spend in each status?" with confidence in data integrity.

---



## Prerequisites

[To be added]

## Installation

[To be added]

## Running the Pipeline

[To be added]
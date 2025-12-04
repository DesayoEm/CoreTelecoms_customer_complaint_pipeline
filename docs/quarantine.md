## Proactive Quarantine Before Load

The system detects FK violations *before* attempting the database load. Invalid records are isolated to a `data_quality_quarantine` table, 
while valid records proceed to the conformance layer.

**Key Insight**: I reject orphaned data myself before Postgres rejects it, and disrupts the pipeline,


### Implementation

#### 1. Quarantine Table Schema
```sql
CREATE TABLE data_quality_quarantine (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255),           -- Which target table failed
    issue_type VARCHAR(100),            -- 'fk_violation', 'schema_error', etc.
    record_data JSONB,                  -- Complete original record
    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN DEFAULT FALSE
);

```

**JSONB** Preserves complete original record for investigation without schema coupling. Can store records from any source table.

#### 2. FK Validation Query (Before Load)
```python
def load_call_logs_with_fk_validation(self, conn, staging_table, target_table):
    """
    Loads call logs with FK validation.
    Quarantines violations, proceeds with valid records.
    """
    
    # STEP 1: Identify and quarantine FK violations
    quarantine_result = conn.execute(
        text(
            f"""
            INSERT INTO data_quality_quarantine (table_name, issue_type, record_data)
            SELECT 
                :table_name,
                'fk_violation',
                row_to_json(s.*)::jsonb
            FROM {staging_table} s
            LEFT JOIN conformed_customers c ON s.customer_id = c.customer_id
            LEFT JOIN conformed_agents a ON s.agent_id = a.agent_id
            WHERE c.customer_id IS NULL OR a.agent_id IS NULL
            """
        ),
        {"table_name": target_table},
    )
    
    violations_quarantined = quarantine_result.rowcount
    if violations_quarantined > 0:
        log.warning(f"Quarantined {violations_quarantined} records with FK violations")
    
    # STEP 2: Load only valid records
    result = conn.execute(
        text(
            f"""
            INSERT INTO {target_table}
            SELECT s.* 
            FROM {staging_table} s
            INNER JOIN conformed_customers c ON s.customer_id = c.customer_id
            INNER JOIN conformed_agents a ON s.agent_id = a.agent_id
            ON CONFLICT (call_log_key) DO UPDATE SET
                resolution_status = EXCLUDED.resolution_status,
                last_updated_at = CURRENT_TIMESTAMP,
                created_at = {target_table}.created_at
            """
        )
    )
    
    rows_inserted = result.rowcount
    self.rows_loaded += rows_inserted
    log.info(f"Loaded {rows_inserted} valid records to {target_table}")
```

**Critical Detail**: The `LEFT JOIN ... WHERE ... IS NULL` pattern identifies orphaned records. These records join successfully to staging but fail to find matching parent records.

#### 3. Query Breakdown

**Part A: Find Orphans**
```sql
SELECT s.*
FROM staging_call_logs s
LEFT JOIN conformed_customers c ON s.customer_id = c.customer_id
LEFT JOIN conformed_agents a ON s.agent_id = a.agent_id
WHERE c.customer_id IS NULL OR a.agent_id IS NULL
```

**Logic**:
- `LEFT JOIN` preserves all staging records, even if parent doesn't exist
- `WHERE ... IS NULL` filters to records where join failed
- Result: Records referencing non-existent customers/agents

**Part B: Quarantine**
```sql
INSERT INTO data_quality_quarantine (table_name, issue_type, record_data)
SELECT 
    'conformed_call_logs',
    'fk_violation',
    row_to_json(s.*)::jsonb  -- Preserves complete record as JSON
FROM [... same query as Part A ...]
```

**Part C: Load Valid Records**
```sql
INSERT INTO conformed_call_logs
SELECT s.*
FROM staging_call_logs s
INNER JOIN conformed_customers c ON s.customer_id = c.customer_id  -- Must exist
INNER JOIN conformed_agents a ON s.agent_id = a.agent_id          -- Must exist
```

**Logic**: `INNER JOIN` only includes records where *both* FKs are valid. Orphaned records were already quarantined, so this loads the clean subset.

---

### Why This Works

**1. Atomicity Per Record Type**
Each record type (valid/invalid) gets its own transaction:
- Quarantine transaction: Inserts orphaned records
- Load transaction: Inserts valid records

Both succeed independently. No coupling.

**2. Idempotency**
Re-running the load:
- Load: `ON CONFLICT ... DO UPDATE` handles duplicates gracefully

**3. Zero Downtime**
Pipeline never blocks. Even if 100% of records are invalid, pipeline completes successfully (just with 100% quarantine rate, triggering alerts).

---


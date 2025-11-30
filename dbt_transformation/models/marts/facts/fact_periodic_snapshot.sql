{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

with complaint_sources as (
    -- Get all complaints regardless of source
    select
        complaint_key,
        complaint_id,
        customer_id,
        agent_id,
        complaint_date,
        complaint_type,
        last_updated_at,
        loaded_at
    from {{ ref('fact_complaint_transaction') }}
),

-- Get current status from raw (most recent state)
call_logs_status as (
    select
        call_log_key as complaint_key,
        resolution_status,
        call_end_time as resolution_date,
        loaded_at as snapshot_date
    from {{ source('raw', 'call_logs') }}
),

sm_complaints_status as (
    select
        sm_complaint_key as complaint_key,
        resolution_status,
        resolution_date,
        loaded_at as snapshot_date
    from {{ source('raw', 'sm_complaints') }}
),

web_complaints_status as (
    select
        web_complaint_key as complaint_key,
        resolution_status,
        resolution_date,
        loaded_at as snapshot_date
    from {{ source('raw', 'web_complaints') }}
),

all_statuses as (
    select * from call_logs_status
    union all
    select * from sm_complaints_status
    union all
    select * from web_complaints_status
),

-- Get latest status for each complaint
latest_status as (
    select
        complaint_key,
        resolution_status,
        resolution_date,
        snapshot_date,
        row_number() over (partition by complaint_key order by snapshot_date desc) as rn
    from all_statuses
),

current_status as (
    select
        complaint_key,
        resolution_status,
        resolution_date,
        snapshot_date
    from latest_status
    where rn = 1
),

-- Calculate age metrics
metrics as (
    select
        cs.complaint_key,
        cs.complaint_id,
        cs.customer_id,
        cs.agent_id,
        cs.complaint_date,
        cs.complaint_type,
        
        -- Snapshot metadata
        current_date() as snapshot_date,
        
        -- Current status
        s.resolution_status,
        s.resolution_date,
        
        -- Age metrics (as of snapshot date)
        datediff('day', cs.complaint_date, current_date()) as days_since_creation,
        
        case 
            when s.resolution_status in ('resolved', 'closed') 
            then datediff('day', cs.complaint_date, s.resolution_date)
            else null
        end as days_to_resolution,
        
        -- Status flags for aggregation
        case when s.resolution_status = 'open' then 1 else 0 end as is_open,
        case when s.resolution_status = 'assigned' then 1 else 0 end as is_assigned,
        case when s.resolution_status = 'in_progress' then 1 else 0 end as is_in_progress,
        case when s.resolution_status = 'resolved' then 1 else 0 end as is_resolved,
        case when s.resolution_status = 'closed' then 1 else 0 end as is_closed,
        
        -- SLA flags (assuming 7-day SLA)
        case 
            when s.resolution_status not in ('resolved', 'closed') 
                and datediff('day', cs.complaint_date, current_date()) > 7 
            then 1 
            else 0 
        end as is_overdue,
        
        case 
            when s.resolution_status in ('resolved', 'closed')
                and datediff('day', cs.complaint_date, s.resolution_date) <= 7
            then 1
            else 0
        end as met_sla,
        
        cs.loaded_at
        
    from complaint_sources cs
    left join current_status s on cs.complaint_key = s.complaint_key
)

select * from metrics
{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

with complaint_sources as (
    -- all complaints from sources
    select
        complaint_key,
        customer_key,
        agent_key,
        complaint_date,
        complaint_type,
        last_updated_at,
        loaded_at,
        created_at
    from {{ ref('fact_complaint_transaction') }}
),


call_logs_status as (
    select
        call_log_key as complaint_key,
        resolution_status,
        call_end_time as resolution_date,
        loaded_at as snapshot_date
    from {{ source('stg', 'call_logs') }}
),

sm_complaints_status as (
    select
        sm_complaint_key as complaint_key,
        resolution_status,
        resolution_date,
        loaded_at as snapshot_date
    from {{ source('stg', 'sm_complaints') }}
),

web_complaints_status as (
    select
        web_complaint_key as complaint_key,
        resolution_status,
        resolution_date,
        loaded_at as snapshot_date
    from {{ source('stg', 'web_complaints') }}
),

all_statuses as (
    select * from call_logs_status
    union all
    select * from sm_complaints_status
    union all
    select * from web_complaints_status
),


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

--  days passed
metrics as (
    select
        cs.complaint_key,
        cs.customer_key,
        cs.agent_key,
        cs.complaint_date,
        cs.complaint_type,
        
    
        current_date() as snapshot_date,
        s.resolution_status,
        s.resolution_date,
        
        datediff('day', cs.complaint_date, current_date()) as days_since_complaint,
        
        case 
            when s.resolution_status in ('resolved', 'closed') 
            then datediff('day', cs.complaint_date, s.resolution_date)
            else null
        end as days_to_resolution,
        
        -- flags for aggregation
        case when s.resolution_status = 'open' then 1 else 0 end as is_open,
        case when s.resolution_status = 'assigned' then 1 else 0 end as is_assigned,
        case when s.resolution_status = 'in_progress' then 1 else 0 end as is_in_progress,
        case when s.resolution_status = 'resolved' then 1 else 0 end as is_resolved,
        case when s.resolution_status = 'closed' then 1 else 0 end as is_closed,
        
        -- 3 day sla 
        case 
            when s.resolution_status not in ('resolved', 'closed') 
                and datediff('day', cs.complaint_date, current_date()) > 3 
            then 1 
            else 0 
        end as is_overdue,
        
        case 
            when s.resolution_status in ('resolved', 'closed')
                and datediff('day', cs.complaint_date, s.resolution_date) <= 3
            then 1
            else 0
        end as met_sla,
        
        cs.loaded_at
        
    from complaint_sources cs
    left join current_status s on cs.complaint_key = s.complaint_key
)

select * from metrics
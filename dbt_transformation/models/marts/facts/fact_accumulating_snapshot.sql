{{
    config(
        materialized='table',
        schema='analytics',
        unique_key='complaint_key'
    )
}}

with complaint_base as (
    select
        complaint_key,
        complaint_id,
        customer_id,
        agent_id,
        complaint_date,
        complaint_type
    from {{ ref('fact_complaint_transaction') }}
),

call_logs_lifecycle as (
    select
        call_log_key as complaint_key,
        request_date as created_date,
        case when resolution_status in ('assigned', 'in_progress', 'resolved', 'closed') 
            then request_date end as assigned_date,
        case when resolution_status in ('in_progress', 'resolved', 'closed') 
            then request_date end as in_progress_date,
        case when resolution_status in ('resolved', 'closed') 
            then call_end_time end as resolved_date,
        case when resolution_status = 'closed' 
            then call_end_time end as closed_date,
        resolution_status as current_status,
        loaded_at
    from {{ source('raw', 'call_logs') }}
),

sm_complaints_lifecycle as (
    select
        sm_complaint_key as complaint_key,
        request_date as created_date,
        case when resolution_status in ('assigned', 'in_progress', 'resolved', 'closed') 
            then request_date end as assigned_date,
        case when resolution_status in ('in_progress', 'resolved', 'closed') 
            then request_date end as in_progress_date,
        case when resolution_status in ('resolved', 'closed') 
            then resolution_date end as resolved_date,
        case when resolution_status = 'closed' 
            then resolution_date end as closed_date,
        resolution_status as current_status,
        loaded_at
    from {{ source('raw', 'sm_complaints') }}
),

web_complaints_lifecycle as (
    select
        web_complaint_key as complaint_key,
        request_date as created_date,
        case when resolution_status in ('assigned', 'in_progress', 'resolved', 'closed') 
            then request_date end as assigned_date,
        case when resolution_status in ('in_progress', 'resolved', 'closed') 
            then request_date end as in_progress_date,
        case when resolution_status in ('resolved', 'closed') 
            then resolution_date end as resolved_date,
        case when resolution_status = 'closed' 
            then resolution_date end as closed_date,
        resolution_status as current_status,
        loaded_at
    from {{ source('raw', 'web_complaints') }}
),

all_lifecycle as (
    select * from call_logs_lifecycle
    union all
    select * from sm_complaints_lifecycle
    union all
    select * from web_complaints_lifecycle
),


latest_lifecycle as (
    select
        complaint_key,
        created_date,
        assigned_date,
        in_progress_date,
        resolved_date,
        closed_date,
        current_status,
        row_number() over (partition by complaint_key order by loaded_at desc) as rn
    from all_lifecycle
),

current_lifecycle as (
    select
        complaint_key,
        created_date,
        assigned_date,
        in_progress_date,
        resolved_date,
        closed_date,
        current_status
    from latest_lifecycle
    where rn = 1
),

-- Join with complaint base and calculate metrics
final as (
    select
        cb.complaint_key,
        cb.complaint_id,
        cb.customer_id,
        cb.agent_id,
        cb.complaint_type,
        
        -- Milestone dates
        cl.created_date,
        cl.assigned_date,
        cl.in_progress_date,
        cl.resolved_date,
        cl.closed_date,
        
        -- Current status
        cl.current_status,
        
        -- Lag calculations (days between milestones)
        datediff('day', cl.created_date, cl.assigned_date) as days_created_to_assigned,
        datediff('day', cl.assigned_date, cl.in_progress_date) as days_assigned_to_in_progress,
        datediff('day', cl.in_progress_date, cl.resolved_date) as days_in_progress_to_resolved,
        datediff('day', cl.resolved_date, cl.closed_date) as days_resolved_to_closed,
        
        -- Total lifecycle time
        datediff('day', cl.created_date, cl.resolved_date) as days_created_to_resolved,
        datediff('day', cl.created_date, cl.closed_date) as days_created_to_closed,
        
        -- Time in current stage (for open complaints)
        case 
            when cl.current_status = 'open' 
            then datediff('day', cl.created_date, current_date())
            when cl.current_status = 'assigned' 
            then datediff('day', cl.assigned_date, current_date())
            when cl.current_status = 'in_progress' 
            then datediff('day', cl.in_progress_date, current_date())
            else null
        end as days_in_current_stage,
        
        -- SLA flags (7-day resolution SLA)
        case 
            when cl.current_status in ('resolved', 'closed')
                and datediff('day', cl.created_date, cl.resolved_date) <= 7
            then 1
            when cl.current_status not in ('resolved', 'closed')
                and datediff('day', cl.created_date, current_date()) > 7
            then 0
            else null
        end as met_resolution_sla,
        
        -- Status flags
        case when cl.current_status = 'open' then 1 else 0 end as is_open,
        case when cl.current_status = 'assigned' then 1 else 0 end as is_assigned,
        case when cl.current_status = 'in_progress' then 1 else 0 end as is_in_progress,
        case when cl.current_status = 'resolved' then 1 else 0 end as is_resolved,
        case when cl.current_status = 'closed' then 1 else 0 end as is_closed,
        
        current_timestamp() as snapshot_timestamp
        
    from complaint_base cb
    left join current_lifecycle cl on cb.complaint_key = cl.complaint_key
)

select * from final
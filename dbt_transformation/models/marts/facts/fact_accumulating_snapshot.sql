{{
    config(
        materialized='incremental',
        schema='analytics',
        unique_key='complaint_key'
    )
}}

with complaint_base as (
    select
        complaint_key,
        customer_key,
        complaint_date_key,
        agent_key,
        complaint_type
    from {{ ref('fact_unified_complaint') }}
),

date_enriched as (
    select
        cb.*,
        d.date_day as full_date,
        d.day_of_week,
    from complaint_base cb
    left join coretelecoms_db.analytics.dim_date d
        on cb.complaint_date_key = d.date_key
),

current_lifecycle as (
    -- Dummy placeholder CTE to allow compilation
    select
        complaint_key,
        'open'::string as current_status,
        current_date() as created_date,
        current_date() as assigned_date,
        current_date() as in_progress_date,
        current_date() as resolved_date,
        current_date() as closed_date
    from complaint_base
),

final as (
    select
        de.complaint_key,
        de.customer_key,
        de.complaint_date_key,
        de.agent_key,
        de.complaint_type,
        de.full_date,
        de.day_of_week,
        cl.created_date,
        cl.assigned_date,
        cl.in_progress_date,
        cl.resolved_date,
        cl.closed_date,
        datediff('day', cl.created_date, cl.assigned_date) as days_created_to_assigned,
        datediff('day', cl.assigned_date, cl.in_progress_date) as days_assigned_to_in_progress,
        datediff('day', cl.in_progress_date, cl.resolved_date) as days_in_progress_to_resolved,
        datediff('day', cl.resolved_date, cl.closed_date) as days_resolved_to_closed,
        datediff('day', cl.created_date, cl.resolved_date) as days_created_to_resolved,
        datediff('day', cl.created_date, cl.closed_date) as days_created_to_closed,
        case
            when cl.current_status = 'open' then datediff('day', cl.created_date, current_date())
            when cl.current_status = 'assigned' then datediff('day', cl.assigned_date, current_date())
            when cl.current_status = 'in_progress' then datediff('day', cl.in_progress_date, current_date())
            else null
        end as days_in_current_stage,
        case 
            when cl.current_status in ('resolved', 'closed')
                and datediff('day', cl.created_date, cl.resolved_date) <= 3
            then 1
            when cl.current_status not in ('resolved', 'closed')
                and datediff('day', cl.created_date, current_date()) > 3
            then 0
            else null
        end as met_resolution_sla,
        case when cl.current_status = 'open' then 1 else 0 end as is_open,
        case when cl.current_status = 'assigned' then 1 else 0 end as is_assigned,
        case when cl.current_status = 'in_progress' then 1 else 0 end as is_in_progress,
        case when cl.current_status = 'resolved' then 1 else 0 end as is_resolved,
        case when cl.current_status = 'closed' then 1 else 0 end as is_closed,
        current_timestamp() as snapshot_timestamp
    from date_enriched de
    left join current_lifecycle cl 
        on de.complaint_key = cl.complaint_key
)

select * from final

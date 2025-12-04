{{
    config(
        materialized='incremental',
        schema='analytics',
        unique_key='complaint_key',
        merge_update_columns=[
            'backlog_date',
            'in_progress_date', 
            'resolved_date',
            'blocked_date',
            'resolution_status'
        ]
    )
}}

with current_lifecycle as (
    select
        complaint_key,
        complaint_date_key,
        cmpt_full_date,
        cmpt_day_of_week,
        complaint_date,
        customer_key,
        agent_key,
        complaint_type,
        complaint_category,
        resolution_status,
        last_updated_at,
        case when resolution_status = 'Backlog' then DATE(last_updated_at) else null end as backlog_date,
        case when resolution_status = 'In-Progress' then DATE(last_updated_at) else null end as in_progress_date,
        case when resolution_status = 'Resolved' then DATE(last_updated_at) else null end as resolved_date,
        case when resolution_status = 'Blocked' then DATE(last_updated_at) else null end as blocked_date
    from {{ ref('fact_unified_complaint') }}
),

final as (
    select
        cl.complaint_key,
        cl.complaint_date_key,
        cl.cmpt_full_date,
        cl.cmpt_day_of_week,
        cl.complaint_date,
        cl.customer_key,
        cl.agent_key,
        cl.complaint_type,
        cl.complaint_category,
        cl.resolution_status,
        cl.last_updated_at,
        
        {% if is_incremental() %}
        -- milestone date preservation
        coalesce(existing.backlog_date, cl.backlog_date) as backlog_date,
        coalesce(existing.in_progress_date, cl.in_progress_date) as in_progress_date,
        coalesce(existing.resolved_date, cl.resolved_date) as resolved_date,
        coalesce(existing.blocked_date, cl.blocked_date) as blocked_date,
        {% else %}
        cl.backlog_date,
        cl.in_progress_date,
        cl.resolved_date,
        cl.blocked_date,
        {% endif %}
        
        -- metrics
        datediff('day', cl.complaint_date, cl.in_progress_date) as days_initiated_to_in_progress,
        datediff('day', cl.complaint_date, cl.resolved_date) as days_initiated_to_resolved,
        datediff('day', cl.complaint_date, cl.blocked_date) as days_initiated_to_blocked,
        datediff('day', cl.complaint_date, cl.backlog_date) as days_initiated_to_backloged,

        datediff('day', cl.in_progress_date, cl.resolved_date) as days_in_progress_to_resolved,
        datediff('day', cl.in_progress_date, cl.backlog_date) as days_in_progress_to_backlog,
        datediff('day', cl.in_progress_date, cl.blocked_date) as days_in_progress_to_blocked,
        datediff('day', cl.backlog_date, cl.resolved_date) as days_backloged_to_resolved,
        
        case
            when cl.resolution_status = 'In-Progress' 
            then datediff('day', cl.in_progress_date, current_date())
            else null
        end as days_in_progress,

        case 
            when cl.resolution_status = 'Resolved'
                and datediff('day', cl.complaint_date, cl.resolved_date) <= 3
            then 1
            when cl.resolution_status != 'Resolved'
                and datediff('day', cl.complaint_date, current_date()) > 3
            then 0
            else null
        end as met_resolution_sla,

        case when cl.resolution_status = 'In-Progress' then 1 else 0 end as is_in_progress,
        case when cl.resolution_status = 'Resolved' then 1 else 0 end as is_resolved,
        case when cl.resolution_status = 'Backlog' then 1 else 0 end as is_backlog,
        case when cl.resolution_status = 'Blocked' then 1 else 0 end as is_blocked,
        
        current_timestamp() as snapshot_timestamp
        
    from current_lifecycle cl
    
    {% if is_incremental() %}
    left join {{ this }} existing 
        on cl.complaint_key = existing.complaint_key
    where cl.last_updated_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
)

select * from final
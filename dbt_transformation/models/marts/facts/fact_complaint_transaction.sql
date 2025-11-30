{{
    config(
        materialized='incremental',
        unique_key='complaint_key',
        on_schema_change='append_new_columns'
    )
}}

with call_logs_source as (
    select * from {{ source('raw', 'call_logs') }}
    {% if is_incremental() %}
    where loaded_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
),

sm_complaints_source as (
    select * from {{ source('raw', 'sm_complaints') }}
    {% if is_incremental() %}
    where loaded_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
),

web_complaints_source as (
    select * from {{ source('raw', 'web_complaints') }}
    {% if is_incremental() %}
    where loaded_at > (select max(last_updated_at) from {{ this }})
    {% endif %}
),

call_logs_transformed as (
    select
        call_log_key as complaint_key,
        call_id as complaint_id,
        customer_id,
        agent_id,
        complaint_category,
        request_date as complaint_date,  
        'call_log' as complaint_type,
        call_start_time,
        call_end_time,
        null as media_channel,
        call_logs_generation_date as complaint_generation_date,
        loaded_at
    from call_logs_source
),

sm_complaints_transformed as (
    select
        sm_complaint_key as complaint_key,
        complaint_id,
        customer_id,
        agent_id,
        complaint_category,
        request_date as complaint_date,
        'social_media' as complaint_type,
        media_channel,
        null as call_start_time,
        null as call_end_time,
        media_complaint_generation_date as complaint_generation_date,
        loaded_at
    from sm_complaints_source
),

web_complaints_transformed as (
    select
        web_complaint_key as complaint_key,
        request_id as complaint_id,
        customer_id,
        agent_id,
        complaint_category,
        null as call_start_time,
        null as call_end_time,
        null as media_channel,
        request_date,
        web_form_generation_date as complaint_generation_date,
        'web_form' as complaint_type,
        last_updated_at,
        loaded_at
    from web_complaints_source
),

final as (
    select * from call_logs_transformed
    union all
    select * from sm_complaints_transformed
    union all
    select * from web_complaints_transformed
)

select * from final
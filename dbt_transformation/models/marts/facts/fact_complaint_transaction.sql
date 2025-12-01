{{
    config(
        materialized="incremental",
        unique_key="complaint_key",
        on_schema_change="append_new_columns",
    )
}}

with
    call_logs_source as (
        select *
        from {{ source("raw", "call_logs") }}
        {% if is_incremental() %}
            where last_updated_at > (select max(last_updated_at) from {{ this }})
        {% endif %}
    ),

    sm_complaints_source as (
        select *
        from {{ source("raw", "sm_complaints") }}
        {% if is_incremental() %}
            where last_updated_at > (select max(last_updated_at) from {{ this }})
        {% endif %}
    ),

    web_complaints_source as (
        select *
        from {{ source("raw", "web_complaints") }}
        {% if is_incremental() %}
            where last_updated_at > (select max(last_updated_at) from {{ this }})
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
            last_updated_at,
            created_at,
            loaded_at
        from call_logs_source
    ),

    call_logs_fct as (
    select
        cl.complaint_key,
        cust.customer_key,
        agt.agent_key,
        to_number(to_char(cl.complaint_date, 'YYYYMMDD')) as complaint_date_key,
        cl.complaint_category,
        cl.complaint_type,
        cl.complaint_date,
        cl.call_start_time,
        cl.call_end_time,
        cl.media_channel,
        cl.complaint_generation_date,
        cl.last_updated_at,
        cl.created_at,
        cl.loaded_at
    from call_logs_transformed cl
    left join {{ ref("dim_customers") }} cust 
        on cl.customer_id = cust.customer_id
        and (cl.complaint_date <= cust.address_expiry_date or cust.is_active = 1)
    left join {{ ref("dim_agents") }} agt
        on cl.agent_id = agt.agent_id
            and cl.complaint_date >= agt.experience_effective_date
            and (cl.complaint_date <= agt.experience_expiry_date or agt.is_active = 1)
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
            null as call_start_time,
            null as call_end_time,
            media_channel,
            media_complaint_generation_date as complaint_generation_date,
            last_updated_at,
            created_at,
            loaded_at
        from sm_complaints_source
    ),

    sm_complaint_fct as (
        select
            sc.complaint_key,
            cust.customer_key,
            agt.agent_key,
            to_number(to_char(sc.complaint_date, 'YYYYMMDD')) as complaint_date_key,
            sc.complaint_category,
            sc.complaint_type,
            sc.complaint_date,
            sc.call_start_time,
            sc.call_end_time,
            sc.media_channel,
            sc.complaint_generation_date,
            sc.last_updated_at,
            sc.created_at,
            sc.loaded_at
        from sm_complaints_transformed sc
        left join {{ ref("dim_customers") }} cust on sc.customer_id = cust.customer_id
        left join
            {{ ref("dim_agents") }} agt
            on sc.agent_id = agt.agent_id
            and sc.complaint_date >= agt.experience_effective_date
            and (sc.complaint_date <= agt.experience_expiry_date or agt.is_active = 1)
    ),

    web_complaints_transformed as (
        select
            web_complaint_key as complaint_key,
            request_id as complaint_id,
            customer_id,
            agent_id,
            complaint_category,
            request_date as complaint_date,
            'web_form' as complaint_type,
            null as call_start_time,
            null as call_end_time,
            null as media_channel,
            web_form_generation_date as complaint_generation_date,
            last_updated_at,
            created_at,
            loaded_at
        from web_complaints_source
    ),

    web_complaint_fct as (
        select
            wc.complaint_key,
            cust.customer_key,
            agt.agent_key,
            to_number(to_char(wc.complaint_date, 'YYYYMMDD')) as complaint_date_key,
            wc.complaint_category,
            wc.complaint_type,
            wc.complaint_date,
            wc.call_start_time,
            wc.call_end_time,
            wc.media_channel,
            wc.complaint_generation_date,
            wc.last_updated_at,
            wc.created_at,
            wc.loaded_at
        from web_complaints_transformed wc
        left join {{ ref("dim_customers") }} cust on wc.customer_id = cust.customer_id
        left join
            {{ ref("dim_agents") }} agt
            on wc.agent_id = agt.agent_id
            and wc.complaint_date >= agt.experience_effective_date
            and (wc.complaint_date <= agt.experience_expiry_date or agt.is_active = 1)
    ),

    final as (
        select * from call_logs_fct
        union all
        select * from sm_complaint_fct
        union all
        select * from web_complaint_fct
    )

select * from final
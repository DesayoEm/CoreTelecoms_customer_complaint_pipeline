{{
    config(
        materialized="incremental",
        unique_key="feature_key",
        schema="feature_store",
        on_schema_change="append_new_columns"
    )
}}

{#
    COMPLAINT FEATURE STORE
    grain: One row per customer per calendar month
    
    Feature Categories:
    - Volume metrics (complaint counts by type/category)
    - Resolution metrics (resolution rates, time to resolution)
    - Behavioral patterns (channel preferences, time patterns)
    - Trend indicators (month-over-month changes)
    
    Refresh Strategy: Incremental by month - recomputes current month + lookback
#}

{% set lookback_months = 12 %}

with date_spine as (
    select distinct
        date_trunc('month', date_day)::date as feature_month,
        to_number(to_char(date_trunc('month', date_day), 'YYYYMMDD')) as feature_month_key
    from {{ ref("dim_date") }}
    where date_day <= current_date()
    {% if is_incremental() %}
        and date_trunc('month', date_day) >= (
            select dateadd('month', -1, max(feature_month)) 
            from {{ this }}
        )
    {% endif %}
),

complaints_base as (
    select
        complaint_key,
        customer_key,
        agent_key,
        complaint_date_key,
        complaint_category,
        complaint_type,
        complaint_date,
        call_start_time,
        call_end_time,
        media_channel,
        resolution_date,
        resolution_status,
        cmpt_day_of_week,
        cmpt_day_of_week_name,
        date_trunc('month', complaint_date)::date as complaint_month
    from {{ ref("fact_unified_complaint") }}
    {% if is_incremental() %}
        where date_trunc('month', complaint_date) >= (
            select dateadd('month', -1, max(feature_month)) 
            from {{ this }}
        )
    {% endif %}
),


-- CUSTOMER-MONTH GRAIN FEATURES


customer_monthly_volume as (
    select
        customer_key,
        complaint_month,
        
        -- Total complaint counts
        count(distinct complaint_key) as total_complaints,
        count(distinct case when complaint_type = 'call_log' then complaint_key end) as call_complaints,
        count(distinct case when complaint_type = 'social_media' then complaint_key end) as social_media_complaints,
        count(distinct case when complaint_type = 'web_form' then complaint_key end) as web_form_complaints,
        
        -- Category breakdown
        count(distinct case when complaint_category = 'Billing' then complaint_key end) as billing_complaints,
        count(distinct case when complaint_category = 'Technical' then complaint_key end) as technical_complaints,
        count(distinct case when complaint_category = 'Service' then complaint_key end) as service_complaints,
        count(distinct case when complaint_category = 'Network' then complaint_key end) as network_complaints,
        
        -- Distinct agents interacted with
        count(distinct agent_key) as distinct_agents_contacted
        
    from complaints_base
    where customer_key is not null
    group by customer_key, complaint_month
),

customer_monthly_resolution as (
    select
        customer_key,
        complaint_month,
        
        -- Resolution metrics
        count(distinct case when resolution_status = 'Resolved' then complaint_key end) as resolved_complaints,
        count(distinct case when resolution_status = 'Pending' then complaint_key end) as pending_complaints,
        count(distinct case when resolution_status = 'Escalated' then complaint_key end) as escalated_complaints,
        
        -- Resolution rate
        round(
            count(distinct case when resolution_status = 'Resolved' then complaint_key end) * 100.0 
            / nullif(count(distinct complaint_key), 0), 
            2
        ) as resolution_rate_pct,
        
        -- Average time to resolution (days) for resolved complaints
        avg(
            case 
                when resolution_status = 'Resolved' and resolution_date is not null
                then datediff('day', complaint_date, resolution_date)
            end
        ) as avg_days_to_resolution,
        
        -- Call duration metrics (for call complaints)
        avg(
            case 
                when call_start_time is not null and call_end_time is not null
                then datediff('minute', call_start_time, call_end_time)
            end
        ) as avg_call_duration_minutes,
        
        max(
            case 
                when call_start_time is not null and call_end_time is not null
                then datediff('minute', call_start_time, call_end_time)
            end
        ) as max_call_duration_minutes
        
    from complaints_base
    where customer_key is not null
    group by customer_key, complaint_month
),

customer_monthly_patterns as (
    select
        customer_key,
        complaint_month,
        
        -- Day of week patterns
        count(distinct case when cmpt_day_of_week between 1 and 5 then complaint_key end) as weekday_complaints,
        count(distinct case when cmpt_day_of_week in (6, 7) then complaint_key end) as weekend_complaints,
        
        -- Most common complaint day
        mode(cmpt_day_of_week_name) as most_frequent_complaint_day,
        
        -- Channel preference (for social media complaints)
        mode(media_channel) as preferred_social_channel,
        
        -- Complaint type preference
        mode(complaint_type) as preferred_complaint_channel
        
    from complaints_base
    where customer_key is not null
    group by customer_key, complaint_month
),

-- ROLLING WINDOW FEATURES 

customer_rolling_features as (
    select
        cv.customer_key,
        cv.complaint_month,
        
        -- 3-month rolling metrics
        sum(cv.total_complaints) over (
            partition by cv.customer_key 
            order by cv.complaint_month 
            rows between 2 preceding and current row
        ) as complaints_rolling_3m,
        
        -- 6-month rolling metrics
        sum(cv.total_complaints) over (
            partition by cv.customer_key 
            order by cv.complaint_month 
            rows between 5 preceding and current row
        ) as complaints_rolling_6m,
        
        -- 12-month rolling metrics
        sum(cv.total_complaints) over (
            partition by cv.customer_key 
            order by cv.complaint_month 
            rows between 11 preceding and current row
        ) as complaints_rolling_12m,
        
        -- Month-over-month change
        cv.total_complaints - lag(cv.total_complaints, 1) over (
            partition by cv.customer_key 
            order by cv.complaint_month
        ) as complaints_mom_change,
        
        -- Months since first complaint (tenure)
        datediff(
            'month',
            first_value(cv.complaint_month) over (
                partition by cv.customer_key 
                order by cv.complaint_month
            ),
            cv.complaint_month
        ) as months_since_first_complaint
        
    from customer_monthly_volume cv
),

-- ALL CUSTOMER FEATURES

customer_features_combined as (
    select
        -- Feature store key
        {{ dbt_utils.generate_surrogate_key(['ds.feature_month_key', 'cv.customer_key']) }} as feature_key,
        
        -- Grain identifiers
        ds.feature_month,
        ds.feature_month_key,
        cv.customer_key,
        
        -- Volume features
        coalesce(cv.total_complaints, 0) as total_complaints,
        coalesce(cv.call_complaints, 0) as call_complaints,
        coalesce(cv.social_media_complaints, 0) as social_media_complaints,
        coalesce(cv.web_form_complaints, 0) as web_form_complaints,
        coalesce(cv.billing_complaints, 0) as billing_complaints,
        coalesce(cv.technical_complaints, 0) as technical_complaints,
        coalesce(cv.service_complaints, 0) as service_complaints,
        coalesce(cv.network_complaints, 0) as network_complaints,
        coalesce(cv.distinct_agents_contacted, 0) as distinct_agents_contacted,
        
        -- Resolution features
        coalesce(cr.resolved_complaints, 0) as resolved_complaints,
        coalesce(cr.pending_complaints, 0) as pending_complaints,
        coalesce(cr.escalated_complaints, 0) as escalated_complaints,
        cr.resolution_rate_pct,
        round(cr.avg_days_to_resolution, 2) as avg_days_to_resolution,
        round(cr.avg_call_duration_minutes, 2) as avg_call_duration_minutes,
        cr.max_call_duration_minutes,
        
        -- Pattern features
        coalesce(cp.weekday_complaints, 0) as weekday_complaints,
        coalesce(cp.weekend_complaints, 0) as weekend_complaints,
        cp.most_frequent_complaint_day,
        cp.preferred_social_channel,
        cp.preferred_complaint_channel,
        
        -- Rolling features
        coalesce(rf.complaints_rolling_3m, 0) as complaints_rolling_3m,
        coalesce(rf.complaints_rolling_6m, 0) as complaints_rolling_6m,
        coalesce(rf.complaints_rolling_12m, 0) as complaints_rolling_12m,
        rf.complaints_mom_change,
        coalesce(rf.months_since_first_complaint, 0) as months_since_first_complaint,
        
        -- Derived flags
        case when cv.total_complaints >= 3 then 1 else 0 end as is_high_volume_month,
        case when cr.escalated_complaints > 0 then 1 else 0 end as has_escalation,
        case 
            when rf.complaints_rolling_3m > rf.complaints_rolling_6m / 2 
            then 1 else 0 
        end as is_trending_up,
        
        -- Metadata
        current_timestamp() as feature_computed_at
        
    from date_spine ds
    cross join (select distinct customer_key from customer_monthly_volume) cust
    left join customer_monthly_volume cv 
        on ds.feature_month = cv.complaint_month 
        and cust.customer_key = cv.customer_key
    left join customer_monthly_resolution cr 
        on cv.customer_key = cr.customer_key 
        and cv.complaint_month = cr.complaint_month
    left join customer_monthly_patterns cp 
        on cv.customer_key = cp.customer_key 
        and cv.complaint_month = cp.complaint_month
    left join customer_rolling_features rf 
        on cv.customer_key = rf.customer_key 
        and cv.complaint_month = rf.complaint_month
    where cv.customer_key is not null
)

select * from customer_features_combined
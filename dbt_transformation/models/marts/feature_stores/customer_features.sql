{{ config(
    materialized='table',
    schema='ml_features'
) }}

with active_customers as (
    select * 
    from {{ ref('dim_customers') }}
    where is_active = 1
),

complaint_metrics as (
    select
        customer_key,
        
        -- complaint volume
        count(distinct complaint_key) as total_complaints,
        count(distinct case when complaint_date >= current_date() - 30 then complaint_key end) as complaints_last_30d,
        count(distinct case when complaint_date >= current_date() - 90 then complaint_key end) as complaints_last_90d,
        count(distinct case when complaint_date >= current_date() - 365 then complaint_key end) as complaints_last_365d,
        
        -- complaint type distribution
        count(distinct case when complaint_type = 'call_log' then complaint_key end) as call_complaints,
        count(distinct case when complaint_type = 'social_media' then complaint_key end) as social_media_complaints,
        count(distinct case when complaint_type = 'web_form' then complaint_key end) as web_form_complaints,
        
        
        min(complaint_date) as first_complaint_date,
        max(complaint_date) as last_complaint_date,
        datediff('day', min(complaint_date), max(complaint_date)) as days_between_first_and_last,
        
        -- since last complaint
        datediff('day', max(complaint_date), current_date()) as days_since_last_complaint,
        
        -- Frequency (complaints per month active)
        case 
            when datediff('day', min(complaint_date), current_date()) > 0
            then count(distinct complaint_key)::float / 
                 (datediff('day', min(complaint_date), current_date()) / 30.0)
            else 0
        end as avg_complaints_per_month
        
    from {{ ref('fact_unified_complaint') }}
    group by customer_key
),

customer_demographics as (
    select
        customer_key,
        customer_id,
        
        gender,
        datediff('year', date_of_birth, current_date()) as age,
        case 
            when datediff('year', date_of_birth, current_date()) < 25 then '18-24'
            when datediff('year', date_of_birth, current_date()) < 35 then '25-34'
            when datediff('year', date_of_birth, current_date()) < 45 then '35-44'
            when datediff('year', date_of_birth, current_date()) < 55 then '45-54'
            when datediff('year', date_of_birth, current_date()) < 65 then '55-64'
            else '65+'
        end as age_group,
        
        -- tenure
        datediff('day', signup_date, current_date()) as days_as_customer,
        datediff('month', signup_date, current_date()) as months_as_customer,
        case 
            when datediff('month', signup_date, current_date()) < 6 then 'New (0-6m)'
            when datediff('month', signup_date, current_date()) < 12 then 'Growing (6-12m)'
            when datediff('month', signup_date, current_date()) < 24 then 'Established (1-2y)'
            else 'Loyal (2y+)'
        end as customer_tenure_segment,
        
        -- location
        state,
        zip_code,
        
        -- activity
        signup_date,
        last_updated_at
        
    from active_customers
),

behavioral_features as (
    select
        d.customer_key,
        
        -- Complaint propensity (complaints per month of tenure)
        case 
            when d.months_as_customer > 0 
            then coalesce(cm.total_complaints, 0)::float / d.months_as_customer::float
            else 0
        end as complaint_rate,
        
        -- recency score (0-100)
        case 
            when cm.days_since_last_complaint is null then 0
            when cm.days_since_last_complaint <= 30 then 100
            when cm.days_since_last_complaint <= 90 then 75
            when cm.days_since_last_complaint <= 180 then 50
            when cm.days_since_last_complaint <= 365 then 25
            else 10
        end as recency_score,
        
        
        -- Risk flags
        case when coalesce(ps.open_complaints, 0) > 3 then 1 else 0 end as has_many_open_complaints,
        case when coalesce(ps.overdue_complaints, 0) > 0 then 1 else 0 end as has_overdue_complaints,
        case when coalesce(ps.sla_compliance_rate, 1) < 0.5 then 1 else 0 end as low_sla_compliance,
        
        -- most used channel
        case 
            when cm.call_complaints >= cm.social_media_complaints 
                and cm.call_complaints >= cm.web_form_complaints then 'call'
            when cm.social_media_complaints >= cm.web_form_complaints then 'social_media'
            else 'web_form'
        end as preferred_complaint_channel
        
    from customer_demographics d
    left join complaint_metrics cm on d.customer_key = cm.customer_key
),

final as (
    select
        -- Keys
        d.customer_key,
        d.customer_id,
        
        -- Demographics
        d.gender,
        d.age,
        d.age_group,
        d.state,
        d.zip_code,
        
        -- Tenure
        d.days_as_customer,
        d.months_as_customer,
        d.customer_tenure_segment,
        d.signup_date,
        
        -- Complaint volume metrics
        coalesce(cm.total_complaints, 0) as total_complaints,
        coalesce(cm.complaints_last_30d, 0) as complaints_last_30d,
        coalesce(cm.complaints_last_90d, 0) as complaints_last_90d,
        coalesce(cm.complaints_last_365d, 0) as complaints_last_365d,
        
        -- Complaint type distribution
        coalesce(cm.call_complaints, 0) as call_complaints,
        coalesce(cm.social_media_complaints, 0) as social_media_complaints,
        coalesce(cm.web_form_complaints, 0) as web_form_complaints,
        
        -- Temporal patterns
        cm.first_complaint_date,
        cm.last_complaint_date,
        cm.days_between_first_and_last,
        cm.days_since_last_complaint,
        coalesce(cm.avg_complaints_per_month, 0) as avg_complaints_per_month,
        
        -- complaint status
        coalesce(ps.open_complaints, 0) as open_complaints,
        coalesce(ps.assigned_complaints, 0) as assigned_complaints,
        coalesce(ps.in_progress_complaints, 0) as in_progress_complaints,
        coalesce(ps.resolved_complaints, 0) as resolved_complaints,
        coalesce(ps.closed_complaints, 0) as closed_complaints,
        
        -- resolution metrics
        ps.avg_days_to_resolution,
        ps.max_days_to_resolution,
        ps.min_days_to_resolution,
        
        -- sla
        coalesce(ps.complaints_met_sla, 0) as complaints_met_sla,
        coalesce(ps.overdue_complaints, 0) as overdue_complaints,
        ps.sla_compliance_rate,
        
        -- Behavioral
        bf.complaint_rate,
        bf.recency_score,
        bf.preferred_complaint_channel,
        
        -- high risk
        bf.has_many_open_complaints,
        bf.has_overdue_complaints,
        bf.low_sla_compliance,
        
        
        current_timestamp() as feature_computed_at,
        d.last_updated_at as customer_last_updated_at
        
    from customer_demographics d
    left join complaint_metrics cm on d.customer_key = cm.customer_key
    left join behavioral_features bf on d.customer_key = bf.customer_key
)

select * from final
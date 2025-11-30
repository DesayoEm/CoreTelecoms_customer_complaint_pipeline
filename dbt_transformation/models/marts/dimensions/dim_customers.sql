{{ config(
    materialized = "incremental",
    unique_key = "customer_key",
     incremental_strategy = "delete+insert"
) }}

with customers_source as (
    select * from {{ source('raw', 'customers') }}
),

{% if is_incremental() %}
current_dim as (
    select * from {{ this }} where is_active = 1
)

{% else %}
current_dim as (
    select * from (select null as customer_id) where 1=0  -- empty
),
{% endif %}


changed_records as (
    select
        s.customer_key,
        s.customer_id,
        s.name,
        s.gender,
        s.date_of_birth,
        s.signup_date,
        s.email,
        s.address,
        s.zip_code,
        s.state,
        s.last_updated_at,
        s.loaded_at,
        d.customer_key as existing_key,
        d.address as old_address,
        d.zip_code as old_zip_code,
        d.state as old_state,
        -- detect changes
        case 
            when d.customer_key is null then 'NEW'  
            when s.address != d.address or s.zip_code != d.zip_code or s.state != d.state then 'CHANGED'
            else 'UNCHANGED'
        end as change_type
    from customers_source s
    left join current_dim d 
        on s.customer_id = d.customer_id 
        and d.is_active = 1
),

-- expire old records 
expired_records as (
    select
        d.customer_key,
        d.customer_id,
        d.name,
        d.gender,
        d.date_of_birth,
        d.signup_date,
        d.email,
        d.address,
        d.zip_code,
        d.address_effective_date,
        current_date() - 1 as address_exp_date,  -- must be expired prcvious day so as to avoid overlap
        0 as is_active, 
        d.state,
        d.last_updated_at,
        d.loaded_at
    from current_dim d
    inner join changed_records c 
        on d.customer_id = c.customer_id 
        and c.change_type = 'CHANGED'
),


new_versions as (
    select
        customer_key,
        customer_id,
        name,
        gender,
        date_of_birth,
        signup_date,
        email,
        address,
        zip_code,
        current_date() as address_effective_date, 
        null as address_exp_date,  
        1 as is_active,  
        state,
        last_updated_at,
        loaded_at
    from changed_records
    where change_type in ('NEW', 'CHANGED')
),


unchanged_records as (
    select
        d.*
    from current_dim d
    inner join changed_records c 
        on d.customer_id = c.customer_id 
        and c.change_type = 'UNCHANGED'
),

final as (
    select * from expired_records
    union all
    select * from new_versions
    union all
    select * from unchanged_records
)

select * from final
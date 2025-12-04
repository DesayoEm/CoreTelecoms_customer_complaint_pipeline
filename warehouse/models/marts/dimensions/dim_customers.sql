{{ config(
    materialized = "incremental",
    unique_key = "customer_key",
    schema='analytics'
) }}

{% set relation_exists = adapter.get_relation(
   database=this.database,
    schema=this.schema,
    identifier=this.identifier
) is not none %}


with customers_source as (
    select * from {{ source('stg', 'customers') }}
),

{% if relation_exists %}
current_dim as(
    select * from {{ this }}
    {% if is_incremental() %}
    where is_active = 1
    {%endif%}
),

{%else%}
current_dim as(
    select
        cast(null as varchar) as customer_key,
        cast(null as varchar) as customer_id,
        cast(null as varchar) as name,
        cast(null as char) as gender,
        cast(null as date) as date_of_birth,
        cast(null as date) as signup_date,
        cast(null as varchar) as email,
        cast(null as varchar) as address,
        cast(null as varchar) as zip_code,
        cast(null as varchar) as state,
        cast(null as date) as address_effective_date,
        cast(null as date) as address_expiry_date,
        cast(null as integer) as is_active,
        cast(null as varchar) as last_updated_at,
        cast(null as varchar) as loaded_at,
        cast(null as varchar) as created_at
    where false
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
        s.created_at,
        d.customer_key as existing_key,
        d.address as old_address,
        d.zip_code as old_zip_code,
        d.state as old_state,
        case 
            when d.customer_key IS NULL then 'NEW'
            when s.address IS DISTINCT FROM d.address 
              or s.zip_code IS DISTINCT FROM d.zip_code 
              or s.state IS DISTINCT FROM d.state 
            then 'CHANGED'
            else 'UNCHANGED'
        end as change_type
    from customers_source s
    left join current_dim d
        on s.customer_id = d.customer_id 
        and d.is_active = 1
), 

expired_records as(
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
        d.state,
        d.address_effective_date,
        current_date() -1 as address_expiry_date,
        0 as is_active,
        d.last_updated_at,
        d.loaded_at,
        d.created_at
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
        state,
        case 
        -- using signup date has address effective date for customer due to lack of historical record from source system
            when not {{ is_incremental() }}
            then cast(signup_date as date)
            else cast(current_date() as date)
        end as address_effective_date,
        null as address_expiry_date,  
        1 as is_active,  
        last_updated_at,
        loaded_at,
        created_at
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
    select * from new_versions
    {% if is_incremental() %}
    union all
    select * from expired_records
    union all
    select * from unchanged_records
    {% endif %}
)
select * from final
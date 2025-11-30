{{ config(
    materialized='incremental',
    unique_key='agent_key',
    schema='analytics'
) }}

{% set relation_exists = adapter.get_relation(
    database=this.database,
    schema=this.schema,
    identifier=this.identifier
) is not none %}

with agents_source as (
    select * from {{ source('raw', 'agents') }}
),

{% if relation_exists %}
current_dim as (
    select * from {{ this }}
    {% if is_incremental() %}
    where is_active = 1
    {% endif %}
),
{% else %}
current_dim as (
    select 
        cast(null as varchar) as agent_key,
        cast(null as varchar) as agent_id,
        cast(null as varchar) as name,
        cast(null as number) as experience,
        cast(null as date) as experience_effective_date,
        cast(null as date) as experience_expiry_date,
        cast(null as integer) as is_active,
        cast(null as varchar) as state,
        cast(null as timestamp) as last_updated_at,
        cast(null as timestamp) as loaded_at
    where false
),
{% endif %}

changed_records as (
    select
        s.agent_key,
        s.agent_id,
        s.name,
        s.experience,
        s.state,
        s.last_updated_at,
        s.loaded_at,
        d.agent_key as existing_key,
        d.experience as old_experience,
        case 
            when d.agent_key is null then 'NEW'
            when s.experience != d.experience then 'CHANGED'
            else 'UNCHANGED'
        end as change_type
    from agents_source s
    left join current_dim d 
        on s.agent_id = d.agent_id
        {% if is_incremental() %}
        and d.is_active = 1
        {% endif %}
),

expired_records as (
    select
        d.agent_key,
        d.agent_id,
        d.name,
        d.experience,
        d.experience_effective_date,
        cast(current_date()-1 as date) as experience_expiry_date,
        cast(0 as integer) as is_active,
        d.state,
        d.last_updated_at,
        d.loaded_at
    from current_dim d
    inner join changed_records c 
        on d.agent_id = c.agent_id 
        and c.change_type = 'CHANGED'
    where {{ is_incremental() }}
),

new_versions as (
    select
        agent_key,
        agent_id,
        name,
        experience,
        cast(current_date() as date) as experience_effective_date,
        cast(null as date) as experience_expiry_date,
        cast(1 as integer) as is_active,  
        state,
        last_updated_at,
        loaded_at
    from changed_records
    where change_type in ('NEW', 'CHANGED')
),

unchanged_records as (
    select
        d.agent_key,
        d.agent_id,
        d.name,
        d.experience,
        d.experience_effective_date,
        cast(d.experience_expiry_date as date) as experience_expiry_date,
        cast(d.is_active as integer) as is_active,
        d.state,
        d.last_updated_at,
        d.loaded_at
    from current_dim d
    inner join changed_records c 
        on d.agent_id = c.agent_id 
        and c.change_type = 'UNCHANGED'
    where {{ is_incremental() }}
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
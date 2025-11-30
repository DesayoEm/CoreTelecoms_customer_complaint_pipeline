{{ config(
    materialized='table',
    unique_key='agent_key',
    schema='analytics'
) }}

with agents_source as (
    select * from {{ source('raw', 'agents') }}
),

current_dim as (
    select * from {{ this }}
    {% if is_incremental() %}
    where is_active = 1  
    {% endif %}
),


changed_records as (
    select
        s.agent_id,
        s.name,
        s.experience,
        s.state,
        s.last_updated_at,
        s.loaded_at,
        d.agent_key as existing_key,
        d.experience as old_experience,
        -- detect if experience changed
        case 
            when d.agent_key is null then 'NEW'  --  new agent
            when s.experience != d.experience then 'CHANGED'  -- experience changed
            else 'UNCHANGED'
        end as change_type
    from agents_source s
    left join current_dim d 
        on s.agent_id = d.agent_id 
        and d.is_active = 1
),

-- expire old records
expired_records as (
    select
        d.agent_key,
        d.agent_id,
        d.name,
        d.experience,
        d.experience_effective_date,
        current_date()-1 as experience_exp_date,  -- must be expired prcvious day so as to avoid overlap
        0 as is_active,  -- no longer active
        d.state,
        d.last_updated_at,
        d.loaded_at
    from current_dim d
    inner join changed_records c 
        on d.agent_id = c.agent_id 
        and c.change_type = 'CHANGED'
),

-- new records for changes
new_versions as (
    select
        agent_key,
        agent_id,
        name,
        experience,
        current_date() as experience_effective_date,  -- effective today
        null as experience_exp_date,  -- open-ended as its current
        1 as is_active,  
        state,
        last_updated_at,
        d.loaded_at
    from changed_records
    where change_type in ('NEW', 'CHANGED')
),

-- unchanged records
unchanged_records as (
    select
        d.*
    from current_dim d
    inner join changed_records c 
        on d.agent_id = c.agent_id 
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
{{ config(
    materialized='table',
    schema='analytics'
) }}

with base_date_dimension as (
    {{ dbt_date.get_date_dimension("2020-01-01", "2030-12-31") }}
)

select
    *,
    to_number(to_char(date_day, 'YYYYMMDD')) as date_key
from base_date_dimension
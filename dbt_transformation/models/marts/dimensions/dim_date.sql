{{ config(
    materialized='table',
    schema='analytics'
) }}

{{ dbt_date.get_date_dimension("2020-01-01", "2030-12-31") }}
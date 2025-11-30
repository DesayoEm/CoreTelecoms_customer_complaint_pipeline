{{ config(
    materialized = "table",
    unique_key = "customer_key",
    schema='analytics'
) }}

with customers_source as (
    select * from {{ source('raw', 'customers') }}
)

select
    *,
    null as address_expiry_date,
    null as address_effective_date,
    null as is_active
from customers_source

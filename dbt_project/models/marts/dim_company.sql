{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_company_info') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ticker']) }} as company_pk,
    ticker,
    company_name,
    sector,
    industry,
    market_cap
from staging
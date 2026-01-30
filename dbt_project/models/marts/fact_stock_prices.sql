{{ config(materialized='table') }}

with prices as (
    select * from {{ ref('stg_stock_prices') }}
),

companies as (
    select * from {{ ref('dim_company') }}
)

select
    p.transaction_id,
    c.company_pk, -- Foreign Key
    p.ticker,
    p.close_price,
    p.trade_timestamp
from prices p
left join companies c
    on p.ticker = c.ticker
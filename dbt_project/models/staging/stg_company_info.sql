with source as (
    select * from {{ source('stock_source', 'COMPANY_INFO_JSON') }}
),

cleaned as (
    select
        ticker,
        name as company_name,
        sector,
        industry,
        market_cap,
        ingestion_time
    from source
)

select * from cleaned
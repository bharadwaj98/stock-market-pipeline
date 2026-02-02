with source as (
    select * from {{ source('stock_source', 'COMPANY_INFO_JSON') }}
),

deduplicated as (
    select
        ticker,
        name as company_name,
        sector,
        industry,
        market_cap,
        ingestion_time,
        -- Window function to identify the latest record per company
        row_number() over (partition by ticker order by ingestion_time desc) as row_num
    from source
)

select
    ticker,
    company_name,
    sector,
    industry,
    market_cap,
    ingestion_time
from deduplicated
where row_num = 1  -- Keep only the latest version
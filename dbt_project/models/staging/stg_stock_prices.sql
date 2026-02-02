with source as (
    select * from {{ source('stock_source', 'STOCK_PRICES_JSON') }}
),

deduplicated as (
    select
        -- Extract fields
        ticker,
        price as open_price,
        price as close_price,
        to_timestamp(timestamp) as trade_timestamp,
        ingestion_time,
        -- Generate ID first to ensure consistency
        {{ dbt_utils.generate_surrogate_key(['ticker', 'timestamp']) }} as transaction_id,
        -- Identify duplicates based on Ticker + Trade Time
        row_number() over (partition by ticker, timestamp order by ingestion_time desc) as row_num
    from source
)

select 
    transaction_id,
    ticker,
    open_price,
    close_price,
    trade_timestamp,
    ingestion_time
from deduplicated
where row_num = 1
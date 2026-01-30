with source as (
    select * from {{ source('stock_source', 'STOCK_PRICES_JSON') }}
),

cleaned as (
    select
        -- Generate a unique key for testing
        {{ dbt_utils.generate_surrogate_key(['ticker', 'timestamp']) }} as transaction_id,
        ticker,
        price as open_price, -- Renaming for clarity
        price as close_price, -- Since we only have one price point, we map it to both
        to_timestamp(timestamp) as trade_timestamp,
        ingestion_time
    from source
)

select * from cleaned
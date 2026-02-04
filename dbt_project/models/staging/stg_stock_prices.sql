{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

with source as (
    select * from {{ source('stock_source', 'STOCK_PRICES_JSON') }}
),

parsed as (
    select
        record_content:ticker::string as ticker,
        -- Safe Casting: If value is NaN or null, default to 0
        coalesce(try_cast(record_content:Close::string as float), 0) as close_price,
        coalesce(try_cast(record_content:Open::string as float), 0) as open_price,
        coalesce(try_cast(record_content:High::string as float), 0) as high_price,
        coalesce(try_cast(record_content:Low::string as float), 0) as low_price,
        coalesce(try_cast(record_content:Volume::string as integer), 0) as volume,
        to_timestamp(record_content:event_time::string) as trade_timestamp,
        ingestion_time
    from source
),

deduplicated as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['ticker', 'trade_timestamp']) }} as transaction_id
    from parsed
)

select * from deduplicated

{% if is_incremental() %}
  WHERE ingestion_time > (SELECT max(ingestion_time) FROM {{ this }})
{% endif %}

qualify row_number() over (partition by transaction_id order by ingestion_time desc) = 1
{{ config(
    materialized='incremental',
    unique_key='ticker' 
) }}

with source as (
    select * from {{ source('stock_source', 'COMPANY_INFO_JSON') }}
),

parsed as (
    select
        record_content:ticker::string as ticker,
        -- Extracting nested JSON fields
        record_content:raw_data:longName::string as company_name,
        record_content:raw_data:sector::string as sector,
        record_content:raw_data:industry::string as industry,
        record_content:raw_data:marketCap::number as market_cap,
        record_content:raw_data:currency::string as currency,
        record_content:raw_data:website::string as website,
        ingestion_time
    from source
)

select * from parsed

-- 1. Filter for incremental runs
{% if is_incremental() %}
  WHERE ingestion_time > (SELECT max(ingestion_time) FROM {{ this }})
{% endif %}

-- 2. DEDUPLICATE: Keep only the latest row per ticker in this batch
qualify row_number() over (partition by ticker order by ingestion_time desc) = 1
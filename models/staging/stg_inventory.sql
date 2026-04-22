-- stg_inventory.sql

with source as (
    select * from {{ source('raw', 'inventory') }}
),

renamed as (
    select
        -- Dates
        cast(snapshot_date as date)                     as snapshot_date,

        -- Identifiers
        cast(product_id as bigint)                      as product_id,

        -- Quantitative Measures
        cast(stock_on_hand as bigint)                   as stock_on_hand,
        cast(units_received as bigint)                  as units_received,
        cast(units_sold as bigint)                      as units_sold,
        cast(stockout_days as bigint)                   as stockout_days,
        cast(days_of_supply as double)                  as days_of_supply,
        cast(fill_rate as double)                       as fill_rate,
        cast(sell_through_rate as double)               as sell_through_rate,

        -- Flags (0/1 integer từ raw data)
        cast(stockout_flag as bigint) = 1               as stockout_flag,
        cast(overstock_flag as bigint) = 1              as overstock_flag,
        cast(reorder_flag as bigint) = 1                as reorder_flag,

        -- Descriptive Attributes
        cast(product_name as varchar)                   as product_name,
        cast(category as varchar)                       as category,
        cast(segment as varchar)                        as segment,

        -- Time breakdown
        cast(year as bigint)                            as year,
        cast(month as bigint)                           as month

    from source
)

select * from renamed
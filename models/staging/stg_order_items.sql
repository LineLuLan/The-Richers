-- stg_order_items.sql

with source as (
    select * from {{ source('raw', 'order_items') }}
),

renamed as (
    select
        -- Identifiers
        cast(order_id as bigint)                        as order_id,
        cast(product_id as bigint)                      as product_id,

        -- Descriptive
        cast(promo_id as varchar)                       as promo_id,
        cast(promo_id_2 as varchar)                     as promo_id_2,

        -- Quantitative
        cast(quantity as bigint)                        as quantity,
        cast(unit_price as decimal(16,2))               as unit_price,
        cast(discount_amount as decimal(16,2))          as discount_amount,

        -- Calculated
        cast(quantity * unit_price as decimal(16,2))    as gross_item_revenue

    from source
)

select * from renamed
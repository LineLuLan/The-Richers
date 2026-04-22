-- stg_returns.sql

with source as (
    select * from {{ source('raw', 'returns') }}
),

renamed as (
    select
        cast(return_id as varchar)                      as return_id,
        cast(order_id as bigint)                        as order_id,
        cast(product_id as bigint)                      as product_id,
        cast(return_date as date)                       as return_date,
        cast(return_reason as varchar)                  as return_reason,
        cast(return_quantity as bigint)                 as return_quantity,
        cast(refund_amount as double)                   as refund_amount
    from source
)

select * from renamed